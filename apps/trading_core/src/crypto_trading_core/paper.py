from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from typing import Any
from uuid import uuid4

from crypto_trading_domain.backtest import (
    Candle,
    FillSide,
    InvalidBacktest,
    PortfolioState,
    StrategyDecision,
    TargetPosition,
    advance_incremental_backtest,
    initialize_incremental_backtest,
    simulate_target_fill,
)

from crypto_trading_core.backtest import load_candle_range
from crypto_trading_core.contracts import (
    BACKTEST_ENGINE_VERSION,
    SHA256_PATTERN,
    STRATEGY_VERSION,
    InvalidBacktestInput,
    PublishedCandleSnapshot,
    canonical_json_bytes,
    is_local_uri,
    load_candle_snapshot,
    normalize_uri,
    parse_utc_minute,
)
from crypto_trading_core.experiment_contracts import (
    EXPERIMENT_ENGINE_VERSION,
    load_experiment_spec,
)
from crypto_trading_core.experiments import ExperimentSettings, _source
from crypto_trading_core.paper_contracts import (
    PAPER_ENGINE_VERSION,
    InvalidPaperTrading,
    PaperCandleInput,
    PaperMutation,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
    StoredPaperCandle,
    decimal38,
    validate_actor,
    validate_command_id,
    validate_reason,
    validate_session_id,
)
from crypto_trading_core.paper_repository import PaperRepository
from crypto_trading_core.storage import ObjectStorage
from crypto_trading_core.validate_experiment import validate_evaluation


@dataclass(frozen=True)
class PaperSettings:
    experiment: ExperimentSettings
    database_url: str
    candle_manifest_prefix: str
    evaluation_manifest_prefix: str
    maximum_candles_per_run: int
    transaction_timeout_seconds: int

    @classmethod
    def from_env(cls) -> PaperSettings:
        maximum = int(os.getenv("PAPER_MAXIMUM_CANDLES_PER_RUN", "10000"))
        timeout = int(os.getenv("PAPER_TRANSACTION_TIMEOUT_SECONDS", "30"))
        if maximum <= 0 or timeout <= 0:
            raise InvalidPaperTrading("paper resource limits must be positive")
        experiment = ExperimentSettings.from_env()
        return cls(
            experiment=experiment,
            database_url=os.getenv(
                "PAPER_DATABASE_URL",
                "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
            ),
            candle_manifest_prefix=os.getenv(
                "PAPER_CANDLE_MANIFEST_PREFIX",
                experiment.backtest.source_manifest_prefix,
            ),
            evaluation_manifest_prefix=os.getenv(
                "PAPER_EVALUATION_MANIFEST_PREFIX", experiment.output_prefix
            ),
            maximum_candles_per_run=maximum,
            transaction_timeout_seconds=timeout,
        )


EvaluationValidator = Callable[..., dict[str, Any]]
RangeLoader = Callable[..., tuple[Candle, ...]]


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _read_json_manifest(
    store: ObjectStorage,
    uri: str,
    expected_sha256: str,
    field: str,
) -> tuple[dict[str, Any], str]:
    body = store.read_bytes(uri)
    digest = hashlib.sha256(body).hexdigest()
    if not SHA256_PATTERN.fullmatch(expected_sha256) or digest != expected_sha256:
        raise InvalidPaperTrading(f"{field} SHA-256 digest does not match")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidPaperTrading(f"{field} is invalid UTF-8 JSON") from error
    if not isinstance(document, dict):
        raise InvalidPaperTrading(f"{field} must be a JSON object")
    return document, digest


def _manifest_location(uri: str, prefix: str, local_development: bool, field: str) -> None:
    if is_local_uri(uri):
        if not local_development:
            raise InvalidPaperTrading(f"local {field} requires --local-development")
    elif not normalize_uri(uri).startswith(normalize_uri(prefix) + "/"):
        raise InvalidPaperTrading(f"{field} is outside the allowed prefix")


def _sealed_spec_and_warmup(
    evaluation_manifest_uri: str,
    evaluation_manifest_sha256: str,
    *,
    approved_by: str,
    approval_note: str,
    maximum_drawdown: Decimal,
    settings: PaperSettings,
    local_development: bool,
    store: ObjectStorage,
    validator: EvaluationValidator,
    range_loader: RangeLoader,
) -> tuple[PaperSessionSpec, tuple[Candle, ...]]:
    _manifest_location(
        evaluation_manifest_uri,
        settings.evaluation_manifest_prefix,
        local_development,
        "evaluation manifest",
    )
    try:
        validator(
            evaluation_manifest_uri,
            evaluation_manifest_sha256,
            settings=settings.experiment,
            local_development=local_development,
            store=store,
        )
    except (InvalidBacktestInput, OSError, ValueError) as error:
        raise InvalidPaperTrading(f"evaluation lineage is invalid: {error}") from error
    evaluation, evaluation_digest = _read_json_manifest(
        store,
        evaluation_manifest_uri,
        evaluation_manifest_sha256,
        "evaluation manifest",
    )
    if evaluation.get("status") != "published" or evaluation.get("execution_mode") != "simulation":
        raise InvalidPaperTrading("evaluation is not a published simulation")
    if evaluation.get("experiment_engine_version") != EXPERIMENT_ENGINE_VERSION:
        raise InvalidPaperTrading("evaluation engine version is unsupported")
    selection_uri = evaluation.get("selection_manifest_uri")
    selection_digest = evaluation.get("selection_manifest_sha256")
    if not isinstance(selection_uri, str) or not isinstance(selection_digest, str):
        raise InvalidPaperTrading("evaluation does not pin its selection")
    selection, _ = _read_json_manifest(
        store, selection_uri, selection_digest, "selection manifest"
    )
    if (
        selection.get("selection_status") != "selected"
        or selection.get("execution_mode") != "simulation"
    ):
        raise InvalidPaperTrading("selection is not an eligible simulation candidate")
    spec_uri = selection.get("experiment_spec_uri")
    spec_digest = selection.get("experiment_spec_raw_sha256")
    if not isinstance(spec_uri, str) or not isinstance(spec_digest, str):
        raise InvalidPaperTrading("selection does not pin its experiment specification")
    try:
        experiment = load_experiment_spec(
            store.read_bytes(spec_uri),
            spec_uri=spec_uri,
            expected_sha256=spec_digest,
            allowed_spec_prefix=settings.experiment.spec_prefix,
            local_development=local_development,
            maximum_candidates=settings.experiment.maximum_candidates,
            maximum_candidate_candle_evaluations=(
                settings.experiment.maximum_candidate_candle_evaluations
            ),
        )
        source = _source(
            experiment,
            settings.experiment,
            store,
            local_development=local_development,
        )
    except (InvalidBacktestInput, OSError, ValueError) as error:
        raise InvalidPaperTrading(f"sealed experiment is invalid: {error}") from error
    selected = evaluation.get("selected_candidate")
    if not isinstance(selected, dict) or selected != selection.get("selected_candidate"):
        raise InvalidPaperTrading("evaluation selected candidate changed")
    candidate = next(
        (
            item
            for item in experiment.candidates
            if item.candidate_id == selected.get("candidate_id")
            and item.fast_period == selected.get("fast_period")
            and item.slow_period == selected.get("slow_period")
        ),
        None,
    )
    if candidate is None:
        raise InvalidPaperTrading("evaluation candidate is not in the sealed specification")
    try:
        warmup = range_loader(
            source,
            exchange=experiment.exchange,
            symbol=experiment.symbol,
            start=experiment.test.end,
            end=experiment.test.end,
            warmup_candles=candidate.slow_period - 1,
            storage_settings=settings.experiment.backtest.storage,
            maximum_input_candles=settings.maximum_candles_per_run,
        )
    except (InvalidBacktestInput, InvalidBacktest, OSError, ValueError) as error:
        raise InvalidPaperTrading(f"paper warm-up could not be loaded: {error}") from error
    if len(warmup) != candidate.slow_period - 1:
        raise InvalidPaperTrading("paper warm-up does not cover the evaluation boundary")
    evaluation_key = evaluation.get("evaluation_key")
    selection_key = evaluation.get("selection_key")
    if not isinstance(evaluation_key, str) or not isinstance(selection_key, str):
        raise InvalidPaperTrading("evaluation identity is incomplete")
    paper_spec = PaperSessionSpec(
        evaluation_manifest_uri=evaluation_manifest_uri,
        evaluation_manifest_sha256=evaluation_digest,
        selection_manifest_uri=selection_uri,
        selection_manifest_sha256=selection_digest,
        candle_manifest_uri=source.manifest_uri,
        candle_manifest_sha256=source.manifest_sha256,
        evaluation_key=evaluation_key,
        selection_key=selection_key,
        exchange=experiment.exchange,
        symbol=experiment.symbol,
        candidate_id=candidate.candidate_id,
        fast_period=candidate.fast_period,
        slow_period=candidate.slow_period,
        starting_cash=experiment.starting_cash,
        fee_bps=experiment.fee_bps,
        slippage_bps=experiment.slippage_bps,
        test_end=experiment.test.end,
        approved_by=validate_actor(approved_by),
        approval_note=validate_reason(approval_note),
        maximum_drawdown=maximum_drawdown,
        strategy_version=STRATEGY_VERSION,
        backtest_engine_version=BACKTEST_ENGINE_VERSION,
        experiment_engine_version=str(evaluation.get("experiment_engine_version")),
    )
    return paper_spec, warmup


def create_paper_session(
    evaluation_manifest_uri: str,
    evaluation_manifest_sha256: str,
    *,
    approved_by: str,
    approval_note: str,
    maximum_drawdown: Decimal,
    settings: PaperSettings,
    repository: PaperRepository,
    local_development: bool,
    store: ObjectStorage | None = None,
    validator: EvaluationValidator = validate_evaluation,
    range_loader: RangeLoader = load_candle_range,
    command_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.experiment.backtest.storage)
    spec, warmup = _sealed_spec_and_warmup(
        evaluation_manifest_uri,
        evaluation_manifest_sha256,
        approved_by=approved_by,
        approval_note=approval_note,
        maximum_drawdown=maximum_drawdown,
        settings=settings,
        local_development=local_development,
        store=storage,
        validator=validator,
        range_loader=range_loader,
    )
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        strategy_state = initialize_incremental_backtest(
            warmup,
            starting_cash=spec.starting_cash,
            slow_period=spec.slow_period,
        )
    except InvalidBacktest as error:
        raise InvalidPaperTrading(str(error)) from error
    session = PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=strategy_state,
        created_at=timestamp,
        updated_at=timestamp,
        last_state_changed_at=timestamp,
    )
    actual_command = validate_command_id(command_id or f"create-{spec.session_id}")
    payload = _digest(
        {
            "action": "create",
            "evaluation_manifest_uri": evaluation_manifest_uri,
            "identity": spec.identity(),
            "local_development": local_development,
        }
    )
    stored, created = repository.create_session(
        session, command_id=actual_command, payload_digest=payload
    )
    return {
        "evaluation_manifest_sha256": stored.spec.evaluation_manifest_sha256,
        "execution_mode": "paper_simulation",
        "paper_engine_version": PAPER_ENGINE_VERSION,
        "session_id": stored.session_id,
        "state": stored.state.value,
        "status": "created" if created else "resolved_existing_session",
    }


def _bounds(source: PublishedCandleSnapshot) -> tuple[datetime, datetime]:
    bounds = source.manifest.get("window_time_bounds")
    if not isinstance(bounds, dict):
        raise InvalidPaperTrading("candle manifest has no window_time_bounds")
    minimum = bounds.get("minimum")
    maximum = bounds.get("maximum")
    if not isinstance(minimum, str) or not isinstance(maximum, str):
        raise InvalidPaperTrading("candle manifest window bounds are invalid")
    try:
        return parse_utc_minute(minimum, "window_time_bounds.minimum"), parse_utc_minute(
            maximum, "window_time_bounds.maximum"
        )
    except InvalidBacktestInput as error:
        raise InvalidPaperTrading(str(error)) from error


def _same_candle(stored: StoredPaperCandle, candle: Candle, source: PaperCandleInput) -> bool:
    return (
        stored.manifest_uri == source.manifest_uri
        and stored.manifest_sha256 == source.manifest_sha256
        and stored.snapshot_key == source.snapshot_key
        and stored.open == decimal38(candle.open, "open", positive=True)
        and stored.high == decimal38(candle.high, "high", positive=True)
        and stored.low == decimal38(candle.low, "low", positive=True)
        and stored.close == decimal38(candle.close, "close", positive=True)
    )


def _ratio_percent(value: Decimal, starting: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 114
        return decimal38(
            (value - starting) / starting * Decimal(100),
            "return",
            allow_negative=True,
        )


def _baseline_step(
    session: PaperSession, candle: Candle
) -> tuple[PortfolioState, Decimal, Decimal, Decimal, Decimal]:
    portfolio = session.baseline_portfolio
    fee = session.baseline_fee
    if portfolio is None:
        decision = StrategyDecision(
            decision_time=candle.window_start,
            observed_candle_window_start=candle.window_start,
            fast_sma=decimal38(candle.open, "baseline fast_sma", positive=True),
            slow_sma=decimal38(candle.open, "baseline slow_sma", positive=True),
            previous_target=TargetPosition.FLAT,
            new_target=TargetPosition.LONG,
            strategy_version="buy-and-hold-long-only-v1",
        )
        portfolio, fill = simulate_target_fill(
            PortfolioState(
                cash=decimal38(session.spec.starting_cash, "starting_cash", positive=True),
                base_quantity=decimal38(Decimal(0), "base_quantity"),
            ),
            decision,
            candle,
            fee_bps=session.spec.fee_bps,
            slippage_bps=session.spec.slippage_bps,
        )
        fee = fill.fee
    equity = decimal38(
        portfolio.cash + portfolio.base_quantity * candle.close, "baseline equity"
    )
    peak = max(session.baseline_peak_equity or session.spec.starting_cash, equity)
    with localcontext() as context:
        context.prec = 114
        drawdown = decimal38((peak - equity) / peak, "baseline drawdown")
    maximum_drawdown = max(session.baseline_maximum_drawdown, drawdown)
    return portfolio, peak, equity, maximum_drawdown, fee


def _pause_mutation(
    session: PaperSession,
    *,
    reason: str,
    now: datetime,
    discovered: int,
) -> PaperMutation:
    paused = session.with_state(PaperSessionState.PAUSED, reason=reason, changed_at=now)
    return PaperMutation(
        session=paused,
        candles=(),
        decisions=(),
        fills=(),
        equity=(),
        report={
            "already_processed": 0,
            "decided": 0,
            "discovered": discovered,
            "execution_mode": "paper_simulation",
            "filled": 0,
            "newly_processed": 0,
            "pause_reason": reason,
            "rejected": discovered,
            "session_id": session.session_id,
            "state": PaperSessionState.PAUSED.value,
            "status": "auto_paused",
        },
    )


def compute_paper_mutation(
    session: PaperSession,
    existing: Mapping[object, StoredPaperCandle],
    candles: tuple[Candle, ...],
    source: PaperCandleInput,
    *,
    now: datetime,
) -> PaperMutation:
    if session.state is not PaperSessionState.ACTIVE:
        raise InvalidPaperTrading(f"paper session is {session.state.value}")
    discovered = len(candles)
    previous: Candle | None = None
    for candle in candles:
        if candle.exchange != session.spec.exchange or candle.symbol != session.spec.symbol:
            return _pause_mutation(
                session,
                reason="candle_identity_changed",
                now=now,
                discovered=discovered,
            )
        if candle.window_start < session.spec.test_end:
            return _pause_mutation(
                session,
                reason="candle_precedes_paper_boundary",
                now=now,
                discovered=discovered,
            )
        if previous is not None and candle.window_start != previous.window_end:
            return _pause_mutation(
                session, reason="candle_sequence_gap", now=now, discovered=discovered
            )
        previous = candle

    already = 0
    unseen: list[Candle] = []
    for candle in candles:
        stored = existing.get(candle.window_start)
        if stored is None:
            unseen.append(candle)
        elif _same_candle(stored, candle, source):
            already += 1
        else:
            return _pause_mutation(
                session,
                reason="processed_candle_conflict",
                now=now,
                discovered=discovered,
            )
    expected_start = (
        session.spec.test_end
        if session.last_candle_time is None
        else session.last_candle_time + timedelta(minutes=1)
    )
    if unseen and unseen[0].window_start != expected_start:
        return _pause_mutation(
            session, reason="candle_sequence_gap", now=now, discovered=discovered
        )
    if any(candle.window_start <= (session.last_candle_time or session.spec.test_end - timedelta(minutes=1)) for candle in unseen):
        return _pause_mutation(
            session, reason="candle_time_moved_backward", now=now, discovered=discovered
        )

    current = session
    new_candles: list[StoredPaperCandle] = []
    decisions: list[Any] = []
    fills: list[Any] = []
    equity_rows: list[Any] = []
    for index, candle in enumerate(unseen):
        try:
            step = advance_incremental_backtest(
                current.strategy_state,
                candle,
                fast_period=current.spec.fast_period,
                slow_period=current.spec.slow_period,
                fee_bps=current.spec.fee_bps,
                slippage_bps=current.spec.slippage_bps,
            )
            baseline, baseline_peak, baseline_equity, baseline_drawdown, baseline_fee = (
                _baseline_step(current, candle)
            )
        except (InvalidBacktest, InvalidPaperTrading, ArithmeticError):
            reason = "portfolio_invariant_failed"
            paused = current.with_state(PaperSessionState.PAUSED, reason=reason, changed_at=now)
            return PaperMutation(
                session=paused,
                candles=tuple(new_candles),
                decisions=tuple(decisions),
                fills=tuple(fills),
                equity=tuple(equity_rows),
                report={
                    "already_processed": already,
                    "decided": len(decisions),
                    "discovered": discovered,
                    "execution_mode": "paper_simulation",
                    "filled": len(fills),
                    "newly_processed": len(new_candles),
                    "pause_reason": reason,
                    "rejected": len(unseen) - index,
                    "session_id": session.session_id,
                    "state": PaperSessionState.PAUSED.value,
                    "status": "auto_paused",
                },
            )
        if step.decision is not None:
            decisions.append(step.decision)
        if step.fill is not None:
            fills.append(step.fill)
        equity_rows.append(step.equity)
        new_candles.append(
            StoredPaperCandle(
                window_start=candle.window_start,
                manifest_uri=source.manifest_uri,
                manifest_sha256=source.manifest_sha256,
                snapshot_key=source.snapshot_key,
                open=decimal38(candle.open, "open", positive=True),
                high=decimal38(candle.high, "high", positive=True),
                low=decimal38(candle.low, "low", positive=True),
                close=decimal38(candle.close, "close", positive=True),
            )
        )
        current = replace(
            current,
            strategy_state=step.state,
            updated_at=now,
            first_candle_time=current.first_candle_time or candle.window_start,
            last_candle_time=candle.window_start,
            processed_candles=current.processed_candles + 1,
            decisions=current.decisions + int(step.decision is not None),
            buys=current.buys
            + int(step.fill is not None and step.fill.side is FillSide.BUY),
            sells=current.sells
            + int(step.fill is not None and step.fill.side is FillSide.SELL),
            total_fees=decimal38(
                current.total_fees + (step.fill.fee if step.fill is not None else Decimal(0)),
                "total_fees",
            ),
            current_equity=step.equity.equity,
            maximum_drawdown=max(current.maximum_drawdown, step.equity.drawdown),
            baseline_portfolio=baseline,
            baseline_peak_equity=baseline_peak,
            baseline_equity=baseline_equity,
            baseline_maximum_drawdown=baseline_drawdown,
            baseline_fee=baseline_fee,
        )
        if step.equity.drawdown > current.spec.maximum_drawdown:
            current = current.with_state(
                PaperSessionState.PAUSED,
                reason="maximum_drawdown_exceeded",
                changed_at=now,
            )
            break

    status = "processed"
    rejected = 0
    if current.state is PaperSessionState.PAUSED:
        status = "auto_paused"
        rejected = len(unseen) - len(new_candles)
    return PaperMutation(
        session=current,
        candles=tuple(new_candles),
        decisions=tuple(decisions),
        fills=tuple(fills),
        equity=tuple(equity_rows),
        report={
            "already_processed": already,
            "decided": len(decisions),
            "discovered": discovered,
            "execution_mode": "paper_simulation",
            "filled": len(fills),
            "newly_processed": len(new_candles),
            "pause_reason": (
                current.last_state_reason if current.state is PaperSessionState.PAUSED else None
            ),
            "rejected": rejected,
            "session_id": session.session_id,
            "state": current.state.value,
            "status": status,
        },
    )


def process_paper_candles(
    session_id: str,
    candle_manifest_uri: str,
    candle_manifest_sha256: str,
    *,
    settings: PaperSettings,
    repository: PaperRepository,
    local_development: bool,
    store: ObjectStorage | None = None,
    range_loader: RangeLoader = load_candle_range,
    command_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    session_id = validate_session_id(session_id)
    session = repository.get_session(session_id)
    _manifest_location(
        candle_manifest_uri,
        settings.candle_manifest_prefix,
        local_development,
        "candle manifest",
    )
    storage = store or ObjectStorage(settings.experiment.backtest.storage)
    body = storage.read_bytes(candle_manifest_uri)
    try:
        snapshot = load_candle_snapshot(
            body,
            manifest_uri=candle_manifest_uri,
            expected_sha256=candle_manifest_sha256,
            allowed_manifest_prefix=settings.candle_manifest_prefix,
            local_development=local_development,
        )
    except (InvalidBacktestInput, OSError, ValueError) as error:
        raise InvalidPaperTrading(f"paper candle manifest is invalid: {error}") from error
    if not is_local_uri(snapshot.output_uri) and not normalize_uri(
        snapshot.output_uri
    ).startswith(normalize_uri(settings.experiment.backtest.source_output_prefix) + "/"):
        raise InvalidPaperTrading("paper candle output is outside the allowed prefix")
    minimum, maximum = _bounds(snapshot)
    if minimum < session.spec.test_end:
        raise InvalidPaperTrading("paper candle publication overlaps the evaluation interval")
    if maximum <= minimum:
        raise InvalidPaperTrading("paper candle publication is empty")
    expected = int((maximum - minimum) / timedelta(minutes=1))
    if expected > settings.maximum_candles_per_run:
        raise InvalidPaperTrading("paper candle publication exceeds the configured cap")
    try:
        candles = range_loader(
            snapshot,
            exchange=session.spec.exchange,
            symbol=session.spec.symbol,
            start=minimum,
            end=maximum,
            warmup_candles=0,
            storage_settings=settings.experiment.backtest.storage,
            maximum_input_candles=settings.maximum_candles_per_run,
        )
    except (InvalidBacktestInput, InvalidBacktest, OSError, ValueError) as error:
        raise InvalidPaperTrading(f"paper candles could not be loaded: {error}") from error
    incomplete_publication = len(candles) != expected
    actual_command = validate_command_id(command_id or str(uuid4()))
    source = PaperCandleInput(
        manifest_uri=candle_manifest_uri,
        manifest_sha256=candle_manifest_sha256,
        snapshot_key=snapshot.snapshot_key,
        command_id=actual_command,
    )
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    payload = _digest(
        {
            "action": "process_candles",
            "manifest_uri": candle_manifest_uri,
            "manifest_sha256": candle_manifest_sha256,
            "snapshot_key": snapshot.snapshot_key,
            "session_id": session_id,
        }
    )
    if incomplete_publication:
        mutator = lambda current, _: _pause_mutation(
            current,
            reason="candle_sequence_gap",
            now=timestamp,
            discovered=len(candles),
        )
    else:
        mutator = lambda current, existing: compute_paper_mutation(
            current, existing, candles, source, now=timestamp
        )
    return repository.execute(
        session_id,
        command_id=actual_command,
        payload_digest=payload,
        actor="paper-system",
        action="process_candles",
        reason=f"process candle snapshot {snapshot.snapshot_key}",
        mutator=mutator,
    )


def set_paper_session_state(
    session_id: str,
    state: PaperSessionState,
    *,
    actor: str,
    reason: str,
    repository: PaperRepository,
    command_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    session_id = validate_session_id(session_id)
    actor = validate_actor(actor)
    reason = validate_reason(reason)
    actual_command = validate_command_id(command_id or str(uuid4()))
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    payload = _digest(
        {
            "action": "set_state",
            "actor": actor,
            "reason": reason,
            "session_id": session_id,
            "state": state.value,
        }
    )

    def mutate(
        session: PaperSession, _: Mapping[object, StoredPaperCandle]
    ) -> PaperMutation:
        allowed = {
            PaperSessionState.ACTIVE: {
                PaperSessionState.PAUSED,
                PaperSessionState.STOPPED,
            },
            PaperSessionState.PAUSED: {
                PaperSessionState.ACTIVE,
                PaperSessionState.STOPPED,
            },
            PaperSessionState.STOPPED: set(),
        }
        if state not in allowed[session.state]:
            raise InvalidPaperTrading(
                f"cannot transition paper session from {session.state.value} to {state.value}"
            )
        updated = session.with_state(state, reason=reason, changed_at=timestamp)
        return PaperMutation(
            session=updated,
            candles=(),
            decisions=(),
            fills=(),
            equity=(),
            report={
                "execution_mode": "paper_simulation",
                "session_id": session_id,
                "state": state.value,
                "status": "state_changed",
            },
        )

    return repository.execute(
        session_id,
        command_id=actual_command,
        payload_digest=payload,
        actor=actor,
        action="set_state",
        reason=reason,
        mutator=mutate,
    )


def paper_session_status(session_id: str, repository: PaperRepository) -> dict[str, Any]:
    session_id = validate_session_id(session_id)
    session = repository.get_session(session_id)
    equity = (
        session.current_equity
        if session.current_equity is not None
        else session.spec.starting_cash
    )
    baseline_equity = (
        session.baseline_equity
        if session.baseline_equity is not None
        else session.spec.starting_cash
    )
    pending = session.strategy_state.pending_decision
    return {
        "baseline": {
            "ending_equity": baseline_equity,
            "fee": session.baseline_fee,
            "maximum_drawdown": session.baseline_maximum_drawdown,
            "percentage_return": _ratio_percent(
                baseline_equity, session.spec.starting_cash
            ),
            "version": "buy-and-hold-long-only-v1",
        },
        "cash": session.strategy_state.portfolio.cash,
        "candidate_id": session.spec.candidate_id,
        "decisions": session.decisions,
        "evaluation_manifest_sha256": session.spec.evaluation_manifest_sha256,
        "evaluation_manifest_uri": session.spec.evaluation_manifest_uri,
        "execution_mode": "paper_simulation",
        "exchange": session.spec.exchange,
        "fast_period": session.spec.fast_period,
        "first_candle_time": session.first_candle_time,
        "last_candle_time": session.last_candle_time,
        "last_state_changed_at": session.last_state_changed_at,
        "last_state_reason": session.last_state_reason,
        "marked_equity": equity,
        "gross_percentage_return": _ratio_percent(
            equity + session.total_fees, session.spec.starting_cash
        ),
        "maximum_drawdown": session.maximum_drawdown,
        "net_percentage_return": _ratio_percent(equity, session.spec.starting_cash),
        "percentage_return": _ratio_percent(equity, session.spec.starting_cash),
        "paper_engine_version": PAPER_ENGINE_VERSION,
        "pending_target": pending.new_target.value if pending else None,
        "pending_target_decision_time": pending.decision_time if pending else None,
        "position_quantity": session.strategy_state.portfolio.base_quantity,
        "processed_candles": session.processed_candles,
        "selection_manifest_sha256": session.spec.selection_manifest_sha256,
        "selection_manifest_uri": session.spec.selection_manifest_uri,
        "session_id": session.session_id,
        "slow_period": session.spec.slow_period,
        "starting_cash": session.spec.starting_cash,
        "state": session.state.value,
        "strategy_version": session.spec.strategy_version,
        "symbol": session.spec.symbol,
        "total_fees": session.total_fees,
        "trades": {"buys": session.buys, "sells": session.sells},
    }
