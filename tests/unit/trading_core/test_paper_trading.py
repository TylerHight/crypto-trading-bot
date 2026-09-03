from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from crypto_trading_core.backtest import BacktestSettings
from crypto_trading_core.experiments import ExperimentSettings
from crypto_trading_core.paper import (
    PaperSettings,
    compute_paper_mutation,
    create_paper_session,
    paper_session_status,
    process_paper_candles,
    set_paper_session_state,
)
from crypto_trading_core.paper_contracts import (
    InvalidPaperTrading,
    PaperCandleInput,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import (
    MemoryPaperRepository,
    PostgresPaperRepository,
    _spec_document,
    _spec_from_document,
)
from crypto_trading_core.storage import StorageSettings
from crypto_trading_domain.backtest import Candle, initialize_incremental_backtest

START = datetime(2026, 2, 1, tzinfo=UTC)


def _candle(minute: int, open_price: str, close_price: str) -> Candle:
    start = START + timedelta(minutes=minute)
    opening = Decimal(open_price)
    closing = Decimal(close_price)
    return Candle(
        exchange="coinbase",
        symbol="BTC-USD",
        window_start=start,
        window_end=start + timedelta(minutes=1),
        open=opening,
        high=max(opening, closing),
        low=min(opening, closing),
        close=closing,
    )


def _spec(*, maximum_drawdown: str = "1") -> PaperSessionSpec:
    return PaperSessionSpec(
        evaluation_manifest_uri="evaluation.json",
        evaluation_manifest_sha256="a" * 64,
        selection_manifest_uri="selection.json",
        selection_manifest_sha256="b" * 64,
        candle_manifest_uri="research-candles.json",
        candle_manifest_sha256="c" * 64,
        evaluation_key="d" * 64,
        selection_key="e" * 64,
        exchange="coinbase",
        symbol="BTC-USD",
        candidate_id="sma-1-2",
        fast_period=1,
        slow_period=2,
        starting_cash=Decimal(1000),
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
        test_end=START,
        approved_by="research-operator",
        approval_note="Reviewed the sealed out-of-sample evaluation",
        maximum_drawdown=Decimal(maximum_drawdown),
        strategy_version="sma-crossover-long-only-v1",
        backtest_engine_version="candle-backtest-engine-v1",
        experiment_engine_version="strategy-experiment-engine-v1",
    )


def _session(*, maximum_drawdown: str = "1") -> PaperSession:
    spec = _spec(maximum_drawdown=maximum_drawdown)
    warmup = (_candle(-1, "10", "10"),)
    state = initialize_incremental_backtest(
        warmup, starting_cash=spec.starting_cash, slow_period=spec.slow_period
    )
    return PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=state,
        created_at=START,
        updated_at=START,
        last_state_changed_at=START,
    )


def _source(index: int) -> PaperCandleInput:
    marker = hashlib.sha256(str(index).encode()).hexdigest()
    return PaperCandleInput(
        manifest_uri=f"paper-{index}.json",
        manifest_sha256=marker,
        snapshot_key=hashlib.sha256(f"snapshot-{index}".encode()).hexdigest(),
        command_id=f"process-{index}",
    )


def _create(repository: MemoryPaperRepository, session: PaperSession) -> None:
    repository.create_session(
        session,
        command_id="create-session",
        payload_digest="f" * 64,
    )


def _process(
    repository: MemoryPaperRepository,
    session_id: str,
    candles: tuple[Candle, ...],
    source: PaperCandleInput,
) -> dict[str, object]:
    return repository.execute(
        session_id,
        command_id=source.command_id,
        payload_digest=source.manifest_sha256,
        actor="paper-system",
        action="process_candles",
        reason="bounded test publication",
        mutator=lambda session, existing: compute_paper_mutation(
            session,
            existing,
            candles,
            source,
            now=START + timedelta(hours=1),
        ),
    )


def test_session_identity_is_stable_and_pins_approval_and_safety_limit() -> None:
    first = _spec()
    assert first.session_id == _spec().session_id
    assert first.session_id != _spec(maximum_drawdown="0.5").session_id
    changed = PaperSessionSpec(
        **{
            **first.__dict__,
            "approval_note": "A distinct explicit operator approval",
        }
    )
    assert first.session_id != changed.session_id
    forward = PaperSessionSpec(
        **{
            **first.__dict__,
            "forward_start": START + timedelta(minutes=10),
        }
    )
    assert first.session_id != forward.session_id
    assert forward.processing_start == START + timedelta(minutes=10)

    with pytest.raises(InvalidPaperTrading, match="cannot precede"):
        PaperSessionSpec(
            **{
                **first.__dict__,
                "forward_start": START - timedelta(minutes=1),
            }
        )


def test_persisted_standalone_spec_without_forward_start_remains_compatible() -> None:
    document = _spec_document(_spec())
    document.pop("forward_start")

    restored = _spec_from_document(document)

    assert restored.forward_start is None
    assert restored.processing_start == restored.test_end
    assert restored.session_id == _spec().session_id


def test_create_session_recovers_only_sealed_parameters_and_checks_digest(tmp_path) -> None:
    def write(name: str, document: dict) -> tuple[str, str]:
        body = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
        path = tmp_path / name
        path.write_bytes(body)
        return str(path), hashlib.sha256(body).hexdigest()

    candle_uri, candle_digest = write(
        "candle.json",
        {
            "candle_count": 20,
            "candle_output_uri": str(tmp_path / "candles" / "runs" / "sealed"),
            "candle_schema_version": "v1",
            "interval": "1m",
            "mode": "apply",
            "snapshot_key": "1" * 64,
            "source_curated_snapshot_key": "2" * 64,
            "status": "published",
        },
    )
    spec_uri, spec_digest = write(
        "spec.json",
        {
            "candidates": [
                {"candidate_id": "sma-1-2", "fast_period": 1, "slow_period": 2},
                {"candidate_id": "sma-2-3", "fast_period": 2, "slow_period": 3},
            ],
            "candle_manifest_sha256": candle_digest,
            "candle_manifest_uri": candle_uri,
            "exchange": "coinbase",
            "experiment_spec_version": "v1",
            "fee_bps": "40.000000000000000000",
            "name": "paper-test",
            "ranges": {
                "test": {
                    "end": "2026-02-01T00:00:00Z",
                    "start": "2026-01-31T23:56:00Z",
                },
                "train": {
                    "end": "2026-01-31T23:48:00Z",
                    "start": "2026-01-31T23:44:00Z",
                },
                "validation": {
                    "end": "2026-01-31T23:54:00Z",
                    "start": "2026-01-31T23:50:00Z",
                },
            },
            "selection_policy": {
                "maximum_train_drawdown": "1.000000000000000000",
                "maximum_validation_drawdown": "1.000000000000000000",
                "minimum_train_fills": 0,
            },
            "slippage_bps": "5.000000000000000000",
            "starting_cash": "1000.000000000000000000",
            "symbol": "BTC-USD",
        },
    )
    selected = {"candidate_id": "sma-1-2", "fast_period": 1, "slow_period": 2}
    selection_uri, selection_digest = write(
        "selection.json",
        {
            "execution_mode": "simulation",
            "experiment_spec_raw_sha256": spec_digest,
            "experiment_spec_uri": spec_uri,
            "selected_candidate": selected,
            "selection_status": "selected",
            "status": "published",
        },
    )
    evaluation_uri, evaluation_digest = write(
        "evaluation.json",
        {
            "evaluation_key": "3" * 64,
            "execution_mode": "simulation",
            "experiment_engine_version": "strategy-experiment-engine-v1",
            "selected_candidate": selected,
            "selection_key": "4" * 64,
            "selection_manifest_sha256": selection_digest,
            "selection_manifest_uri": selection_uri,
            "status": "published",
        },
    )
    experiment = ExperimentSettings(
        backtest=BacktestSettings(
            storage=StorageSettings(),
            source_manifest_prefix="unused",
            source_output_prefix="unused",
            output_prefix=str(tmp_path / "backtests"),
            maximum_input_candles=100,
        ),
        spec_prefix="unused",
        output_prefix=str(tmp_path),
        maximum_candidates=50,
        maximum_candidate_candle_evaluations=1000,
    )
    settings = PaperSettings(
        experiment=experiment,
        database_url="unused",
        candle_manifest_prefix="unused",
        evaluation_manifest_prefix="unused",
        maximum_candles_per_run=100,
        transaction_timeout_seconds=10,
    )
    repository = MemoryPaperRepository()
    validator_calls = []

    def validator(*args, **kwargs):
        validator_calls.append((args, kwargs))
        return {"status": "valid"}

    def warmup_loader(*args, **kwargs):
        assert kwargs["start"] == START
        assert kwargs["end"] == START
        assert kwargs["warmup_candles"] == 1
        return (_candle(-1, "10", "10"),)

    report = create_paper_session(
        evaluation_uri,
        evaluation_digest,
        approved_by="operator-1",
        approval_note="Reviewed sealed evidence",
        maximum_drawdown=Decimal("0.25"),
        settings=settings,
        repository=repository,
        local_development=True,
        validator=validator,
        range_loader=warmup_loader,
        now=START,
    )
    stored = repository.get_session(report["session_id"])
    assert validator_calls
    assert stored.spec.fast_period == 1
    assert stored.spec.fee_bps == Decimal("40.000000000000000000")
    assert stored.spec.test_end == START
    assert report["execution_mode"] == "paper_simulation"

    with pytest.raises(InvalidPaperTrading, match="digest"):
        create_paper_session(
            evaluation_uri,
            "0" * 64,
            approved_by="operator-1",
            approval_note="Reviewed sealed evidence",
            maximum_drawdown=Decimal("0.25"),
            settings=settings,
            repository=repository,
            local_development=True,
            validator=validator,
            range_loader=warmup_loader,
        )


def test_fixed_input_is_identical_in_one_batch_or_one_candle_batches() -> None:
    candles = (
        _candle(0, "11", "11"),
        _candle(1, "12", "12"),
        _candle(2, "9", "9"),
        _candle(3, "8", "8"),
        _candle(4, "13", "13"),
    )
    whole = MemoryPaperRepository()
    split = MemoryPaperRepository()
    _create(whole, _session())
    _create(split, _session())

    _process(whole, _spec().session_id, candles, _source(100))
    for index, candle in enumerate(candles):
        _process(split, _spec().session_id, (candle,), _source(index))

    whole_session = whole.get_session(_spec().session_id)
    split_session = split.get_session(_spec().session_id)
    assert split_session.strategy_state == whole_session.strategy_state
    assert split_session.current_equity == whole_session.current_equity
    assert split_session.total_fees == whole_session.total_fees
    assert split_session.baseline_equity == whole_session.baseline_equity
    assert split.decisions[_spec().session_id] == whole.decisions[_spec().session_id]
    assert split.fills[_spec().session_id] == whole.fills[_spec().session_id]
    assert split.equity[_spec().session_id] == whole.equity[_spec().session_id]


def test_exact_retry_is_idempotent_and_command_payload_conflicts_fail() -> None:
    repository = MemoryPaperRepository()
    session = _session()
    _create(repository, session)
    candles = (_candle(0, "11", "11"), _candle(1, "12", "12"))
    source = _source(1)
    first = _process(repository, session.session_id, candles, source)
    retry = _process(repository, session.session_id, candles, source)
    assert first["newly_processed"] == 2
    assert retry["status"] == "resolved_existing_command"
    assert len(repository.candles[session.session_id]) == 2

    with pytest.raises(InvalidPaperTrading, match="reused"):
        repository.execute(
            session.session_id,
            command_id=source.command_id,
            payload_digest="0" * 64,
            actor="paper-system",
            action="process_candles",
            reason="changed payload",
            mutator=lambda current, existing: compute_paper_mutation(
                current, existing, candles, source, now=START
            ),
        )


def test_gap_and_conflicting_processed_candle_pause_without_advancing() -> None:
    repository = MemoryPaperRepository()
    session = _session()
    _create(repository, session)
    report = _process(repository, session.session_id, (_candle(1, "12", "12"),), _source(1))
    assert report["status"] == "auto_paused"
    assert report["pause_reason"] == "candle_sequence_gap"
    assert repository.get_session(session.session_id).processed_candles == 0

    repository = MemoryPaperRepository()
    _create(repository, session)
    candle = _candle(0, "11", "11")
    _process(repository, session.session_id, (candle,), _source(2))
    conflict = _process(repository, session.session_id, (candle,), _source(3))
    assert conflict["pause_reason"] == "processed_candle_conflict"
    assert repository.get_session(session.session_id).processed_candles == 1


def test_drawdown_pauses_after_committing_breach_and_before_later_candles() -> None:
    repository = MemoryPaperRepository()
    session = _session(maximum_drawdown="0.2")
    _create(repository, session)
    report = _process(
        repository,
        session.session_id,
        (
            _candle(0, "12", "12"),
            _candle(1, "12", "1"),
            _candle(2, "1", "1"),
        ),
        _source(4),
    )
    stored = repository.get_session(session.session_id)
    assert report["status"] == "auto_paused"
    assert report["pause_reason"] == "maximum_drawdown_exceeded"
    assert report["newly_processed"] == 2
    assert report["rejected"] == 1
    assert stored.state is PaperSessionState.PAUSED
    assert stored.processed_candles == 2


def test_lifecycle_is_audited_and_stopped_is_terminal() -> None:
    repository = MemoryPaperRepository()
    session = _session()
    _create(repository, session)
    paused = set_paper_session_state(
        session.session_id,
        PaperSessionState.PAUSED,
        actor="operator-1",
        reason="Reviewing paper behavior",
        repository=repository,
        command_id="pause-1",
        now=START + timedelta(minutes=1),
    )
    assert paused["state"] == "paused"
    set_paper_session_state(
        session.session_id,
        PaperSessionState.ACTIVE,
        actor="operator-1",
        reason="Review completed",
        repository=repository,
        command_id="resume-1",
        now=START + timedelta(minutes=2),
    )
    set_paper_session_state(
        session.session_id,
        PaperSessionState.STOPPED,
        actor="operator-1",
        reason="Forward test complete",
        repository=repository,
        command_id="stop-1",
        now=START + timedelta(minutes=3),
    )
    with pytest.raises(InvalidPaperTrading, match="cannot transition"):
        set_paper_session_state(
            session.session_id,
            PaperSessionState.ACTIVE,
            actor="operator-1",
            reason="Invalid restart",
            repository=repository,
            command_id="restart-1",
        )
    assert [event["action"] for event in repository.events[session.session_id]] == [
        "create",
        "set_state",
        "set_state",
        "set_state",
    ]


def test_status_reports_strategy_and_forward_baseline_without_mutation() -> None:
    repository = MemoryPaperRepository()
    session = _session()
    _create(repository, session)
    _process(
        repository,
        session.session_id,
        (_candle(0, "11", "11"), _candle(1, "12", "12")),
        _source(9),
    )
    before = repository.get_session(session.session_id)
    report = paper_session_status(session.session_id, repository)
    after = repository.get_session(session.session_id)
    assert before == after
    assert report["execution_mode"] == "paper_simulation"
    assert report["processed_candles"] == 2
    assert report["baseline"]["version"] == "buy-and-hold-long-only-v1"
    assert report["marked_equity"] == before.current_equity


def test_paper_boundary_has_no_exchange_or_order_submission_dependency() -> None:
    import crypto_trading_core.paper as paper_module
    import crypto_trading_core.paper_repository as repository_module

    source = inspect.getsource(paper_module) + inspect.getsource(repository_module)
    assert "crypto_exchange_adapters" not in source
    assert "place_order" not in source


def test_database_connection_errors_do_not_expose_credentials(monkeypatch) -> None:
    secret = "never-print-this-password"
    repository = PostgresPaperRepository(
        f"postgresql://paper_app:{secret}@127.0.0.1:1/crypto_trading"
    )

    class BrokenDriver:
        @staticmethod
        def connect(database_url, **kwargs):
            raise RuntimeError(database_url)

    monkeypatch.setattr(repository, "_psycopg", lambda: (BrokenDriver, None, None))
    with pytest.raises(InvalidPaperTrading) as captured:
        repository.get_session("a" * 64)
    assert secret not in str(captured.value)


def test_bounded_publication_loading_and_missing_minute_auto_pause(tmp_path) -> None:
    def settings() -> PaperSettings:
        return PaperSettings(
            experiment=ExperimentSettings(
                backtest=BacktestSettings(
                    storage=StorageSettings(),
                    source_manifest_prefix="unused",
                    source_output_prefix="unused",
                    output_prefix=str(tmp_path / "backtests"),
                    maximum_input_candles=100,
                ),
                spec_prefix="unused",
                output_prefix=str(tmp_path),
                maximum_candidates=50,
                maximum_candidate_candle_evaluations=1000,
            ),
            database_url="unused",
            candle_manifest_prefix="unused",
            evaluation_manifest_prefix="unused",
            maximum_candles_per_run=100,
            transaction_timeout_seconds=10,
        )

    body = json.dumps(
        {
            "candle_count": 2,
            "candle_output_uri": str(tmp_path / "paper-candles" / "runs" / "one"),
            "candle_schema_version": "v1",
            "interval": "1m",
            "mode": "apply",
            "snapshot_key": "9" * 64,
            "source_curated_snapshot_key": "8" * 64,
            "status": "published",
            "window_time_bounds": {
                "maximum": "2026-02-01T00:02:00Z",
                "minimum": "2026-02-01T00:00:00Z",
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    manifest = tmp_path / "paper-manifest.json"
    manifest.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()

    repository = MemoryPaperRepository()
    session = _session()
    _create(repository, session)
    loaded = (_candle(0, "11", "11"), _candle(1, "12", "12"))

    def complete_loader(*args, **kwargs):
        assert kwargs["start"] == START
        assert kwargs["end"] == START + timedelta(minutes=2)
        return loaded

    report = process_paper_candles(
        session.session_id,
        str(manifest),
        digest,
        settings=settings(),
        repository=repository,
        local_development=True,
        range_loader=complete_loader,
        command_id="bounded-publication",
        now=START + timedelta(hours=1),
    )
    assert report["newly_processed"] == 2

    repository = MemoryPaperRepository()
    _create(repository, session)
    paused = process_paper_candles(
        session.session_id,
        str(manifest),
        digest,
        settings=settings(),
        repository=repository,
        local_development=True,
        range_loader=lambda *args, **kwargs: loaded[:1],
        command_id="incomplete-publication",
        now=START + timedelta(hours=1),
    )
    assert paused["status"] == "auto_paused"
    assert paused["pause_reason"] == "candle_sequence_gap"
    assert repository.get_session(session.session_id).processed_candles == 0


def test_delayed_forward_start_uses_fresh_warmup_and_counts_only_forward_candles(
    tmp_path,
) -> None:
    forward_start = START + timedelta(minutes=10)
    base_spec = _spec()
    spec = PaperSessionSpec(**{**base_spec.__dict__, "forward_start": forward_start})
    stale_warmup = (_candle(-1, "10", "10"),)
    session = PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=initialize_incremental_backtest(
            stale_warmup,
            starting_cash=spec.starting_cash,
            slow_period=spec.slow_period,
        ),
        created_at=START,
        updated_at=START,
        last_state_changed_at=START,
    )
    repository = MemoryPaperRepository()
    _create(repository, session)

    body = json.dumps(
        {
            "candle_count": 3,
            "candle_output_uri": str(tmp_path / "paper-candles" / "runs" / "forward"),
            "candle_schema_version": "v1",
            "interval": "1m",
            "mode": "apply",
            "snapshot_key": "7" * 64,
            "source_curated_snapshot_key": "8" * 64,
            "status": "published",
            "window_time_bounds": {
                "maximum": "2026-02-01T00:12:00Z",
                "minimum": "2026-02-01T00:09:00Z",
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    manifest = tmp_path / "forward-manifest.json"
    manifest.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()

    experiment = ExperimentSettings(
        backtest=BacktestSettings(
            storage=StorageSettings(),
            source_manifest_prefix=str(tmp_path),
            source_output_prefix=str(tmp_path),
            output_prefix=str(tmp_path / "backtests"),
            maximum_input_candles=100,
        ),
        spec_prefix=str(tmp_path),
        output_prefix=str(tmp_path),
        maximum_candidates=50,
        maximum_candidate_candle_evaluations=1000,
    )
    settings = PaperSettings(
        experiment=experiment,
        database_url="unused",
        candle_manifest_prefix=str(tmp_path),
        evaluation_manifest_prefix=str(tmp_path),
        maximum_candles_per_run=100,
        transaction_timeout_seconds=10,
    )
    loaded = (
        _candle(9, "50", "50"),
        _candle(10, "40", "40"),
        _candle(11, "39", "39"),
    )

    def loader(*args, **kwargs):
        assert kwargs["start"] == forward_start
        assert kwargs["end"] == forward_start + timedelta(minutes=2)
        assert kwargs["warmup_candles"] == 1
        return loaded

    report = process_paper_candles(
        session.session_id,
        str(manifest),
        digest,
        settings=settings,
        repository=repository,
        local_development=True,
        range_loader=loader,
        command_id="delayed-forward-publication",
        now=forward_start + timedelta(minutes=2),
    )

    stored = repository.get_session(session.session_id)
    assert report["newly_processed"] == 2
    assert report["discovered"] == 2
    assert stored.first_candle_time == forward_start
    assert stored.last_candle_time == forward_start + timedelta(minutes=1)
    assert stored.processed_candles == 2
    assert repository.candles[session.session_id].keys() == {
        forward_start,
        forward_start + timedelta(minutes=1),
    }
    assert repository.decisions[session.session_id] == []


def test_delayed_forward_start_missing_warmup_pauses_without_processing(tmp_path) -> None:
    forward_start = START + timedelta(minutes=10)
    base = _session()
    spec = PaperSessionSpec(**{**base.spec.__dict__, "forward_start": forward_start})
    session = replace(base, spec=spec)
    repository = MemoryPaperRepository()
    _create(repository, session)
    body = json.dumps(
        {
            "candle_count": 2,
            "candle_output_uri": str(tmp_path / "paper-candles" / "runs" / "missing"),
            "candle_schema_version": "v1",
            "interval": "1m",
            "mode": "apply",
            "snapshot_key": "6" * 64,
            "source_curated_snapshot_key": "8" * 64,
            "status": "published",
            "window_time_bounds": {
                "maximum": "2026-02-01T00:12:00Z",
                "minimum": "2026-02-01T00:09:00Z",
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    manifest = tmp_path / "missing-warmup-manifest.json"
    manifest.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    settings = PaperSettings(
        experiment=ExperimentSettings(
            backtest=BacktestSettings(
                storage=StorageSettings(),
                source_manifest_prefix=str(tmp_path),
                source_output_prefix=str(tmp_path),
                output_prefix=str(tmp_path / "backtests"),
                maximum_input_candles=100,
            ),
            spec_prefix=str(tmp_path),
            output_prefix=str(tmp_path),
            maximum_candidates=50,
            maximum_candidate_candle_evaluations=1000,
        ),
        database_url="unused",
        candle_manifest_prefix=str(tmp_path),
        evaluation_manifest_prefix=str(tmp_path),
        maximum_candles_per_run=100,
        transaction_timeout_seconds=10,
    )

    report = process_paper_candles(
        session.session_id,
        str(manifest),
        digest,
        settings=settings,
        repository=repository,
        local_development=True,
        range_loader=lambda *args, **kwargs: (
            _candle(10, "40", "40"),
            _candle(11, "39", "39"),
        ),
        command_id="missing-forward-warmup",
        now=forward_start + timedelta(minutes=2),
    )

    assert report["status"] == "auto_paused"
    assert report["pause_reason"] == "candle_sequence_gap"
    assert repository.get_session(session.session_id).processed_candles == 0
