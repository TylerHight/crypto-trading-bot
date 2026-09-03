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
from crypto_trading_core.finalize_paper_pilot import build_parser as finalize_parser
from crypto_trading_core.paper import PaperSettings
from crypto_trading_core.paper_contracts import (
    InvalidPaperTrading,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import MemoryPaperRepository
from crypto_trading_core.pilot_contracts import Pilot, PilotState, load_pilot_plan
from crypto_trading_core.pilot_repository import MemoryPilotRepository
from crypto_trading_core.pilots import (
    PilotSettings,
    finalize_paper_pilot,
    pilot_snapshot_document,
    report_paper_pilot,
    run_paper_pilot_cycle,
    start_paper_pilot,
    validate_pilot_publication,
)
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from crypto_trading_domain.backtest import Candle, initialize_incremental_backtest

START = datetime(2026, 9, 15, tzinfo=UTC)


def _plan_document(**changes):
    value = {
        "evaluation_manifest_sha256": "a" * 64,
        "evaluation_manifest_uri": "s3a://crypto-data/evaluations/manifest.json",
        "maximum_conflict_events": 0,
        "maximum_data_gap_events": 0,
        "maximum_drawdown": "0.200000000000000000",
        "maximum_unplanned_pauses": 0,
        "minimum_calendar_days": 7,
        "minimum_excess_return_over_buy_and_hold": "-100.000000000000000000",
        "minimum_fills": 0,
        "minimum_processed_candles": 1,
        "name": "btc-usd-sma-forward-v1",
        "pilot_plan_version": "v1",
        "start_not_before": "2026-09-15T00:00:00Z",
    }
    value.update(changes)
    return value


def _load(**changes):
    body = json.dumps(_plan_document(**changes), separators=(",", ":"), sort_keys=True).encode()
    return load_pilot_plan(
        body,
        expected_sha256=hashlib.sha256(body).hexdigest(),
        maximum_processed_candles=1000,
        maximum_fills=100,
    )


def _paper_settings(tmp_path) -> PaperSettings:
    storage = StorageSettings()
    return PaperSettings(
        experiment=ExperimentSettings(
            backtest=BacktestSettings(
                storage=storage,
                source_manifest_prefix=str(tmp_path),
                source_output_prefix=str(tmp_path),
                output_prefix=str(tmp_path),
                maximum_input_candles=1000,
            ),
            spec_prefix=str(tmp_path),
            output_prefix=str(tmp_path),
            maximum_candidates=10,
            maximum_candidate_candle_evaluations=10000,
        ),
        database_url="unused",
        candle_manifest_prefix=str(tmp_path),
        evaluation_manifest_prefix=str(tmp_path),
        maximum_candles_per_run=1000,
        transaction_timeout_seconds=10,
    )


def _settings(tmp_path) -> PilotSettings:
    return PilotSettings(
        paper=_paper_settings(tmp_path),
        output_prefix=str(tmp_path / "pilots"),
        maximum_plan_processed_candles=1000,
        maximum_plan_fills=100,
    )


def _session(plan, *, processed=1, drawdown="0", test_end=START) -> PaperSession:
    spec = PaperSessionSpec(
        evaluation_manifest_uri=plan.evaluation_manifest_uri,
        evaluation_manifest_sha256=plan.evaluation_manifest_sha256,
        selection_manifest_uri="selection.json",
        selection_manifest_sha256="b" * 64,
        candle_manifest_uri="candles.json",
        candle_manifest_sha256="c" * 64,
        evaluation_key="d" * 64,
        selection_key="e" * 64,
        exchange="coinbase",
        symbol="BTC-USD",
        candidate_id="sma-1-2",
        fast_period=1,
        slow_period=2,
        starting_cash=Decimal(1000),
        fee_bps=Decimal(0),
        slippage_bps=Decimal(0),
        test_end=test_end,
        approved_by="operator",
        approval_note="Approved forward pilot",
        maximum_drawdown=plan.maximum_drawdown,
        strategy_version="sma-crossover-long-only-v1",
        backtest_engine_version="candle-backtest-engine-v1",
        experiment_engine_version="strategy-experiment-engine-v1",
        pilot_id=plan.pilot_id,
        forward_start=plan.start_not_before,
    )
    warmup = Candle(
        exchange="coinbase", symbol="BTC-USD",
        window_start=test_end - timedelta(minutes=1), window_end=test_end,
        open=Decimal(10), high=Decimal(10), low=Decimal(10), close=Decimal(10),
    )
    strategy = initialize_incremental_backtest(
        (warmup,), starting_cash=spec.starting_cash, slow_period=spec.slow_period
    )
    return PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=strategy,
        created_at=START,
        updated_at=START,
        last_state_changed_at=START,
        processed_candles=processed,
        current_equity=Decimal(1000),
        baseline_equity=Decimal(1000),
        maximum_drawdown=Decimal(drawdown),
    )


def _repositories(tmp_path, *, plan=None, processed=1, drawdown="0"):
    plan = plan or _load()
    paper = MemoryPaperRepository()
    session = _session(plan, processed=processed, drawdown=drawdown)
    paper.create_session(session, command_id="create", payload_digest="f" * 64)
    pilot = Pilot(
        plan=plan,
        session_id=session.session_id,
        state=PilotState.REGISTERED,
        approved_by="operator",
        approval_note="Approved forward pilot",
        created_at=START - timedelta(minutes=1),
        local_development=True,
    )
    repository = MemoryPilotRepository(paper)
    repository.create_pilot(pilot, command_id="register", payload_digest="1" * 64)
    return pilot, paper, repository


def test_plan_is_strict_digest_pinned_bounded_and_identity_sensitive() -> None:
    plan = _load()
    assert plan.pilot_id == _load().pilot_id
    assert plan.pilot_id != _load(minimum_fills=1).pilot_id
    with pytest.raises(InvalidPaperTrading, match="fields"):
        _load(unexpected=True)
    with pytest.raises(InvalidPaperTrading, match="digest"):
        body = json.dumps(_plan_document()).encode()
        load_pilot_plan(
            body, expected_sha256="0" * 64,
            maximum_processed_candles=1000, maximum_fills=100,
        )
    with pytest.raises(InvalidPaperTrading, match="7 through 90"):
        _load(minimum_calendar_days=6)
    with pytest.raises(InvalidPaperTrading, match="decimal string"):
        _load(maximum_drawdown=0.2)


def test_registration_must_precede_start_and_retries_same_pilot(tmp_path, monkeypatch) -> None:
    plan = _load()
    body = json.dumps(_plan_document(), separators=(",", ":"), sort_keys=True).encode()
    plan_path = tmp_path / "plan.json"
    plan_path.write_bytes(body)
    paper = MemoryPaperRepository()
    session = _session(plan)
    paper.create_session(session, command_id="create", payload_digest="f" * 64)
    repository = MemoryPilotRepository(paper)

    def existing_session(*args, **kwargs):
        assert kwargs["forward_start"] == START
        return {"session_id": session.session_id}

    monkeypatch.setattr("crypto_trading_core.pilots.create_paper_session", existing_session)
    arguments = {
        "approved_by": "operator",
        "approval_note": "Approved pinned forward experiment",
        "settings": _settings(tmp_path),
        "paper_repository": paper,
        "pilot_repository": repository,
        "local_development": True,
        "store": ObjectStorage(StorageSettings()),
        "now": START - timedelta(minutes=1),
    }
    first = start_paper_pilot(
        str(plan_path), hashlib.sha256(body).hexdigest(), **arguments
    )
    retry = start_paper_pilot(
        str(plan_path), hashlib.sha256(body).hexdigest(), **arguments
    )
    assert first["pilot_id"] == retry["pilot_id"]
    assert retry["status"] == "resolved_existing_pilot"
    assert validate_pilot_publication(
        first["manifest_uri"], first["manifest_sha256"],
        store=ObjectStorage(StorageSettings()), expected_kind="paper_pilot_registration",
    )["status"] == "valid"
    with pytest.raises(InvalidPaperTrading, match="no later"):
        start_paper_pilot(
            str(plan_path), hashlib.sha256(body).hexdigest(),
            **{**arguments, "now": START + timedelta(minutes=1)},
        )


def test_cycle_wrapper_checks_forward_boundary_and_is_idempotent(tmp_path, monkeypatch) -> None:
    pilot, paper, repository = _repositories(tmp_path)
    output = tmp_path / "candles" / "runs" / "cycle"
    manifest = {
        "candle_count": 2,
        "candle_output_uri": str(output),
        "candle_schema_version": "v1",
        "interval": "1m",
        "mode": "apply",
        "snapshot_key": "9" * 64,
        "source_curated_snapshot_key": "8" * 64,
        "status": "published",
        "window_time_bounds": {
            "maximum": "2026-09-15T00:01:00Z",
            "minimum": "2026-09-14T23:59:00Z",
        },
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    path = tmp_path / "cycle.json"
    path.write_bytes(body)
    calls = 0

    def process(*args, **kwargs):
        nonlocal calls
        calls += 1
        return {
            "discovered": 1, "newly_processed": 1, "rejected": 0,
            "session_id": pilot.session_id, "state": "active", "status": "processed",
        }

    monkeypatch.setattr("crypto_trading_core.pilots.process_paper_candles", process)
    arguments = {
        "command_id": "forward-cycle-1",
        "settings": _settings(tmp_path),
        "paper_repository": paper,
        "pilot_repository": repository,
        "store": ObjectStorage(StorageSettings()),
        "now": START,
    }
    digest = hashlib.sha256(body).hexdigest()
    first = run_paper_pilot_cycle(pilot.pilot_id, str(path), digest, **arguments)
    retry = run_paper_pilot_cycle(pilot.pilot_id, str(path), digest, **arguments)
    assert first["newly_processed"] == 1
    assert retry["status"] == "resolved_existing_command"
    assert calls == 1
    manifest["window_time_bounds"]["minimum"] = "2026-09-15T00:00:00Z"
    changed = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    path.write_bytes(changed)
    with pytest.raises(InvalidPaperTrading, match="forward warm-up"):
        run_paper_pilot_cycle(
            pilot.pilot_id, str(path), hashlib.sha256(changed).hexdigest(),
            **{**arguments, "command_id": "forward-cycle-2"},
        )


def test_cycle_processes_forward_start_after_sealed_evaluation_end(tmp_path) -> None:
    plan = _load()
    paper = MemoryPaperRepository()
    session = _session(
        plan,
        processed=0,
        test_end=START - timedelta(days=1),
    )
    paper.create_session(session, command_id="create-delayed", payload_digest="f" * 64)
    pilot = Pilot(
        plan=plan,
        session_id=session.session_id,
        state=PilotState.REGISTERED,
        approved_by="operator",
        approval_note="Approved delayed forward pilot",
        created_at=START - timedelta(minutes=1),
        local_development=True,
    )
    repository = MemoryPilotRepository(paper)
    repository.create_pilot(pilot, command_id="register-delayed", payload_digest="1" * 64)
    output = tmp_path / "candles" / "runs" / "delayed"
    manifest = {
        "candle_count": 3,
        "candle_output_uri": str(output),
        "candle_schema_version": "v1",
        "interval": "1m",
        "mode": "apply",
        "snapshot_key": "7" * 64,
        "source_curated_snapshot_key": "8" * 64,
        "status": "published",
        "window_time_bounds": {
            "maximum": "2026-09-15T00:02:00Z",
            "minimum": "2026-09-14T23:59:00Z",
        },
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    path = tmp_path / "delayed-cycle.json"
    path.write_bytes(body)

    def loader(*args, **kwargs):
        assert kwargs["start"] == START
        assert kwargs["warmup_candles"] == 1
        return (
            Candle(
                exchange="coinbase", symbol="BTC-USD",
                window_start=START - timedelta(minutes=1), window_end=START,
                open=Decimal(50), high=Decimal(50), low=Decimal(50), close=Decimal(50),
            ),
            Candle(
                exchange="coinbase", symbol="BTC-USD",
                window_start=START, window_end=START + timedelta(minutes=1),
                open=Decimal(40), high=Decimal(40), low=Decimal(40), close=Decimal(40),
            ),
            Candle(
                exchange="coinbase", symbol="BTC-USD",
                window_start=START + timedelta(minutes=1),
                window_end=START + timedelta(minutes=2),
                open=Decimal(39), high=Decimal(39), low=Decimal(39), close=Decimal(39),
            ),
        )

    report = run_paper_pilot_cycle(
        pilot.pilot_id,
        str(path),
        hashlib.sha256(body).hexdigest(),
        command_id="delayed-forward-cycle",
        settings=_settings(tmp_path),
        paper_repository=paper,
        pilot_repository=repository,
        store=ObjectStorage(StorageSettings()),
        now=START + timedelta(minutes=2),
        range_loader=loader,
    )

    stored = paper.get_session(session.session_id)
    assert report["status"] == "processed"
    assert report["newly_processed"] == 2
    assert stored.state is PaperSessionState.ACTIVE
    assert stored.first_candle_time == START
    assert stored.processed_candles == 2
    assert repository.evidence(pilot.pilot_id, START + timedelta(minutes=2))["data_gap_events"] == 0

    retry = run_paper_pilot_cycle(
        pilot.pilot_id,
        str(path),
        hashlib.sha256(body).hexdigest(),
        command_id="delayed-forward-cycle",
        settings=_settings(tmp_path),
        paper_repository=paper,
        pilot_repository=repository,
        store=ObjectStorage(StorageSettings()),
        now=START + timedelta(minutes=3),
        range_loader=lambda *args, **kwargs: pytest.fail("exact retry reloaded candles"),
    )
    assert retry["status"] == "resolved_existing_command"
    assert paper.get_session(session.session_id).processed_candles == 2

    cumulative_manifest = {
        **manifest,
        "candle_count": 4,
        "candle_output_uri": str(tmp_path / "candles" / "runs" / "cumulative"),
        "snapshot_key": "6" * 64,
        "window_time_bounds": {
            "maximum": "2026-09-15T00:03:00Z",
            "minimum": "2026-09-14T23:59:00Z",
        },
    }
    cumulative_body = json.dumps(
        cumulative_manifest, separators=(",", ":"), sort_keys=True
    ).encode()
    cumulative_path = tmp_path / "cumulative-cycle.json"
    cumulative_path.write_bytes(cumulative_body)

    cumulative_candles = (
        Candle(
            exchange="coinbase", symbol="BTC-USD",
            window_start=START, window_end=START + timedelta(minutes=1),
            open=Decimal(40), high=Decimal(40), low=Decimal(40), close=Decimal(40),
        ),
        Candle(
            exchange="coinbase", symbol="BTC-USD",
            window_start=START + timedelta(minutes=1),
            window_end=START + timedelta(minutes=2),
            open=Decimal(39), high=Decimal(39), low=Decimal(39), close=Decimal(39),
        ),
        Candle(
            exchange="coinbase", symbol="BTC-USD",
            window_start=START + timedelta(minutes=2),
            window_end=START + timedelta(minutes=3),
            open=Decimal(38), high=Decimal(38), low=Decimal(38), close=Decimal(38),
        ),
    )

    def cumulative_loader(*args, **kwargs):
        assert kwargs["start"] == START
        assert kwargs["end"] == START + timedelta(minutes=3)
        assert kwargs["warmup_candles"] == 0
        return cumulative_candles

    cumulative = run_paper_pilot_cycle(
        pilot.pilot_id,
        str(cumulative_path),
        hashlib.sha256(cumulative_body).hexdigest(),
        command_id="cumulative-forward-cycle",
        settings=_settings(tmp_path),
        paper_repository=paper,
        pilot_repository=repository,
        store=ObjectStorage(StorageSettings()),
        now=START + timedelta(minutes=3),
        range_loader=cumulative_loader,
    )
    assert cumulative["already_processed"] == 2
    assert cumulative["newly_processed"] == 1
    assert paper.get_session(session.session_id).processed_candles == 3
    assert len(paper.candles[session.session_id]) == 3
    assert repository.evidence(pilot.pilot_id, START + timedelta(minutes=3))["data_gap_events"] == 0

    conflicting_manifest = {
        **cumulative_manifest,
        "candle_count": 5,
        "candle_output_uri": str(tmp_path / "candles" / "runs" / "conflicting"),
        "snapshot_key": "5" * 64,
        "window_time_bounds": {
            "maximum": "2026-09-15T00:04:00Z",
            "minimum": "2026-09-14T23:59:00Z",
        },
    }
    conflicting_body = json.dumps(
        conflicting_manifest, separators=(",", ":"), sort_keys=True
    ).encode()
    conflicting_path = tmp_path / "conflicting-cycle.json"
    conflicting_path.write_bytes(conflicting_body)

    conflicting = run_paper_pilot_cycle(
        pilot.pilot_id,
        str(conflicting_path),
        hashlib.sha256(conflicting_body).hexdigest(),
        command_id="conflicting-forward-cycle",
        settings=_settings(tmp_path),
        paper_repository=paper,
        pilot_repository=repository,
        store=ObjectStorage(StorageSettings()),
        now=START + timedelta(minutes=4),
        range_loader=lambda *args, **kwargs: (
            Candle(
                exchange="coinbase", symbol="BTC-USD",
                window_start=START, window_end=START + timedelta(minutes=1),
                open=Decimal(41), high=Decimal(41), low=Decimal(41), close=Decimal(41),
            ),
            *cumulative_candles[1:],
            Candle(
                exchange="coinbase", symbol="BTC-USD",
                window_start=START + timedelta(minutes=3),
                window_end=START + timedelta(minutes=4),
                open=Decimal(37), high=Decimal(37), low=Decimal(37), close=Decimal(37),
            ),
        ),
    )
    assert conflicting["status"] == "auto_paused"
    assert conflicting["pause_reason"] == "processed_candle_conflict"
    assert paper.get_session(session.session_id).processed_candles == 3
    assert repository.evidence(pilot.pilot_id, START + timedelta(minutes=4))["conflict_events"] == 1


def test_snapshot_metrics_and_same_day_publication_are_immutable(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path)
    as_of = START + timedelta(days=1)
    document = pilot_snapshot_document(
        pilot, as_of=as_of, paper_repository=paper, pilot_repository=repository
    )
    assert document["interim_only"] is True
    assert document["metrics"]["expected_candles"] == 1440
    assert document["metrics"]["missing_candles"] == 1439
    first = report_paper_pilot(
        pilot.pilot_id, as_of, settings=_settings(tmp_path),
        paper_repository=paper, pilot_repository=repository, now=as_of,
    )
    retry = report_paper_pilot(
        pilot.pilot_id, as_of, settings=_settings(tmp_path),
        paper_repository=paper, pilot_repository=repository, now=as_of,
    )
    assert retry["status"] == "resolved_existing_snapshot"
    validated = validate_pilot_publication(
        first["manifest_uri"], first["manifest_sha256"],
        store=ObjectStorage(StorageSettings()), expected_kind="paper_pilot_snapshot",
    )
    assert validated["status"] == "valid"
    with pytest.raises(InvalidPaperTrading, match="conflicts"):
        report_paper_pilot(
            pilot.pilot_id, as_of + timedelta(minutes=1), settings=_settings(tmp_path),
            paper_repository=paper, pilot_repository=repository,
            now=as_of + timedelta(minutes=1),
        )


def test_assessment_pass_is_deterministic_terminal_and_has_no_threshold_flags(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path)
    result = finalize_paper_pilot(
        pilot.pilot_id,
        reviewed_by="operator", review_note="Reviewed immutable forward evidence",
        settings=_settings(tmp_path), paper_repository=paper,
        pilot_repository=repository, now=START + timedelta(days=8),
    )
    assert result["verdict"] == "pass"
    assert result["eligibility"] == "eligible_for_execution_design_review"
    assert result["live_trading_enabled"] is False
    assert validate_pilot_publication(
        result["manifest_uri"], result["manifest_sha256"],
        store=ObjectStorage(StorageSettings()), expected_kind="paper_pilot_assessment",
    )["status"] == "valid"
    assert repository.get_pilot(pilot.pilot_id).state is PilotState.COMPLETED
    assert paper.get_session(pilot.session_id).state is PaperSessionState.STOPPED
    assert "maximum-drawdown" not in finalize_parser().format_help()
    assert "minimum" not in inspect.signature(finalize_paper_pilot).parameters
    retry = finalize_paper_pilot(
        pilot.pilot_id,
        reviewed_by="operator", review_note="Reviewed immutable forward evidence",
        settings=_settings(tmp_path), paper_repository=paper,
        pilot_repository=repository, now=START + timedelta(days=8),
    )
    assert retry["status"] == "resolved_existing_assessment"


def test_assessment_fail_can_finalize_early_on_definitive_breach(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path, drawdown="0.3")
    result = finalize_paper_pilot(
        pilot.pilot_id,
        reviewed_by="operator", review_note="Reviewed drawdown breach",
        settings=_settings(tmp_path), paper_repository=paper,
        pilot_repository=repository, now=START + timedelta(days=1),
    )
    assert result["verdict"] == "fail"
    assert repository.get_pilot(pilot.pilot_id).state is PilotState.FAILED


def test_assessment_is_inconclusive_after_window_when_evidence_is_insufficient(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path, processed=0)
    result = finalize_paper_pilot(
        pilot.pilot_id,
        reviewed_by="operator", review_note="Reviewed incomplete evidence",
        settings=_settings(tmp_path), paper_repository=paper,
        pilot_repository=repository, now=START + timedelta(days=8),
    )
    assert result["verdict"] == "inconclusive"
    assert repository.get_pilot(pilot.pilot_id).state is PilotState.INCONCLUSIVE


def test_assessment_is_inconclusive_when_immutable_input_evidence_is_missing(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path)
    repository.run_cycle(
        pilot.pilot_id, command_id="missing-evidence-cycle", payload_digest="8" * 64,
        manifest_uri=str(tmp_path / "missing-candle-manifest.json"),
        manifest_sha256="9" * 64, now=START,
        runner=lambda: {"discovered": 1, "newly_processed": 0, "rejected": 0},
    )
    result = finalize_paper_pilot(
        pilot.pilot_id,
        reviewed_by="operator", review_note="Reviewed unavailable immutable evidence",
        settings=_settings(tmp_path), paper_repository=paper,
        pilot_repository=repository, now=START + timedelta(days=8),
    )
    assert result["verdict"] == "inconclusive"
    assert result["criteria"]["immutable_evidence"]["passes"] is False


def test_nonterminal_pilot_cannot_finalize_before_minimum_window(tmp_path) -> None:
    pilot, paper, repository = _repositories(tmp_path)
    with pytest.raises(InvalidPaperTrading, match="minimum duration"):
        finalize_paper_pilot(
            pilot.pilot_id,
            reviewed_by="operator", review_note="Too early",
            settings=_settings(tmp_path), paper_repository=paper,
            pilot_repository=repository, now=START + timedelta(days=1),
        )


def test_command_reuse_and_terminal_cycle_guards(tmp_path) -> None:
    pilot, _paper, repository = _repositories(tmp_path)
    calls = 0

    def runner():
        nonlocal calls
        calls += 1
        return {"discovered": 1, "newly_processed": 1, "rejected": 0, "status": "processed"}

    first = repository.run_cycle(
        pilot.pilot_id, command_id="cycle-1", payload_digest="2" * 64,
        manifest_uri="candle.json", manifest_sha256="3" * 64,
        now=START, runner=runner,
    )
    retry = repository.run_cycle(
        pilot.pilot_id, command_id="cycle-1", payload_digest="2" * 64,
        manifest_uri="candle.json", manifest_sha256="3" * 64,
        now=START, runner=runner,
    )
    assert first["status"] == "processed"
    assert retry["status"] == "resolved_existing_command"
    assert calls == 1
    with pytest.raises(InvalidPaperTrading, match="different arguments"):
        repository.run_cycle(
            pilot.pilot_id, command_id="cycle-1", payload_digest="4" * 64,
            manifest_uri="changed.json", manifest_sha256="4" * 64,
            now=START, runner=runner,
        )
    repository.pilots[pilot.pilot_id] = replace(
        repository.get_pilot(pilot.pilot_id), state=PilotState.CANCELLED
    )
    with pytest.raises(InvalidPaperTrading, match="terminal"):
        repository.run_cycle(
            pilot.pilot_id, command_id="cycle-2", payload_digest="5" * 64,
            manifest_uri="candle.json", manifest_sha256="3" * 64,
            now=START, runner=runner,
        )


def test_failed_cycle_is_audited_without_leaking_failure_text(tmp_path) -> None:
    pilot, _paper, repository = _repositories(tmp_path)

    def rejected():
        raise InvalidPaperTrading("postgresql://user:super-secret@host/database")

    with pytest.raises(InvalidPaperTrading, match="processing was rejected"):
        repository.run_cycle(
            pilot.pilot_id, command_id="rejected-cycle", payload_digest="6" * 64,
            manifest_uri="candle.json", manifest_sha256="7" * 64,
            now=START, runner=rejected,
        )
    record = repository.cycle_records(pilot.pilot_id)[0]
    assert record["success"] is False
    assert record["result"] == {
        "failure_reason": "cycle_processing_rejected",
        "status": "failed",
    }
    assert "super-secret" not in json.dumps(record, default=str)


def test_pilot_boundary_has_no_exchange_or_order_submission_dependency() -> None:
    import crypto_trading_core.pilots as pilot_module

    source = inspect.getsource(pilot_module)
    assert "crypto_exchange_adapters" not in source
    assert "place_order" not in source
    assert "private_key" not in source
