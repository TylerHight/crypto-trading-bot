from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from crypto_trading_core.backtest import BacktestSettings
from crypto_trading_core.experiments import ExperimentSettings
from crypto_trading_core.paper import PaperSettings, compute_paper_mutation
from crypto_trading_core.paper_contracts import (
    InvalidPaperTrading,
    PaperCandleInput,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_core.pilot_contracts import Pilot, PilotState, load_pilot_plan
from crypto_trading_core.pilot_repository import PostgresPilotRepository
from crypto_trading_core.pilots import (
    PilotSettings,
    finalize_paper_pilot,
    report_paper_pilot,
    validate_pilot_publication,
)
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from crypto_trading_domain.backtest import Candle, initialize_incremental_backtest

pytestmark = pytest.mark.integration
START = datetime(2026, 9, 15, tzinfo=UTC)


def _enabled() -> bool:
    return os.getenv("RUN_PILOT_INTEGRATION_TESTS") == "1"


def _plan(marker: str):
    body = json.dumps(
        {
            "evaluation_manifest_sha256": hashlib.sha256(marker.encode()).hexdigest(),
            "evaluation_manifest_uri": f"s3a://crypto-data/{marker}/evaluation.json",
            "maximum_conflict_events": 0,
            "maximum_data_gap_events": 0,
            "maximum_drawdown": "1.000000000000000000",
            "maximum_unplanned_pauses": 0,
            "minimum_calendar_days": 7,
            "minimum_excess_return_over_buy_and_hold": "-100.000000000000000000",
            "minimum_fills": 0,
            "minimum_processed_candles": 1,
            "name": "integration-forward-pilot",
            "pilot_plan_version": "v1",
            "start_not_before": "2026-09-15T00:00:00Z",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return load_pilot_plan(
        body,
        expected_sha256=hashlib.sha256(body).hexdigest(),
        maximum_processed_candles=1000,
        maximum_fills=100,
    )


def _session(plan) -> PaperSession:
    spec = PaperSessionSpec(
        evaluation_manifest_uri=plan.evaluation_manifest_uri,
        evaluation_manifest_sha256=plan.evaluation_manifest_sha256,
        selection_manifest_uri="s3a://crypto-data/selection.json",
        selection_manifest_sha256="b" * 64,
        candle_manifest_uri="s3a://crypto-data/research-candles.json",
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
        test_end=START,
        approved_by="integration-test",
        approval_note="Accelerated integration pilot",
        maximum_drawdown=plan.maximum_drawdown,
        strategy_version="sma-crossover-long-only-v1",
        backtest_engine_version="candle-backtest-engine-v1",
        experiment_engine_version="strategy-experiment-engine-v1",
        pilot_id=plan.pilot_id,
        forward_start=plan.start_not_before,
    )
    warmup = Candle(
        exchange="coinbase",
        symbol="BTC-USD",
        window_start=START - timedelta(minutes=1),
        window_end=START,
        open=Decimal(10),
        high=Decimal(10),
        low=Decimal(10),
        close=Decimal(10),
    )
    return PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=initialize_incremental_backtest(
            (warmup,), starting_cash=Decimal(1000), slow_period=2
        ),
        created_at=START - timedelta(minutes=1),
        updated_at=START - timedelta(minutes=1),
        last_state_changed_at=START - timedelta(minutes=1),
    )


def _settings(storage: StorageSettings, root: str, database_url: str) -> PilotSettings:
    backtest = BacktestSettings(
        storage=storage,
        source_manifest_prefix=f"s3a://crypto-data/{root}",
        source_output_prefix=f"s3a://crypto-data/{root}",
        output_prefix=f"s3a://crypto-data/{root}",
        maximum_input_candles=1000,
    )
    return PilotSettings(
        paper=PaperSettings(
            experiment=ExperimentSettings(
                backtest=backtest,
                spec_prefix=f"s3a://crypto-data/{root}",
                output_prefix=f"s3a://crypto-data/{root}",
                maximum_candidates=10,
                maximum_candidate_candle_evaluations=10000,
            ),
            database_url=database_url,
            candle_manifest_prefix=f"s3a://crypto-data/{root}",
            evaluation_manifest_prefix=f"s3a://crypto-data/{root}",
            maximum_candles_per_run=1000,
            transaction_timeout_seconds=10,
        ),
        output_prefix=f"s3a://crypto-data/{root}/paper-pilots/v1",
        maximum_plan_processed_candles=1000,
        maximum_plan_fills=100,
    )


@pytest.mark.skipif(
    not _enabled(),
    reason="set RUN_PILOT_INTEGRATION_TESTS=1 with local PostgreSQL and MinIO available",
)
def test_pilot_lifecycle_is_restart_safe_serialized_and_immutable() -> None:
    import boto3
    import psycopg

    database_url = os.getenv(
        "PAPER_DATABASE_URL",
        "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
    )
    root = f"integration/pilots/{uuid4()}"
    storage_settings = StorageSettings(
        endpoint_url=os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store = ObjectStorage(storage_settings)
    s3 = boto3.client(
        "s3",
        endpoint_url=storage_settings.endpoint_url,
        aws_access_key_id=storage_settings.access_key,
        aws_secret_access_key=storage_settings.secret_key,
        region_name="us-east-1",
    )
    paper = PostgresPaperRepository(database_url, transaction_timeout_seconds=10)
    repository = PostgresPilotRepository(paper)
    repository.migrate()
    plan = _plan(root)
    session = _session(plan)
    paper.create_session(
        session, command_id="create-pilot-integration", payload_digest="f" * 64
    )
    assert (
        paper.get_session(session.session_id).spec.forward_start
        == plan.start_not_before
    )
    pilot = Pilot(
        plan=plan,
        session_id=session.session_id,
        state=PilotState.REGISTERED,
        approved_by="integration-test",
        approval_note="Accelerated integration pilot",
        created_at=START - timedelta(minutes=1),
        local_development=False,
    )
    manifest_uri = f"s3a://crypto-data/{root}/cycle.json"
    manifest_body = b'{"fixture":"immutable-candle-publication"}'
    manifest_sha256 = hashlib.sha256(manifest_body).hexdigest()
    s3.put_object(Bucket="crypto-data", Key=f"{root}/cycle.json", Body=manifest_body)
    try:
        stored, created = repository.create_pilot(
            pilot, command_id="register-pilot-integration", payload_digest="1" * 64
        )
        assert created is True
        assert stored.pilot_id == pilot.pilot_id

        candle = Candle(
            exchange="coinbase",
            symbol="BTC-USD",
            window_start=START,
            window_end=START + timedelta(minutes=1),
            open=Decimal(11),
            high=Decimal(11),
            low=Decimal(11),
            close=Decimal(11),
        )

        def run(index: int):
            source = PaperCandleInput(
                manifest_uri=manifest_uri,
                manifest_sha256=manifest_sha256,
                snapshot_key="2" * 64,
                command_id=f"paper-cycle-{index}",
            )
            return PostgresPilotRepository(
                PostgresPaperRepository(database_url, transaction_timeout_seconds=10)
            ).run_cycle(
                pilot.pilot_id,
                command_id=f"pilot-cycle-{index}",
                payload_digest=hashlib.sha256(str(index).encode()).hexdigest(),
                manifest_uri=manifest_uri,
                manifest_sha256=manifest_sha256,
                now=START,
                runner=lambda: paper.execute(
                    session.session_id,
                    command_id=source.command_id,
                    payload_digest=source.manifest_sha256,
                    actor="paper-system",
                    action="process_candles",
                    reason="accelerated integration cycle",
                    mutator=lambda current, existing: compute_paper_mutation(
                        current, existing, (candle,), source, now=START
                    ),
                ),
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            reports = list(executor.map(run, (1, 2)))
        assert sorted(report["newly_processed"] for report in reports) == [0, 1]
        retry = run(1)
        assert retry["status"] == "resolved_existing_command"
        assert paper.get_session(session.session_id).processed_candles == 1

        restarted = PostgresPilotRepository(
            PostgresPaperRepository(database_url, transaction_timeout_seconds=10)
        )
        snapshot = report_paper_pilot(
            pilot.pilot_id,
            START + timedelta(days=1),
            settings=_settings(storage_settings, root, database_url),
            paper_repository=paper,
            pilot_repository=restarted,
            store=store,
            now=START + timedelta(days=1),
        )
        assert (
            validate_pilot_publication(
                snapshot["manifest_uri"],
                snapshot["manifest_sha256"],
                store=store,
                expected_kind="paper_pilot_snapshot",
            )["status"]
            == "valid"
        )
        assessment = finalize_paper_pilot(
            pilot.pilot_id,
            reviewed_by="integration-test",
            review_note="Reviewed accelerated PostgreSQL and MinIO evidence",
            settings=_settings(storage_settings, root, database_url),
            paper_repository=paper,
            pilot_repository=restarted,
            store=store,
            now=START + timedelta(days=8),
        )
        assert assessment["verdict"] == "pass"
        assert paper.get_session(session.session_id).state is PaperSessionState.STOPPED
        with pytest.raises(InvalidPaperTrading, match="terminal"):
            run(3)
    finally:
        with psycopg.connect(database_url) as connection:
            for table in (
                "paper_pilot_snapshots",
                "paper_pilot_cycles",
                "paper_pilot_commands",
                "paper_pilots",
                "paper_session_events",
                "paper_equity",
                "paper_fills",
                "paper_decisions",
                "paper_candle_inputs",
                "paper_sessions",
            ):
                id_column = (
                    "pilot_id" if table.startswith("paper_pilot") else "session_id"
                )
                connection.execute(
                    f"DELETE FROM {table} WHERE {id_column} = %s",
                    [pilot.pilot_id if id_column == "pilot_id" else session.session_id],
                )
        objects = s3.list_objects_v2(Bucket="crypto-data", Prefix=root).get(
            "Contents", []
        )
        if objects:
            s3.delete_objects(
                Bucket="crypto-data",
                Delete={"Objects": [{"Key": item["Key"]} for item in objects]},
            )
