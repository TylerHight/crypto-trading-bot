from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from crypto_trading_core.paper import compute_paper_mutation, paper_session_status
from crypto_trading_core.paper_contracts import (
    PaperCandleInput,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_domain.backtest import Candle, initialize_incremental_backtest

pytestmark = pytest.mark.integration
START = datetime(2026, 3, 1, tzinfo=UTC)


def _enabled() -> bool:
    return os.getenv("RUN_PAPER_INTEGRATION_TESTS") == "1"


def _candle(minute: int, price: str) -> Candle:
    start = START + timedelta(minutes=minute)
    value = Decimal(price)
    return Candle(
        exchange="coinbase",
        symbol="BTC-USD",
        window_start=start,
        window_end=start + timedelta(minutes=1),
        open=value,
        high=value,
        low=value,
        close=value,
    )


def _session(marker: str) -> PaperSession:
    spec = PaperSessionSpec(
        evaluation_manifest_uri="s3a://crypto-data/evaluations/manifest.json",
        evaluation_manifest_sha256=hashlib.sha256(marker.encode()).hexdigest(),
        selection_manifest_uri="s3a://crypto-data/selections/manifest.json",
        selection_manifest_sha256="b" * 64,
        candle_manifest_uri="s3a://crypto-data/candles/manifest.json",
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
        approved_by="integration-test",
        approval_note=f"PostgreSQL integration {marker}",
        maximum_drawdown=Decimal(1),
        strategy_version="sma-crossover-long-only-v1",
        backtest_engine_version="candle-backtest-engine-v1",
        experiment_engine_version="strategy-experiment-engine-v1",
    )
    strategy = initialize_incremental_backtest(
        (_candle(-1, "10"),), starting_cash=spec.starting_cash, slow_period=2
    )
    return PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=strategy,
        created_at=START,
        updated_at=START,
        last_state_changed_at=START,
    )


def _source(index: int) -> PaperCandleInput:
    digest = hashlib.sha256(f"manifest-{index}".encode()).hexdigest()
    return PaperCandleInput(
        manifest_uri=f"s3a://crypto-data/paper/{digest}/manifest.json",
        manifest_sha256=digest,
        snapshot_key=hashlib.sha256(f"snapshot-{index}".encode()).hexdigest(),
        command_id=f"process-{index}",
    )


def _execute(
    repository: PostgresPaperRepository,
    session_id: str,
    source: PaperCandleInput,
    candles: tuple[Candle, ...],
    *,
    command_id: str | None = None,
):
    command = command_id or source.command_id
    return repository.execute(
        session_id,
        command_id=command,
        payload_digest=source.manifest_sha256,
        actor="paper-system",
        action="process_candles",
        reason="PostgreSQL integration publication",
        mutator=lambda current, existing: compute_paper_mutation(
            current,
            existing,
            candles,
            source,
            now=START + timedelta(hours=1),
        ),
    )


@pytest.mark.skipif(
    not _enabled(),
    reason="set RUN_PAPER_INTEGRATION_TESTS=1 with local PostgreSQL available",
)
def test_postgres_paper_state_is_transactional_restartable_and_concurrency_safe() -> None:
    import psycopg

    database_url = os.getenv(
        "PAPER_DATABASE_URL",
        "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
    )
    repository = PostgresPaperRepository(database_url, transaction_timeout_seconds=10)
    repository.migrate()
    session = _session(str(uuid4()))
    try:
        stored, created = repository.create_session(
            session,
            command_id="create-integration-session",
            payload_digest="f" * 64,
        )
        assert created is True
        assert stored == session

        first_source = _source(1)
        first = _execute(
            repository,
            session.session_id,
            first_source,
            (_candle(0, "11"), _candle(1, "12")),
        )
        assert first["newly_processed"] == 2
        retry = _execute(
            repository,
            session.session_id,
            first_source,
            (_candle(0, "11"), _candle(1, "12")),
        )
        assert retry["status"] == "resolved_existing_command"

        restarted = PostgresPaperRepository(database_url, transaction_timeout_seconds=10)
        second = _execute(
            restarted,
            session.session_id,
            _source(2),
            (_candle(2, "9"),),
        )
        assert second["newly_processed"] == 1

        concurrent_source = _source(3)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    _execute,
                    PostgresPaperRepository(database_url, transaction_timeout_seconds=10),
                    session.session_id,
                    concurrent_source,
                    (_candle(3, "8"),),
                    command_id=f"concurrent-{index}",
                )
                for index in range(2)
            ]
        reports = [future.result() for future in futures]
        assert sorted(report["newly_processed"] for report in reports) == [0, 1]
        assert repository.get_session(session.session_id).processed_candles == 4

        with psycopg.connect(database_url) as connection:
            before = connection.execute(
                "SELECT count(*) FROM paper_session_events WHERE session_id = %s",
                [session.session_id],
            ).fetchone()[0]
        status = paper_session_status(session.session_id, restarted)
        assert status["processed_candles"] == 4
        with psycopg.connect(database_url) as connection:
            after = connection.execute(
                "SELECT count(*) FROM paper_session_events WHERE session_id = %s",
                [session.session_id],
            ).fetchone()[0]
        assert before == after
    finally:
        with psycopg.connect(database_url) as connection:
            for table in (
                "paper_session_events",
                "paper_equity",
                "paper_fills",
                "paper_decisions",
                "paper_candle_inputs",
                "paper_sessions",
            ):
                connection.execute(
                    f"DELETE FROM {table} WHERE session_id = %s",
                    [session.session_id],
                )
