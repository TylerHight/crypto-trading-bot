"""
Seed the local PostgreSQL database with sample paper trading data for SQL practice.

Usage:
    python scripts/seed_sample_data.py

Requires the postgres container to be running:
    docker compose up postgres -d
"""

from __future__ import annotations

import hashlib
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

sys.path.insert(0, "apps/trading_core/src")

from crypto_trading_core.paper import compute_paper_mutation
from crypto_trading_core.paper_contracts import (
    PaperCandleInput,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_domain.backtest import Candle, initialize_incremental_backtest

import os

DATABASE_URL = os.getenv(
    "PAPER_DATABASE_URL",
    "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
)

# Each scenario is (name, fast_period, slow_period, prices, starting_cash)
# Prices are a sequence of BTC-USD close prices per minute starting at START
SCENARIOS: list[tuple[str, int, int, list[str], str]] = [
    (
        "sma-crossover-bullish",
        3,
        7,
        # rising trend — triggers a BUY then holds
        ["29000", "29100", "29300", "29600", "30000", "30500", "31000", "31200", "31500", "31800"],
        "10000",
    ),
    (
        "sma-crossover-bearish",
        3,
        7,
        # rising then sharp fall — triggers BUY then SELL
        ["29000", "29500", "30000", "30500", "31000", "29000", "27000", "25000", "24000", "23000"],
        "10000",
    ),
    (
        "sma-crossover-flat",
        2,
        5,
        # sideways chop — no clear signal
        ["50000", "50100", "49900", "50050", "49950", "50020", "49980", "50010", "49990", "50000"],
        "5000",
    ),
]

START = datetime(2026, 1, 1, tzinfo=UTC)


def _make_candle(minute: int, price: str) -> Candle:
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


def _make_session(name: str, fast: int, slow: int, prices: list[str]) -> PaperSession:
    marker = hashlib.sha256(name.encode()).hexdigest()
    spec = PaperSessionSpec(
        evaluation_manifest_uri=f"s3a://crypto-data/sample/{name}/evaluation.json",
        evaluation_manifest_sha256=marker,
        selection_manifest_uri=f"s3a://crypto-data/sample/{name}/selection.json",
        selection_manifest_sha256="b" * 64,
        candle_manifest_uri=f"s3a://crypto-data/sample/{name}/candles.json",
        candle_manifest_sha256="c" * 64,
        evaluation_key="d" * 64,
        selection_key="e" * 64,
        exchange="coinbase",
        symbol="BTC-USD",
        candidate_id=f"sma-{fast}-{slow}",
        fast_period=fast,
        slow_period=slow,
        starting_cash=Decimal("10000"),
        fee_bps=Decimal("25"),
        slippage_bps=Decimal("5"),
        test_end=START,
        approved_by="seed-script",
        approval_note=f"Sample data: {name}",
        maximum_drawdown=Decimal("1"),
        strategy_version="sma-crossover-long-only-v1",
        backtest_engine_version="candle-backtest-engine-v1",
        experiment_engine_version="strategy-experiment-engine-v1",
    )
    # initialize_incremental_backtest requires exactly slow_period - 1 warmup candles
    # with consecutive timestamps; use negative minute indices so they precede the data
    warmup = tuple(_make_candle(i - (slow - 1), prices[0]) for i in range(slow - 1))
    strategy = initialize_incremental_backtest(
        warmup, starting_cash=spec.starting_cash, slow_period=slow
    )
    return PaperSession(
        spec=spec,
        state=PaperSessionState.ACTIVE,
        strategy_state=strategy,
        created_at=START,
        updated_at=START,
        last_state_changed_at=START,
    )


def seed() -> None:
    repo = PostgresPaperRepository(DATABASE_URL, transaction_timeout_seconds=30)
    repo.migrate()
    print("Migrations applied.")

    for name, fast, slow, prices, _ in SCENARIOS:
        session = _make_session(name, fast, slow, prices)
        stored, created = repo.create_session(
            session,
            command_id=f"seed-create-{name}",
            payload_digest=hashlib.sha256(name.encode()).hexdigest(),
        )
        status = "created" if created else "already existed"
        print(f"  Session '{name}' ({stored.session_id[:12]}…): {status}")

        if created:
            candles = tuple(_make_candle(i, p) for i, p in enumerate(prices))
            digest = hashlib.sha256(f"seed-candles-{name}".encode()).hexdigest()
            source = PaperCandleInput(
                manifest_uri=f"s3a://crypto-data/sample/{name}/run.json",
                manifest_sha256=digest,
                snapshot_key=hashlib.sha256(f"snapshot-{name}".encode()).hexdigest(),
                command_id=f"seed-process-{name}",
            )
            result = repo.execute(
                stored.session_id,
                command_id=source.command_id,
                payload_digest=source.manifest_sha256,
                actor="seed-script",
                action="process_candles",
                reason="sample data population",
                mutator=lambda s, e, _candles=candles, _source=source: compute_paper_mutation(
                    s, e, _candles, _source, now=START + timedelta(hours=1)
                ),
            )
            print(f"    Processed {result.get('newly_processed', 0)} candles.")

    print("\nDone. Connect DBeaver to localhost:5432 / crypto_trading and query away.")


if __name__ == "__main__":
    seed()
