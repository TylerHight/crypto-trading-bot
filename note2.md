## Simple overview

This is currently a production-like market-data and strategy-research platform—not a live trading bot.

It continuously collects public Coinbase trades, preserves them, builds trustworthy analytical datasets, tests a simple strategy, and can run that strategy in a durable paper-trading pilot. It has no ability to place real orders or access a private exchange account.

## Data flow

```text
Coinbase public WebSocket
        ↓
Collector: validates and normalizes BTC-USD/ETH-USD trades
        ↓
Kafka: durable raw-trade and data-quality topics
        ↓
Spark streaming raw sink
        ↓
MinIO: immutable raw Parquet files + processing checkpoint
        ↓
Raw integrity audit
        ↓
Curated trades: validation, deduplication, quarantine
        ↓
One-minute OHLCV candles
        ↓
Backtests and sealed strategy experiments
        ↓
Operator-approved paper pilot
        ↓
PostgreSQL portfolio state + immutable daily reports
        ↓
Final pass, fail, or inconclusive assessment
```

Collection and raw storage run continuously. Auditing, curation, candle generation, experiments, and paper-pilot cycles are intentionally bounded commands—they do not run automatically in the background yet.

## Current operating state

Right now:

- Kafka is healthy.
- MinIO is healthy.
- PostgreSQL is healthy.
- The Spark raw sink is healthy.
- The Coinbase collector is running and stable.
- The optional Kafka browser UI is stopped.
- Public BTC-USD and ETH-USD data continues to be collected.
- No paper pilot or paper session has been registered yet.
- No real trading is possible.

The most recent sealed evidence contains:

- 508,105 Kafka records reconciled exactly to raw Parquet.
- 380,429 curated logical trades after duplicate removal and quarantine.
- 1,029 one-minute candles.
- Zero conflicting duplicate trades.
- One sealed out-of-sample strategy evaluation.

These are frozen snapshots; the raw collection has continued growing since they were created.

The detailed status is recorded in [note.md](C:/Development/Projects/crypto-trading-bot/note.md:3).

## Functionality that exists

### Market-data collection

The collector connects to Coinbase’s public WebSocket and processes BTC-USD and ETH-USD trades. It monitors heartbeats, malformed messages, connection health, and sequence gaps.

It publishes:

- Raw trade envelopes to `market.trades.raw.v1`.
- Health and quality observations to `market.data.quality.v1`.

### Durable raw storage

Spark continuously reads Kafka and writes byte-preserving Parquet files into MinIO. Kafka topic, partition, offset, headers, keys, and values are retained so the archive can be checked against Kafka exactly.

Spark checkpoints allow the sink to restart without intentionally rereading everything.

### Data assurance and recovery

The project can:

- Compare every retained Kafka position with raw Parquet.
- Detect missing or duplicated archive positions.
- Detect malformed event values.
- Preserve audit reports as immutable evidence.
- Compare suspicious periods with Coinbase’s public REST API.
- Repair specifically confirmed gaps through a bounded, auditable backfill.

### Curated trades and candles

Batch jobs transform raw deliveries into trusted analytical data:

- Exact redeliveries are deduplicated.
- Invalid records are quarantined.
- Conflicting duplicates fail closed.
- Prices and sizes use exact decimals.
- One-minute OHLCV/VWAP candles are created deterministically.
- Every publication is immutable and identified by a SHA-256 digest.

### Backtesting

The trading core implements a deterministic, long-only SMA crossover:

- Signals are calculated at candle close.
- Orders are simulated at the next candle’s open.
- Fees and slippage are included.
- It tracks decisions, fills, equity, and drawdown.
- Results can be independently replayed and validated.

### Sealed experiments

The experiment workflow prevents selecting a strategy after seeing its test result:

1. Candidates are compared on train and validation periods.
2. One candidate is sealed.
3. The test period is opened exactly once.
4. Immutable manifests record the complete lineage.

The current candidate is SMA 5/20. Its short out-of-sample test returned approximately −0.4302%, versus −0.3730% for buy-and-hold. That slight underperformance is why the pilot requires explicit approval rather than being launched silently.

### Durable paper trading

Paper mode stores simulated portfolio state in PostgreSQL and supports:

- Restart-safe processing.
- Idempotent command retries.
- Concurrent-cycle serialization.
- Simulated fills, fees, positions, cash, and equity.
- Automatic pauses for gaps, conflicts, invariant failures, or drawdown breaches.
- Explicit audited pause/resume/stop actions.

### Pre-registered pilot workflow

The prepared [pilot plan](C:/Development/Projects/crypto-trading-bot/pilots/btc-usd-sma-forward-v1.json) defines a 30-day paper-only experiment with criteria fixed before it starts.

It supports:

- Registration with operator approval.
- Forward-only candle processing.
- Immutable daily snapshots.
- Final deterministic `pass`, `fail`, or `inconclusive` assessment.
- Independent artifact validation.

A pass only means “eligible for execution-system design review.” It never enables real trading.

## How to use it

Set up Python:

```powershell
uv python install 3.11
uv sync --python 3.11 --all-packages --all-groups
.\.venv\Scripts\Activate.ps1
```

Start or inspect the live pipeline:

```powershell
podman machine start
podman compose up -d --build
podman compose ps
podman compose logs --tail 50 collector raw-sink
```

Start the optional Kafka UI:

```powershell
podman compose up -d kafka-ui
```

Then open:

- Kafka UI: <http://localhost:8083>
- MinIO console: <http://localhost:9001>

Run the raw integrity audit:

```powershell
.\scripts\run_raw_integrity_audit.ps1
```

Run tests:

```powershell
python -m pytest -q
python -m ruff check apps jobs packages tests
python -m mypy -p crypto_trading_core
```

The latest complete run passed 243 tests, with 9 optional environment-gated tests skipped. The PostgreSQL/MinIO pilot integration was also enabled and passed separately.

The complete operational commands are documented in:

- [Market-data runbook](C:/Development/Projects/crypto-trading-bot/docs/runbooks/local-market-data-pipeline.md)
- [Spark jobs guide](C:/Development/Projects/crypto-trading-bot/jobs/spark/README.md)
- [Trading-core guide](C:/Development/Projects/crypto-trading-bot/apps/trading_core/README.md)

## What remains

The software is ready, but the actual paper pilot has not started. It still needs:

1. Explicit operator approval acknowledging the weak short OOS result.
2. Registration before its planned September 7 UTC start.
3. Fresh candle publications and bounded processing cycles.
4. Daily snapshots during the real 30-day period.
5. Final review on or after October 7.

There is still no live execution gateway, private exchange authentication, automatic order placement, or automatic promotion to real trading.