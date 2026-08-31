# Integration Tests

Integration tests verify real component interaction using disposable local dependencies, preferably through Docker Compose or Testcontainers.

Examples include collector-to-Kafka publication, Spark reads and writes against Kafka and MinIO, PostgreSQL inbox/outbox behavior, dbt against DuckDB, and safe restart recovery. Tests must create isolated topics, schemas, buckets or prefixes, and database namespaces and must clean them up without touching developer or shared environments.

The implemented raw-pipeline test publishes a uniquely identifiable record to
local Kafka and waits for Spark to archive it in MinIO. It verifies the Kafka
identity, key, value, and headers from the resulting Parquet row, then runs the
bounded raw-integrity audit and requires its JSON report to pass with the new
record inside the captured partition range. Rebuild `raw-sink` first, and run
the test only against the local Compose environment:

```powershell
podman compose build raw-sink
$env:RUN_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q tests\integration\test_raw_market_trades_pipeline.py
Remove-Item Env:RUN_INTEGRATION_TESTS
```

The Coinbase reconciliation integration test uses a local stub HTTP server and
temporary Parquet/report directories. It proves that one REST-only identity is
persisted with exit code `2` while the raw fixture remains byte-for-byte
unchanged. It never calls Coinbase, Kafka, or shared MinIO data:

```powershell
$env:RUN_RECONCILIATION_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q `
  tests\integration\test_coinbase_trade_reconciliation.py
Remove-Item Env:RUN_RECONCILIATION_INTEGRATION_TESTS
```

The market-candle integration test creates a pinned curated-trade snapshot in
local MinIO, runs the one-minute candle job in dry-run and apply modes, and
checks deterministic OHLCV, half-open event-time windows, historical-backfill
lineage, manifest publication, DuckDB validation, and idempotent reruns:

```powershell
podman compose up -d minio
podman compose build raw-sink
$env:RUN_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q `
  tests\integration\test_market_candles_pipeline.py
Remove-Item Env:RUN_INTEGRATION_TESTS
podman compose stop minio
```

The strategy-experiment integration test runs the two-stage research workflow
through MinIO. It proves the sealed selection contains train/validation rows but
no test metrics, evaluates the immutable winner against buy-and-hold on the OOS
range, validates both stages, checks idempotency, and detects a corrupted
comparison artifact:

```powershell
podman compose up -d minio
$env:RUN_EXPERIMENT_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q `
  tests\integration\test_strategy_experiment_pipeline.py
Remove-Item Env:RUN_EXPERIMENT_INTEGRATION_TESTS
podman compose stop minio
```

The candle-backtest integration test publishes a seven-candle fixture to local
MinIO and runs the actual trading-core CLI. It checks exact next-open fills,
costs and summary counts, direct S3 result validation, idempotent reruns, changed
parameter identity, and fail-closed artifact tamper detection. Only MinIO is
started; the test has no Kafka, execution-gateway, or exchange dependency:

```powershell
podman compose up -d minio
$env:RUN_BACKTEST_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q `
  tests\integration\test_candle_backtest_pipeline.py
Remove-Item Env:RUN_BACKTEST_INTEGRATION_TESTS
podman compose stop minio
```
