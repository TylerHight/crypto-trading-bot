# Historical Backfill and Reconciliation

This application owns bounded historical market-data reads, source-to-archive
reconciliation, and later idempotent backfill workflows. Scheduling belongs in
`orchestration/airflow/`; exchange HTTP behavior stays in
`packages/exchange_adapters/`.

## Historical research candles

`prepare-historical-candles` downloads a fixed BTC-USD one-minute range from
Coinbase Exchange's public candle endpoint. The checked-in plan is
[`datasets/btc-usd-history-v1.json`](../../datasets/btc-usd-history-v1.json).
It covers June 10 through September 8, 2026 UTC plus 239 warmup minutes.

Coinbase limits each request to 300 candles and warns that intervals without
ticks have no candle. The downloader uses non-overlapping pages, bounded retries,
a 0.15-second request interval, and a 1,800-request limit. Each response is saved
with its hash before normalization, so an interrupted run resumes from its pages.

```powershell
$env:PYTHONPATH='apps/historical_backfill/src;packages/exchange_adapters/src;packages/domain/src'
$planDigest=(Get-FileHash datasets/btc-usd-history-v1.json -Algorithm SHA256).Hash.ToLower()
.venv311\Scripts\python.exe -m crypto_historical_backfill.historical_candles `
  --plan datasets/btc-usd-history-v1.json --plan-sha256 $planDigest
```

The publication is a separate `exchange-ohlcv-v1` source under
`analytics/historical_candles/v1/`. It does not claim the trade-level lineage,
VWAP, or trade counts of live-pipeline candles. Its manifest lists every source
page, output Parquet file, digest, duplicate, conflict, and missing-minute range.

The real download saved all 433 requested pages and 129,834 candles. Coinbase
published no candle for five minutes: June 29 13:10–13:14 UTC and July 6 01:38
UTC. Targeted checks against both Coinbase public candle endpoints returned no
candle for those minutes. No replacement prices were created.

Use the coverage-only research command to validate a publication without
choosing or evaluating a strategy:

```powershell
$env:PYTHONPATH='apps/trading_core/src;packages/domain/src'
$env:BACKTEST_S3_ENDPOINT='http://127.0.0.1:9000'
$env:BACKTEST_S3_ACCESS_KEY='minioadmin'
$env:BACKTEST_S3_SECRET_KEY='minioadmin'
$env:BACKTEST_CANDLE_MANIFEST_PREFIX='s3a://crypto-data/analytics/historical_candles/v1/manifests'
$env:BACKTEST_CANDLE_OUTPUT_PREFIX='s3a://crypto-data/analytics/historical_candles/v1/runs'
$digest=(Get-FileHash experiments/btc-usd-historical-readiness-v1.json -Algorithm SHA256).Hash.ToLower()
.venv311\Scripts\python.exe -m crypto_trading_core.longer_research `
  --spec experiments/btc-usd-historical-readiness-v1.json `
  --spec-sha256 $digest --coverage-only --local-development
```

The current result is `inconclusive`: 129,595 of 129,600 research minutes are
present. Acquisition is complete, but backtesting remains blocked until a fixed
gap-handling policy is defined and tested.

## Coinbase trade reconciliation

`reconcile-coinbase-trades` compares one explicit Coinbase product/time range
with raw Parquet by `(symbol, source_event_id)`. It is read-only with respect to
Coinbase, Kafka, raw Parquet, and Spark checkpoints. Its only writes are
append-only JSON reports and confirmed identity findings.

The command requires a passing raw-integrity audit report completed at or after
the requested interval. This prevents a Kafka-to-Parquet problem from being
misclassified as a WebSocket/source gap.

The Coinbase adapter calls the Advanced Trade public market-trades endpoint. A
response containing the configured result limit is recursively split into
smaller time windows. Empty, unsplittable, or request-capped coverage remains
unresolved rather than producing a false pass.

Install the workspace package for local development:

```powershell
python -m pip install -e .\packages\domain
python -m pip install -e .\packages\exchange_adapters
python -m pip install -e .\apps\historical_backfill
```

Run a small bounded comparison:

```powershell
$env:RECONCILIATION_S3_ENDPOINT = "http://127.0.0.1:9000"
$env:RECONCILIATION_S3_ACCESS_KEY = "minioadmin"
$env:RECONCILIATION_S3_SECRET_KEY = "minioadmin"

reconcile-coinbase-trades `
  --symbol BTC-USD `
  --start-at 2026-08-25T14:00:00Z `
  --end-at 2026-08-25T14:05:00Z `
  --raw-integrity-report .\raw-integrity-audit.txt
```

Use `COINBASE_API_BEARER_TOKEN` when the selected Coinbase endpoint requires an
authenticated request. Tokens and object-storage credentials are never written
to reports.

Exit codes are:

- `0`: complete comparison with no REST-only identities.
- `2`: complete comparison with confirmed REST-only identities.
- `3`: unresolved coverage, failed prerequisite, or bounded REST failure.
- Other nonzero values: invalid configuration or unexpected execution failure.

Reports are written under `reports/event_date=YYYY-MM-DD/`. Full confirmed safe
identity findings are separately written under `findings/event_date=YYYY-MM-DD/`.
Both filenames use a unique run ID and append-only creation.

## Confirmed-gap backfill

`backfill-coinbase-trades` accepts exactly one reconciliation report. It checks
the linked findings digest, run ID, key, count and configured prefix; re-runs
bounded Coinbase coverage; and checks raw Parquet immediately before any send.
The default mode is a non-publishing dry run:

```powershell
backfill-coinbase-trades `
  --reconciliation-report `
    s3a://crypto-data/reconciliation/coinbase-trades/reports/event_date=2026-08-25/<run-id>.json
```

After reviewing the dry-run JSON, explicitly publish through the canonical
Kafka-to-Parquet path:

```powershell
backfill-coinbase-trades `
  --reconciliation-report <report-uri> `
  --apply
```

Apply mode creates one conditional claim per finding, waits for every Kafka
acknowledgement, durably records its topic/partition/offset, and resolves only
after that exact position appears once in the bounded Kafka-ingestion-time raw
Parquet partitions selected from the acknowledgement timestamp. A timeout remains
`published_pending_archive`; retrying checks Parquet and the durable receipt and
does not blindly publish. A claim without a receipt is
`unresolved_ambiguous_publication` and requires investigation.

Exit codes are `0` for fully resolved/no action, `2` for dry-run-ready work,
`3` for partial, retryable, pending or ambiguous outcomes, and `4` for invalid
or permanent input. State documents contain only safe identities and Kafka
positions, never complete payloads or credentials.
