# Trading Core

The first trading-core increment is a bounded, deterministic backtest. It reads
one SHA-pinned one-minute candle publication, runs
`sma-crossover-long-only-v1`, and publishes simulated decisions, next-open
fills, an equity curve, and a summary. It has no exchange adapter dependency and
cannot place orders.

Reusable strategy, portfolio, and exact-decimal rules live in
`packages/domain/src/crypto_trading_domain/backtest.py`. This application owns
only manifest verification, DuckDB reads, S3/local storage, CLI configuration,
and result publication.

## Timing and costs

At a candle close, the strategy compares the configured fast and slow simple
moving averages, including that close. A changed target fills at the next
candle's open; a final decision with no later candle remains unfilled. The v1
engine is long-only, all-in/all-out, rejects missing one-minute candles, and
marks any ending position to the final close without forced liquidation.

Buy slippage increases the next open and sell slippage decreases it. Fees are a
percentage of simulated notional. Financial values use `Decimal` and publish as
`decimal(38,18)`; affordable buy quantity rounds down so cash cannot become
negative.

## Run locally

Pin a downloaded candle manifest by its exact bytes:

```powershell
$manifest = Resolve-Path .\candle-manifest.json
$digest = (Get-FileHash $manifest -Algorithm SHA256).Hash.ToLowerInvariant()

backtest-market-candles `
  --candle-manifest $manifest `
  --candle-manifest-sha256 $digest `
  --exchange coinbase `
  --symbol BTC-USD `
  --start 2026-01-01T00:00:00Z `
  --end 2026-02-01T00:00:00Z `
  --starting-cash 10000 `
  --fast-period 5 `
  --slow-period 20 `
  --fee-bps 40 `
  --slippage-bps 5 `
  --output .\artifacts\backtests\v1 `
  --local-development
```

The candle manifest may point to local Parquet or S3-compatible storage. Local
input or output always requires `--local-development`. S3 settings are:

```text
BACKTEST_S3_ENDPOINT
BACKTEST_S3_ACCESS_KEY
BACKTEST_S3_SECRET_KEY
BACKTEST_S3_REGION
BACKTEST_CANDLE_MANIFEST_PREFIX
BACKTEST_CANDLE_OUTPUT_PREFIX
BACKTEST_OUTPUT_PREFIX
BACKTEST_MAXIMUM_INPUT_CANDLES
```

Fee and slippage inputs are mandatory. They may be zero for a controlled test,
but zero-cost output should not be interpreted as realistic performance.

## Result publication

Artifacts are written beneath
`<output>/runs/<run-id>/` as `decisions.parquet`, `fills.parquet`,
`equity_curve.parquet`, and `summary.json`. The append-only discoverable
manifest is published last at
`<output>/manifests/<backtest-key>/manifest.json`. Identical inputs resolve the
existing publication; version, parameter, range, or candle-digest changes create
a different key.

Validate all hashes, schemas, source lineage, strategy output, fill timing,
portfolio math, equity, drawdown, and summary values by replaying the pinned
input:

```powershell
validate-backtest `
  --manifest .\artifacts\backtests\v1\manifests\<backtest-key>\manifest.json `
  --local-development
```

The command reports `execution_mode=simulation`. It does not publish Kafka
signals or commands, use private credentials, contact an exchange, or claim
that one backtest demonstrates profitability.

Future paper and live modes will add PostgreSQL state, synchronous risk, and an
execution boundary. They must reuse the domain strategy semantics and remain
separate from this immutable research workflow.

## Sealed train/validation/test experiment

Strategy experiments are deliberately split into two commands. Preparation
hash-pins a strict JSON specification, runs every declared SMA candidate only on
train and validation ranges, applies the versioned eligibility/ranking policy,
and publishes a sealed selection with `test_data_accessed=false`:

```powershell
$spec = Resolve-Path .\experiments\btc-usd-sma-v1.json
$digest = (Get-FileHash $spec -Algorithm SHA256).Hash.ToLowerInvariant()

prepare-strategy-experiment `
  --spec $spec `
  --spec-sha256 $digest `
  --output .\artifacts\strategy-experiments\v1 `
  --local-development
```

Review and hash the immutable selection manifest. The evaluation command has no
candidate, cost, or range override flags; it runs exactly the sealed candidate
and `buy-and-hold-long-only-v1` on the specification's test interval:

```powershell
$selection = Resolve-Path .\artifacts\strategy-experiments\v1\selections\<key>\manifest.json
$selectionDigest = (Get-FileHash $selection -Algorithm SHA256).Hash.ToLowerInvariant()

evaluate-strategy-experiment `
  --selection-manifest $selection `
  --selection-manifest-sha256 $selectionDigest `
  --output .\artifacts\strategy-experiments\v1 `
  --local-development

validate-strategy-experiment selection `
  --manifest $selection `
  --manifest-sha256 $selectionDigest `
  --local-development
```

Use the `evaluation` validator subcommand for the resulting evaluation manifest.
Validators replay the underlying pinned backtests and baselines, verify artifact
hashes and schemas, and recalculate selection or comparison results.

Additional configuration:

```text
EXPERIMENT_SPEC_PREFIX
EXPERIMENT_OUTPUT_PREFIX
EXPERIMENT_MAXIMUM_CANDIDATES
EXPERIMENT_MAXIMUM_CANDIDATE_CANDLE_EVALUATIONS
```

Selection eligibility requires the configured minimum train fills and maximum
train/validation drawdowns. Eligible candidates rank by validation return,
validation drawdown, validation fees, then candidate ID. All losing and rejected
candidates remain in the artifacts. No eligible candidate exits with code `2`
and prevents test evaluation.

## Durable paper trading

Paper mode forward-tests one sealed, evaluated candidate without exchange
authentication or order-placement capability. It advances only from immutable,
SHA-pinned candle publications and stores its portfolio, pending target,
checkpoint, decisions, simulated fills, equity, and audit events in PostgreSQL.

Start PostgreSQL and apply the forward-only migration:

```powershell
podman compose up -d postgres
$env:PAPER_DATABASE_URL = "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading"
migrate-paper-database
```

Create a session after reviewing a validated OOS evaluation:

```powershell
create-paper-session `
  --evaluation-manifest <evaluation-manifest-uri> `
  --evaluation-manifest-sha256 <64-lowercase-hex-digest> `
  --approved-by research-operator `
  --approval-note "Reviewed OOS comparison and approved a forward simulation" `
  --maximum-drawdown 0.20
```

Process one bounded publication, retry it safely, and inspect state:

```powershell
process-paper-candles `
  --session-id <session-id> `
  --candle-manifest <candle-manifest-uri> `
  --candle-manifest-sha256 <64-lowercase-hex-digest> `
  --command-id paper-candles-2026-09-01

show-paper-session --session-id <session-id>
```

Lifecycle changes are explicit and audited:

```powershell
set-paper-session-state `
  --session-id <session-id> `
  --state paused `
  --actor research-operator `
  --reason "Reviewing forward behavior"
```

Allowed transitions are `active -> paused|stopped` and
`paused -> active|stopped`; stopped is terminal. Gaps, conflicting previously
processed candles, portfolio invariant failures, and maximum-drawdown breaches
fail closed. Processing is transactional and row-locked so retries, restarts,
and concurrent invocations cannot duplicate state changes.

Configuration:

```text
PAPER_DATABASE_URL
PAPER_CANDLE_MANIFEST_PREFIX
PAPER_EVALUATION_MANIFEST_PREFIX
PAPER_MAXIMUM_CANDLES_PER_RUN
PAPER_TRANSACTION_TIMEOUT_SECONDS
```

Paper mode reuses the same SMA, next-open fill, fee, slippage, and rounding
rules as backtesting. It remains a simulation and does not establish future
profitability. This increment intentionally has no exchange-adapter dependency
and cannot submit an order.
