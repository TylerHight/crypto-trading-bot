# Trading Core

## Longer strategy research

Run the fixed 90-day BTC experiment with `python -m crypto_trading_core.longer_research`.
It first publishes a coverage report. Missing minutes (including warmup) stop
selection and produce **inconclusive**. It never fills gaps with invented candles.
If coverage passes, the existing engine selects using train and validation only,
then evaluates the selected strategy against buy-and-hold after fees and slippage.

The checked-in configuration is
[`btc-usd-longer-research-v1.json`](../../experiments/btc-usd-longer-research-v1.json):
60 train days, 15 validation days, and 15 test days ending September 8, 2026 UTC.
It fixes SMA 5/20, 15/60, and 60/240, starting cash 10,000, fees 40 basis points,
and slippage 5 basis points. These are research assumptions, not exchange quotes.

For the existing local Python 3.11 environment, run from the repository root:

```powershell
$env:PYTHONPATH='apps/trading_core/src;packages/domain/src'
$env:BACKTEST_S3_ENDPOINT='http://127.0.0.1:9000'
$env:BACKTEST_S3_ACCESS_KEY='minioadmin'
$env:BACKTEST_S3_SECRET_KEY='minioadmin'
$researchDigest=(Get-FileHash experiments/btc-usd-longer-research-v1.json -Algorithm SHA256).Hash.ToLower()
.venv311\Scripts\python.exe -m crypto_trading_core.longer_research --spec experiments/btc-usd-longer-research-v1.json --spec-sha256 $researchDigest --local-development
```

With the standard uv workspace setup, `uv run --all-packages run-longer-research`
accepts the same arguments. `--local-development` permits the local configuration;
output still defaults to local MinIO. Exit 2 means inconclusive or no candidate;
exit 4 means rejected inputs. Exit 0 means evaluated, not necessarily recommended.

Results live under `analytics/strategy_experiments/v1/longer_research/`:

- `registrations/<name>.json` locks the configuration before candle inspection.
- `runs/<key>/spec.json` preserves the exact configuration bytes.
- `runs/<key>/coverage.json` pins candle and curated manifests, all candle file
  SHA-256 hashes, and every missing minute as `[start, end)` UTC ranges with counts.
- `engine/` contains the existing immutable selection and OOS publications.
- `reports/<key>/manifest.json` links the evidence and gives the recommendation.
  No-candidate and insufficient-data reports explicitly omit OOS results.

Coverage reads timestamp and lineage columns only. It hashes and temporarily
copies opaque Parquet bytes; this is not a claim that test files were never
touched. Price queries during selection are restricted to dates before the test
boundary. Evaluation uses the same frozen bytes. All new publications are
append-only and checked by read-back. An identical rerun resolves the same result;
changed inputs or a changed configuration under the same name are rejected.
Use a new reviewed configuration/name for a new experiment; preserve this attempt
and do not tune candidates against its test results.

Open the dashboard at <http://127.0.0.1:8090>. Its top summary shows **Not enough
data**, **No strategy selected**, or **Test complete**. A completed test shows the
selected strategy, its return, buy-and-hold return, and the difference. A trial
recommendation requires strictly positive excess return after costs. The dashboard
and runner do not register, approve, or start a paper trial.

On September 8 the real input had only **924 of 129,600 required BTC minutes** in
the fixed window (926 BTC candles overall). The published result is inconclusive:
**do not start a paper trial**. Prepare a complete historical publication before
planning another experiment. The existing short SMA evaluation is preserved.

## Original backtest workflow

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

## Pre-registered paper pilot

Development is pinned by the repository `.python-version` to Python 3.11; all
workspace packages support Python 3.11 and 3.12 and explicitly exclude 3.13.
With `uv` installed, create the reproducible environment and confirm Spark uses
the same interpreter for its driver and workers:

```powershell
uv python install 3.11
uv sync --python 3.11 --all-packages --all-groups
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pytest -q
```

Copy `pilots/pilot-plan.example.json`, replace its evaluation URI and digest,
choose a still-future aligned UTC start, and review every threshold before
hashing it. The raw bytes are the commitment; editing even whitespace changes
the identity.

```powershell
$plan = Resolve-Path .\pilots\btc-usd-sma-forward-v1.json
$digest = (Get-FileHash $plan -Algorithm SHA256).Hash.ToLowerInvariant()

start-paper-pilot --plan $plan --plan-sha256 $digest `
  --approved-by research-operator `
  --approval-note "Reviewed sealed OOS evidence and precommitted pilot criteria"

run-paper-pilot-cycle --pilot-id <pilot-id> `
  --candle-manifest <new-forward-candle-manifest> `
  --candle-manifest-sha256 <digest> --command-id cycle-2026-09-15-001

report-paper-pilot --pilot-id <pilot-id> --as-of 2026-09-16T00:00:00Z

finalize-paper-pilot --pilot-id <pilot-id> `
  --reviewed-by research-operator --review-note "Reviewed immutable evidence"
```

For a production-like run, set `PAPER_DATABASE_URL`, the existing S3 settings,
and `PAPER_PILOT_OUTPUT_PREFIX`. Local manifests require
`--local-development` at registration; that marker is persisted for the whole
pilot. A production pilot therefore cannot later opt into local synthetic
inputs. Each cycle is bounded to one published Coinbase candle manifest and a
stable command ID. The first publication must cover the selected strategy's
complete contiguous SMA warm-up immediately before `start_not_before`; those
pre-start candles initialize strategy state but are not stored or counted as
forward evidence. This allows a sealed evaluation to end before the
pre-registered forward start without treating the intentional boundary as a
data gap. Later publications may be cumulative: candles before the forward
start are ignored, previously processed forward candles must reproduce their
stored OHLC values exactly, and only the contiguous unseen suffix advances the
session.

The registration, daily snapshot, and final assessment manifests are written
append-only under `analytics/paper_pilots/v1/{pilots,snapshots,assessments}`.
Validate any publication independently:

```powershell
validate-paper-pilot snapshot --manifest <manifest-uri> --manifest-sha256 <digest>
validate-paper-pilot assessment --manifest <manifest-uri> --manifest-sha256 <digest>
```

The finalizer has no threshold flags. It rejects an early request unless a
definitive safety failure already exists, recomputes metrics from PostgreSQL,
verifies every recorded candle and snapshot digest, stops the paper session,
and makes the pilot terminal. A pass means only
`eligible_for_execution_design_review`; it never enables live execution.

Recommended operational walkthrough:

1. Start and health-check Kafka, MinIO, PostgreSQL, the collector, and raw sink.
2. Publish and validate fresh curated trades and one-minute Coinbase candles.
3. Validate the sealed OOS evaluation and register the plan before its start.
4. Run two distinct bounded cycles, retry one command ID, and compare row counts.
5. Restart the CLI process, run another cycle, and inspect checkpoint continuity.
6. Use `set-paper-session-state` for an audited planned pause and resume.
7. Publish and independently validate one snapshot per UTC day.
8. Finalize only at the precommitted end or after a definitive safety failure.

Accelerated fixtures validate all mechanics and verdicts. They do not count as
real forward evidence. The operational milestone remains open until an actual
pilot completes its precommitted observation window.

Additional configuration:

```text
PAPER_PILOT_OUTPUT_PREFIX
PAPER_PILOT_MAXIMUM_PLAN_CANDLES
PAPER_PILOT_MAXIMUM_PLAN_FILLS
```
