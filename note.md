# User story: Seal and evaluate a strategy out of sample

## Story

As a strategy researcher,
I want a reproducible experiment that selects one fixed SMA configuration using
only train and validation data and then evaluates it once on an unseen test
interval against buy-and-hold,
so that I can judge whether the strategy deserves paper-trading work without
look-ahead, silent parameter changes, or selective reporting.

## Why this is next

The project can now turn a pinned candle snapshot into a deterministic simulated
trading result:

```text
published candle manifest
    -> versioned SMA decisions
    -> next-open simulated fills with explicit costs
    -> immutable equity and performance artifacts
```

One successful backtest is not evidence of a useful strategy. The next risk is
research leakage: trying parameters while looking at the final test period and
then presenting the best result as if it were unseen.

Address that before paper trading. This story consumes the existing backtest
engine rather than changing its timing, cost, or portfolio semantics. It adds a
small, auditable experiment workflow with a hard boundary between parameter
selection and out-of-sample evaluation.

## Outcome

Given one SHA-pinned `market_candles.v1` snapshot and a finite, checked-in set of
SMA candidates, support two separate commands:

1. **Prepare and seal**: evaluate every candidate on train and validation
   intervals, choose one candidate by a deterministic documented policy, and
   publish an immutable sealed-selection manifest without reading test candles.
2. **Evaluate**: accept only the sealed-selection manifest pinned by digest,
   evaluate exactly its selected candidate and a buy-and-hold baseline on the
   test interval, and publish an immutable comparison report.

Both stages must remain simulation-only. They must not publish trading signals,
place orders, use private credentials, or contact an exchange.

## User-visible commands

Preparation should look similar to:

```powershell
prepare-strategy-experiment `
  --spec .\experiments\btc-usd-sma-v1.json `
  --spec-sha256 <64-lowercase-hex-digest> `
  --output s3a://crypto-data/analytics/strategy_experiments/v1
```

Out-of-sample evaluation should be a separate invocation:

```powershell
evaluate-strategy-experiment `
  --selection-manifest s3a://crypto-data/analytics/strategy_experiments/v1/selections/<selection-key>/manifest.json `
  --selection-manifest-sha256 <64-lowercase-hex-digest> `
  --output s3a://crypto-data/analytics/strategy_experiments/v1
```

Provide equivalent module entry points for development and testing. Local paths
must require explicit local-development mode, consistent with the backtest CLI.

## Experiment specification v1

The preparation command accepts one UTF-8 JSON document pinned by the SHA-256 of
its exact bytes. Check in a JSON Schema and a documented example.

The specification must contain at least:

```json
{
  "experiment_spec_version": "v1",
  "name": "btc-usd-sma-v1",
  "candle_manifest_uri": "s3a://crypto-data/analytics/market_candles/v1/manifests/<snapshot-key>/manifest.json",
  "candle_manifest_sha256": "<64-lowercase-hex>",
  "exchange": "coinbase",
  "symbol": "BTC-USD",
  "starting_cash": "10000.000000000000000000",
  "fee_bps": "40",
  "slippage_bps": "5",
  "ranges": {
    "train": {"start": "2025-01-01T00:00:00Z", "end": "2025-07-01T00:00:00Z"},
    "validation": {"start": "2025-07-01T00:00:00Z", "end": "2025-10-01T00:00:00Z"},
    "test": {"start": "2025-10-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"}
  },
  "candidates": [
    {"candidate_id": "sma-5-20", "fast_period": 5, "slow_period": 20},
    {"candidate_id": "sma-10-50", "fast_period": 10, "slow_period": 50}
  ],
  "selection_policy": {
    "minimum_train_fills": 2,
    "maximum_train_drawdown": "0.500000000000000000",
    "maximum_validation_drawdown": "0.500000000000000000"
  }
}
```

Reject unknown top-level and nested fields so misspelled settings cannot be
silently ignored.

## Specification validation

- Verify the raw specification digest before parsing.
- Require `experiment_spec_version = "v1"`.
- Require a nonempty stable experiment name.
- Apply the existing exchange, symbol, UTC timestamp, decimal, basis-point, and
  candle-manifest safety rules.
- Require at least two and at most 50 candidates.
- Candidate IDs must be unique and match a conservative lowercase identifier
  pattern.
- Candidate `(fast_period, slow_period)` pairs must be unique.
- Every candidate must satisfy `0 < fast_period < slow_period`.
- All selection thresholds must be explicit, finite, and within their documented
  bounds.
- The complete requested candle count, including independent warm-up for each
  range and the buy-and-hold observations, must fit the configured resource cap.

A specification digest, candle digest, candidate, cost, range, policy, or engine
version change must produce a new selection identity.

## Time ranges and warm-up

Every range is half-open: `[start, end)`.

- All boundaries must be aligned UTC minutes.
- Require `train.start < train.end <= validation.start < validation.end <=
  test.start < test.end`.
- Gaps between ranges are allowed and must be reported.
- Overlap is forbidden.
- Each range must contain at least two evaluation candles.
- Each SMA run loads its own preceding `slow_period - 1` candles from the same
  pinned snapshot for warm-up.
- Warm-up candles may initialize indicators but never contribute decisions,
  fills, returns, or observations before that range's start.
- Continue to reject missing one-minute candles inside a candidate's required
  warm-up or evaluation sequence.

Train and validation runs are independent simulations that each begin with the
same starting cash and zero base holdings. Portfolio state must not flow from
one range to another.

## Hard test-data embargo

The preparation stage must not read, query, deserialize, summarize, or validate
candle rows whose `window_start` is at or after `ranges.test.start`.

It may preserve the test start and end strings from the pinned experiment
specification and may validate the source manifest itself. It must not inspect
test prices, counts by test partition, missing-minute status, or test-period
performance.

Make this separation visible in the API: the preparation data reader receives
only train and validation ranges. Do not load the full snapshot and filter test
rows later in Python.

The sealed-selection manifest must state:

```text
test_data_accessed = false
```

The integration test must use an instrumented reader or forbidden test fixture
that fails immediately if preparation attempts to access the test interval.

## Candidate backtests

Reuse the implemented `sma-crossover-long-only-v1` strategy and backtest engine
without copying their calculations.

For every candidate, run:

- One train backtest.
- One validation backtest.

Use the exact starting cash, fee, slippage, next-open execution, quantity
rounding, terminal-decision, and mark-to-market behavior from the existing
engine. Persist each underlying backtest key and manifest digest in the
experiment artifacts.

Do not modify SMA behavior or cost assumptions for an individual range or
candidate.

## Buy-and-hold baseline v1

Add an infrastructure-neutral, versioned baseline named
`buy-and-hold-long-only-v1`.

For each reported range:

1. Start with the same cash and zero base quantity.
2. Buy the maximum affordable base quantity at the first evaluation candle's
   open using the same buy slippage, fee, scale, and round-down rules as the SMA
   simulated broker.
3. Make no further trades.
4. Mark the position to every candle close.
5. Do not force-liquidate at the end.

The baseline produces a fill, equity curve, drawdown, total fee, ending equity,
and percentage return using the same result types where practical. Baseline
warm-up is unnecessary, but it must use the exact same evaluation candles and
range boundaries as the candidate comparison.

Use shared portfolio and broker functions; do not implement slightly different
fee or slippage arithmetic in the experiment application.

## Deterministic selection policy v1

Selection uses train and validation results only.

First mark a candidate eligible when all of the following hold:

- Train fill count is at least `minimum_train_fills`.
- Train maximum drawdown is no greater than `maximum_train_drawdown`.
- Validation maximum drawdown is no greater than
  `maximum_validation_drawdown`.

Rank eligible candidates by this exact total ordering:

1. Highest validation percentage return.
2. Lowest validation maximum drawdown.
3. Lowest validation total fees.
4. Lexicographically smallest `candidate_id`.

Decimal comparisons must use exact stored values, not floats or formatted
strings.

Report train and validation excess return relative to buy-and-hold, but do not
use the baseline or test result as an undocumented tie-breaker.

If no candidate is eligible, publish a valid sealed result with
`selection_status = "no_candidate_selected"`. The evaluation command must then
refuse to open the test interval. This is a legitimate research outcome, not an
infrastructure failure.

## Sealed-selection publication

Calculate a deterministic selection key from canonical JSON containing at
least:

```text
experiment specification SHA-256
candle snapshot key and manifest SHA-256
all train and validation ranges
all candidates and selection thresholds
starting cash and costs
strategy, baseline, backtest-engine, experiment-engine, and result-schema versions
```

Runtime timestamps, host names, temporary paths, and run IDs must not affect the
key.

Publish immutable artifacts such as:

```text
analytics/strategy_experiments/v1/runs/<run-id>/selection/
    candidate_results.parquet
    baseline_results.parquet
    selection_summary.json

analytics/strategy_experiments/v1/selections/<selection-key>/manifest.json
```

Publish the manifest last with create-if-absent semantics. It must include:

- The specification URI and digest.
- Candle manifest URI, digest, snapshot key, and selected output URI.
- Exact costs, policy, candidates, and train/validation ranges.
- Test range boundaries copied from the specification.
- `test_data_accessed = false`.
- Eligibility outcome and reason for every candidate.
- The selected candidate and deterministic rank evidence, or the explicit
  no-selection outcome.
- Underlying backtest and baseline identities.
- Artifact schemas, row counts, byte counts, and SHA-256 digests.
- All component versions.

An identical preparation must resolve the existing selection. A conflict for the
same key must fail closed.

## Out-of-sample evaluation

The evaluation command must:

1. Pin and validate the sealed-selection manifest by exact SHA-256.
2. Recalculate its selection key and verify all referenced artifact hashes.
3. Require `selection_status = "selected"` and
   `test_data_accessed = false`.
4. Load the original experiment specification and candle manifest by their
   recorded digests.
5. Verify the test boundaries and selected parameters exactly match the sealed
   selection.
6. Run only the selected SMA candidate on the test interval.
7. Run `buy-and-hold-long-only-v1` on that same test interval.
8. Publish the full candidate and baseline artifacts plus a comparison report.

The command has no parameter override flags. Changing a candidate, cost, range,
or source requires a new preparation.

Calculate an evaluation key from the sealed-selection key and digest plus all
component versions. The OOS result is immutable and idempotent:

```text
analytics/strategy_experiments/v1/runs/<run-id>/evaluation/
    strategy_decisions.parquet
    strategy_fills.parquet
    strategy_equity_curve.parquet
    baseline_fills.parquet
    baseline_equity_curve.parquet
    comparison.json

analytics/strategy_experiments/v1/evaluations/<evaluation-key>/manifest.json
```

## Comparison report

For train, validation, and test, report the SMA candidate and buy-and-hold values
side by side:

- Starting and ending equity.
- Absolute and percentage return.
- Maximum drawdown.
- Fill count and total fees.
- Gross traded notional.
- Percentage of candles spent long.
- Excess absolute and percentage return versus buy-and-hold.
- Effective range and candle count.

The OOS report must clearly label:

```text
selection_basis = train_and_validation_only
evaluation_range = out_of_sample
execution_mode = simulation
```

Always publish losing, underperforming, and high-drawdown results. Do not suppress
negative metrics, replace the selected candidate after evaluation, or rewrite an
existing report.

Do not automatically declare the strategy profitable, statistically
significant, or approved for live trading. The report is evidence for human
review.

## Versioning and schemas

Define constants for at least:

```text
experiment_spec_version = v1
experiment_engine_version = strategy-experiment-engine-v1
selection_policy_version = validation-return-selection-v1
baseline_version = buy-and-hold-long-only-v1
experiment_result_schema_version = v1
```

Check in language-neutral schemas for:

- Experiment specification.
- Candidate range result.
- Baseline range result.
- Sealed-selection manifest.
- OOS comparison result.

Breaking changes require a new version and new deterministic identities. A code
change that can alter any calculation must change the applicable component
version before publication.

## Validation commands

Provide standalone validators for both publication stages.

The selection validator must verify:

- Specification, candle, and artifact digests.
- Candidate completeness and uniqueness.
- Train/validation range and version consistency.
- Underlying backtest and baseline reproducibility.
- Eligibility calculations and exact deterministic ranking.
- Selected parameters match the winning row.
- No test-derived artifact or metric is present.
- Selection identity matches its manifest key.

The evaluation validator must additionally verify:

- The sealed-selection digest and identity.
- Test range and selected parameters cannot be overridden.
- Strategy and baseline results reproduce from pinned candles.
- Detailed artifacts reconcile to every summary metric.
- Excess-return calculations are exact.
- Evaluation identity matches its manifest key.

Validators must work with local files and configured S3-compatible storage.

## Bounded execution and reporting

- Cap candidate count at 50.
- Cap candles per range and total candidate-candle evaluations.
- Query only one exchange, symbol, interval, and requested range at a time.
- Push event-date and timestamp predicates into DuckDB.
- Keep deterministic ordering by `window_start`.
- Reports may contain aggregate performance values but must never contain S3
  secrets or private credentials.
- Log all losing candidates; sample lists must remain bounded.
- A failed stage may leave an unreferenced run directory but must never publish a
  success manifest.

## Tests

### Unit tests

Cover at least:

- Specification digest, unknown-field, decimal, range, and candidate validation.
- Duplicate IDs and duplicate period pairs.
- Candidate-count and resource caps.
- Independent range warm-up and starting portfolios.
- Buy-and-hold uses the first evaluation open and exact shared cost math.
- Baseline and SMA use identical mark-to-market and drawdown calculations.
- Every eligibility gate.
- Every selection tie-break in its documented order.
- No-candidate-selected behavior.
- Canonical identity stability and version sensitivity.
- Changed candidate order does not change selection or identity after canonical
  candidate sorting.
- Negative and baseline-underperforming results remain in reports.

### Leakage tests

Prove preparation cannot observe the test period:

- Use a reader double that raises if asked for a timestamp at or after
  `test.start`.
- Change every test-period price while leaving the specification, train, and
  validation data unchanged; the sealed selection and its artifacts must remain
  identical.
- Change a validation price; the selection key or selected evidence must change.
- Verify the evaluation command has no candidate or cost override arguments.

### Integration test

Using an isolated MinIO prefix:

1. Publish a pinned candle fixture containing train, validation, and test ranges.
2. Include candidates that exercise eligibility and deterministic tie-breaking.
3. Run the real preparation command.
4. Validate the sealed selection and prove it contains no test metrics.
5. Run the real evaluation command from the pinned selection digest.
6. Assert exact selected-candidate and buy-and-hold test artifacts.
7. Run the evaluation validator.
8. Repeat both commands and require idempotent existing publications.
9. Tamper with the selection, an underlying result, and the comparison artifact;
   each must fail closed.
10. Verify only MinIO was needed and no order or exchange component was contacted.
11. Clean only the isolated prefix.

## Documentation

Document:

- How to author, hash, and review an experiment specification.
- Why preparation and evaluation are separate commands.
- Exact split, warm-up, eligibility, ranking, and tie-break semantics.
- Buy-and-hold timing and cost formulas.
- How to pin and validate both manifests.
- How to inspect every losing candidate and OOS result.
- Why this workflow reduces leakage but does not establish statistical
  significance or future profitability.

## Exit behavior

- `0`: successful publication or valid idempotent resolution.
- `2`: valid preparation completed with no eligible candidate; no evaluation is
  permitted.
- `4`: invalid input, digest mismatch, leakage guard, validation failure, or
  conflicting immutable publication.
- Other nonzero values: unexpected infrastructure or execution failure.

## Out of scope

- Generating or mutating candidate parameters automatically
- Grid, random, Bayesian, genetic, or machine-learning optimization
- Selecting parameters using test-period results
- Walk-forward optimization or nested cross-validation
- Statistical significance, confidence intervals, Monte Carlo analysis, or
  claims of profitability
- Strategies other than the existing SMA candidate and buy-and-hold baseline
- Multiple symbols, cross-asset allocation, shorts, or leverage
- New fill models, spread estimation, latency, rejection, or partial fills
- Live candle consumption, paper trading, order commands, or exchange access
- PostgreSQL state, kill switches, and restart recovery
- Dashboards, dbt marts, Airflow scheduling, and cloud infrastructure

## Follow-up story

Build a restart-safe live paper-trading walking skeleton around the same strategy
and portfolio interfaces. Consume closed candle events, persist decisions and a
single-account ledger in PostgreSQL, apply stale-data and exposure limits, and
support an operator kill switch. Use a paper execution adapter only; private
exchange order placement remains a later, separately gated story.

## Definition of done

The story is complete when a reviewed experiment specification can be prepared
without any test-candle access; the chosen configuration is sealed by immutable
identity and cannot be changed during evaluation; the selected SMA strategy and
the exact-cost buy-and-hold baseline are reproducibly evaluated on the unseen
test range; every candidate, loss, drawdown, and comparison remains visible;
tampering or leakage fails closed; identical invocations resolve the same
publications; and neither stage has any exchange-order capability.
