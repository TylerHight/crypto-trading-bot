# Pre-registered longer-horizon strategy research

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: verified raw archive, curated-trade snapshot, and one-minute
  candle snapshot

## User story

As a strategy owner,
I want to compare a small, fixed set of simple BTC-USD strategies over a
longer, separated train/validation/test period,
so that I can decide whether any strategy has enough evidence to justify a
future paper-only trial.

## Problem

The current SMA 5/20 out-of-sample result was slightly worse than buy-and-hold
after costs. The data pipeline is now healthy, but that result is too weak to
support a paper trial. The next step is better research evidence, not more
pilot automation.

## Scope

1. Create a research-input coverage report for BTC-USD one-minute candles.
   It must identify time range, missing minutes, source manifests, and their
   SHA-256 digests.
2. Require enough verified history for the experiment: at least 90 calendar
   days, with every missing minute reported. If this requirement is not met,
   publish an inconclusive coverage report and stop before strategy selection.
3. Check in one immutable experiment configuration before reading the test
   period. It must define the data lineage, fixed fee/slippage assumptions,
   train/validation/test boundaries, and a small fixed candidate list.
4. Compare only the configured simple strategies and buy-and-hold. Do not add
   or tune a candidate after test-period results are known.
5. Publish immutable selection and OOS evaluation manifests, including the
   selected candidate or an explicit "no candidate" result.
6. Show a short plain-language research summary in the local dashboard:
   selected strategy, test-period return, buy-and-hold return, excess return,
   and whether the evidence supports a future paper-only trial.

## Non-goals

- Starting, registering, or approving a paper trial.
- Live exchange access, credentials, or order placement.
- Claiming that a backtest proves future profitability.
- Rewriting the existing sealed SMA 5/20 evaluation.

## Acceptance criteria

1. The coverage report is immutable, digest-pinned, and reports all missing
   candle minutes before an experiment can select a strategy.
2. The experiment refuses to run selection when the 90-day coverage rule is
   not met, and returns a clear inconclusive result instead.
3. Selection is run without test-period access and records that fact in its
   manifest.
4. OOS evaluation uses the exact selected candidate, input manifest digests,
   and precommitted cost assumptions.
5. The report compares net strategy return with net buy-and-hold return and
   records excess return after fees and slippage.
6. A paper-trial recommendation is possible only when the sealed OOS result
   is positive versus buy-and-hold after costs; a negative or insufficient
   result produces "do not start a paper trial."
7. Re-running the same configuration and input manifests resolves the same
   immutable publication or rejects conflicting output.
8. The dashboard remains read-only and presents the result without requiring
   an operator to inspect raw JSON.

## Validation plan

- Unit-test coverage calculations, missing-minute reporting, split boundaries,
  fee/slippage handling, and test-period access blocking.
- Use a deterministic fixture to prove selection cannot change after an OOS
  result exists.
- Run an end-to-end local experiment using SHA-pinned candle data and validate
  the published manifests by read-back.
- Confirm the dashboard shows the same result as the immutable evaluation
  manifest.
- Run the relevant Spark, trading-core, dashboard, lint, and type checks.

## Operational constraint

The current 1,862-candle publication is valid for pipeline verification but is
not enough history for this story's 90-day research requirement. The story may
finish with an inconclusive coverage result if sufficient verified historical
data cannot be prepared. That is a useful and safe outcome.

## Delivery and validation — September 8, 2026

Delivered the coverage-gated runner in
[`longer_research.py`](../../../apps/trading_core/src/crypto_trading_core/longer_research.py)
and the fixed
[`BTC research configuration`](../../../experiments/btc-usd-longer-research-v1.json).
The existing selection/backtest engine is reused; the original short evaluation
has not been rewritten. Usage is in the
[trading-core runbook](../../../apps/trading_core/README.md#longer-strategy-research).

The configuration fixes June 10 through September 8 UTC, with 60/15/15-day
train/validation/test splits, SMA 5/20, 15/60, and 60/240, and 40/5-basis-point
fee/slippage assumptions. An append-only name registration locks its digest
before candle inspection. Coverage hashes all input files and reads timestamp
and lineage columns only. Selection's price loader rejects test-period requests;
evaluation reads the same frozen file bytes after selection is sealed.

The real run finished **inconclusive**, as allowed by this story:

- 924 of 129,600 required BTC minutes are present in the fixed window.
- All 128,676 missing minutes are recorded in 19 half-open UTC ranges; missing
  warmup is reported separately. The source has 926 BTC candles overall.
- 667 candle Parquet files and both candle/curated source manifests are digest-pinned.
- No strategy was selected, no OOS price evaluation ran, and no paper trial was
  registered, approved, or started. Recommendation: **do not start a paper trial**.
- The dashboard top card shows **Not enough data** and the required next action.

Publication identities:

```text
Research key: aae96856d8da3ad46f5769ce783167357405e3f5126fc00f848e02e2a08a17a2
Spec SHA-256: 75f3a9bad2eae1e701f87a7284dbfed29b7273faacfe2bb5e2facb199b8360de
Coverage SHA-256: 22d439244723d8275a982142e837bbac5168c20f5c169e93aab945f5eff11386
Report SHA-256: 594911fe68d0b0df132c59f1a2541a740c2d791f3dd761579cb6b2244d83c5b7
Report prefix: s3a://crypto-data/analytics/strategy_experiments/v1/longer_research/reports/
Report suffix: <research-key>/manifest.json
```

Validation recorded under Python 3.11:

- Trading-core and dashboard regression suite: **74 passed** before the final
  additional immutability cases; the final research suite separately passed all
  **12 tests** after those additions.
- Two complete 90-day local Parquet fixtures exercised selected-candidate/OOS
  and no-candidate outcomes, exact cost comparison, manifest read-back, repeated
  publication resolution, and rejection of changed candidates after sealing.
- Coverage tests verified missing ranges, duplicates, warmup gaps, insufficient
  history stopping selection, changed file bytes, changed configurations, and
  altered saved recommendations. A direct attempted test-price query during
  selection was rejected. Positive, zero, and negative excess returns were tested.
- Dashboard tests cover inconclusive, evaluated, invalid, and corrupt research,
  positive/zero/negative recommendations, summary visibility, and report matching.
- Real PostgreSQL/MinIO dashboard read-only integration: **passed**. Its fixture
  uses an isolated research prefix and cleans up its own objects.
- Spark one-minute candle suite: **18 passed**.
- Ruff across apps/jobs/packages/tests and mypy across trading core/dashboard:
  **passed**. `git diff --check` passed.
- Repeated the real MinIO run: identical report digest. Read back coverage and
  verified its digest and missing-minute total. Both `GET /api/status` and the
  rendered page at `http://127.0.0.1:8090` matched the report and showed no pilot.

The next research step needs a complete historical candle publication and a new
reviewed, fixed experiment configuration. Synthetic fixtures validate the software;
they are not evidence for starting a real paper trial.
