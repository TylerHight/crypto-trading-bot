# Gap-aware sealed strategy selection

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed stories 0003, 0004, and 0005

## User story

As a strategy owner, I want the approved fixed SMA candidates evaluated only on
real, contiguous historical-candle segments so that missing exchange minutes
cannot create hidden indicator values, simulated fills, or returns.

## Problem

The historical source and gap policy are approved, but the existing backtest
engine correctly rejects any gap. It therefore cannot evaluate the approved
90-day dataset without either inventing data or an explicit segment-aware
implementation. The existing sealed train/validation/test boundary must remain
intact.

## Scope

- Read the approved immutable policy-review record, policy, historical dataset,
  and coverage report by SHA-256.
- Partition each train, validation, and test interval into maximal contiguous
  one-minute candle segments using only exchange-published candles.
- Reset each SMA after a gap; discard any pending decision at a segment end;
  never carry a position, fill, equity observation, or return across a gap.
- Exclude a segment that cannot provide its required warmup plus at least two
  evaluation candles, and publish the reason and exact excluded range.
- Run the existing fixed candidates only on train and validation segments,
  pre-register a deterministic segment aggregation rule, and seal selection
  before reading any test-price column.
- Evaluate only the sealed winner on valid test segments and publish segment
  evidence, source gaps, exclusions, costs, and hashes.
- Show the result, limits, and next action in the read-only dashboard.

## Non-goals

- Filling missing candles, carrying a simulated position through a source gap,
  changing candidates/dates/costs after review, or optimizing against test data.
- Claiming that segmented research return is a continuously tradeable return.
- Starting a paper trial, placing an order, or using exchange credentials.
- Adding leverage, shorting, derivatives, or additional assets.

## Acceptance criteria

1. The runner rejects any policy review, report, source manifest, policy digest,
   or source-gap inventory that differs from the approved evidence.
2. Every evaluated indicator window, fill, and equity observation is wholly
   within one contiguous source segment; tests prove no result crosses a gap.
3. Segment eligibility and the aggregation formula are fixed and included in the
   sealed configuration before train/validation prices are read.
4. Selection reads only train and validation segments. Test-price access is
   blocked until an immutable selection record has been published.
5. The test evaluation uses only the sealed candidate and retains per-segment
   metrics, exclusions, source lineage, fees, and slippage.
6. The aggregate result is labeled research-only and does not support a paper
   trial by itself, regardless of its return.
7. Tests cover gaps at range boundaries and in the middle, too-short segments,
   pending-order cancellation, tampering, selection/test isolation, and
   dashboard read-only rendering.

## Validation plan

- Build deterministic fixtures with known candle gaps and compare segment
  boundaries, warmup resets, fills, and exclusions against expected outputs.
- Attempt to read test prices during selection and confirm rejection.
- Run the approved BTC-USD policy/dataset end to end and verify immutable
  publication read-back plus dashboard consistency.
- Run trading-core, dashboard, lint, and type checks.

## Rollout constraints

This is research evidence only. A completed result may reject all candidates or
remain inconclusive. It must not register or start a paper pilot; a future story
would require a separate decision about whether segmented historical evidence is
meaningful enough to justify a forward simulation.

## Delivery and validation — September 9, 2026

Implemented `run-gap-aware-research`. It verifies the explicit approved review,
policy digest, source manifest, coverage report, and source-gap inventory before
reading prices. It divides the historical candles into maximal one-minute source
segments, resets the SMA and simulated portfolio at every segment boundary, and
records exact short-segment exclusions. Train and validation selection completes
and is published before the runner can read test prices. Any selected candidate
would be evaluated only on separate test segments. Results are explicitly
research-only and can never authorize a paper trial.

The approved BTC-USD input was run end-to-end and produced immutable report
`1a446a32d9a719f7a9b49a4ebd8b885c0042f8f3f307feca471bf4fa20234e01` at
`s3a://crypto-data/analytics/strategy_experiments/v1/gap_aware_research/reports/f9eaeaff0636f86fa1a1440367637b646c5e302ecbbfcad903ead4f72e827ef3/manifest.json`.
No fixed candidate met the pre-registered train/validation rule, so the runner
correctly did not read test prices or produce an out-of-sample result.

Focused trading-core and dashboard tests passed, including gap boundaries,
short-segment exclusions, terminal pending-order cancellation, review tampering,
selection/test isolation, and read-only dashboard rendering. Ruff passed.

Story 0007 records the failed selection decision and prevents the obsolete
paper-pilot draft from being used as if this result had selected a strategy.
