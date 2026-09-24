# Medium-horizon BTC momentum study

## Status

**Not tested; no strategy is approved for paper or live trading.** The required
continuous, pinned candle history is not available in this repository. The
existing 90-day BTC publication contains 926 of 129,600 required minutes and
the previously sealed gap-aware SMA study selected no candidate. Neither result
is evidence of profitability.

## Rationale

The candidate is a long-only time-series momentum rule implemented by the
existing `sma-crossover-long-only-v1` engine. The research is deliberately
limited to liquid spot BTC-USD and uses the engine's next-open execution, exact
decimal accounting, fee, and adverse-slippage treatment.

Liu and Tsyvinski find a time-series momentum effect in cryptocurrency returns;
Liu, Tsyvinski, and Wu also identify momentum as a cross-sectional crypto risk
factor. Those are empirical motivations, not a forecast that the effect will
persist or be tradable after costs.

The present 5/20, 15/60, and 60/240-minute candidates are very short relative
to that evidence. Frequent crossovers are also especially exposed to the
repository's assumed 40 bps fee plus 5 bps one-way slippage. This study uses
only a small, pre-committed medium-horizon grid to avoid selecting a lucky
parameter set.

Sources:

- [Risks and Returns of Cryptocurrency - NBER](https://www.nber.org/papers/w24877)
- [Common Risk Factors in Cryptocurrency - NBER](https://www.nber.org/papers/w25882)
- [Optimal Trend-Following With Transaction Costs - SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4282126)

## Pre-registered candidate grid

All periods are one-minute candles. A complete history must include the maximum
warm-up before each range; no missing minute may be filled or interpolated.

| Candidate | Fast SMA | Slow SMA | Interpretation |
| --- | ---: | ---: | --- |
| `sma-720-10080` | 12 hours | 7 days | Short medium-horizon trend |
| `sma-1440-20160` | 1 day | 14 days | Medium-horizon trend |
| `sma-10080-40320` | 7 days | 28 days | Slow trend filter |

Required study design:

- At least 240 continuous calendar days of BTC-USD one-minute candles, with a
  28-day warm-up before the train boundary.
- A fixed 120/30/30-day train/validation/test split. Pin the candle manifest
  and the exact JSON experiment specification before reading test prices.
- Use at least the existing 40 bps fee and 5 bps one-way slippage assumptions;
  rerun the selected candidate with 2x and 3x costs as sensitivity tests.
- Select only from train and validation using the repository's sealed
  experiment workflow; run the test interval once, after selection.

## Promotion gates

The result remains **do not trade** unless all of the following hold:

1. Complete, gap-free coverage for every warm-up and evaluation minute.
2. Positive out-of-sample excess return versus the cost-matched buy-and-hold
   baseline, after base costs and after the 2x-cost sensitivity test.
3. Maximum drawdown at or below a pre-registered 25% limit in validation and
   test, and at least one train fill.
4. A separate paper-trading period with enough closed trades to measure actual
   fill costs. No live-order authority follows from a backtest or paper result.

If a candidate fails, preserve the sealed artifact and return to a new,
independently specified hypothesis. Do not retune the grid using its test data.

## How to implement the study when history is ready

1. Publish a new complete historical candle manifest; do not alter an existing
   sealed experiment or manifest.
2. Create a new `experiment_spec_version: v1` JSON under `experiments/` using
   the three candidate pairs above, the newly pinned manifest and digest, and
   the fixed ranges/costs above.
3. Run `prepare-strategy-experiment`, review the selection manifest, then run
   `evaluate-strategy-experiment` exactly once. The existing engine already
   implements the specified SMA rule with next-open fills and transaction costs.
4. Run `validate-strategy-experiment` / `validate-backtest` and retain the
   immutable manifests with the report.
