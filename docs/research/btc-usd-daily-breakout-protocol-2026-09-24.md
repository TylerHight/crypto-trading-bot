# Prospective BTC daily breakout study

Status: **retired on September 24, 2026, before the prospective test began**.
The [fixed historical screen and decision](btc-usd-breakout-historical-screen-2026-09-24.md)
explain why the scheduled capture was disabled. The specification and the
protocol below remain as registered evidence, not an active trading plan.

## Decision and rationale

Retire the 7/28-day SMA experiment reviewed on September 23. Its positive first
test did not replicate in the frozen follow-up. Preserve its original specs,
responses, results, and review; do not rerank those candidates on exposed dates.

The next single hypothesis is a long-only daily price-channel breakout. The
economic conjecture is that persistent moves may repay entry and exit costs.
[Moskowitz, Ooi, and Pedersen's time-series momentum research](https://www.aqr.com/Insights/Research/Journal-Article/Time-Series-Momentum)
motivates examining trends, but studies diversified futures and forwards, not
this BTC rule. The 20/10-day windows below are engineering choices, not a claim
established by that paper. This remains the same broad trend-following family
as the rejected work and is not independent evidence that momentum works.

## Fixed protocol

The exact-byte input is
[`btc-usd-daily-breakout-v1.json`](../../experiments/btc-usd-daily-breakout-v1.json),
SHA-256 `41551c83ec1e42a5dcb4be61ad6ffc889afb171a94300245c27daa55d4e18084`.
Its Git attribute prevents automatic line-ending conversion from changing that
digest. Register it before September 25, 2026 00:00 UTC, before fetching data.

- Asset/source: BTC-USD, public Coinbase Exchange daily OHLCV, UTC boundaries.
- Warmup: September 5 through September 24, 2026 (20 days).
- Observation: September 25, 2026 through March 23, 2027, inclusive (180 days).
- Start with 10,000 simulated USD and no position. After a daily close, enter
  if that close is strictly above the highest high of the preceding 20 days.
  Exit if it is strictly below the lowest low of the preceding 10 days.
  Both channels exclude the candle that generates the signal.
- Fill a decision at the next daily open, with adverse slippage and fees.
  The final signal remains unfilled without another observation candle.
  Mark any final position at the last close; do not force a liquidation.
- Charge 40 basis points of fees and 5 basis points of adverse slippage per
  fill; also report 2x and 3x these costs. These are fixed research assumptions,
  not a quote for an account's current exchange fee tier.
- Compare to buy-and-hold with identical entry costs and to zero-return cash.
- The research gate requires positive return and strictly positive excess over
  buy-and-hold, maximum daily-close drawdown at most 25%, and at least five
  completed round trips at both 1x and 2x costs. Report 3x as sensitivity.
  Trade count is an evidence floor, not proof of statistical significance.

There is one candidate, no optimization, and no train/validation selection.
All future observations belong to one test. Synthetic fixtures verify the
implementation; previously exposed prices cannot establish its performance.
The runner withholds a performance report until the full period has closed
and its one-hour settlement delay has passed. Earliest evaluation is
March 24, 2027 01:00 UTC. A failed or underactive test does not authorize changing
the thresholds, extending the end date, or reusing the observations as a new
holdout. Any new attempt must disclose this attempt and fix a fresh protocol.

## Capture and integrity

[Coinbase's candle API](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)
supports daily candles and up to 300 points per request, and warns that history
may be incomplete. This 200-day warmup-plus-observation window fits that bound.
Capture only days closed for at least one hour. Reject missing or malformed
days; never fabricate candles. Archive exact response bytes, SHA-256 hashes,
and immutable capture manifests. Preserve the first sealed observation of
each day so later vendor revisions cannot silently rewrite the experiment.

Use the [capture runbook](../runbooks/daily-breakout-study.md). Its commands use
only the public market-data API. Capture does not calculate interim strategy
returns. The source remains exchange-published historical bars retrieved after
close, not independently reconstructed tick data. A local hash and timestamp
provide auditability, not an independent public timestamp or a data backup.

All outcomes remain research-only. `paper_trial_eligible` is always false:
the operational paper engine does not implement this daily rule. Passing the
research gate would justify review and a separately tested execution adapter,
not deployment or a claim of repeatable profits.

The collector waits an hour after close to seal bars. A next-open fill is a
backtest assumption, not an order achievable by this delayed collector. Before
any execution study, assess signal availability, spread, market impact, and
latency against a live feed. Drawdown here is sampled at daily closes and can
understate intraday losses.
