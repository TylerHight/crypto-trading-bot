# BTC-USD daily momentum research review (2026-09-23)

Decision: **do not start a paper pilot or enable live trading**. This is a
research result with one out-of-sample entry, not evidence of a repeatable edge.

## Source recovery

The Coinbase trade audit of the pinned one-minute historical publication found
36 fully reconstructable gaps covering 62 minutes. The other two gaps, both on
May 8, cover 402 minutes and returned zero trades from the public historical
trade endpoint. Its exact responses and SHA-256 hashes are saved in
`artifacts/historical_candles/gap_audits_v1/runs/008ddfa9-1b38-4ed7-80be-c9b0a4bba86d/`.
No replacement candle was published for either May gap. The original sealed
one-minute study remains blocked by incomplete coverage.

Gap audit report SHA-256:
`14621275696c547c1c3e2e049c50c067b2b02a5009655b01ca593dc9bfab1f2e`.

## Daily study

The daily study uses actual Coinbase Exchange daily OHLCV candles, which were
complete over its precommitted dates. The fixed specification is
`experiments/btc-usd-daily-momentum-v1.json` (SHA-256
`35a7e6324ea5a5bcd6364b7f461d8c7e0209162fc6d053b24ac055cf38629830`).
Training and validation prices were fetched and sealed before the test call.
The selection manifest reports `test_prices_accessed=false`. Its SHA-256 is
`8e851f9266d6c0c137dc99bb45526caec65f394d0efa27daf18607d2db51f15c`.

The 7/28-day SMA was selected because it had the highest validation return:
**0% with zero validation fills**. The other candidates had negative validation
returns after costs. Training return for 7/28 was **-6.54%** with six fills.

| Cost assumption | Strategy test return | Buy-and-hold test return | Excess |
| --- | ---: | ---: | ---: |
| 1x (40 bps fee, 5 bps slippage per side) | +4.31% | +1.09% | +3.22 pp |
| 2x | +3.85% | +0.64% | +3.21 pp |
| 3x | +3.38% | +0.19% | +3.19 pp |

The out-of-sample interval was June 21 to July 21, 2026 UTC. The strategy made
one buy and no sell. The evaluation report SHA-256 is
`d3c16f3693369380712dc7a88e5c8539b2eee473223ec4dd6600083bb55c776c`.
The report's `paper_trial_eligible=true` field reflects only its numerical
return and drawdown gate. This review declines promotion because validation
had no participation, training lost money, and the test observed no completed
round trip. The existing paper engine also processes one-minute SMA rules, not
this daily strategy.

## Frozen second interval

Before querying more prices, the selected 7/28-day rule and July 21 through
September 19 UTC interval were pinned in
`experiments/btc-usd-daily-forward-v1.json` (SHA-256
`e2e19bcc9a8927fb8a83991be43d8d1edb0d0137370d5e9d67a8ec1a18e6525c`).
The forward query verified its 27-day overlap against the first sealed test
source and found complete Coinbase daily candles. No parameter changed.

| Cost assumption | Strategy forward return | Buy-and-hold forward return | Excess |
| --- | ---: | ---: | ---: |
| 1x | -1.95% | +23.46% | -25.41 pp |
| 2x | -4.56% | +22.91% | -27.47 pp |
| 3x | -7.10% | +22.36% | -29.47 pp |

The strategy made six fills at each cost level. The forward report SHA-256 is
`355d0b70d9bd2aa28f9ff73d5e48eb845015826039e681b47fee6a7514e6b4f8`.
This failed replication reinforces the decision against a paper pilot.

## September 24 implementation correction

The v2 engine now rejects zero-fill validation and nonpositive train/validation
returns. Its research gate requires completed test trades and positive returns
after costs, and its operational paper eligibility remains false. Legacy v1
selections and cached flags cannot drive further evaluation. The original
specifications and artifacts above remain unchanged and audit-readable.

The v1 buy-and-hold calculation also added fees and slippage instead of charging
fees on the slipped execution price. V2 corrects that sizing. The numbers above
are the archived v1 outputs, not restated v2 calculations; do not treat them as
newly validated performance. The strategy's negative frozen follow-up remains
the reason to retire it. No candidate has been reranked on these exposed dates.

## Next evidence to collect

Preserve these artifacts and close this specific 7/28-day hypothesis. Any new
strategy requires a distinct precommitted specification and new untouched
forward period; do not choose a new grid using these test results. The
one-minute source remains subject to the two May gaps.
