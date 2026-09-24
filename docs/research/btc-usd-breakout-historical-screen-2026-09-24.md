# BTC-USD 20/10-day breakout: historical screen and decision

Decision on September 24, 2026: retire this breakout hypothesis before its
September 25 prospective test begins. The daily capture task
`CryptoResearch-BtcDailyBreakoutV1` was disabled and left installed for an
auditable, reversible change. The registered specification and 19 captured
warmup days remain intact. No prospective test performance exists.

The fixed historical screen used Coinbase daily candles from January 1, 2020
through December 31, 2025, plus 20 warmup days. Its exact specification is
[`btc-usd-breakout-historical-screen-v1.json`](../../experiments/btc-usd-breakout-historical-screen-v1.json),
SHA-256 `eaeb8e63f9080d7462cc8c38c5daaa43a07bdcb9e64b9a69739374fcfa54a10e`.
It pins the previously registered 20/10-day breakout rule without changing its
parameters or 40-basis-point fee and 5-basis-point slippage assumptions. Eight
complete source responses and the result are archived under
`artifacts/strategy_experiments/daily_breakout_historical_screen_v1/`.
Report SHA-256:
`9e3d1515fdaedb826c0148088810b5519b331b19ac9c0e1a153095440706d6fc`.

Reproduce the screen, without retuning it, using the installed
`screen-historical-breakout` command or the equivalent module invocation:

```powershell
$env:PYTHONPATH='apps/trading_core/src;packages/domain/src;packages/exchange_adapters/src'
.venv311\Scripts\python.exe -m crypto_trading_core.historical_breakout_screen `
  --study-spec experiments/btc-usd-daily-breakout-v1.json `
  --study-spec-sha256 41551c83ec1e42a5dcb4be61ad6ffc889afb171a94300245c27daa55d4e18084 `
  --screen-spec experiments/btc-usd-breakout-historical-screen-v1.json `
  --screen-spec-sha256 eaeb8e63f9080d7462cc8c38c5daaa43a07bdcb9e64b9a69739374fcfa54a10e `
  --output artifacts/strategy_experiments/daily_breakout_historical_screen_v1
```

Sealed source chunks are reused, so this command reads the saved history and
rejects changed source or result bytes instead of fetching them again.

| Cost multiple | Breakout return | Buy-and-hold return | Difference | Largest daily-close drawdown |
| --- | ---: | ---: | ---: | ---: |
| 1x | +661.29% | +1115.59% | -454.30 percentage points | 53.61% |
| 2x | +481.15% | +1110.16% | -629.02 percentage points | 57.29% |
| 3x | +343.62% | +1104.78% | -761.15 percentage points | 60.97% |

There were 30 completed breakout round trips in the continuous six-year run.
The large positive absolute return mainly reflects BTC's large rise over the
period. The strategy trailed simply holding BTC after costs and exceeded its
own predeclared 25% maximum drawdown threshold. On independent calendar-year
resets, it beat buy-and-hold in only 2021 and 2022 at 1x costs; it lost 40.22%
in 2022 despite outperforming the larger buy-and-hold loss. At 2x costs it
beat buy-and-hold only in 2022. The 2025 strategy return was -16.63% at 1x.

This is an exploratory historical screen, not a fresh out-of-sample result.
It is useful here as a quick falsification check: the rule does not merit a
six-month wait as the next sole research path. The earlier 7/28-day SMA also
failed its frozen follow-up. Neither result establishes a repeatable profit
source. Future work should start from a different economic hypothesis and
cost/validation plan, not tune these exposed trend rules against the same
years. Source responses and both rejected specifications should be retained.
