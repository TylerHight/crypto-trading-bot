# BTC-USD daily selloff-reversal: historical screen and decision

Decision on September 28, 2026: reject this selloff-reversal hypothesis. None
of the four fixed candidates passed the train and validation selection gates,
so no candidate was selected and the 2025 reporting-period calculation was not
run. No paper or live trading is authorized.

The fixed specification is
[`btc-usd-daily-selloff-reversal-screen-v1.json`](../../experiments/btc-usd-daily-selloff-reversal-screen-v1.json),
SHA-256 `918e6ae6ccd268f5c9c105a14252b4d09ebf72c7bed809bfe3f6c23abeaaeae8`.
It tests a simple idea: after BTC closes at least 5% or 10% below the prior
day's close, buy at the following daily open and hold for one or three complete
daily sessions. It charges 40 basis points per transaction plus 5 basis points
of adverse one-way slippage, with no leverage or short positions.

The source is the immutable Coinbase daily-candle publication already used by
the rejected breakout screen. Its report SHA-256 is
`9e3d1515fdaedb826c0148088810b5519b331b19ac9c0e1a153095440706d6fc`.
Because that history was already accessed, this is an exploratory historical
screen, not fresh out-of-sample evidence.

## Fixed calendar partitions and selection rule

| Partition | Dates | Use |
| --- | --- | --- |
| Train | 2020-01-01 through 2022-12-31 | Initial candidate evidence |
| Validation | 2023-01-01 through 2024-12-31 | Candidate check and selection |
| Reporting | 2025-01-01 through 2025-12-31 | Reserved for a selected candidate only |

A candidate required at least five train entries, three validation entries,
positive strategy return, positive excess return versus cost-matched
buy-and-hold, and drawdown no greater than 25% in both train and validation.
The eligible candidate with the largest validation excess return would have
been selected. None qualified.

## Base-cost result

| Candidate | Train return | Train excess vs buy-and-hold | Train entries | Validation return | Validation excess vs buy-and-hold | Validation entries | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 5% selloff, hold 1 day | -18.57% | -148.22 pp | 71 | -5.38% | -467.54 pp | 19 | Rejected: negative return/excess; 47.89% train drawdown |
| 5% selloff, hold 3 days | -21.82% | -151.47 pp | 60 | -11.23% | -473.40 pp | 17 | Rejected: negative return/excess; 66.17% train drawdown |
| 10% selloff, hold 1 day | +41.02% | -88.63 pp | 12 | 0.00% | -462.16 pp | 0 | Rejected: underperformed; no validation entry |
| 10% selloff, hold 3 days | +0.39% | -129.26 pp | 12 | 0.00% | -462.16 pp | 0 | Rejected: underperformed; no validation entry |

Buy-and-hold returned +129.65% in train and +462.16% in validation after the
same entry cost. The strategy's apparent positive return in one 10% candidate
therefore was not evidence of an edge: it substantially lagged the simple
benchmark and did not trade in validation.

## Reproduce

The saved candle chunks are reused and their hashes are checked before any
calculation. This command does not fetch market data or change the source
publication:

```powershell
$env:PYTHONPATH='apps/trading_core/src;packages/domain/src;packages/exchange_adapters/src'
.venv311\Scripts\python.exe -m crypto_trading_core.historical_mean_reversion_screen `
  --spec experiments/btc-usd-daily-selloff-reversal-screen-v1.json `
  --spec-sha256 918e6ae6ccd268f5c9c105a14252b4d09ebf72c7bed809bfe3f6c23abeaaeae8 `
  --output artifacts/strategy_experiments/daily_mean_reversion_historical_screen_v1 `
  --base .
```

The immutable local result report SHA-256 is
`54c7365e72f5e4e8af85226b6d423506c037977bc864a63dc666aa36d836e308`.
The next strategy study must start from another independently specified idea;
do not retune these thresholds or holding periods against this history.
