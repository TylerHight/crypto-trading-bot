# Coinbase USD-spot daily relative strength: historical screen and decision

Decision on September 28, 2026: reject all three fixed weekly
relative-strength candidates. No candidate beat the cost-matched BTC buy-and-
hold benchmark in both train and validation, and all breached at least one
predeclared drawdown gate. No 2025 reporting-period calculation, paper trial,
or live trading is authorized.

The fixed specification is
[`usd-spot-daily-relative-strength-screen-v1.json`](../../experiments/usd-spot-daily-relative-strength-screen-v1.json),
SHA-256 `c7249436646941d85461fe112d1160619c518f9f11138903bdee45a71e92ff53`.
It downloaded and sealed complete Coinbase daily candles for BTC-USD, ETH-USD,
LTC-USD, and BCH-USD from September 10, 2019 through December 31, 2025.

Each candidate starts in cash, ranks the four assets by its fixed trailing
close-to-close return every seven days, and holds the highest-ranked asset from
the next daily open. It switches only if another asset becomes the leader.
Every purchase and sale uses 40 basis points of fee plus 5 basis points of
adverse one-way slippage. There is no leverage or short selling.

This yields a decision immediately because it uses completed historical daily
candles. It is still exploratory historical evidence, not proof that the rule
will work on future prices.

## Fixed partitions and selection rule

| Partition | Dates | Purpose |
| --- | --- | --- |
| Train | 2020-01-01 through 2022-12-31 | Initial candidate evidence |
| Validation | 2023-01-01 through 2024-12-31 | Independent selection check within this historical screen |
| Reporting | 2025-01-01 through 2025-12-31 | Reserved for a selected candidate only |

Selection required positive strategy return, positive excess return against
cost-matched BTC buy-and-hold, at least two completed train switches, at least
one completed validation switch, and drawdown no greater than 50% in both
train and validation. The qualifying candidate with the greatest validation
excess return would have been selected. None qualified.

## Base-cost result

| Candidate | Train return | Train excess vs BTC | Train max drawdown | Validation return | Validation excess vs BTC | Validation max drawdown | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Weekly leader, 14-day lookback | +37.63% | -92.03 pp | 93.14% | -21.07% | -483.23 pp | 66.99% | Rejected: underperformed and breached drawdown limits |
| Weekly leader, 56-day lookback | +105.99% | -23.66 pp | 88.94% | +24.12% | -438.05 pp | 47.30% | Rejected: underperformed BTC in both periods; train drawdown failed |
| Weekly leader, 112-day lookback | +71.89% | -57.76 pp | 87.07% | +113.73% | -348.43 pp | 63.77% | Rejected: underperformed BTC and breached drawdown limits |

The positive raw returns do not indicate a usable edge. BTC buy-and-hold gained
roughly 129.65% in train and 462.16% in validation after the same initial
cost, so each candidate lagged the simpler benchmark by a large margin.

## Reproduce

The first run saved all 32 public source responses locally and checked each
response's SHA-256 before calculating. A rerun reuses those exact responses;
it does not need another market-data request.

```powershell
$env:PYTHONPATH='apps/trading_core/src;packages/domain/src;packages/exchange_adapters/src'
.venv311\Scripts\python.exe -m crypto_trading_core.historical_relative_strength_screen `
  --spec experiments/usd-spot-daily-relative-strength-screen-v1.json `
  --spec-sha256 c7249436646941d85461fe112d1160619c518f9f11138903bdee45a71e92ff53 `
  --output artifacts/strategy_experiments/daily_relative_strength_historical_screen_v1
```

The immutable local report SHA-256 is
`2c60e6649b797c2c275c99937e9afca072f368ee47f4ea973a1396ba5d0571f3`.
Do not tune these three lookbacks, change the asset universe, or relax the
gates against this history. Any next study must use a different, separately
specified hypothesis.
