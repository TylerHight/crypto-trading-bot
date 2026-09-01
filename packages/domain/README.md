# Domain Package

The domain package contains infrastructure-neutral trading concepts and rules.

Expected contents include money and quantity value objects, signals, order intents, order-state transitions, portfolio calculations, risk limits, strategy and clock interfaces, execution-mode abstractions, and reconciliation invariants.

This package must remain deterministic and easy to unit test. It must not import Kafka, Spark, Airflow, AWS SDKs, database drivers, HTTP frameworks, or exchange SDKs. Persistence and transport layers translate to and from domain types at their boundaries.

`MarketTradeRawEvent`, `market_trade_event()`, and `event_id_for_trade()` live
here so live collection and historical backfill cannot drift. The producer
field records provenance, but is deliberately excluded from deterministic
trade identity.

`backtest.py` supplies the first reusable trading vertical: typed candles,
target-position decisions, a versioned long-only SMA strategy, exact simulated
fills, portfolio state, and a no-look-ahead event-time runner. It contains no
storage, query-engine, transport, or exchange imports.

The same module also provides `buy-and-hold-long-only-v1`. It purchases at the
first evaluation open with the shared simulated broker, applies identical costs
and rounding, and marks the position to each close without forced liquidation.

The module also exposes an incremental backtest state and one-candle advance
operation. Paper mode persists that state between bounded invocations, keeping
its SMA target, pending next-open fill, portfolio, and drawdown semantics
identical to a single deterministic backtest run.
