# Backtest results v1

These schemas define the immutable decision, simulated-fill, and equity-curve
Parquet rows produced by `sma-crossover-long-only-v1`. Timestamps are UTC event
times. Every financial value is stored as `decimal(38,18)` and serialized as an
exact scale-18 string when represented in JSON.

A decision observes one closed candle. Its fill, when present, uses the next
one-minute candle's open. `drawdown` is the nonnegative fraction below the
running equity peak. Empty decision and fill artifacts retain the checked-in
Parquet schema.
