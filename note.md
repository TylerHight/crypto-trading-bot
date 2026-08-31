# User story: Run a reproducible candle-driven strategy backtest

## Story

As a strategy researcher,
I want to run one deterministic long-only strategy against a pinned one-minute
candle snapshot,
so that I can evaluate trading behavior and realistic costs without look-ahead,
mutable data, or any possibility of placing a real order.

## Why this is next

The project now has a complete, reproducible market-data path:

```text
Coinbase live collection / reviewed historical backfill
    -> immutable raw Kafka archive
    -> validated and deduplicated curated trades
    -> deterministic one-minute candle snapshots
```

The next increment should consume that data instead of adding another assurance
layer. A bounded backtest is the smallest safe step into the trading plane: it
proves strategy timing, portfolio accounting, cost modeling, and reproducibility
without requiring PostgreSQL, live candle streaming, private exchange access, or
an execution gateway.

Build one complete vertical slice around a deliberately simple moving-average
strategy. Keep the strategy and portfolio rules infrastructure-neutral so later
paper and live modes can reuse them.

## Outcome

Given one published `market_candles.v1` manifest pinned by its SHA-256 digest,
run a long-only moving-average crossover strategy for one exchange, one symbol,
and one bounded UTC interval. Produce immutable backtest artifacts containing:

- The exact input and configuration used.
- Every target-position decision and simulated fill.
- A candle-by-candle equity curve.
- A deterministic performance summary.
- Enough version and lineage metadata to reproduce the run.

The backtest must never import or call an exchange client, publish an order
command, or use private credentials.

## User-visible command

Provide a command similar to:

```powershell
python -m crypto_trading_core.backtest `
  --candle-manifest .\candle-manifest.json `
  --candle-manifest-sha256 <64-lowercase-hex-digest> `
  --exchange coinbase `
  --symbol BTC-USD `
  --start 2026-01-01T00:00:00Z `
  --end 2026-02-01T00:00:00Z `
  --starting-cash 10000.00 `
  --fast-period 5 `
  --slow-period 20 `
  --fee-bps 40 `
  --slippage-bps 5 `
  --output .\artifacts\backtests
```

The exact module layout may follow the existing workspace conventions, but the
command and domain engine must be usable independently of Spark and Kafka.

## Pinned input contract

Require both the manifest location and the expected SHA-256 digest. Validate the
raw manifest bytes before parsing them.

Accept only a manifest that declares:

- `status = "published"`
- `candle_schema_version = "v1"`
- `interval = "1m"`
- A valid 64-character candle snapshot key.
- An immutable candle output URI under a run-scoped path.
- A nonnegative candle count.

Read only the manifest-selected Parquet files. Do not discover input by listing
an analytics root or by choosing a latest directory.

Local paths are allowed explicitly for development and tests. Remote paths must
use the configured S3-compatible endpoint and allowed prefix. Reject a source
and result location that overlap.

## Requested market range

- `start` is inclusive and `end` is exclusive.
- Both values must be UTC and aligned to a one-minute boundary.
- Require `start < end`.
- Filter to exactly one exchange and symbol before loading rows into Python.
- Sort by `window_start`; file order is never meaningful.
- Reject duplicate candle keys, invalid OHLCV rows, or non-increasing time.
- Reject gaps inside the warm-up or evaluation sequence for this first strategy.
  The v1 strategy operates on consecutive one-minute observations and must not
  silently treat missing minutes as a normal sampling interval.

The engine may load the preceding `slow_period - 1` candles from the same pinned
snapshot for indicator warm-up. Those candles may initialize indicators but may
not create decisions, fills, equity observations, or performance returns before
`start`.

Fail clearly when the selected snapshot does not contain enough contiguous
history for warm-up and at least two evaluation candles.

## Strategy v1: long-only SMA crossover

Implement one versioned strategy named `sma-crossover-long-only-v1`.

Parameters:

- `fast_period`: positive integer.
- `slow_period`: positive integer greater than `fast_period`.

At the close of each evaluation candle:

1. Update both simple moving averages using candle closes through that candle.
2. Target `LONG` when `fast_sma > slow_sma`.
3. Target `FLAT` when `fast_sma <= slow_sma`.
4. Emit a decision only when the target position changes.

The strategy starts flat. It never shorts, pyramids, or emits a target larger
than the available portfolio.

Strategy code receives immutable candle observations and returns target-position
decisions. It must not know about DuckDB, Parquet, S3, manifests, environment
variables, wall-clock time, or output files.

## No-look-ahead execution semantics

A decision made from the candle ending at time `T` cannot fill inside that
candle. Queue it for the open of the next candle.

For example:

```text
14:19 candle closes at 14:20 -> strategy computes a LONG target
14:20 candle opens           -> simulator fills the buy at the 14:20 open
14:20 candle closes at 14:21 -> strategy may compute the next target
```

Apply a queued fill before showing the strategy the next candle's close. The
final candle may create a decision, but if no later candle exists that decision
is recorded as unfilled and must not change cash, holdings, fees, or return.

Add a regression fixture in which the last candle contains an extreme price
move. The fixture must prove that the strategy cannot buy or sell at a price
that was only knowable at that same candle's close.

## Portfolio and simulated broker v1

Use a single quote-currency cash balance and a single base-asset position.

- Start with the requested positive cash balance and zero base quantity.
- A transition from `FLAT` to `LONG` buys the maximum affordable quantity.
- A transition from `LONG` to `FLAT` sells the entire base position.
- No borrowing, leverage, shorting, partial fills, or rejected fills.
- Do not force-liquidate an open position at the end. Mark it to the final close
  and report both cash and base quantity.

For a buy at the next candle open:

```text
execution_price = open_price * (1 + slippage_bps / 10_000)
fee_rate        = fee_bps / 10_000
base_quantity   = floor_to_scale_18(cash / (execution_price * (1 + fee_rate)))
gross_notional  = base_quantity * execution_price
fee             = gross_notional * fee_rate
cash_after      = cash - gross_notional - fee
```

For a sell:

```text
execution_price = open_price * (1 - slippage_bps / 10_000)
gross_notional  = base_quantity * execution_price
fee             = gross_notional * fee_rate
cash_after      = cash + gross_notional - fee
base_after      = 0
```

Require nonnegative fee and slippage basis points and reject values at or above
10,000. Both values must be explicit command inputs; do not imply that zero-cost
results are realistic.

## Decimal rules

- Use `Decimal` for cash, quantities, prices, fees, and equity.
- Never convert financial values through binary floating point.
- Use sufficient intermediate precision for multiply and divide operations.
- Store published monetary and quantity values at scale 18.
- Round affordable buy quantity down so a fill cannot create negative cash.
- Use round-half-even for other scale-18 publication values.
- Reject overflow instead of rounding to an invalid or infinite value.

After every fill, require cash and base quantity to be nonnegative. At every
candle close:

```text
equity = cash + base_quantity * close
```

## Domain boundaries

Add infrastructure-neutral backtesting concepts to `packages/domain`, including
typed equivalents of:

- `Candle`
- `TargetPosition` (`FLAT` or `LONG`)
- `StrategyDecision`
- `SimulatedFill`
- `PortfolioState`
- `Strategy` protocol
- `Clock` or explicit event-time progression

Keep the moving-average strategy, portfolio state transitions, cost math, and
backtest loop deterministic and unit-testable without DuckDB or object storage.

The trading-core application layer owns:

- CLI configuration.
- Manifest verification.
- DuckDB candle reads with partition and predicate pushdown.
- Conversion between rows and domain values.
- Artifact publication and operator reporting.

The domain package must not import Spark, DuckDB, boto3, Kafka, database drivers,
HTTP clients, or exchange adapters.

## Reproducible run identity

Calculate a deterministic backtest key from canonical JSON containing at least:

```text
candle snapshot key
candle manifest SHA-256
exchange and symbol
start and end
starting cash
fast and slow periods
fee and slippage basis points
strategy version
backtest engine version
result schema version
```

Runtime values such as run ID, host name, temporary paths, and `created_at` must
not affect the key.

Running the same versioned inputs twice must produce the same decisions, fills,
equity values, summary metrics, and backtest key. A changed input digest,
parameter, strategy version, or engine version must produce a different key.

## Immutable result artifacts

Write results under a run-scoped location such as:

```text
analytics/backtests/v1/runs/<run-id>/
    decisions.parquet
    fills.parquet
    equity_curve.parquet
    summary.json
```

Publish the discoverable manifest last:

```text
analytics/backtests/v1/manifests/<backtest-key>/manifest.json
```

The manifest must include:

- Publication status and timestamps.
- Run ID and deterministic backtest key.
- Candle manifest URI, digest, snapshot key, and selected output URI.
- Exact strategy, portfolio, and cost parameters.
- Strategy, engine, and result-schema versions.
- Requested and effective candle ranges.
- Row counts and SHA-256 digest for every result artifact.
- Performance summary.

Use create-if-absent manifest publication. If the same key already exists,
validate its immutable identity and return it as the existing result. Fail on a
conflicting manifest. A failed run may leave an unreferenced run directory but
must never publish a success manifest.

## Result schemas

Check in explicit schemas for the result artifacts.

Each decision must include at least:

```text
decision_time
observed_candle_window_start
fast_sma
slow_sma
previous_target
new_target
strategy_version
```

Each fill must include at least:

```text
fill_time
decision_time
side
base_quantity
reference_open_price
execution_price
gross_notional
fee
cash_after
base_after
```

Each equity observation must include at least:

```text
window_start
close
cash
base_quantity
position
equity
drawdown
```

Keys, types, decimal precision, timestamp semantics, and nullability must be
documented and validated before publication.

## Performance summary

Report at least:

- Starting and ending marked-to-market equity.
- Absolute and percentage return.
- Maximum drawdown using the running equity peak.
- Number of decisions, buys, sells, and unfilled terminal decisions.
- Total fees and gross traded notional.
- Percentage of evaluation candles spent long.
- Requested range, effective range, warm-up count, and evaluation candle count.

Do not claim profitability, annualized performance, Sharpe ratio, or statistical
significance in this first story. A single bounded result is an engineering and
research artifact, not evidence that a strategy will make money.

## Bounded execution and safe reporting

- Require a configured maximum input-candle count and reject larger runs.
- Push exchange, symbol, date, and interval predicates into DuckDB.
- Iterate in deterministic time order without loading unnecessary symbols.
- Log identifiers, versions, counts, ranges, and aggregate metrics.
- Never log S3 secrets or private credentials.
- Make it explicit in the report that execution mode is `simulation` and that no
  exchange side effects are available.

## Validation command

Provide a DuckDB-based or pure-Python validation command that accepts one
backtest manifest and verifies:

- Artifact hashes, schemas, and row counts match the manifest.
- Decision, fill, and equity timestamps are ordered and inside the allowed range.
- Every fill refers to an earlier decision and the next eligible candle open.
- Portfolio arithmetic reconciles after every fill.
- Cash and base quantity never become negative.
- Equity and drawdown recompute exactly from the stored values.
- Summary totals equal the detailed artifacts.
- Recalculated backtest identity equals the manifest key.

## Tests

### Unit tests

Cover at least:

- Fast and slow period validation.
- Warm-up behavior and the first eligible decision.
- Equal averages target `FLAT`.
- Decisions fill only at the next candle open.
- A terminal decision remains unfilled.
- Buy and sell cost arithmetic with exact expected decimals.
- Buy quantity rounds down and never overdraws cash.
- Mark-to-market equity and maximum drawdown.
- No short, leverage, negative cash, or negative holdings state is reachable.
- Decimal overflow and invalid basis-point rejection.
- Reordered source files produce identical results.
- Any material input or version change changes the backtest key.

### Integration test

Create a small isolated candle snapshot with prices chosen to trigger one buy and
one sell. Then:

1. Publish the fixture and its candle manifest to local MinIO.
2. Run the real backtest command against the pinned manifest.
3. Assert the exact decisions, next-open fills, fees, holdings, equity curve,
   drawdown, and final summary.
4. Run the standalone result validator.
5. Repeat the identical command and require the existing backtest publication to
   be resolved without duplicate manifests.
6. Change one cost parameter and require a distinct backtest key.
7. Tamper with the candle manifest or a result artifact and prove validation
   fails closed.
8. Verify that no Kafka order topic, execution gateway, or exchange adapter is
   contacted.
9. Clean only the isolated test prefix.

## Documentation

Document:

- How to select and pin a candle manifest.
- The exact SMA and next-open timing rules.
- Warm-up and missing-minute behavior.
- Fee, slippage, quantity-rounding, and mark-to-market formulas.
- Local and MinIO commands.
- Result artifact schemas and validation.
- How reproducible identity and idempotent reruns work.
- Why this result must not be interpreted as proof of profitability.

## Out of scope

- Live or paper order placement
- Private exchange APIs or credentials
- Kafka strategy-signal or order-command topics
- PostgreSQL ledgers, inboxes, outboxes, and restart recovery
- Short selling, leverage, multiple simultaneous assets, or portfolio allocation
- Limit orders, partial fills, rejected orders, latency, or stochastic slippage
- Parameter search, optimization, or automatic strategy selection
- Train/validation/test orchestration or claims of statistical significance
- Streaming candles and continuously updating a running backtest
- Web dashboards and cloud deployment

## Follow-up story

Add a versioned experiment runner that evaluates fixed strategy configurations
over explicit train, validation, and out-of-sample ranges. It should compare
against a buy-and-hold baseline, prevent parameter selection from observing the
out-of-sample interval, and publish a reproducible comparison report. Only after
that should the same strategy interface be connected to live paper trading.

## Definition of done

The story is complete when an operator can pin one published candle snapshot,
run the versioned SMA strategy twice with identical inputs, and receive the same
backtest identity and exact artifacts; decisions provably use only closed
candles and fill no earlier than the next candle open; costs and portfolio state
reconcile with exact decimal arithmetic; the result validator detects tampering;
and the entire path operates without any exchange-order capability.
