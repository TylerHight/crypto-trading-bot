# User story: Build deterministic one-minute market candles

## Story

As a market-data consumer,
I want published curated trades aggregated into deterministic one-minute candles,
so that charts, research, backtests, and later strategy code can use a stable
OHLCV/VWAP time series without regrouping individual trades.

## Why this is next

The pipeline now has a trustworthy query-facing trade layer:

```text
Coinbase live collection / reviewed historical backfill
    -> immutable raw Kafka archive
    -> validated and deduplicated curated trades
    -> append-only curated snapshot manifest
```

The next useful analytical primitive is a candle dataset. It exercises event-time
aggregation, deterministic ordering, decimal arithmetic, and snapshot lineage
without prematurely adding strategy execution, scheduling, or a continuously
revising streaming service.

Build the bounded batch publication first. Put aggregation in a shared Spark
DataFrame transform so a future streaming entry point can reuse the definitions.

## Outcome

Given one published curated market-trades v1 manifest, produce one immutable,
manifest-backed candle snapshot containing at most one row per:

```text
(exchange, symbol, interval, window_start)
```

The first supported interval is exactly one minute. Each row must contain
deterministic OHLC prices, base and quote volume, VWAP, trade count, bounded
lineage, and the source curated-snapshot identity.

## Architecture decisions

### Consume a manifest, not a mutable prefix

The job must accept the URI of a published curated-trades manifest. It must:

- Require `status` to be `published`.
- Require curated schema version `v1`.
- Verify the manifest digest and required fields.
- Read only the manifest's `curated_output_uri`.
- Revalidate the curated schema and row count before aggregation.
- Reject a raw-data prefix, unpublished run directory, or current/latest pointer.

The candle snapshot key must include the source curated snapshot key and digest,
candle schema version, interval, and transform version.

### Use market event time

Assign trades to half-open UTC windows:

```text
[window_start, window_end)
```

For the one-minute interval:

```text
window_start = event_time truncated to the minute
window_end   = window_start + 1 minute
```

A trade at exactly `14:01:00Z` belongs to the `14:01` candle, never the `14:00`
candle. Kafka time and ingestion time must not determine the candle window.

### Define deterministic trade order

Opening and closing prices require a total ordering. Within a candle, order by:

```text
event_time ASC,
kafka_topic ASC,
kafka_partition ASC,
kafka_offset ASC,
event_id ASC
```

The first row supplies `open` and `first_event_id`. The last row supplies `close`
and `last_event_id`. This rule must be independent of Spark partitioning, input
file order, and replay order.

Curated `event_id` is already unique, so the final tie-breaker is defensive and
keeps the ordering definition total.

### Preserve fixed-precision arithmetic

Use decimal arithmetic throughout:

- OHLC prices: `decimal(38,18)`.
- Base volume: sum of curated `size`, represented as `decimal(38,18)`.
- Quote volume: sum of curated `notional`, represented as `decimal(38,18)`.
- VWAP: `quote_volume / base_volume`, rounded to scale 18 with round-half-even.

Do not convert prices, sizes, notionals, volumes, or VWAP to binary floating
point. Detect accumulator or output overflow and fail publication rather than
silently returning null, infinity, or a truncated number.

### Omit empty windows

Do not synthesize candles for minutes with no trades. This story publishes
trade-derived candles, not a forward-filled price series.

Consumers that require a dense calendar may join against a calendar table in a
later analytics story. The absence of a row means no validated trade occurred
for that market and minute in the source snapshot.

### Publish immutable snapshot versions

Write successful runs beneath a unique run directory:

```text
analytics/market_candles/v1/runs/<run-id>/
  interval=1m/
  event_date=YYYY-MM-DD/
  event_hour=HH/
  *.parquet

analytics/market_candles/v1/manifests/<snapshot-key>/manifest.json
```

Publish the append-only manifest only after all output validation succeeds.
Consumers discover a candle snapshot through its manifest, not by listing run
directories.

An identical source snapshot and transform configuration must return the existing
manifest without writing another logical snapshot.

## Canonical candle schema v1

Define and document a checked-in schema containing at least:

```text
exchange                       lowercase string, not null
symbol                         uppercase string, not null
interval                       constant "1m", not null
window_start                   UTC timestamp, not null
window_end                     UTC timestamp, not null
open                           decimal(38,18), not null
high                           decimal(38,18), not null
low                            decimal(38,18), not null
close                          decimal(38,18), not null
base_volume                    decimal(38,18), not null
quote_volume                   decimal(38,18), not null
vwap                           decimal(38,18), not null
trade_count                    positive integer, not null
first_event_id                 UUID/string, not null
last_event_id                  UUID/string, not null
minimum_kafka_timestamp        UTC timestamp, not null
maximum_kafka_timestamp        UTC timestamp, not null
minimum_ingested_at            UTC timestamp, not null
maximum_ingested_at            UTC timestamp, not null
source_curated_snapshot_key    string, not null
candle_schema_version          constant "v1", not null
candle_transform_version       string, not null
created_at                     UTC timestamp, not null
event_date                     date derived from window_start
event_hour                     two-digit hour derived from window_start
```

Do not include complete trade payloads, arrays of every event ID, credentials, or
unbounded provenance collections in a candle row.

## Candle calculations

For each `(exchange, symbol, one-minute window)`:

```text
open         = price of the first deterministically ordered trade
high         = maximum price
low          = minimum price
close        = price of the last deterministically ordered trade
base_volume  = sum(size)
quote_volume = sum(notional)
vwap         = quote_volume / base_volume
trade_count  = count(*)
```

Required invariants:

```text
low <= open <= high
low <= close <= high
base_volume > 0
quote_volume > 0
vwap > 0
trade_count > 0
window_end = window_start + 1 minute
event_date = UTC date(window_start)
event_hour = UTC hour(window_start)
```

Do not calculate VWAP as the unweighted average of trade prices.

## Late trades and historical backfills

This bounded batch story does not mutate a previously published candle snapshot.

If a later curated snapshot contains additional reviewed historical trades, it
produces a different candle snapshot key and a new immutable publication. A
candle for the same minute may therefore differ between snapshot versions. The
manifest lineage makes that revision explicit.

Downstream consumers must select one candle manifest and stay on that version for
a reproducible query or backtest. A current-pointer policy and streaming
watermark/revision protocol are deferred until a consumer needs them.

## Shared transformation boundary

Add a reusable Spark function similar to:

```python
def aggregate_market_candles(
    curated_trades: DataFrame,
    *,
    interval: str,
    source_snapshot_key: str,
    transform_version: str,
    created_at: datetime,
) -> DataFrame:
    ...
```

The transform must:

- Receive a DataFrame and explicit deterministic metadata.
- Create no Spark session.
- Open no storage, network, or manifest connection.
- Contain no batch-versus-streaming source branch.
- Avoid actions such as `collect()` on the complete input.
- Return the canonical candle projection.

The bounded entry point owns argument parsing, manifest loading, source and sink
configuration, publication, and validation.

## Proposed entry point

```powershell
python -m jobs.spark.entrypoints.build_market_candles `
  --curated-manifest <published-curated-manifest> `
  --output s3a://crypto-data/analytics/market_candles/v1
```

Dry run is the default. It validates the source snapshot, calculates candles and
reports, and writes nothing.

Publication requires:

```powershell
python -m jobs.spark.entrypoints.build_market_candles ... --apply
```

Only `--interval 1m` is accepted in v1. Unsupported intervals must fail clearly
rather than being interpreted approximately.

## Dry-run report

The bounded report must include:

- Run ID, mode, status, and timestamps.
- Source manifest URI and SHA-256 digest.
- Source curated snapshot key and trade count.
- Candle snapshot key and transform versions.
- Interval and output prefix.
- Total candle count and trade-count reconciliation.
- Candle counts by exchange, symbol, event date, and event hour.
- Minimum and maximum window timestamps.
- Minimum, maximum, and percentile trade counts per candle.
- Bounded samples containing identifiers and counts, but no market prices,
  volumes, complete trades, or credentials.

Dry run exits `2` when ready for publication and writes no run directory,
manifest, checkpoint, or quarantine object.

## Count reconciliation

Before publication, prove:

```text
source_curated_trades = sum(candle.trade_count)
```

Also prove:

- Every source trade maps to exactly one supported candle key.
- Every candle key is unique.
- Every candle has at least one source trade.
- The source manifest's curated count equals the rows actually read.

A reconciliation failure is permanent for that attempted publication and must
prevent manifest creation.

## Manifest

The append-only candle manifest must contain at least:

```text
candle_run_id
snapshot_key
candle_schema_version
candle_transform_version
interval
mode
started_at
completed_at
status
source_curated_manifest_uri
source_curated_manifest_sha256
source_curated_snapshot_key
source_curated_schema_version
source_curated_trades
candle_count
trade_count_sum
partition_columns
candle_output_uri
output_files
output_bytes
window_time_bounds
```

Do not include OHLC values, VWAP, volumes, complete event lists, raw messages, or
credentials in the manifest.

## Output validation

Before publishing the manifest:

1. Read back every produced Parquet file.
2. Verify the checked-in candle v1 schema.
3. Prove candle-key uniqueness.
4. Prove all numeric and window invariants.
5. Reconcile `sum(trade_count)` with source curated rows.
6. Verify event-date and event-hour partition values.
7. Verify output file and byte metrics.
8. Verify at least one known historical-backfill minute is represented.

A failed validation leaves only an unreferenced run directory. Recovery creates
a new run ID from the same source manifest; it must not overwrite or manually
promote the failed output.

## DuckDB validation

Add a validation command or documented query that accepts one candle manifest
and proves:

- The output schema matches v1.
- Candle keys are unique and non-null.
- OHLC relationships are valid.
- Volumes, VWAP, and trade counts are positive.
- Windows are exactly one minute.
- Partition dates and hours match window starts.
- `sum(trade_count)` matches the source curated count in the manifest.
- A known backfilled fixture contributes to the expected historical minute.

## Run states and exit codes

Use explicit states:

```text
dry_run_ready
published
resolved_existing_snapshot
failed_source_manifest
failed_input_contract
failed_reconciliation
failed_retryable
failed_permanent
```

Exit codes:

- `0`: published or identical successful snapshot already exists.
- `2`: dry run completed and is ready for explicit publication.
- `4`: invalid source evidence, schema, configuration, or permanent validation
  failure.
- Other nonzero values: unexpected infrastructure or execution failure.

## Required tests

Add focused tests for at least:

- A single trade produces one candle with identical OHLC and VWAP equal to price.
- Multiple trades calculate exact OHLC, volumes, VWAP, and count.
- Opening and closing prices use the documented total order.
- Reversing input and repartitioning produce equivalent candles.
- Trades at `HH:MM:00` and immediately before the next boundary land correctly.
- BTC and ETH produce independent candles.
- Different exchanges remain independent.
- Empty minutes produce no rows.
- Event time, not Kafka or ingestion time, selects the minute.
- A historical trade ingested later lands in its historical minute.
- Decimal calculations never use floats.
- VWAP uses quote volume divided by base volume with round-half-even.
- Decimal sum or division overflow prevents publication.
- OHLC and volume invariants are checked.
- Source trade count equals summed candle trade count.
- Candle keys are unique.
- Invalid, unpublished, malformed, or wrong-version curated manifests fail.
- Manifest digest mismatch fails.
- Dry run writes nothing and exits `2`.
- Apply publishes the manifest only after read-back validation.
- Failure before manifest creation leaves no published snapshot.
- Identical input returns the original manifest.
- A newer curated snapshot produces a different candle snapshot key.
- Reports and samples are bounded and contain no market values.

Add a Compose integration test that:

1. Uses an isolated curated snapshot with deterministic BTC and ETH fixtures
   spanning at least two minute boundaries.
2. Includes two trades sharing an event timestamp but different Kafka positions.
3. Includes a historical event with a later Kafka timestamp.
4. Proves dry run writes nothing.
5. Runs the real Spark candle job against MinIO.
6. Reads output with DuckDB and verifies exact OHLCV/VWAP values.
7. Verifies `sum(trade_count)` equals curated input rows.
8. Verifies historical event-date and event-hour partitions.
9. Reruns apply and proves no second logical snapshot is published.

The test must use isolated prefixes and must not delete or reset shared raw data,
curated data, Kafka topics, MinIO volumes, or Spark checkpoints.

## Documentation

Document:

- How to choose and preserve a curated source manifest.
- Dry-run and apply commands.
- Candle formulas and deterministic ordering.
- Decimal scale and rounding.
- Empty-window behavior.
- Late-backfill snapshot versioning.
- Manifest selection for reproducible backtests.
- DuckDB validation.
- Safe recovery from unreferenced partial output.

## Out of scope

- Continuous candle streaming service
- Streaming watermarks and mutable late-data updates
- A current/latest snapshot pointer
- Dense calendar generation or forward-filled candles
- Intervals other than one minute
- Cross-exchange composite prices
- Order-book candles or bid/ask spreads
- dbt marts and semantic metrics
- Airflow scheduling
- Strategy signals, backtesting engines, execution, or risk controls

## Follow-up story

Build a versioned research/backtesting interface over curated trades and
one-minute candle manifests. It should pin all inputs by manifest digest, define
train/test time ranges, prevent look-ahead, record strategy parameters and code
versions, and produce reproducible performance and data-lineage reports without
placing orders.

## Definition of done

The story is complete when any published curated-trade snapshot can be converted
repeatedly into the same manifest-backed one-minute candle dataset; OHLCV/VWAP
results are exact, event-time-based, deterministically ordered, and reconciled to
every source trade; late backfills create an explicit new snapshot rather than
silently mutating history; and DuckDB can validate and query the published candle
snapshot directly.
