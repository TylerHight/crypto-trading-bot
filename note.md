# User story: Build a curated, deduplicated market-trade dataset

## Story

As a market-data consumer,
I want raw Kafka trade records parsed, normalized, validated, and deduplicated
into a versioned curated Parquet dataset,
so that analytics, candles, backtests, and future trading logic operate on one
logical row per exchange trade instead of transport-level deliveries.

## Why this is next

The raw pipeline and its recovery path are now proven end to end:

```text
Coinbase WebSocket / reviewed REST backfill
    -> canonical Kafka event
    -> immutable raw Parquet
    -> exact Kafka-position audit
```

The final local audit after backfill validation passed with:

```text
Kafka records:             1,943,360
Parquet records:           1,943,360
Missing Kafka positions:   0
Duplicate Kafka positions: 0
Invalid event values:      0
Duplicate logical IDs:     28,316 (warning)
```

That is the right raw-layer behavior: every accepted Kafka delivery is retained,
including source redelivery. It is not yet the right interface for analysis.
Consumers should not repeatedly decode JSON, interpret exchange strings, or
independently decide how to handle duplicate deterministic event IDs.

The project has spent enough time on additional incident-control machinery for
now. A durable incident registry and Airflow automation are deferred until the
pipeline produces queryable market data and a downstream consumer needs that
operational sophistication.

## Outcome

Produce an immutable curated snapshot with:

- One validated logical row per deterministic `event_id`.
- Fixed-precision price and size values.
- Explicit source and provenance fields.
- Deterministic handling of exact duplicates and conflicting duplicates.
- Safe quarantine records for invalid input.
- A manifest that identifies the exact raw Kafka-position snapshot used.
- The same transformation logic available to later batch replay and streaming
  entry points.

This story creates curated trades only. One-minute candles are the next story.

## Architecture decisions

### Raw remains immutable

Do not rewrite, compact, delete, or deduplicate the raw dataset. Curated output
is a derived projection and may be regenerated from preserved raw Kafka records.

### Start with a bounded batch snapshot

Implement the shared Spark DataFrame transformation now, but materialize the
first curated dataset with a bounded batch job rather than adding another
continuous service immediately.

The batch job must consume a passing raw-integrity audit report that freezes the
Kafka topic and exclusive ending offset for every partition. It then reads raw
Parquet records only within those position bounds. This makes the snapshot
repeatable even if the collector continues producing later records.

Future streaming and replay entry points must call the same transformation
module rather than reimplement parsing rules.

### Partition curated data by market event time

Raw Parquet is partitioned by Kafka ingestion time. Curated trades should be
partitioned by the normalized trade `event_time`:

```text
event_date=YYYY-MM-DD/
```

Retain `kafka_timestamp` and `ingested_at` as separate columns for latency and
late-arrival analysis. A historical backfill therefore lands in its historical
curated event date while preserving the later Kafka ingestion timestamp.

### Curated snapshots are append-only publications

Write each completed snapshot beneath a unique run directory and publish an
append-only manifest only after validation succeeds:

```text
curated/market_trades/v1/runs/<run-id>/event_date=YYYY-MM-DD/*.parquet
curated/market_trades/v1/manifests/<snapshot-key>/<run-id>.json
```

Do not expose a partial staging directory as a completed snapshot. Consumers use
the manifest URI supplied by the job. A catalog/current-pointer mechanism can be
added with the later analytics-platform story.

## Canonical curated schema

Define and document a checked-in v1 schema containing at least:

```text
event_id                 UUID/string, not null
exchange                 normalized lowercase string, not null
symbol                   normalized uppercase string, not null
source_event_id          string, not null
event_time               UTC timestamp, not null
ingested_at              UTC timestamp, not null
kafka_timestamp          UTC timestamp, not null
price                    decimal(38, 18), not null
size                     decimal(38, 18), not null
notional                 decimal(38, 18), not null
source_side              BUY or SELL, not null
source_sequence          nonnegative integer or null
producer                 known producer provenance, not null
trace_id                 UUID/string, not null
correlation_id           UUID/string or null
causation_id             UUID/string or null
kafka_topic              string, not null
kafka_partition          nonnegative integer, not null
kafka_offset             nonnegative integer, not null
curated_schema_version   v1
curated_at               UTC timestamp, not null
event_date               date derived from event_time
```

Use decimal arithmetic throughout. Do not convert prices, sizes, or notionals to
binary floating point.

Call the Coinbase field `source_side` until its exact maker/taker semantics are
documented. Do not rename it to buyer side, seller side, aggressor side, or trade
direction based on an assumption.

Producer provenance is retained but is not part of logical trade identity.

## Input contract validation

For each raw Kafka row:

1. Require non-null topic, partition, offset, Kafka timestamp, key, value, and
   required event headers.
2. Decode `kafka_value` as strict UTF-8 JSON.
3. Validate the complete `market.trade.raw.v1` envelope.
4. Require the configured canonical topic and supported producer.
5. Require aware timestamps and normalize them to UTC.
6. Require the Kafka key to equal `<exchange-lowercase>:<SYMBOL-UPPERCASE>`.
7. Require headers to match the event type and schema version in the value.
8. Validate the Coinbase payload fields used by the curated schema:
   - `trade_id`
   - `product_id`
   - `price`
   - `size`
   - `side`
   - `time`
9. Require payload identity and event time to match the canonical envelope.
10. Parse price and size as positive fixed-precision decimals within the schema
    bounds.
11. Normalize side to the documented `BUY`/`SELL` enum without guessing missing
    values.
12. Recompute `notional = price * size` using explicit decimal scale and rounding
    policy.

Do not silently coerce malformed, missing, negative, zero, overflowing, or
non-finite numeric values.

## Quarantine behavior

Invalid inputs must not enter curated trades. Write a separate immutable
quarantine dataset with only safe diagnostic fields:

```text
curation_run_id
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
failure_code
failure_field
event_id_when_safe
value_sha256
quarantined_at
```

Do not store a second complete Kafka value, raw Coinbase payload, malformed
message excerpt, authorization data, or credentials in quarantine.

Use stable failure codes such as:

```text
invalid_utf8
invalid_json
invalid_envelope_contract
invalid_kafka_key
invalid_headers
unsupported_exchange
payload_identity_mismatch
payload_time_mismatch
invalid_price
invalid_size
invalid_side
decimal_overflow
conflicting_duplicate
```

Every invalid input increments exactly one primary quarantine reason. Reports may
include bounded secondary diagnostics but must not inflate rejected-row counts.

## Deterministic deduplication

Deduplicate by canonical `event_id`, not by price/time proximity or Kafka
position.

### Exact logical duplicates

When all immutable curated trade fields match for one event ID:

- Emit one curated row.
- Select the representative row deterministically by the lowest ordered
  `(kafka_topic, kafka_partition, kafka_offset)`.
- Record total deliveries and duplicate deliveries in the run report.
- Preserve the selected Kafka provenance on the curated row.

This covers Coinbase redelivery and a live/backfill record that represents the
same source trade without creating a second logical fact.

### Conflicting duplicates

When one event ID maps to different immutable values such as symbol, source trade
ID, event time, price, size, or source side:

- Emit no curated row for that event ID.
- Quarantine the group as `conflicting_duplicate`.
- Include only safe identity, Kafka positions, differing field names, and hashes
  in bounded evidence.
- Make the run status unresolved or failed according to an explicit threshold.

Never choose a winner from conflicting economic facts based only on latest
arrival or producer name.

## Proposed entry point

Add a bounded Spark entry point such as:

```powershell
python -m jobs.spark.entrypoints.curate_market_trades `
  --raw-integrity-report <passing-audit-report> `
  --raw-input s3a://crypto-data/raw/market_trade_raw/v1 `
  --output s3a://crypto-data/curated/market_trades/v1 `
  --quarantine-output s3a://crypto-data/quarantine/market_trades/v1
```

Dry run is the default. It validates the audit snapshot, computes counts and
samples, and writes no curated or quarantine data.

Publication requires:

```powershell
python -m jobs.spark.entrypoints.curate_market_trades ... --apply
```

Environment variables or bounded options should provide S3/MinIO settings,
timeouts, maximum input rows when configured for local tests, sample limits,
decimal precision, and output prefixes. Credentials must not appear in command
examples or reports.

## Snapshot input validation

Before Spark writes output:

- The raw-integrity report status is exactly `passed`.
- Its topic is exactly `market.trades.raw.v1`.
- Every partition has valid earliest and exclusive ending offsets.
- Missing and duplicate Kafka-position counts are zero.
- The report is beneath a configured evidence prefix, or a local file is allowed
  only in explicit local-development mode.
- The report digest is captured in the curation manifest.
- The raw input and output prefixes are distinct.
- Curated and quarantine prefixes are distinct.
- The snapshot key derived from topic, partition bounds, transform version, and
  schema version is deterministic.

A rerun of the same successful snapshot key must return the existing manifest
without writing another logical snapshot.

## Run states

Use explicit states:

```text
dry_run_ready
published
resolved_existing_snapshot
unresolved_conflicting_duplicates
failed_input_contract
failed_retryable
failed_permanent
```

No run is `published` until:

1. Curated and quarantine writes complete.
2. Output schemas are read back and validated.
3. Curated `event_id` uniqueness is proven.
4. Counts reconcile to the frozen raw input.
5. The append-only manifest is durably created.

## Count reconciliation

The terminal report and manifest must prove:

```text
raw_rows_in_snapshot
  = valid_deliveries
  + quarantined_input_rows

valid_deliveries
  = curated_logical_trades
  + exact_duplicate_deliveries
  + conflicting_duplicate_deliveries
```

Define the conflicting-group counting policy precisely so the equations remain
unambiguous and tested.

Also report:

- Input topic and partition offset bounds.
- Unique event IDs examined.
- Curated rows by exchange, symbol, source producer, and event date.
- Minimum and maximum event, ingestion, and Kafka timestamps.
- Late-arrival latency percentiles using bounded aggregations.
- Quarantine counts by stable failure code.
- Output files, bytes, and row counts.
- Bounded safe samples without payloads or numeric market values.

## Manifest

Each append-only manifest should contain at least:

```text
curation_run_id
snapshot_key
curated_schema_version
transform_version
mode
started_at
completed_at
status
raw_integrity_report_uri
raw_integrity_report_sha256
input_topic
partition_offset_bounds
raw_rows_in_snapshot
curated_logical_trades
exact_duplicate_deliveries
conflicting_duplicate_deliveries
quarantined_input_rows
quarantine_counts
curated_output_uri
quarantine_output_uri
output_files
output_bytes
event_time_bounds
kafka_time_bounds
```

Do not include prices, sizes, complete source payloads, Kafka values, or
credentials.

## Query validation

Add a small DuckDB validation command or documented query that reads one
manifest's curated output and proves:

- The schema matches v1.
- `event_id` is unique and non-null.
- Price, size, and notional are positive decimals.
- Symbols and sides are normalized.
- Event dates match event timestamps.
- The known backfilled fixture appears once despite later ingestion time.

This is validation and exploration, not yet a dbt mart or production catalog.

## Exit codes

- `0`: published successfully or the identical successful snapshot already
  exists.
- `2`: dry run completed and is ready for explicit publication.
- `3`: conflicting duplicates or another unresolved quality result.
- `4`: invalid audit evidence, schema, configuration, or permanent contract
  failure.
- Other nonzero values: unexpected infrastructure or execution failure.

## Acceptance criteria

- A checked-in schema documents curated market trades v1.
- The shared Spark transform is independent of batch versus streaming input.
- The batch job consumes an exact passing Kafka-position snapshot.
- Dry run writes no curated or quarantine objects.
- Apply writes immutable run-scoped output and publishes the manifest last.
- Raw Kafka keys, values, headers, Parquet objects, and checkpoints are never
  modified.
- Coinbase price and size become fixed-precision positive decimals.
- Payload identity and time must match the canonical envelope.
- Exact duplicate event IDs create one curated logical trade.
- Live and historical-backfill producers use the same logical deduplication rule.
- Conflicting duplicates do not silently choose an economic value.
- Invalid inputs are quarantined with safe bounded evidence.
- Curated rows retain enough Kafka provenance to trace back to raw.
- Curated partitions use market event date, not Kafka ingestion date.
- Count reconciliation succeeds before a manifest is published.
- The same snapshot can be rerun without another logical publication.
- A later snapshot with higher exclusive Kafka offsets receives a distinct key.
- DuckDB can query the published snapshot and validate its invariants.
- Documentation explains dry run, apply, manifest selection, quarantine review,
  deterministic rerun, and safe recovery from partial staging output.

## Required tests

Add focused tests for at least:

- Valid collector and backfill fixtures produce the same curated schema.
- Numeric strings become exact decimal values without float conversion.
- Notional uses the documented scale and rounding policy.
- Zero, negative, non-numeric, non-finite, and overflowing decimals quarantine.
- Unsupported or missing side values quarantine.
- Payload/envelope trade ID, product, and timestamp mismatches quarantine.
- Kafka key and header mismatches quarantine.
- Naive timestamps and unsupported producer values quarantine.
- One valid input produces one curated row with correct provenance.
- Two exact duplicates produce one deterministic representative.
- Reversing input order produces byte-equivalent logical results.
- A collector record and backfill record for the same source trade produce one
  curated row.
- Same event ID with different price, size, side, symbol, or event time becomes
  a conflicting duplicate with no curated winner.
- BTC and ETH remain independent.
- Historical event time with a later Kafka timestamp lands in the historical
  event-date partition.
- Quarantine samples contain hashes and safe positions but no payload or market
  values.
- Count reconciliation equations hold for mixed valid, duplicate, conflicting,
  and invalid input.
- Invalid raw-integrity status, topic, bounds, prefix, or digest is rejected.
- Dry run creates no output.
- Apply publishes the manifest only after output validation.
- A simulated failure before manifest creation leaves no published snapshot.
- Rerunning an existing snapshot returns its original manifest.
- A higher Kafka ending offset creates a different snapshot key.
- Manifest samples and aggregation cardinality are bounded.

Add a Compose integration test that:

1. Uses an isolated raw-integrity snapshot and unique event IDs.
2. Includes one live-style record, its exact source redelivery, one
   historical-backfill-style record, and one invalid fixture.
3. Proves dry run writes nothing.
4. Runs the real Spark batch curation job against MinIO raw Parquet.
5. Verifies one logical row for each distinct valid event ID.
6. Verifies the invalid row appears only in quarantine.
7. Reads the output with DuckDB and checks decimals and provenance.
8. Reruns apply and proves no second logical snapshot is published.

The test must use an isolated output prefix and must not delete or reset shared
Kafka, raw Parquet, MinIO volumes, or Spark checkpoints.

## Operational validation

Before apply:

1. Preserve the passing raw-integrity report used as the snapshot boundary.
2. Run dry mode and review counts, conflicts, quarantine reasons, and event-time
   ranges.
3. Confirm raw, curated, quarantine, and manifest prefixes are distinct.
4. Record the snapshot key and expected curated row count.

After apply:

1. Preserve the manifest URI and digest.
2. Read back every output file schema and reconcile counts.
3. Query event-ID uniqueness and decimal constraints with DuckDB.
4. Verify at least one known backfill event appears exactly once.
5. Rerun the identical snapshot and confirm it returns the existing manifest.
6. Keep any conflicting duplicate unresolved; do not manually delete one raw
   delivery to make curation pass.

## Out of scope

- One-minute or higher-interval candles
- A continuous curated streaming service
- Watermark and late-data policy for live windowed aggregation
- Airflow scheduling
- PostgreSQL incident registry
- dbt facts, dimensions, marts, or generated documentation
- Glue Catalog or Athena publication
- Raw Parquet compaction or deletion
- Curated incremental merge/upsert optimization
- Automatic conflict resolution
- Strategy signals, backtesting, order execution, or risk controls
- Multi-exchange payload normalization beyond the explicit Coinbase v1 mapping

## Follow-up story

Build deterministic one-minute candles from curated trades with shared batch and
streaming transformation logic. Define event-time windows, watermark and late
backfill behavior, empty-window policy, decimal OHLC/VWAP calculations, revision
semantics, and a replay test proving live and batch candles match for the same
curated snapshot.

## Definition of done

The story is complete when a passing frozen raw snapshot can be curated
repeatedly into the same manifest-backed dataset, every valid Coinbase trade is
represented by one fixed-precision logical row regardless of source redelivery
or live/backfill provenance, invalid or conflicting records are safely
quarantined, and DuckDB can query the result without decoding raw Kafka JSON.
