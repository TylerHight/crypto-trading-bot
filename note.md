# User story: Audit Kafka-to-Parquet data integrity

## Story

As a data pipeline operator,
I want a read-only audit that compares retained Kafka records with raw Parquet
records,
so that I can identify missing or duplicated archived records without changing
Kafka, Parquet, or Spark checkpoint data.

## Why this is next

The existing integration test proves that one newly published Kafka record can
reach Parquet. It does not prove that every existing Kafka record was archived
exactly once.

The raw Parquet layer already stores the fields needed for a reliable audit:

```text
kafka_topic
kafka_partition
kafka_offset
kafka_key
kafka_value
kafka_headers
```

The combination of `kafka_topic`, `kafka_partition`, and `kafka_offset`
identifies the original Kafka log position. Comparing those identities is more
reliable than comparing only row counts.

## Scope

Create a manually runnable, read-only Spark batch audit that:

1. Captures a fixed Kafka offset range for every topic partition.
2. Reads retained Kafka records inside that range.
3. Reads raw Parquet records for the same range.
4. Finds Kafka records that are missing from Parquet.
5. Finds Kafka positions that appear more than once in Parquet.
6. Reports invalid event JSON and duplicate event IDs as separate findings.
7. Produces a small machine-readable JSON summary and readable console output.
8. Exits unsuccessfully when integrity failures are present.

The audit must not publish messages, rewrite Parquet, delete checkpoints, or
repair data automatically.

## Important distinction

This story validates the pipeline boundary:

```text
Kafka -> Spark raw sink -> Parquet
```

It does not prove that Coinbase delivered every trade to the collector.

Historical Coinbase WebSocket completeness requires a separate reconciliation
against an authoritative source such as Coinbase REST data. The archived trade
records cannot reconstruct every old WebSocket sequence because heartbeat and
subscription envelopes were not written to Kafka.

## Proposed entry point

Add a command such as:

```powershell
python -m jobs.spark.entrypoints.audit_raw_market_trades
```

The command should reuse the existing Kafka, MinIO, and S3A configuration where
possible. Audit-specific settings may be added for the report path and optional
date or offset bounds.

## Required audit checks

### 1. Missing Parquet records

Perform a left anti-join from Kafka to Parquet using:

```text
kafka topic = kafka_topic
partition   = kafka_partition
offset      = kafka_offset
```

Every returned row represents a Kafka record for which no raw Parquet record
was found.

### 2. Duplicate archived positions

Group Parquet rows by:

```text
kafka_topic, kafka_partition, kafka_offset
```

Any group with a count greater than one is an archive integrity failure. Spark
may receive duplicate source events, but it must not archive the same Kafka log
position more than once.

### 3. Invalid event values

Decode `kafka_value` as UTF-8 JSON and count values that:

- Cannot be decoded as UTF-8.
- Cannot be parsed as JSON.
- Are not JSON objects.
- Lack required envelope fields such as `event_id`, `event_type`, or
  `schema_version`.

This first version may check required fields without implementing a complete
JSON Schema validator inside Spark.

### 4. Duplicate event IDs

Group successfully parsed records by `event_id` and report IDs with more than
one occurrence.

Duplicate event IDs are reported separately from duplicate Kafka positions.
They may indicate a source redelivery rather than a raw archive failure and
should not be silently removed by this audit.

### 5. Audit boundaries

Capture Kafka end offsets before reading data and use those fixed values as the
audit upper boundary. This prevents records arriving during the audit from
making the Kafka and Parquet totals appear inconsistent.

For each partition, record:

- Earliest retained Kafka offset.
- Fixed exclusive ending offset.
- Minimum and maximum archived offset observed.
- Kafka record count within the range.
- Parquet record count within the range.

Only compare Parquet rows inside the retained Kafka range. Older Parquet rows
may legitimately remain after the corresponding Kafka records expire.

## Suggested report

Write a JSON result similar to:

```json
{
  "topic": "market.trades.raw.v1",
  "started_at": "2026-08-20T16:00:00Z",
  "completed_at": "2026-08-20T16:01:00Z",
  "status": "passed",
  "partitions": [
    {
      "partition": 0,
      "earliest_offset": 0,
      "ending_offset_exclusive": 1540818,
      "kafka_records": 1540818,
      "parquet_records_in_range": 1540818,
      "missing_from_parquet": 0,
      "duplicate_parquet_positions": 0
    }
  ],
  "invalid_event_values": 0,
  "duplicate_event_ids": 0
}
```

Do not include complete Kafka values or credentials in the report. For sample
findings, include only a limited number of safe identifiers such as partition,
offset, and event ID.

## Acceptance criteria

- The audit runs as a Spark batch job and does not start a streaming query.
- Kafka end offsets are fixed at the beginning of the audit.
- Kafka and Parquet are compared by topic, partition, and offset.
- Missing Parquet records are counted and a bounded sample is reported.
- Duplicate Parquet positions are counted and a bounded sample is reported.
- Invalid event values are counted without crashing the entire audit.
- Duplicate event IDs are reported separately from duplicate Kafka positions.
- Parquet rows older than Kafka's earliest retained offset are not labeled as
  failures.
- A passing audit exits with code `0`.
- Missing or duplicate Kafka positions cause a nonzero exit code.
- The audit performs no writes except its report output.
- The command and report fields are documented in the local pipeline runbook.

## Tests

Add focused tests with small in-memory Spark DataFrames for at least:

- Identical Kafka and Parquet identities produce no findings.
- A Kafka identity absent from Parquet is reported as missing.
- A repeated Parquet identity is reported as a duplicate.
- The same record count with one missing and one duplicate still fails.
- Multiple Kafka partitions are compared independently.
- Parquet history below Kafka's earliest retained offset is ignored.
- Invalid JSON is counted and does not stop other checks.
- Repeated `event_id` values are reported separately.
- Sample findings are capped so reports cannot grow without limit.

Add one optional Compose integration test that publishes uniquely identifiable
records, waits for the raw sink, runs the audit, and verifies a passing report.

## Operational validation

Before running the completed audit, preserve the current Kafka, MinIO, and
checkpoint volumes. Do not reset data to make a failed audit pass.

Check the raw sink logs for existing data-loss evidence:

```powershell
podman compose logs raw-sink 2>&1 |
    Select-String -Pattern "data loss|offset.*range|unavailable|exception|terminated"
```

Run the audit and save its output:

```powershell
podman compose run --rm raw-sink `
  /opt/spark/bin/spark-submit `
  --master local[2] `
  /opt/spark/work-dir/jobs/spark/entrypoints/audit_raw_market_trades.py
```

Review every nonzero finding before attempting recovery. In particular, do not
delete Spark checkpoints or raw Parquet files until the original evidence has
been preserved.

## Out of scope

- Automatically repairing or backfilling missing Parquet records.
- Deleting duplicate raw records.
- Resetting Spark checkpoints.
- Comparing historical trades with Coinbase REST.
- Proving historical WebSocket envelope or heartbeat completeness.
- Building a dashboard or scheduled alert.
- Curated-layer deduplication.

## Follow-up story

After the Kafka-to-Parquet audit is reliable, add Coinbase trade
reconciliation. That story should compare archived `(symbol, source_event_id)`
values with bounded Coinbase REST trade results and persist unresolved source
gaps separately from Kafka-to-Parquet failures.

## Definition of done

The story is complete when a developer can run one documented command and get
a deterministic pass/fail report showing whether every retained Kafka record
in the captured range exists exactly once in raw Parquet, without modifying the
pipeline data being audited.
