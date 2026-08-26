# User story: Restore confirmed Coinbase trade gaps idempotently

## Story

As a market-data pipeline operator,
I want to backfill only Coinbase trades that a completed reconciliation has
confirmed are missing from the raw archive,
so that recoverable WebSocket source gaps can be repaired through the canonical
Kafka-to-Parquet path without rewriting raw data or creating logically new
trades when a repair is retried.

## Why this is next

The pipeline now has two distinct integrity checks:

```text
Kafka position audit:
Kafka -> raw Parquet

Coinbase reconciliation:
Coinbase REST trade identity -> archived trade identity
```

The first identifies archive failures. The second identifies Coinbase trades
that are absent before Kafka. Neither check repairs data, which is the correct
safe starting point.

The next increment should restore only reviewed, confirmed reconciliation
findings. It must publish through `market.trades.raw.v1` so the existing raw
sink, checkpoint, and Kafka-position audit remain the authoritative archive
boundary. Direct Parquet writes would bypass that evidence and are forbidden.

## Dependencies

A backfill may run only when all of the following are available:

1. A persisted reconciliation report with status `gaps_found`.
2. Its referenced append-only findings document.
3. Matching reconciliation and findings run IDs and reconciliation keys.
4. A complete Coinbase REST result for the original symbol and interval.
5. A passing Kafka-to-Parquet raw-integrity audit completed after that interval.

The job must reject an arbitrary list of trade IDs. Confirmed reconciliation
output is the only supported repair input.

## Important delivery guarantee

Kafka producer idempotence prevents duplicate sends within a producer session;
it does not provide exactly-once behavior across every process crash and retry.
The repair guarantee for this story is therefore:

```text
stable logical identity + bounded at-least-once publication + explicit verification
```

Every attempt must reuse the deterministic event ID derived from:

```text
exchange + symbol + source_event_id
```

Before publishing, the job checks whether the source identity is already in raw
Parquet. After Kafka acknowledges a publish, the job records the returned topic,
partition, and offset and waits for that exact Kafka position to appear in raw
Parquet. A retry checks the archive again before publishing.

If a process fails after Kafka accepts a record but before the acknowledgement
or state is durably recorded, the outcome is ambiguous. The job must preserve
that state and verify Kafka/Parquet before deciding whether another publish is
safe. It must never silently report exactly-once delivery.

## Canonical event contract prerequisite

The canonical event model and `event_id_for_trade()` currently belong to the
collector application. One application must not import another application.

Move the exchange-neutral `MarketTradeRawEvent`, deterministic trade identity,
and event-construction behavior into a shared package that both the live
collector and historical backfill application can use. The shared package must
not depend on Kafka, Spark, boto3, or an exchange SDK.

The checked-in `market.trade.raw.v1` contract currently identifies
`apps.collector` as the only producer. Extend it compatibly to permit the
explicit known producer `apps.historical_backfill` while keeping all existing
collector events valid. Add contract fixtures for both producers and document
that producer provenance is not part of trade identity.

Do not create a separate backfill topic or raw Parquet writer in this story.

## Scope

Create a manually runnable, dry-run-first command that:

1. Accepts exactly one reconciliation report URI.
2. Loads the referenced findings document and validates their linkage.
3. Rejects reports that are not `gaps_found`, are malformed, or lack confirmed
   findings.
4. Re-runs bounded Coinbase REST coverage for the report's original symbol and
   `[start_at, end_at)` interval.
5. Requires every persisted finding to still exist in the complete REST result.
6. Reads the relevant raw Parquet partitions immediately before publication.
7. Classifies findings already present in the archive as
   `skipped_already_archived`.
8. Builds missing trades with the same shared canonical model used by live
   collection.
9. Reuses `event_id_for_trade()` and sets `source_sequence` to `null` when REST
   does not provide the WebSocket envelope sequence.
10. Publishes only when the operator supplies an explicit `--apply` flag.
11. Uses the normal symbol-based Kafka key and existing event/schema headers.
12. Enables Kafka idempotence and requires an acknowledgement for every send.
13. Records the acknowledged topic, partition, and offset without recording
    credentials or complete payloads.
14. Waits for each acknowledged Kafka position to appear in raw Parquet.
15. Writes append-only attempt, finding-state, and terminal run documents.
16. Produces bounded console output and one machine-readable JSON line.

The command must not rewrite Parquet, alter Spark checkpoints, delete Kafka
records, or repair findings from a different reconciliation run.

## Proposed entry point

Add the command to `apps/historical_backfill`:

```powershell
python -m crypto_historical_backfill.backfill_coinbase_trades `
  --reconciliation-report `
    s3a://crypto-data/reconciliation/coinbase-trades/reports/event_date=2026-08-25/<run-id>.json
```

That invocation is a dry run. It validates current source coverage and reports
what would be published.

Mutation requires the explicit flag:

```powershell
python -m crypto_historical_backfill.backfill_coinbase_trades `
  --reconciliation-report <report-uri> `
  --apply
```

Kafka, Coinbase, object-storage, retry, and verification-timeout settings should
come from environment variables or bounded command options. Credentials must
never appear in command examples, reports, logs, Kafka headers, or exception
messages.

## Input validation

Validate all persisted input before contacting Kafka:

- Report status is exactly `gaps_found`.
- Report exchange is `coinbase`.
- Symbol and UTC interval are present and within configured maximum bounds.
- Findings URI is beneath the configured reconciliation findings prefix.
- Report and findings contain the same run ID and reconciliation key.
- Finding count matches `missing_from_archive`.
- Every finding contains only the expected symbol, source trade ID, and aware
  event timestamp.
- Duplicate finding identities are rejected rather than silently collapsed.
- Optional `incident_id` is retained as correlation metadata.
- Raw-integrity evidence is passing, for `market.trades.raw.v1`, and new enough
  to cover the original interval.

Add a SHA-256 digest of the canonical findings document to newly persisted
reconciliation reports. Backfill must verify the digest when present. Reports
created before the digest exists require an explicit compatibility mode and
must remain dry-run-only until an operator reviews them.

## Publication contract

For each still-missing Coinbase trade, construct:

```text
event_type       = market.trade.raw
schema_version   = v1
exchange         = coinbase
symbol           = normalized Coinbase product ID
source_event_id  = Coinbase trade_id
source_sequence  = null
event_id         = event_id_for_trade(exchange, symbol, source_event_id)
producer         = apps.historical_backfill
correlation_id   = reconciliation incident_id, when present
causation_id     = reconciliation run_id
payload          = validated Coinbase REST trade object
```

Use the same Kafka key and headers as live collection:

```text
key:              coinbase:<SYMBOL>
event_type:       market.trade.raw
schema_version:   v1
```

Do not generate a new event ID on a retry. `ingested_at` and `trace_id` may be
new per publication attempt; the deterministic trade identity must not change.

## Finding state machine

Track each confirmed identity independently:

```text
confirmed_missing
dry_run_ready
skipped_already_archived
publish_claimed
published_pending_archive
resolved_backfilled
unresolved_source_changed
unresolved_ambiguous_publication
failed_retryable
failed_permanent
```

State rules:

- `dry_run_ready`: current REST coverage still contains the finding and the
  archive still does not.
- `skipped_already_archived`: the source identity reached raw Parquet before
  this attempt; no publish occurs.
- `publish_claimed`: an append-only conditional claim prevents two repair
  processes from intentionally publishing the same finding concurrently.
- `published_pending_archive`: Kafka acknowledged the record and the receipt
  contains its topic, partition, and offset.
- `resolved_backfilled`: that exact Kafka position was observed once in raw
  Parquet.
- `unresolved_source_changed`: the finding is no longer present in a newly
  complete Coinbase REST result or its immutable fields changed.
- `unresolved_ambiguous_publication`: the process cannot determine whether a
  publish succeeded and must not report resolution.

A stale claim may be recovered only by first checking the archive and any
durable Kafka receipt. Never resolve a finding solely because a claim exists.

## Run states

The terminal run status is derived from all findings:

```text
dry_run_ready
resolved_no_action_needed
resolved_backfilled
partially_resolved
unresolved
failed_retryable
failed_permanent
```

- `resolved_no_action_needed`: every finding was already archived before any
  publish.
- `resolved_backfilled`: every remaining finding was acknowledged and verified
  at its exact Kafka position.
- `partially_resolved`: at least one finding resolved and at least one did not.
- No run may be `resolved_*` while an identity is pending, ambiguous, or failed.

## Suggested terminal report

```json
{
  "backfill_run_id": "8b3a4086-f5cf-4c10-90ae-3dbeb56398cc",
  "reconciliation_run_id": "e32bf4ea-4e84-4d6a-91dc-7d651a49252c",
  "reconciliation_key": "coinbase:BTC-USD:2026-08-25T14:00:00Z:2026-08-25T14:05:00Z:v3",
  "mode": "apply",
  "symbol": "BTC-USD",
  "started_at": "2026-08-26T15:00:00Z",
  "completed_at": "2026-08-26T15:00:12Z",
  "status": "resolved_backfilled",
  "confirmed_findings": 1,
  "already_archived": 0,
  "publish_attempted": 1,
  "kafka_acknowledged": 1,
  "archive_verified": 1,
  "ambiguous": 0,
  "failed": 0,
  "samples": {
    "resolved_backfilled": [
      {
        "symbol": "BTC-USD",
        "source_event_id": "123456789",
        "kafka_topic": "market.trades.raw.v1",
        "kafka_partition": 0,
        "kafka_offset": 1909736
      }
    ]
  }
}
```

Samples must be capped. Do not include complete Coinbase payloads, prices,
sizes, Kafka values, authorization headers, or credentials.

## Exit codes

- `0`: apply mode resolved every finding, or no publication was needed.
- `2`: dry run validated at least one finding that remains ready to publish.
- `3`: unresolved, ambiguous, partial, or retryable failure.
- `4`: invalid/permanent input or contract failure.
- Other nonzero codes: unexpected execution failure.

## Acceptance criteria

- Dry run is the default and performs no Kafka publication.
- `--apply` is required before any trade is published.
- Only a validated `gaps_found` reconciliation report and its linked findings
  may drive publication.
- A newly complete REST read must still contain every finding being repaired.
- The archive is checked immediately before each publication.
- An already archived finding is skipped without publishing.
- Live and backfill paths use the same shared canonical event model and
  deterministic event-ID function.
- Existing collector events remain valid after producer provenance is extended.
- Backfill events identify `apps.historical_backfill` as their producer.
- Every Kafka send uses idempotence, the canonical key and headers, and requires
  an acknowledgement.
- Kafka acknowledgement receipts record only safe identity and log-position
  metadata.
- A finding resolves only after its acknowledged topic, partition, and offset
  appears exactly once in raw Parquet.
- Timeout waiting for Parquet remains `published_pending_archive` or unresolved;
  it never becomes a false failure that triggers an immediate blind republish.
- Re-running after successful archival publishes nothing new.
- Partial publication never produces a fully resolved run.
- An ambiguous crash window is reported explicitly.
- All retry counts, timeouts, claim ages, REST windows, publication counts, and
  finding sample sizes are bounded.
- The command performs no writes except canonical Kafka publication and
  append-only backfill state/report documents.
- The local runbook documents dry run, apply, verification, safe retry, and
  ambiguous-state investigation.

## Required tests

Add focused tests for at least:

- A report not in `gaps_found` is rejected.
- Report/findings run-ID, reconciliation-key, count, prefix, and digest
  mismatches are rejected.
- Duplicate finding identities are rejected.
- A finding absent from a newly complete REST result becomes
  `unresolved_source_changed` and is not published.
- Dry run identifies ready work and creates no producer.
- An already archived identity is skipped without publication.
- One missing identity produces one canonical event with the deterministic
  event ID, correct producer, key, headers, and causation metadata.
- Existing live collector fixtures remain valid after contract evolution.
- A Kafka acknowledgement records the exact topic, partition, and offset.
- The acknowledged position appearing once in Parquet resolves the finding.
- A verification timeout does not blindly republish.
- Re-running a resolved finding publishes nothing.
- A bounded retry uses the same deterministic event ID.
- Two concurrent attempts cannot both acquire the same publish claim.
- A simulated crash before acknowledgement produces an ambiguous state.
- One successful and one failed finding creates `partially_resolved`.
- BTC and ETH reconciliation runs remain independent.
- All report samples are capped and contain no payload fields.

Add an optional Compose integration test that:

1. Uses a stubbed Coinbase REST response and isolated reconciliation report.
2. Produces one confirmed missing finding.
3. Verifies dry run publishes nothing.
4. Runs apply mode against local Kafka and the real raw sink.
5. Captures the Kafka acknowledgement.
6. Waits for the exact position in MinIO Parquet.
7. Re-runs apply mode and proves no second record is published.
8. Runs the Kafka-to-Parquet audit and verifies it still passes.

The integration test must use uniquely identifiable records and must not delete
or reset shared Kafka, MinIO, or checkpoint state.

## Operational validation

Before apply mode:

1. Preserve the reconciliation report, findings, and raw-integrity report.
2. Run a new dry run and inspect every ready, changed, and already-archived
   identity.
3. Confirm the configured Kafka topic, MinIO endpoint, and symbol.
4. Preserve current raw-sink logs and verify the sink is healthy.
5. Record the intended reconciliation and backfill run IDs.

After apply mode:

1. Preserve Kafka acknowledgement receipts.
2. Verify each exact topic/partition/offset in raw Parquet.
3. Re-run apply mode and confirm it publishes nothing.
4. Run the Kafka-to-Parquet integrity audit and preserve its passing report.
5. Keep ambiguous or partial findings open until evidence resolves them.

Never delete checkpoints or raw Parquet to make verification pass.

## Out of scope

- Automatically scheduling or triggering repair from every quality event
- Repairing an arbitrary operator-supplied trade ID
- Direct writes to raw Parquet
- Kafka or Parquet deletion and checkpoint reset
- Coinbase intervals whose REST completeness cannot be established
- Curated-layer deduplication
- Multi-exchange repair
- Reconstructing WebSocket envelopes, sequence numbers, or heartbeats
- Trading decisions based on repaired data
- Claiming exactly-once publication across an unrecorded crash window

## Follow-up story

After manual backfill is reliable, add durable incident resolution and bounded
orchestration. Link reconciliation and backfill terminal states to the original
data-quality incident, schedule only eligible unresolved incidents, enforce one
active repair per reconciliation key, and alert on partial or ambiguous states.

## Definition of done

The story is complete when an operator can dry-run one confirmed reconciliation,
explicitly apply its repairs through canonical Kafka, observe every acknowledged
position exactly once in raw Parquet, safely rerun without another publication,
and receive an honest durable state for every successful, skipped, partial,
failed, or ambiguous finding without modifying existing pipeline data directly.
