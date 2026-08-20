# Development plan: WebSocket data integrity

These stories extend integrity coverage to the part of the pipeline before
Kafka:

```text
Coinbase WebSocket
    -> WebSocket integrity monitoring
    -> Kafka
    -> raw Parquet integrity audit
```

They should be implemented in order. Story 1 creates durable incidents and
connection health records. Story 2 uses those incidents to determine when and
where trade reconciliation is required.

## Story 1: Persist WebSocket integrity observations and health summaries

### User story

As a market-data pipeline operator,
I want WebSocket anomalies and positive health signals stored as structured
events,
so that I can prove the Coinbase continuity monitor is operating and investigate
problems after collector logs have expired.

### Why this is needed

The collector currently detects sequence gaps, duplicate or out-of-order
sequences, heartbeat-counter gaps, and malformed messages. These findings are
written to logs, but logs alone do not provide a durable incident history.

The absence of warnings is also ambiguous. It can mean either that the feed is
healthy or that the monitoring path stopped receiving messages. Periodic
positive health summaries remove that ambiguity.

### Scope

1. Define and document a versioned data-quality event contract.
2. Create a Kafka topic such as `market.data.quality.v1`.
3. Publish durable observations for:
   - WebSocket connection opened
   - WebSocket connection closed
   - Sequence gap
   - Duplicate sequence
   - Out-of-order sequence
   - Heartbeat-counter gap
   - Heartbeat silence
   - Malformed JSON or malformed Coinbase payload
   - Reconnect attempt and successful recovery
4. Emit a periodic connection-health summary, initially every 60 seconds.
5. Preserve structured context without stopping valid trade ingestion.
6. Add operational instructions for viewing incidents and proving the monitor is
   active.

### Proposed event fields

Every observation should include enough context to identify the affected feed
session:

```text
event_id
event_type
schema_version
exchange
connection_id
observation_type
detected_at
channel
affected_symbols
previous_sequence
current_sequence
missing_from
missing_to
last_heartbeat_counter
seconds_since_last_message
envelopes_observed
trades_observed
heartbeats_observed
sequence_gap_count
duplicate_sequence_count
out_of_order_sequence_count
malformed_message_count
producer
trace_id
```

Fields that do not apply to a particular observation may be null. Do not place
complete raw messages in routine health events. For malformed messages, store a
bounded safe excerpt or cryptographic hash so reports cannot grow without limit
or accidentally capture future sensitive fields.

### Heartbeat-silence behavior

Counter comparison only detects missed heartbeats after another heartbeat
arrives. Add a configurable timeout based on a monotonic clock so complete
silence can also be detected.

When the timeout expires:

1. Emit one heartbeat-silence incident for the affected connection.
2. Log the same incident for immediate operator visibility.
3. Reconnect the WebSocket.
4. Start a new connection ID and new sequence baseline.
5. Avoid repeatedly publishing the same silence incident while the original
   connection is still being closed.

### Periodic health summary

At a configurable interval, publish one summary per active connection containing
at least:

- Connection ID and subscribed symbols
- Total envelopes, trades, and heartbeats observed
- Last envelope sequence and heartbeat counter
- Age of the last message and last heartbeat
- Gap, duplicate, out-of-order, and malformed totals
- Connection start time

Normal messages must not produce one log line per envelope. The periodic summary
provides positive evidence without flooding logs.

### Acceptance criteria

- A checked-in schema defines `market.data.quality.v1` events.
- Local infrastructure creates the data-quality topic idempotently.
- Every continuity anomaly currently logged by the collector also produces a
  structured data-quality event.
- Connection start, disconnect, reconnect, and recovery are represented as
  structured events.
- Malformed JSON and payload failures increment a counter and produce a bounded
  structured observation.
- Missing heartbeats are detected within a configurable timeout even when no
  later heartbeat arrives.
- A health summary is emitted at a configurable interval while a connection is
  active.
- A new WebSocket connection receives a new connection ID and fresh sequence
  baseline.
- An anomaly does not prevent later valid trades from reaching the normal trade
  topic.
- A failure to publish a data-quality observation is logged clearly and does not
  silently masquerade as a healthy monitor.
- Unit and integration tests pass with the complete project suite.
- The runbook explains how to find recent incidents and confirm health summaries
  continue to arrive.

### Required tests

- A sequence jump publishes the exact missing range.
- Duplicate and out-of-order values publish the correct observation types.
- A skipped heartbeat counter publishes a heartbeat-gap event.
- An injected monotonic clock triggers heartbeat silence without sleeping in
  real time.
- Silence produces one incident and forces a reconnect.
- A reconnect creates a new connection ID and resets sequence state.
- Malformed JSON and malformed heartbeat values produce bounded observations.
- Periodic summaries contain increasing envelope and heartbeat counters.
- Valid trade publication continues after every anomaly type.
- A fake WebSocket test exercises the full collector path without contacting
  Coinbase or contaminating Kafka production-like data.

### Out of scope

- Automatically recovering missing trades
- Coinbase REST reconciliation
- Backfilling Kafka records
- Archiving every complete WebSocket envelope
- A production dashboard or paging integration
- Proving historical completeness from before these events existed

### Definition of done

The story is complete when an operator can query durable records to see both
WebSocket incidents and recent positive health summaries, and a test proves that
heartbeat silence is detected even when no subsequent message arrives.

---

## Story 2: Reconcile suspicious intervals and backfill missing Coinbase trades

### User story

As a market-data pipeline operator,
I want detected WebSocket gaps and disconnect windows reconciled against
Coinbase REST trades,
so that missing trades can be identified, restored idempotently, and tracked to
a durable resolution state.

### Dependency

Story 1 must be complete first. Reconciliation needs durable incident IDs,
connection boundaries, affected symbols, and timestamps to define safe,
repeatable REST query windows.

### Scope

1. Consume unresolved WebSocket integrity incidents that may imply missing
   trades:
   - Sequence gaps
   - Heartbeat silence
   - Unexpected disconnects
   - Reconnect intervals
2. Determine a bounded time window per affected symbol using the last
   known-good trade and first post-recovery trade.
3. Query Coinbase REST market trades for that window with pagination, rate-limit
   handling, retries, and explicit maximum bounds.
4. Compare Coinbase `(product_id, trade_id)` identities with archived
   `(symbol, source_event_id)` identities.
5. Publish missing trades through the normal canonical Kafka topic.
6. Reuse `event_id_for_trade()` so repeated reconciliation produces the same
   deterministic event ID.
7. Emit durable reconciliation status events linked to the original incident.
8. Leave incidents unresolved when the authoritative source cannot provide
   enough history or the comparison cannot be completed safely.

### Reconciliation states

Use explicit states rather than treating every completed job as successful:

```text
pending
running
resolved_no_missing_trades
resolved_backfilled
unresolved_source_history_unavailable
failed_retryable
failed_permanent
```

Each state transition should retain:

- Incident ID and reconciliation run ID
- Affected symbol
- Requested and actual time bounds
- REST pages and trades examined
- Archived trades examined
- Missing trade IDs found
- Backfill records successfully published
- Retry count and failure reason
- Started and completed timestamps

### Idempotent backfill behavior

For every missing Coinbase trade:

1. Normalize it through the same Coinbase trade model used by live collection.
2. Derive the existing deterministic event ID from exchange, symbol, and source
   trade ID.
3. Publish it to `market.trades.raw.v1` using the normal symbol-based Kafka key.
4. Set correlation or causation metadata to the originating integrity incident.
5. Mark the incident resolved only after the expected identities can be observed
   in the archive or verified through a bounded follow-up check.

The immutable raw layer may contain source redeliveries. Curated processing
remains responsible for deduplicating by deterministic `event_id`.

### Acceptance criteria

- Reconciliation runs only for durable incidents with explicit affected symbols
  and bounded time windows.
- REST requests enforce maximum window sizes, pagination limits, timeouts,
  retries, and rate-limit backoff.
- Comparison uses Coinbase trade identity, not WebSocket envelope sequence.
- A missing REST trade produces one deterministic backfill event.
- Re-running the same incident produces the same event ID and does not create a
  logically new trade.
- An interval with no missing trades resolves as
  `resolved_no_missing_trades`.
- An interval with successfully restored trades resolves as
  `resolved_backfilled`.
- Insufficient REST history remains explicitly unresolved.
- Partial publication or API failure remains retryable and never reports a
  false successful resolution.
- Reconciliation events link back to the original WebSocket incident.
- Backfilled trades pass the existing Kafka-to-Parquet integrity audit.
- The runbook explains how to list unresolved incidents, retry safe failures,
  and verify a backfill.

### Required tests

- No difference between REST and archive produces no backfill.
- One REST-only trade produces one canonical backfill event.
- Multiple pages are combined before comparison.
- Duplicate REST results do not create duplicate logical backfills.
- Re-running a completed incident produces the same deterministic IDs.
- Rate limiting and temporary failures use bounded retry behavior.
- An unavailable historical interval remains unresolved.
- A partial Kafka publication does not mark the incident resolved.
- BTC and ETH incidents are reconciled independently.
- Event-time boundaries include trades exactly at the start and end without
  double-counting adjacent windows.
- A Compose integration test detects a controlled missing archived trade,
  backfills it, and verifies it reaches Parquet.

### Safety requirements

- Never delete Kafka records, Parquet files, or Spark checkpoints during
  reconciliation.
- Never mark an incident resolved solely because the REST request returned an
  empty page.
- Cap every REST query window and number of pages.
- Preserve the original incident and every reconciliation attempt for audit.
- Backfill only market trades whose identity and symbol are validated.
- Provide a dry-run mode that reports missing identities without publishing.

### Out of scope

- Reconstructing every historical WebSocket envelope
- Repairing intervals outside Coinbase's available REST history
- Deleting raw source redeliveries
- Curated-layer deduplication
- Automatic trading decisions based on reconciliation results
- Multi-exchange reconciliation before the Coinbase implementation is stable

### Definition of done

The story is complete when a bounded incident can be reconciled repeatedly with
the same result, genuinely missing trades are restored through the canonical
pipeline, and every incident ends in an accurate durable resolved, unresolved,
or failed state.
