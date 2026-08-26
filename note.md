# User story: Reconcile archived Coinbase trades against REST

## Story

As a market-data pipeline operator,
I want a bounded reconciliation between Coinbase REST trades and archived raw
trade events,
so that I can identify source trades missed by the WebSocket collection path
without confusing them with Kafka-to-Parquet archive failures.

## Why this is next

The Kafka-to-Parquet integrity audit now proves that every retained Kafka record
in a captured range exists exactly once in raw Parquet. It does not prove that
Coinbase delivered every trade to the collector or that the collector received
every WebSocket update.

The collector already emits durable observations for sequence gaps, heartbeat
silence, disconnects, and reconnects. Those observations identify suspicious
time intervals. The next step is to compare trades archived during a bounded
interval with Coinbase's REST trade results using the source trade identity:

```text
Coinbase REST: product_id + trade_id
Raw archive:   symbol     + source_event_id
```

This story deliberately detects and persists source gaps before any automated
repair is introduced. A later story can backfill confirmed missing identities
through the canonical Kafka path.

## Dependency

The raw integrity audit must pass for the relevant retained Kafka range before
a REST-only identity is classified as a Coinbase/WebSocket source gap. If raw
integrity fails, reconciliation must stop with an unresolved status because the
missing trade may instead be a Kafka-to-Parquet archive failure.

## Important source limitation

Coinbase's market-trades REST endpoint accepts a product, start time, end time,
and result limit. A response at the requested limit may be truncated and must
not be treated as complete.

The reconciler must split full result windows into smaller time segments until
each segment is demonstrably below the configured limit or a configured minimum
segment duration is reached. If completeness still cannot be established, the
run remains unresolved rather than reporting a false pass.

REST responses may overlap at time boundaries. Normalize every result and
deduplicate by `(product_id, trade_id)`, then apply the requested interval
locally as start-inclusive and end-exclusive:

```text
[start_at, end_at)
```

An empty REST response alone is not evidence that no trades occurred.

## Scope

Create a manually runnable reconciliation job that:

1. Accepts one Coinbase product and an explicit UTC start and end timestamp.
2. Rejects unbounded, inverted, naive-timezone, or excessively large windows.
3. Optionally records the durable data-quality incident that motivated the run.
4. Requires evidence that the Kafka-to-Parquet audit passed before classifying
   missing source trades.
5. Queries Coinbase market trades with timeouts, bounded retries, rate-limit
   backoff, a maximum page/request count, and recursive time-window splitting.
6. Normalizes REST trades through the shared exchange trade model.
7. Reads only relevant raw Parquet date/hour partitions and safely parses valid
   `market.trade.raw.v1` Coinbase events.
8. Compares REST and archive identities by `(symbol, source_event_id)`.
9. Reports REST-only identities as missing from the archive.
10. Reports archive-only identities, duplicate REST identities, malformed
    archive values, and duplicate archived source identities separately.
11. Writes an append-only machine-readable run report and a bounded console
    summary without including credentials or complete trade payloads.
12. Persists confirmed REST-only identities in a reconciliation findings path
    separate from raw Parquet and Kafka-position audit reports.

The first version must not publish trades, modify raw Parquet, delete
checkpoints, or mark an incident repaired.

## Proposed entry point

Build the first implementation in `apps/historical_backfill` and expose a
command such as:

```powershell
python -m crypto_historical_backfill.reconcile_coinbase_trades `
  --symbol BTC-USD `
  --start-at 2026-08-25T14:00:00Z `
  --end-at 2026-08-25T14:05:00Z `
  --raw-input s3a://crypto-data/raw/market_trade_raw/v1 `
  --report-output s3a://crypto-data/reconciliation/coinbase-trades
```

An optional `--incident-id` may link the run to a durable feed-quality
observation. Environment variables should supply API and object-storage
credentials; credentials must never be accepted in report fields.

## Reconciliation states

Use explicit terminal states:

```text
passed
gaps_found
unresolved_raw_integrity_failure
unresolved_source_history_unavailable
unresolved_source_result_truncated
failed_retryable
failed_permanent
```

State meanings:

- `passed`: REST coverage was established and every REST trade identity was
  found in the archive.
- `gaps_found`: REST coverage was established and one or more REST identities
  were absent from the archive.
- `unresolved_*`: the comparison could not safely establish completeness.
- `failed_retryable`: a bounded retry may succeed without changing inputs.
- `failed_permanent`: configuration or input validation must be corrected.

## Required comparison checks

### 1. REST trades missing from the archive

Perform a set difference from normalized REST identities to archived identities
using:

```text
REST product_id = archived symbol
REST trade_id   = archived source_event_id
```

Every returned identity is a candidate trade missed before Kafka. It becomes a
confirmed reconciliation finding only when REST coverage and raw archive
integrity are both established.

### 2. Archive-only identities

Report archived identities absent from the REST response separately. They must
not be deleted or automatically labeled corrupt. Possible explanations include
REST history limits, request-boundary behavior, aliasing, or source behavior
that requires investigation.

Archive-only findings make the result unresolved unless the adapter can prove
that the REST response completely covers the requested interval.

### 3. Duplicate identities

Count duplicates independently on each side:

- Duplicate REST `(product_id, trade_id)` identities are normalized to one
  comparison identity and reported as a source/API observation.
- Duplicate archived `(symbol, source_event_id)` identities are reported as raw
  source redeliveries and are not confused with duplicate Kafka positions.

### 4. Malformed archive events

Count values that cannot be decoded or parsed, are not JSON objects, fail the
required `market.trade.raw.v1` envelope fields, or contain an event timestamp or
symbol inconsistent with the requested interval.

Malformed in-range archive events make the reconciliation unresolved. The job
must continue far enough to produce a bounded diagnostic report.

## Deterministic boundaries and identity

Record the exact requested range and every actual REST sub-window. Apply the
same `[start_at, end_at)` filter to normalized REST and archived events after
parsing, regardless of endpoint boundary behavior.

Create a deterministic `reconciliation_key` from:

```text
exchange + symbol + start_at + end_at + source API version
```

Each execution may have a unique `run_id`, but repeating identical inputs must
produce the same reconciliation key and the same sorted identity findings when
the two sources have not changed.

## Suggested report

```json
{
  "run_id": "e32bf4ea-4e84-4d6a-91dc-7d651a49252c",
  "reconciliation_key": "coinbase:BTC-USD:2026-08-25T14:00:00Z:2026-08-25T14:05:00Z:v3",
  "incident_id": null,
  "exchange": "coinbase",
  "symbol": "BTC-USD",
  "start_at": "2026-08-25T14:00:00Z",
  "end_at_exclusive": "2026-08-25T14:05:00Z",
  "started_at": "2026-08-25T14:10:00Z",
  "completed_at": "2026-08-25T14:10:03Z",
  "status": "passed",
  "raw_integrity_status": "passed",
  "rest_requests": 3,
  "rest_trades": 824,
  "archived_trades": 824,
  "missing_from_archive": 0,
  "archive_only": 0,
  "duplicate_rest_identities": 0,
  "duplicate_archived_identities": 0,
  "malformed_archive_values": 0,
  "samples": {
    "missing_from_archive": [],
    "archive_only": []
  }
}
```

Samples must contain only a capped number of safe identifiers and timestamps.
Do not include prices, sizes, complete payloads, authorization headers, API
secrets, S3 credentials, or Kafka values.

## Exit codes

- `0`: reconciliation passed with complete source coverage and no missing
  archived identities.
- `2`: complete comparison found confirmed REST-only identities.
- `3`: reconciliation is unresolved and must not be treated as a pass.
- Other nonzero codes: configuration or unexpected execution failure.

## Acceptance criteria

- The command requires one symbol and explicit timezone-aware start/end bounds.
- The configured maximum interval, REST request count, result limit, retry
  count, timeout, and minimum split duration are enforced.
- Full REST responses are split; an unsplittable full response is unresolved.
- REST retries honor rate-limit guidance and never retry without a bound.
- Both sources are normalized and filtered to the same `[start_at, end_at)`
  interval.
- Comparison uses `(symbol, source_event_id)`, not WebSocket sequence numbers,
  event IDs, row counts, or Kafka offsets.
- The relevant Kafka-to-Parquet audit must pass before a source gap is
  confirmed.
- Missing archived identities are counted, sampled safely, and persisted in a
  reconciliation-specific append-only location.
- Archive-only, duplicate-source, and malformed-event findings are reported
  separately.
- Empty or truncated REST results never cause a false passing result.
- Re-running unchanged inputs produces the same reconciliation key and sorted
  findings.
- A passing reconciliation exits `0`, confirmed gaps exit `2`, and unresolved
  coverage exits `3`.
- The job writes only its report and reconciliation findings.
- The command, settings, report fields, state meanings, and investigation steps
  are documented in the local pipeline runbook.

## Required tests

Add focused tests for at least:

- Identical REST and archive identities pass.
- One REST-only identity is reported as missing from the archive.
- One archive-only identity is reported separately.
- Equal row counts with one REST-only and one archive-only identity do not pass.
- Duplicate REST results do not create duplicate missing findings.
- Duplicate archived source identities are counted independently.
- BTC and ETH comparisons remain independent.
- Start-boundary trades are included and end-boundary trades are excluded.
- A full REST response causes the window to split.
- Overlapping split responses are deduplicated by source identity.
- A full response at the minimum split duration remains unresolved.
- Rate limiting and temporary failures use bounded retries.
- An empty REST result does not automatically pass.
- Malformed archive JSON is counted without hiding other findings.
- A failed raw-integrity prerequisite prevents source-gap classification.
- Repeated inputs produce the same reconciliation key and finding order.
- Every sample collection is capped.

Add an optional integration test using a stubbed REST server and temporary raw
Parquet fixtures. It should exercise multiple REST windows, one controlled
REST-only trade, report persistence, and exit code `2` without publishing to
Kafka or modifying the raw fixture.

Do not make the normal test suite depend on live Coinbase history. A separate
manual smoke test may call Coinbase with a very small recent interval.

## Operational validation

Before reconciliation:

1. Preserve Kafka, MinIO, and checkpoint volumes.
2. Run the raw integrity audit and retain its JSON report.
3. Choose the smallest suspicious interval supported by a durable incident.
4. Confirm the symbol and UTC bounds before making REST requests.

After reconciliation:

1. Preserve the run report and REST request metadata.
2. Investigate every REST-only and archive-only identity.
3. Do not publish a repair until source coverage is established.
4. Do not delete raw data or checkpoints to make the comparison pass.

## Out of scope

- Automatically publishing or backfilling missing trades
- Marking data-quality incidents repaired
- Scheduling reconciliation with Airflow
- Reconstructing historical WebSocket envelopes or heartbeat sequences
- Repairing intervals outside Coinbase's available REST history
- Deleting raw source redeliveries
- Curated-layer deduplication
- Multi-exchange reconciliation
- Trading decisions based on reconciliation results

## Follow-up story

Add idempotent backfill for confirmed reconciliation findings. Normalize each
confirmed Coinbase trade through the shared `ExchangeTrade` model, reuse
`event_id_for_trade()`, publish through `market.trades.raw.v1`, link the event to
the reconciliation run or incident, and mark the finding resolved only after a
bounded follow-up proves that the identity reached raw Parquet exactly once by
Kafka position.

## Definition of done

The story is complete when an operator can provide one symbol and bounded UTC
interval, receive a deterministic pass, gaps-found, or unresolved result backed
by safely persisted identity findings, and rerun the same comparison without
changing Kafka, raw Parquet, or Spark checkpoints.
