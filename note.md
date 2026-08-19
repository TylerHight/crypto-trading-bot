# Implementation guide: Detect Coinbase WebSocket data gaps

## Goal

Detect when Coinbase WebSocket messages are missing, duplicated, late, or out of
order. Detect missing heartbeat messages as well. When a problem is found, keep
collecting new trades, but record enough information to investigate and later
backfill the affected interval.

This work protects the first boundary of the pipeline:

```text
Coinbase WebSocket -> collector -> Kafka -> Spark -> MinIO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This story verifies this boundary.
```

The existing Kafka-to-Parquet integration test verifies a later boundary. It
does not prove that Coinbase delivered every WebSocket message.

## Current implementation status

The first continuity-monitoring slice is now implemented:

- The generic tracker lives in
  [sequence_tracker.py](packages/exchange_adapters/src/crypto_exchange_adapters/sequence_tracker.py).
- The previous `==` versus `=` state-update bug is corrected.
- `SequenceResult` records status, previous value, current value, and any exact
  missing range.
- `CoinbaseFeedContinuityMonitor` observes every raw WebSocket envelope before
  channel filtering or trade-batch expansion.
- One outer `sequence_num` stream is tracked per WebSocket connection.
- `heartbeat_counter` is tracked as a separate counter stream.
- A new tracker is created for each connection because the outer sequence
  restarts at zero after reconnecting.
- Gap, duplicate, and out-of-order observations are logged without stopping
  ingestion.
- Focused tracker and adapter tests pass: 16 passed.
- The exchange-adapter package passes mypy.

Still intentionally deferred:

- Detecting total heartbeat silence with a timer
- Persisting gap observations outside logs
- REST reconciliation and backfill
- Metrics and an unresolved-gap dashboard

## File placement

The generic sequence-tracking code belongs in the package that receives
Coinbase messages:

```text
packages/exchange_adapters/src/crypto_exchange_adapters/sequence_tracker.py
```

`coinbase.py` imports it without violating the dependency rules:

```python
from .sequence_tracker import SequenceTracker
```

Do not add a second copy in `apps/collector`. Two copies would eventually behave
differently and make fixes confusing.

The responsibilities should be separated like this:

```text
sequence_tracker.py
    Generic integer-sequence comparison only

coinbase.py
    Understand Coinbase channels and message shapes
    Choose the correct tracking key
    Extract sequence_num and heartbeat_counter
    Log or emit continuity observations

collector application
    Publish normalized trades to Kafka
    Eventually expose operational metrics and health state
```

## Step 1: Correct and finish the generic tracker

Use a result that includes the numbers involved. Including `previous` and
`current` makes logging and testing easier because callers do not need to reach
inside the tracker.

```python
from dataclasses import dataclass
from typing import Literal


# Literal tells the type checker exactly which status strings are permitted.
# A typo such as "out-of-order" will then be caught during type checking.
SequenceStatus = Literal[
    "initialized",
    "ok",
    "duplicate",
    "out_of_order",
    "gap",
]


@dataclass(frozen=True)
class SequenceResult:
    """Describe how one sequence number compares with the previous one."""

    status: SequenceStatus

    # `previous` is None only for the first observation of a tracking key.
    previous: int | None
    current: int

    # These are populated only for a gap.
    missing_from: int | None = None
    missing_to: int | None = None


class SequenceTracker:
    """Remember and compare the latest integer for multiple named streams."""

    def __init__(self) -> None:
        # Example contents:
        # {
        #     "coinbase:advanced-trade-websocket": 144,
        #     "coinbase:heartbeat-counter": 3049,
        # }
        self._last: dict[str, int] = {}

    def observe(self, key: str, current: int) -> SequenceResult:
        """Save `current` when appropriate and return the comparison result."""

        previous = self._last.get(key)

        # The first number establishes a baseline. It cannot prove that earlier
        # messages were complete because the tracker has nothing to compare yet.
        if previous is None:
            self._last[key] = current
            return SequenceResult(
                status="initialized",
                previous=None,
                current=current,
            )

        # This is the expected case: 140 is followed by 141.
        if current == previous + 1:
            self._last[key] = current
            return SequenceResult(
                status="ok",
                previous=previous,
                current=current,
            )

        # The same message may have been delivered more than once. Do not move
        # the saved state because it already contains this value.
        if current == previous:
            return SequenceResult(
                status="duplicate",
                previous=previous,
                current=current,
            )

        # An older message arrived late. Do not move the saved state backward.
        # For example, after seeing 144, receiving 142 must not make 142 the new
        # baseline.
        if current < previous:
            return SequenceResult(
                status="out_of_order",
                previous=previous,
                current=current,
            )

        # The remaining case is a forward jump greater than one. Save the most
        # recent value so following messages are compared with the newest known
        # position.
        self._last[key] = current
        return SequenceResult(
            status="gap",
            previous=previous,
            current=current,
            missing_from=previous + 1,
            missing_to=current - 1,
        )
```

Why state changes in only three places:

| Result | Update saved state? | Reason |
|---|---:|---|
| `initialized` | Yes | Establish the first baseline |
| `ok` | Yes | Advance normally |
| `gap` | Yes | Continue from the newest observed position |
| `duplicate` | No | The saved value is already correct |
| `out_of_order` | No | Never move the baseline backward |

## Step 2: Test the tracker before integrating it

Create a focused unit test file. Keeping these tests independent of WebSockets,
Kafka, and MinIO makes them fast and easy to understand.

Suggested location:

```text
tests/unit/collector/test_sequence_tracker.py
```

Minimum tests:

```python
from crypto_exchange_adapters.sequence_tracker import SequenceTracker


def test_first_observation_initializes_the_stream() -> None:
    tracker = SequenceTracker()

    result = tracker.observe("coinbase:websocket", 100)

    assert result.status == "initialized"
    assert result.previous is None
    assert result.current == 100


def test_consecutive_observation_advances_the_stream() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 101)

    assert result.status == "ok"
    assert result.previous == 100
    assert result.current == 101


def test_forward_jump_reports_the_exact_missing_range() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 104)

    assert result.status == "gap"
    assert result.missing_from == 101
    assert result.missing_to == 103


def test_gap_updates_the_baseline() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 104)

    result = tracker.observe("coinbase:websocket", 105)

    assert result.status == "ok"


def test_duplicate_does_not_change_the_baseline() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 101)

    assert result.status == "ok"


def test_out_of_order_message_does_not_move_the_baseline_backward() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 101)
    tracker.observe("coinbase:websocket", 99)

    result = tracker.observe("coinbase:websocket", 102)

    assert result.status == "ok"


def test_independent_counter_streams_have_independent_state() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:heartbeat-counter", 500)

    envelope = tracker.observe("coinbase:websocket", 101)
    heartbeat = tracker.observe("coinbase:heartbeat-counter", 501)

    assert envelope.status == "ok"
    assert heartbeat.status == "ok"
```

The `test_gap_updates_the_baseline` test specifically prevents regression of the
earlier `==` instead of `=` bug. Type checking cannot catch that bug because
comparing two integers is valid Python even when the result is accidentally
ignored.

Run only these tests while developing:

```powershell
.\.venv\Scripts\python.exe -m pytest -q `
  tests\unit\collector\test_sequence_tracker.py
```

## Step 3: Observe complete Coinbase messages

Continuity must be checked in `CoinbasePublicTradeClient.trades()` immediately
after JSON decoding and before `parse_message()` expands one message into
individual trades.

Old flow:

```python
message = json.loads(raw_message)

for trade in self.parse_message(message):
    yield trade
```

Implemented flow:

```python
# Decode one complete Coinbase WebSocket message.
message = json.loads(raw_message)
if not isinstance(message, dict):
    raise TypeError("WebSocket message must be a JSON object")

# Check the complete envelope before splitting it into trades. This happens for
# every channel, including heartbeats and subscription acknowledgements.
for stream, result in continuity_monitor.observe_message(message):
    report_continuity_observation(connection_id, stream, result)

# Normal trade parsing and publication continue even if continuity monitoring
# reports a duplicate, late message, or gap.
for trade in self.parse_message(message):
    yield trade
```

Do not call the tracker once per trade. Coinbase can batch several trades into
one WebSocket message, and every trade in that message can share the same outer
`sequence_num`. Calling once per trade would falsely label the second trade as
a duplicate.

## Step 4: Track the connection-wide envelope sequence

Live raw-envelope inspection established the actual scope used by Coinbase
Advanced Trade. `sequence_num` advances once for every envelope on one
WebSocket connection, across products and channels:

```text
channel          sequence_num  product
market_trades               0  BTC-USD
market_trades               1  ETH-USD
subscriptions               2
subscriptions               3
market_trades               4  BTC-USD
heartbeats                  9
market_trades              10  BTC-USD
```

Therefore:

- Do not use `product_id` in the sequence-tracker key.
- Do not use a separate key for each channel.
- Observe subscription, heartbeat, trade, and unknown future envelopes.
- Create a fresh monitor for each new connection because the counter restarts
  at zero.

The Kafka key `coinbase:BTC-USD` remains correct. It controls Kafka partitioning
and is unrelated to the scope of Coinbase's WebSocket counter.

The implementation uses one constant stream name inside a connection-specific
monitor:

```python
ENVELOPE_SEQUENCE_STREAM = "coinbase:advanced-trade-websocket"

sequence = message.get("sequence_num")
if sequence is not None:
    # Reject bool explicitly because bool is a subclass of int in Python.
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise TypeError("Coinbase sequence_num must be an integer or null")

    result = self._tracker.observe(ENVELOPE_SEQUENCE_STREAM, sequence)
```

A Kafka view cannot prove envelope continuity. One Coinbase envelope can create
several Kafka trade records, while heartbeat and subscription envelopes create
no trade records. Continuity must be checked before that fan-out/filtering.

## Step 5: Track heartbeat counters

The adapter already subscribes to the heartbeat channel, but `parse_message()`
currently ignores it. Inspect heartbeats in `observe_continuity()` before that
method is called.

```python
def observe_heartbeat_continuity(self, message: dict[str, Any]) -> None:
    events = message.get("events", [])

    if not isinstance(events, list):
        raise TypeError("Coinbase heartbeat events must be a list")

    for event in events:
        if not isinstance(event, dict):
            raise TypeError("Coinbase heartbeat event must be an object")

        # Coinbase encodes this counter as a JSON string, for example "3049".
        counter = int(event["heartbeat_counter"])
        result = self._tracker.observe("coinbase:heartbeat-counter", counter)
        self._report_sequence_result("coinbase:heartbeat-counter", result)
```

The outer envelope sequence is observed first for every channel. Heartbeat
messages then contribute one additional, independent counter observation:

```python
def observe_message(self, message: dict[str, Any]) -> None:
    self.observe_envelope_sequence(message)

    if message.get("channel") == "heartbeats":
        self.observe_heartbeat_continuity(message)
```

Unknown channels still participate in the outer sequence and should not be
ignored by envelope monitoring. Their channel-specific payloads can be ignored
by trade parsing.

## Step 6: Report anomalies without stopping collection

A sequence gap is a data-quality incident, but raising an exception immediately
would disconnect the collector and potentially create a larger gap. Continue
collecting and report the observation.

An initial implementation can use structured log fields:

```python
def _report_sequence_result(
    self,
    key: str,
    result: SequenceResult,
) -> None:
    if result.status == "gap":
        LOGGER.error(
            "Coinbase sequence gap stream=%s previous=%s current=%s "
            "missing_from=%s missing_to=%s",
            key,
            result.previous,
            result.current,
            result.missing_from,
            result.missing_to,
        )
    elif result.status == "duplicate":
        LOGGER.warning(
            "Duplicate Coinbase sequence stream=%s sequence=%s",
            key,
            result.current,
        )
    elif result.status == "out_of_order":
        LOGGER.warning(
            "Out-of-order Coinbase sequence stream=%s previous=%s current=%s",
            key,
            result.previous,
            result.current,
        )
```

Do not log every `"ok"` result at `INFO`; BTC and ETH trade volume would create
large, noisy logs. Counters or debug logging are better for the normal path.

The later durable observation should contain:

```json
{
  "exchange": "coinbase",
  "stream": "coinbase:advanced-trade-websocket",
  "connection_id": "a generated UUID",
  "affected_products": ["BTC-USD", "ETH-USD"],
  "previous_sequence": 140,
  "current_sequence": 144,
  "missing_from": 141,
  "missing_to": 143,
  "detected_at": "2026-08-19T17:00:00Z",
  "status": "unresolved"
}
```

## Step 7: Detect a heartbeat that stops completely

Counter comparison detects skipped heartbeat messages only after another
heartbeat arrives. It cannot detect silence by itself.

Add a configurable heartbeat timeout, for example five seconds. The monitor
should remember when the last valid heartbeat arrived using a monotonic clock.
A monotonic clock is appropriate for elapsed time because system clock changes
cannot make it move backward.

The receive loop must handle both cases:

- No WebSocket messages arrive: a timed receive should expire.
- Trade messages continue but heartbeats stop: compare the current monotonic
  time with the last heartbeat time after each received message.

When the timeout is exceeded:

1. Log a stale-heartbeat incident.
2. Record the affected connection and time window.
3. Close/reconnect the WebSocket.
4. End the current connection session and create a fresh sequence monitor after
   reconnecting.

Make the timeout configurable in `CollectorSettings`; do not hard-code it in
the receive loop.

## Step 8: Preserve state across reconnects and process restarts

Coinbase's outer `sequence_num` restarts at zero on a new WebSocket connection.
Create `CoinbaseFeedContinuityMonitor` inside the connection block so every
connection receives a fresh tracker and connection ID.

Do not compare a new connection's sequence zero with the final sequence from an
older connection. A disconnect is its own degraded interval; record its start
and end timestamps and reconcile the subscribed products separately.

Container/process restart also loses all in-memory observations. Durable gap
records are therefore still required for operational history.

For the first delivery, it is acceptable to ship in-memory detection if the
limitation is documented. The next increment should persist:

- Final envelope sequence per connection
- Final heartbeat counter per connection
- Last successfully received timestamp
- Connection start and disconnect timestamps
- Open data-gap records

Do not store this state only in Spark checkpoints. The gap occurs before Kafka,
while Spark checkpoints describe progress after Kafka.

## Step 9: Reconcile detected gaps through REST

Sequence detection tells us that something may be missing; it does not recover
the missing trades.

For each unresolved product gap:

1. Record the last known-good trade time and the first post-gap trade time.
2. Query Coinbase REST trades for that bounded interval or trade-ID range.
3. Compare REST `trade_id` values with archived `source_event_id` values.
4. Publish missing trades through the normal canonical Kafka path.
5. Reuse `event_id_for_trade()` so replaying a trade produces the same event ID.
6. Mark the gap resolved only after the expected trade IDs are present.
7. Keep an explicit unresolved or failed state if Coinbase can no longer supply
   the interval.

Raw storage may contain source redeliveries. The later curated layer should
deduplicate by deterministic `event_id`.

## Step 10: Add Coinbase-specific tests

Extend `tests/unit/collector/test_coinbase_adapter.py` or create a separate
continuity test module. Cover at least:

- A valid first envelope initializes connection state.
- BTC, ETH, subscription, and heartbeat envelopes advance the same sequence.
- Two consecutive envelopes return `ok`.
- A forward jump logs the exact missing sequence range.
- A duplicate message is reported without moving state.
- An out-of-order message is reported without moving state backward.
- A message containing multiple trades for one product is observed once.
- Envelope sequence and heartbeat counter are tracked independently.
- A heartbeat string counter is parsed and tracked.
- A skipped heartbeat counter creates a gap.
- Malformed heartbeat counters are rejected and logged.
- A heartbeat timeout forces a reconnect.
- A WebSocket reconnect starts a new connection ID and sequence baseline.
- Trade parsing and publication continue after a reported gap.

Use injected/fake clocks and fake messages for timeout tests. Do not make unit
tests sleep for five real seconds and do not connect to Coinbase from unit tests.

## Step 11: Validate the completed change

Run the focused tests first:

```powershell
.\.venv\Scripts\python.exe -m pytest -q `
  tests\unit\collector\test_sequence_tracker.py `
  tests\unit\collector\test_coinbase_adapter.py
```

Then run the full normal suite:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Finally, rebuild the collector and inspect live logs:

```powershell
podman compose up -d --build collector
podman compose logs --follow collector
```

Expected live behavior:

- Normal messages do not flood `INFO` logs.
- Disconnects and stale heartbeats are visible.
- Duplicate, out-of-order, and gap observations include stream and sequence
  details.
- A detected anomaly does not stop normal trade publication.
- Kafka offsets continue increasing after the collector reconnects.

## Suggested delivery order

Keep the work reviewable by implementing it in small slices:

1. Correct and relocate `SequenceTracker`.
2. Add complete tracker unit tests.
3. Wire connection-wide envelope observation into `coinbase.py`.
4. Add cross-product and cross-channel continuity tests.
5. Add heartbeat-counter observation and tests.
6. Add heartbeat timeout/reconnect behavior and tests.
7. Emit durable gap observations and metrics.
8. Add REST reconciliation and deterministic backfill.
9. Add an operational view of unresolved gaps.

## Definition of done

This story is complete when:

- The outer sequence is checked exactly once for every envelope on a connection.
- BTC, ETH, heartbeat, subscription, and unknown-channel envelopes share that
  connection sequence.
- Heartbeat counters are checked for duplicates, reordering, and gaps.
- Complete heartbeat silence is detected within the configured timeout.
- The collector continues receiving data after a sequence anomaly.
- Every anomaly records enough context to identify the connection, potentially
  affected products, and missing range.
- State behavior across reconnects and restarts is documented and tested.
- Missing trades can be reconciled through a bounded, idempotent REST backfill.
- Unit tests cover every state transition and pass with the full project suite.
- The runbook explains how to see unresolved gaps and confirm recovery.
