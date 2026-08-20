# Market Data Collector

The collector is the always-on public market-data ingestion service.

Responsibilities:

- Connect and reconnect to the selected exchange WebSocket.
- Subscribe to configured instruments and preserve source identifiers and timestamps.
- Validate the event envelope without performing analytical transformations.
- Publish raw events to versioned Kafka topics with stable keys.
- Publish durable connection health and source-integrity observations.

The collector does not build candles, write analytical marts, make trading decisions, or call private trading endpoints. Exchange-neutral interfaces belong in `packages/exchange_adapters/`; shared event definitions belong in `schemas/`.

## WebSocket integrity events

Trades continue to use `market.trades.raw.v1`. The collector separately writes
WebSocket monitoring records to `market.data.quality.v1`, keyed by
`exchange:connection_id`. Keeping the topics separate prevents operational
observations from being mistaken for trades.

The quality topic includes connection open/close and recovery events, envelope
sequence anomalies, heartbeat-counter gaps, heartbeat silence, malformed-message
fingerprints, and periodic positive health summaries. A malformed message contains
only a bounded excerpt and SHA-256 hash, never an unbounded raw payload.

Important settings, shown with their defaults, are:

| Environment variable | Default | Meaning |
|---|---:|---|
| `COLLECTOR_KAFKA_QUALITY_TOPIC` | `market.data.quality.v1` | Durable quality-event destination |
| `COLLECTOR_HEARTBEAT_TIMEOUT_SECONDS` | `10` | Reconnect after no valid heartbeat for this long |
| `COLLECTOR_HEALTH_SUMMARY_INTERVAL_SECONDS` | `60` | Positive health-event interval |
| `COLLECTOR_MALFORMED_MESSAGE_EXCERPT_LENGTH` | `256` | Maximum diagnostic excerpt characters (maximum 1024) |

The heartbeat timeout uses a monotonic clock, so wall-clock adjustments cannot
hide or invent a silence interval. Each reconnect gets a new connection ID and a
fresh sequence baseline. If publishing a quality event fails, the collector logs
an error with the connection ID and observation type; it still processes valid
trades so an observability outage does not become a market-data outage.

The language-neutral contract and example are in
`schemas/events/market.data.quality.v1/`.
