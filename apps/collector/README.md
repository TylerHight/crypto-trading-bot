# Market Data Collector

The collector is the always-on public market-data ingestion service.

Responsibilities:

- Connect and reconnect to the selected exchange WebSocket.
- Subscribe to configured instruments and preserve source identifiers and timestamps.
- Validate the event envelope without performing analytical transformations.
- Publish raw events to versioned Kafka topics with stable keys.
- Expose health, throughput, reconnect, and source-gap metrics.

The collector does not build candles, write analytical marts, make trading decisions, or call private trading endpoints. Exchange-neutral interfaces belong in `packages/exchange_adapters/`; shared event definitions belong in `schemas/`.
