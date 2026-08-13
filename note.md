Implemented and finished the Kafka-to-MinIO raw pipeline.

Key files:

- [docker-compose.yml](C:/Development/Projects/crypto-trading-bot/docker-compose.yml)
- [raw_market_trades.py](C:/Development/Projects/crypto-trading-bot/jobs/spark/entrypoints/raw_market_trades.py)
- [raw_kafka.py](C:/Development/Projects/crypto-trading-bot/jobs/spark/transforms/raw_kafka.py)
- [integration test](C:/Development/Projects/crypto-trading-bot/tests/integration/test_raw_market_trades_pipeline.py)
- [operating guide](C:/Development/Projects/crypto-trading-bot/infra/compose/README.md)

Completed:

- Containerized Coinbase collector
- Kafka and automatic topic creation
- MinIO and automatic bucket creation
- Spark Structured Streaming raw sink
- Byte-preserving Kafka key, value, and headers
- Kafka topic/partition/offset/timestamp metadata
- UTC date/hour Parquet partitioning
- Persistent Spark checkpoints
- Health checks and localhost-only ports
- Unit, integration, lint, and type-check coverage
- Startup and inspection documentation

Verification results:

- 25 tests passed; one live integration test skipped by default
- Live Kafka-to-Parquet integration test passed
- Ruff passed
- mypy passed across 16 source files
- Clean `podman compose down` followed by `up -d --build` passed
- Restart recovery advanced from offset 5,283 to 5,700 with zero duplicate Kafka identities

The stack is currently running:

- Kafka UI: <http://localhost:8083>
- MinIO: <http://localhost:9001>
- MinIO login: `minioadmin` / `minioadmin`

The initializer containers showing `Exited (0)` is expected. Changes remain uncommitted.