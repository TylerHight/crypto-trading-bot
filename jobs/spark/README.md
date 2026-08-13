# Spark Jobs

This directory owns PySpark Structured Streaming, batch, and deterministic replay implementations.

The same DataFrame transformation functions should serve live and replay entry points. Environment differences—local Spark versus EMR Serverless, MinIO versus S3, and local Kafka versus MSK—are supplied through configuration rather than transformation branches.

Keep Spark entry-point wiring in `entrypoints/`, reusable DataFrame logic in `transforms/`, and Spark-facing schema definitions in `schemas/`. Exchange clients, Airflow DAGs, dbt models, and trading decisions do not belong here.

## Implemented raw sink

`entrypoints/raw_market_trades.py` consumes `market.trades.raw.v1` with Structured Streaming and writes immutable Parquet to `s3a://crypto-data/raw/market_trade_raw/v1`. It uses `s3a://crypto-data/checkpoints/raw-market-trades-v1` for durable Kafka offset and commit state.

`transforms/raw_kafka.py` projects Kafka metadata without parsing the event envelope. Keys, values, and header values remain binary so the raw layer can reproduce the consumed record exactly. See the [local pipeline guide](../../infra/compose/README.md) for runtime commands.
