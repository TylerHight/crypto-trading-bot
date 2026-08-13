# Local pipeline with Podman Compose

The root [`docker-compose.yml`](../../docker-compose.yml) runs the current Phase 1 pipeline:

```text
Coinbase WebSocket -> collector -> Kafka -> Spark raw sink -> MinIO Parquet
```

The stack contains:

- Kafka in single-node KRaft mode and a one-shot topic initializer;
- Kafbat UI at <http://localhost:8083>;
- the containerized Coinbase collector;
- MinIO and a one-shot `crypto-data` bucket initializer;
- MinIO console at <http://localhost:9001>; and
- a checkpointed PySpark Structured Streaming raw sink.

Kafka is available from Windows at `localhost:9092` and inside Compose at `kafka:29092`. MinIO's S3 API is available at `localhost:9000`. The local-only MinIO credentials are `minioadmin` / `minioadmin`.

## Start

From the repository root:

```powershell
podman machine start
podman compose up -d --build
podman compose ps
```

`podman machine start` may report that the machine is already running. That is harmless. The first startup pulls the Spark and MinIO images and downloads Spark's Kafka and S3 connector JARs; later starts reuse named caches.

The `kafka-init` and `minio-init` services should exit with status `0`. They are one-shot initializers, not failed services. Kafka, MinIO, the collector, Kafka UI, and `raw-sink` remain running.

Follow application logs with:

```powershell
podman compose logs --follow collector raw-sink
```

## Inspect Kafka

List topics:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Stream records, including their keys:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic market.trades.raw.v1 `
  --from-beginning `
  --property print.key=true
```

Kafka UI provides browser-based topic and message inspection at <http://localhost:8083>.

## Inspect raw Parquet

Open <http://localhost:9001>, sign in with `minioadmin` / `minioadmin`, and browse the `crypto-data` bucket. Raw records use this layout:

```text
raw/market_trade_raw/v1/
  event_date=YYYY-MM-DD/
    event_hour=HH/
      *.snappy.parquet

checkpoints/raw-market-trades-v1/
```

List Parquet objects from PowerShell:

```powershell
podman compose exec minio /bin/sh -c `
  'mc alias set inspect http://localhost:9000 minioadmin minioadmin >/dev/null && mc find inspect/crypto-data --name "*.parquet"'
```

Each Parquet row preserves the original Kafka key, value, and headers as binary plus the topic, partition, offset, timestamp, and timestamp type. `event_date` and `event_hour` are UTC storage partitions derived from the Kafka timestamp. Event parsing and normalization intentionally belong to the later curated layer.

## Test

Run the normal suite; the live integration test is skipped unless explicitly enabled:

```powershell
.\.venv\Scripts\uv.exe sync --all-packages --all-extras
.\.venv\Scripts\python.exe -m pytest -q
```

With the Compose stack running, verify that a known Kafka record is archived byte-for-byte in Parquet:

```powershell
$env:RUN_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q tests\integration\test_raw_market_trades_pipeline.py
Remove-Item Env:RUN_INTEGRATION_TESTS
```

## Stop or reset

Stop services while retaining Kafka messages, MinIO objects, checkpoints, and the Spark dependency cache:

```powershell
podman compose down
```

Delete all local pipeline data and caches:

```powershell
podman compose down --volumes
```

The reset is destructive: all locally archived Kafka and MinIO data is removed.

PostgreSQL, Airflow, dbt, and trading services remain future phases and are intentionally absent from this stack.
