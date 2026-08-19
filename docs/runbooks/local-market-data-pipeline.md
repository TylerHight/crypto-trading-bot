# Local market-data pipeline operations

## Purpose

Use this runbook to start, verify, monitor, restart, and stop the local Phase 1 market-data pipeline:

```text
Coinbase WebSocket -> collector -> Kafka -> Spark raw sink -> MinIO Parquet
```

This procedure verifies more than container health. It checks that Coinbase records are entering Kafka, Spark is committing progress, raw Parquet objects are being created, and a known Kafka record survives storage unchanged.

## Scope and safety

This runbook applies only to the local Podman Compose environment defined in [`docker-compose.yml`](../../docker-compose.yml). It uses public Coinbase market data and development-only MinIO credentials.

Routine start, restart, and `podman compose down` commands preserve the named Kafka and MinIO volumes. Do not run `podman compose down --volumes` unless you intend to delete all local Kafka messages, MinIO objects, Spark checkpoints, and the Spark connector cache.

Do not delete or manually edit the Spark checkpoint independently of the raw Parquet output. Treat them as one recovery unit.

## Prerequisites

- Windows with WSL 2 and Podman installed.
- The existing `podman-machine-default` VM. Do not run `podman machine init` if it already exists.
- PowerShell opened at the repository root.
- Internet access for Coinbase and for the first container/JAR download.
- The repository's `.venv`, prepared with all development dependencies when running tests.

Confirm the working directory:

```powershell
Get-Location
```

Expected path:

```text
C:\Development\Projects\crypto-trading-bot
```

## Start the pipeline

Start the Podman VM:

```powershell
podman machine start
```

It is harmless if Podman reports that the machine is already running. Give a newly started VM 15–30 seconds to finish its Windows-to-WSL socket forwarding, then verify it:

```powershell
podman info --format "host={{.Host.OS}} rootless={{.Host.Security.Rootless}} runtime={{.Host.OCIRuntime.Name}}"
```

Expected evidence includes `host=linux`, `rootless=true`, and a runtime such as `crun`.

Build and start the stack:

```powershell
podman compose up -d --build
podman compose ps
```

On the first run, Spark and its Kafka/S3 connector artifacts can take several minutes to download. Expected steady state:

| Service | Expected state | Purpose |
|---|---|---|
| `kafka` | `Up ... (healthy)` | Stores incoming trade events |
| `minio` | `Up ... (healthy)` | Stores raw Parquet and Spark checkpoints |
| `collector` | `Up` | Reads public Coinbase trades and publishes Kafka events |
| `raw-sink` | `Up ... (healthy)` | Archives Kafka records to MinIO with Spark |
| `kafka-ui` | `Up` | Browser-based Kafka inspection |
| `kafka-init` | `Exited (0)` | Successfully created or verified the topic |
| `minio-init` | `Exited (0)` | Successfully created or verified the bucket |

`Exited (0)` is success for the two one-shot initializer services.

If Podman cannot connect even though the VM claims to be running, follow [Podman machine connection recovery](podman-machine-connection-recovery.md).

## Verify data flow

Complete each check in order. A later check cannot compensate for a failed earlier one.

### 1. Confirm service health

```powershell
podman compose ps
```

If a long-running service is absent, exited, unhealthy, or repeatedly restarting, inspect it before continuing:

```powershell
podman compose logs --tail 100 collector
podman compose logs --tail 100 raw-sink
podman compose logs --tail 100 kafka
podman compose logs --tail 100 minio
```

### 2. Confirm the collector receives Coinbase data

```powershell
podman compose logs --tail 100 collector
```

Expected evidence includes:

```text
Connected to Coinbase public WebSocket for BTC-USD,ETH-USD
```

The collector logs successful delivery at `DEBUG`, so the absence of one log line per trade is expected at the default `INFO` level.

### 3. Confirm Kafka contains and continues receiving records

List the topic:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Expected topic:

```text
market.trades.raw.v1
```

Capture the current end offset:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh `
  --bootstrap-server localhost:9092 `
  --topic market.trades.raw.v1
```

Example:

```text
market.trades.raw.v1:0:5701
```

Run the command again after several seconds. With active BTC-USD and ETH-USD trading, the final number should increase.

For browser inspection, open <http://localhost:8083>, select the `local` cluster, and open `market.trades.raw.v1`. A normal record has a key such as `coinbase:BTC-USD` and a JSON value with `event_type` equal to `market.trade.raw` and `schema_version` equal to `v1`.

### 4. Confirm Spark is running and committing progress

```powershell
podman compose logs --tail 100 raw-sink
```

Warnings about the native Hadoop library, missing Hadoop metrics configuration, or adaptive execution being disabled for streaming are expected locally. Exceptions, a terminated streaming query, or repeated container restarts are not expected.

List Spark checkpoint commits:

```powershell
podman compose exec minio /bin/sh -c `
  'mc alias set inspect http://localhost:9000 minioadmin minioadmin >/dev/null && mc find inspect/crypto-data/checkpoints/raw-market-trades-v1/commits'
```

Expected evidence is a growing sequence such as:

```text
inspect/crypto-data/checkpoints/raw-market-trades-v1/commits/0
inspect/crypto-data/checkpoints/raw-market-trades-v1/commits/1
```

### 5. Confirm raw Parquet is stored in MinIO

List raw objects:

```powershell
podman compose exec minio /bin/sh -c `
  'mc alias set inspect http://localhost:9000 minioadmin minioadmin >/dev/null && mc find inspect/crypto-data --name "*.parquet"'
```

Expected layout:

```text
inspect/crypto-data/raw/market_trade_raw/v1/
  event_date=YYYY-MM-DD/
    event_hour=HH/
      part-....snappy.parquet
```

The date and hour partitions use the Kafka timestamp in UTC. You can also browse the `crypto-data` bucket at <http://localhost:9001> using `minioadmin` / `minioadmin`.

### 6. Verify record fidelity end to end

The live integration test publishes a uniquely identifiable record to Kafka, waits for Spark, reads the resulting Parquet object from MinIO, and verifies its topic, partition, offset, binary key/value, and headers.

Prepare dependencies if needed:

```powershell
.\.venv\Scripts\uv.exe sync --all-packages --all-extras
```

Run the test:

```powershell
$env:RUN_INTEGRATION_TESTS = "1"
try {
    .\.venv\Scripts\python.exe -m pytest -q `
      tests\integration\test_raw_market_trades_pipeline.py
} finally {
    Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue
}
```

Expected result:

```text
1 passed
```

This is the strongest current proof that data is being imported and stored correctly.

## Routine monitoring

Follow the two application processes:

```powershell
podman compose logs --follow collector raw-sink
```

Press `Ctrl+C` to stop following logs; the containers continue running.

For a concise operational check, verify all four signals:

1. `podman compose ps` shows Kafka, MinIO, and the raw sink healthy.
2. The Kafka end offset increases.
3. Spark checkpoint commit numbers increase.
4. New Parquet objects appear beneath the current UTC date/hour prefix.

Container health alone proves process availability, not end-to-end data flow.

## Restart and recovery procedures

### Restart the collector

Use this after a persistent Coinbase connection problem:

```powershell
podman compose restart collector
podman compose logs --follow collector
```

Verify that it reconnects and that the Kafka end offset resumes increasing.

### Restart the raw sink

Use this after a transient Spark or MinIO failure:

```powershell
podman compose up -d --no-deps raw-sink
podman compose ps
podman compose logs --tail 100 raw-sink
```

Use `up`, not `restart`, here. With the Podman Compose provider, `restart` can fail
when the sink's successful one-shot initializer dependencies are already in an
`Exited (0)` state. The `up` command also starts an existing stopped sink without
recreating its named data volumes or Spark checkpoint.

Verify all of the following:

- `raw-sink` becomes healthy.
- New checkpoint commit numbers appear.
- Parquet objects continue to appear.
- The live integration test passes.

Spark resumes from `checkpoints/raw-market-trades-v1`; do not remove the checkpoint as a routine retry mechanism.

### Restart the complete stack without deleting data

```powershell
podman compose down
podman compose up -d
podman compose ps
```

Named volumes remain intact. Repeat the end-to-end verification after startup.

## Troubleshooting matrix

| Symptom | Likely cause | Immediate evidence | Action |
|---|---|---|---|
| `podman ps` cannot connect | Stale WSL/Podman socket proxy | `podman machine list`, connection error port | Use the [Podman recovery runbook](podman-machine-connection-recovery.md) |
| Collector exits or reconnects continuously | Coinbase network/feed issue or Kafka unavailable | Collector logs | Verify internet access and Kafka health; restart collector after dependency recovery |
| Kafka end offset does not increase | Collector is disconnected or publication is failing | Collector logs and Kafka health | Restore collector-to-Kafka flow before inspecting Spark |
| Kafka grows but checkpoint commits do not | Raw sink cannot consume or commit | Raw-sink logs | Verify Kafka, MinIO, connector cache, and checkpoint access |
| Raw sink exits with `Timeout ... before the position ... could be determined` | Kafka or the Podman VM was temporarily unavailable to Spark | `podman compose ps` and raw-sink logs | Restore Kafka/Podman health, then run `podman compose up -d --no-deps raw-sink` |
| Checkpoints grow but no Parquet appears | Output commit or object-storage problem | Raw-sink and MinIO logs | Verify bucket access and `RAW_SINK_OUTPUT_PATH` |
| `kafka-init` or `minio-init` shows `Exited (0)` | Normal one-shot completion | Exit code `0` | No action |
| Duplicate `event_id` values exist in raw data | Possible source redelivery; raw is intentionally immutable | Inspect event values | Handle during the future curated deduplication stage |
| Duplicate `(topic, partition, offset)` rows exist | Raw archive correctness failure | Integration/recovery analysis | Stop the raw sink and preserve Kafka, MinIO, and checkpoints for investigation |
| Spark reports data loss or unavailable offsets | Kafka retention passed unread checkpoint offsets | Raw-sink exception with `failOnDataLoss=true` | Stop changes, preserve evidence, and investigate before any checkpoint reset |

## Stop the pipeline

Stop services while retaining data:

```powershell
podman compose down
```

Optionally stop the VM after Compose has shut down:

```powershell
podman machine stop
```

## Destructive local reset

Use only when all local Kafka messages, archived Parquet, and checkpoints can be discarded. This cannot be undone through the project.

First confirm that the current directory is the intended repository and inspect the services:

```powershell
Get-Location
podman compose ps
```

Then remove containers and named volumes:

```powershell
podman compose down --volumes
```

Start with an empty local environment:

```powershell
podman compose up -d --build
```

Repeat every verification step. Do not use a destructive reset to conceal an unexplained data-loss, duplication, or checkpoint incident.

## Escalation and evidence preservation

Stop and preserve evidence before making further changes when:

- `(topic, partition, offset)` appears more than once in raw Parquet;
- Kafka contains a record that never reaches Parquet after the sink is healthy and multiple trigger intervals have passed;
- Spark reports that checkpoint state is corrupt or Kafka data was lost;
- MinIO data or a named volume disappears unexpectedly; or
- recovery would require deleting a checkpoint, volume, or Podman machine.

Capture this evidence:

```powershell
podman compose ps
podman compose --no-ansi logs --tail 500 collector
podman compose --no-ansi logs --tail 500 raw-sink
podman compose --no-ansi logs --tail 500 kafka
podman compose --no-ansi logs --tail 500 minio
podman system connection list
podman machine list
```

Record the relevant Kafka topic, partition, offsets, UTC time window, and MinIO object prefix. Do not delete data or restart repeatedly until the evidence is saved.
