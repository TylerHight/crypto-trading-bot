# Local dependencies with Podman Compose

The root [`docker-compose.yml`](../../docker-compose.yml) starts the dependencies used by the currently implemented market-data collector:

- one Apache Kafka broker in KRaft mode (ZooKeeper is not required);
- a one-shot initializer that creates `market.trades.raw.v1`; and
- Kafbat UI for inspecting topics and messages at <http://localhost:8083>.

Kafka is available to applications running on the Windows host at `localhost:9092`. Containers on the Compose network use `kafka:29092` instead. Kafka data is kept in the named `kafka-data` volume.

## Start

Run these commands from the repository root:

```powershell
podman machine start
podman compose up -d
podman compose ps
```

`podman machine start` can report that the machine is already running; that is harmless. The collector's checked-in example configuration already uses `localhost:9092` and needs no Kafka-specific changes.

Start the collector in a separate terminal:

```powershell
.\.venv\Scripts\crypto-collector.exe
```

## Verify

List the automatically created topic:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Stream collector events in the terminal:

```powershell
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic market.trades.raw.v1 `
  --from-beginning `
  --property print.key=true
```

Kafka UI is available at <http://localhost:8083>.

## Stop or reset

Stop the services without deleting Kafka data:

```powershell
podman compose down
```

Delete the local Kafka volume as well, resetting all topics and messages:

```powershell
podman compose down --volumes
```

The broader architecture calls for MinIO, PostgreSQL, Spark, and Airflow later. They are intentionally not in this Compose file because no currently implemented application uses them yet.
