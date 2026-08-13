from collections.abc import Mapping
from dataclasses import dataclass
from os import environ


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


@dataclass(frozen=True)
class RawSinkSettings:
    """Environment configuration for the raw Kafka-to-object-storage sink."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    output_path: str
    checkpoint_path: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    trigger_interval: str

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "RawSinkSettings":
        source = environ if values is None else values
        output_path = _required(
            source,
            "RAW_SINK_OUTPUT_PATH",
            "s3a://crypto-data/raw/market_trade_raw/v1",
        ).rstrip("/")
        checkpoint_path = _required(
            source,
            "RAW_SINK_CHECKPOINT_PATH",
            "s3a://crypto-data/checkpoints/raw-market-trades-v1",
        ).rstrip("/")

        if output_path == checkpoint_path:
            raise ValueError("Raw output and checkpoint paths must be different")
        if not output_path.startswith("s3a://"):
            raise ValueError("RAW_SINK_OUTPUT_PATH must use the s3a:// scheme")
        if not checkpoint_path.startswith("s3a://"):
            raise ValueError("RAW_SINK_CHECKPOINT_PATH must use the s3a:// scheme")

        return cls(
            kafka_bootstrap_servers=_required(
                source, "RAW_SINK_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
            kafka_topic=_required(
                source, "RAW_SINK_KAFKA_TOPIC", "market.trades.raw.v1"
            ),
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            s3_endpoint=_required(source, "RAW_SINK_S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=_required(source, "RAW_SINK_S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=_required(source, "RAW_SINK_S3_SECRET_KEY", "minioadmin"),
            trigger_interval=_required(
                source, "RAW_SINK_TRIGGER_INTERVAL", "10 seconds"
            ),
        )
