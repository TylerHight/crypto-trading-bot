from collections.abc import Mapping
from dataclasses import dataclass
from os import environ


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


def _positive_int(
    values: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    """Read a positive integer while producing a useful configuration error."""

    raw_value = values.get(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
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


@dataclass(frozen=True)
class RawAuditSettings:
    """Configuration for the bounded Kafka-to-Parquet integrity audit."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    input_path: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    sample_limit: int

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "RawAuditSettings":
        source = environ if values is None else values
        input_path = _required(
            source,
            "RAW_AUDIT_INPUT_PATH",
            "s3a://crypto-data/raw/market_trade_raw/v1",
        ).rstrip("/")

        if not input_path.startswith("s3a://"):
            raise ValueError("RAW_AUDIT_INPUT_PATH must use the s3a:// scheme")

        return cls(
            kafka_bootstrap_servers=_required(
                source,
                "RAW_AUDIT_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:29092",
            ),
            kafka_topic=_required(
                source,
                "RAW_AUDIT_KAFKA_TOPIC",
                "market.trades.raw.v1",
            ),
            input_path=input_path,
            s3_endpoint=_required(
                source,
                "RAW_AUDIT_S3_ENDPOINT",
                "http://minio:9000",
            ),
            s3_access_key=_required(
                source,
                "RAW_AUDIT_S3_ACCESS_KEY",
                "minioadmin",
            ),
            s3_secret_key=_required(
                source,
                "RAW_AUDIT_S3_SECRET_KEY",
                "minioadmin",
            ),
            sample_limit=_positive_int(
                source,
                "RAW_AUDIT_SAMPLE_LIMIT",
                20,
            ),
        )
