from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def _positive_integer(value: str, name: str, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not minimum <= parsed <= maximum:
        raise ValueError(f"{name} must be from {minimum} through {maximum}")
    return parsed


@dataclass(frozen=True)
class DashboardSettings:
    """Read-only local dashboard configuration."""

    host: str = "127.0.0.1"
    port: int = 8090
    refresh_seconds: int = 30
    stale_after_seconds: int = 180
    artifact_stale_after_seconds: int = 86_400
    request_timeout_seconds: int = 3
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_quality_topic: str = "market.data.quality.v1"
    kafka_scan_records: int = 200
    s3_endpoint_url: str | None = "http://127.0.0.1:9000"
    s3_access_key: str | None = field(default="minioadmin", repr=False)
    s3_secret_key: str | None = field(default="minioadmin", repr=False)
    s3_region: str = "us-east-1"
    s3_bucket: str = "crypto-data"
    raw_prefix: str = "raw/market_trade_raw/v1/"
    raw_audit_prefix: str = "reconciliation/raw-integrity/"
    curated_manifest_prefix: str = "curated/market_trades/v1/manifests/"
    candle_manifest_prefix: str = "analytics/market_candles/v1/manifests/"
    selection_manifest_prefix: str = "analytics/strategy_experiments/v1/selections/"
    evaluation_manifest_prefix: str = "analytics/strategy_experiments/v1/evaluations/"
    research_report_prefix: str = "analytics/strategy_experiments/v1/longer_research/reports/"
    historical_manifest_prefix: str = "analytics/historical_candles/v1/manifests/"
    database_url: str = field(
        default="postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
        repr=False,
    )
    pilot_plan_path: Path = Path("pilots/btc-usd-sma-forward-v1.json")

    def __post_init__(self) -> None:
        if self.host not in LOOPBACK_HOSTS:
            raise ValueError("operator dashboard may bind only to a loopback host")
        if not 0 <= self.port <= 65535:
            raise ValueError("operator dashboard port must be from 0 through 65535")
        if self.refresh_seconds < 5:
            raise ValueError("operator dashboard refresh must be at least 5 seconds")
        if self.stale_after_seconds < self.refresh_seconds:
            raise ValueError("stale threshold must be at least one refresh interval")
        if self.artifact_stale_after_seconds < self.refresh_seconds:
            raise ValueError("artifact stale threshold must be at least one refresh interval")
        if self.request_timeout_seconds <= 0:
            raise ValueError("dashboard request timeout must be positive")
        if self.kafka_scan_records <= 0:
            raise ValueError("dashboard Kafka scan count must be positive")
        if not self.s3_bucket.strip():
            raise ValueError("dashboard S3 bucket must not be empty")

    @classmethod
    def from_env(cls) -> DashboardSettings:
        return cls(
            host=os.getenv("OPERATOR_DASHBOARD_HOST", "127.0.0.1"),
            port=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_PORT", "8090"),
                "OPERATOR_DASHBOARD_PORT",
                minimum=1,
                maximum=65535,
            ),
            refresh_seconds=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_REFRESH_SECONDS", "30"),
                "OPERATOR_DASHBOARD_REFRESH_SECONDS",
                minimum=5,
                maximum=3600,
            ),
            stale_after_seconds=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_STALE_AFTER_SECONDS", "180"),
                "OPERATOR_DASHBOARD_STALE_AFTER_SECONDS",
                minimum=5,
                maximum=86_400,
            ),
            artifact_stale_after_seconds=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_ARTIFACT_STALE_AFTER_SECONDS", "86400"),
                "OPERATOR_DASHBOARD_ARTIFACT_STALE_AFTER_SECONDS",
                minimum=5,
                maximum=604_800,
            ),
            request_timeout_seconds=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_REQUEST_TIMEOUT_SECONDS", "3"),
                "OPERATOR_DASHBOARD_REQUEST_TIMEOUT_SECONDS",
                minimum=1,
                maximum=60,
            ),
            kafka_bootstrap_servers=os.getenv(
                "OPERATOR_DASHBOARD_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
            ),
            kafka_quality_topic=os.getenv(
                "OPERATOR_DASHBOARD_KAFKA_QUALITY_TOPIC", "market.data.quality.v1"
            ),
            kafka_scan_records=_positive_integer(
                os.getenv("OPERATOR_DASHBOARD_KAFKA_SCAN_RECORDS", "200"),
                "OPERATOR_DASHBOARD_KAFKA_SCAN_RECORDS",
                minimum=1,
                maximum=10_000,
            ),
            s3_endpoint_url=os.getenv("OPERATOR_DASHBOARD_S3_ENDPOINT", "http://127.0.0.1:9000"),
            s3_access_key=os.getenv("OPERATOR_DASHBOARD_S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=os.getenv("OPERATOR_DASHBOARD_S3_SECRET_KEY", "minioadmin"),
            s3_region=os.getenv("OPERATOR_DASHBOARD_S3_REGION", "us-east-1"),
            s3_bucket=os.getenv("OPERATOR_DASHBOARD_S3_BUCKET", "crypto-data"),
            raw_prefix=os.getenv("OPERATOR_DASHBOARD_RAW_PREFIX", "raw/market_trade_raw/v1/"),
            raw_audit_prefix=os.getenv(
                "OPERATOR_DASHBOARD_RAW_AUDIT_PREFIX", "reconciliation/raw-integrity/"
            ),
            curated_manifest_prefix=os.getenv(
                "OPERATOR_DASHBOARD_CURATED_MANIFEST_PREFIX",
                "curated/market_trades/v1/manifests/",
            ),
            candle_manifest_prefix=os.getenv(
                "OPERATOR_DASHBOARD_CANDLE_MANIFEST_PREFIX",
                "analytics/market_candles/v1/manifests/",
            ),
            selection_manifest_prefix=os.getenv(
                "OPERATOR_DASHBOARD_SELECTION_MANIFEST_PREFIX",
                "analytics/strategy_experiments/v1/selections/",
            ),
            evaluation_manifest_prefix=os.getenv(
                "OPERATOR_DASHBOARD_EVALUATION_MANIFEST_PREFIX",
                "analytics/strategy_experiments/v1/evaluations/",
            ),
            research_report_prefix=os.getenv(
                "OPERATOR_DASHBOARD_RESEARCH_REPORT_PREFIX",
                "analytics/strategy_experiments/v1/longer_research/reports/",
            ),
            historical_manifest_prefix=os.getenv(
                "OPERATOR_DASHBOARD_HISTORICAL_MANIFEST_PREFIX",
                "analytics/historical_candles/v1/manifests/",
            ),
            database_url=os.getenv(
                "OPERATOR_DASHBOARD_DATABASE_URL",
                "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
            ),
            pilot_plan_path=Path(
                os.getenv(
                    "OPERATOR_DASHBOARD_PILOT_PLAN",
                    "pilots/btc-usd-sma-forward-v1.json",
                )
            ),
        )
