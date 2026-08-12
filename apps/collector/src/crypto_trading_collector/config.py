from typing import Literal, Self

from pydantic import AnyUrl, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CollectorSettings(BaseSettings):
    """Validated environment configuration for the collector process."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="COLLECTOR_",
        extra="ignore",
        validate_default=True,
    )

    exchange: Literal["coinbase"] = "coinbase"
    websocket_url: AnyUrl = AnyUrl("wss://advanced-trade-ws.coinbase.com")
    symbols: list[str] = Field(default_factory=lambda: ["BTC-USD"])

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "market.trades.raw.v1"
    kafka_client_id: str = "market-data-collector"
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_SSL"] = "PLAINTEXT"
    kafka_sasl_mechanism: (
        Literal[
            "PLAIN",
            "SCRAM-SHA-256",
            "SCRAM-SHA-512",
        ]
        | None
    ) = None
    kafka_sasl_username: SecretStr | None = None
    kafka_sasl_password: SecretStr | None = None

    reconnect_initial_seconds: float = Field(default=1.0, gt=0)
    reconnect_max_seconds: float = Field(default=30.0, gt=0)
    kafka_poll_timeout_seconds: float = Field(default=0.1, gt=0)
    kafka_flush_timeout_seconds: float = Field(default=10.0, gt=0)
    kafka_queue_full_retries: int = Field(default=3, ge=0)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, symbols: list[str]) -> list[str]:
        cleaned = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        if not cleaned:
            raise ValueError("At least one non-empty symbol is required")
        return list(dict.fromkeys(cleaned))

    @field_validator("kafka_bootstrap_servers", "kafka_topic", "kafka_client_id")
    @classmethod
    def validate_non_empty_setting(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty")
        return cleaned

    @model_validator(mode="after")
    def validate_kafka_authentication(self) -> Self:
        authentication_values = (
            self.kafka_sasl_mechanism,
            self.kafka_sasl_username,
            self.kafka_sasl_password,
        )

        if self.kafka_security_protocol == "SASL_SSL":
            if not all(authentication_values):
                raise ValueError("SASL_SSL requires a mechanism, username, and password")
        elif any(authentication_values):
            raise ValueError("SASL credentials require SASL_SSL")

        return self
