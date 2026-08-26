from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from crypto_trading_domain import (
    MarketTradeRawEvent,
    event_id_for_trade,
    market_trade_event,
    utc_now,
)
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]

__all__ = [
    "MarketDataQualityEvent",
    "MarketTradeRawEvent",
    "event_id_for_trade",
    "market_trade_event",
    "utc_now",
]


class MarketDataQualityEvent(BaseModel):
    """Validated Kafka envelope for a WebSocket integrity observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: Literal["market.data.quality"] = "market.data.quality"
    schema_version: Literal["v1"] = "v1"

    exchange: NonEmptyString
    connection_id: UUID
    observation_type: Literal[
        "connection_opened",
        "connection_closed",
        "connection_recovered",
        "reconnect_scheduled",
        "reconnect_attempt",
        "sequence_gap",
        "duplicate_sequence",
        "out_of_order_sequence",
        "heartbeat_gap",
        "heartbeat_silence",
        "malformed_message",
        "health_summary",
    ]
    detected_at: AwareDatetime
    connection_started_at: AwareDatetime
    affected_symbols: list[NonEmptyString] = Field(min_length=1)
    channel: str | None = None
    stream: str | None = None

    previous_sequence: int | None = Field(default=None, ge=0)
    current_sequence: int | None = Field(default=None, ge=0)
    missing_from: int | None = Field(default=None, ge=0)
    missing_to: int | None = Field(default=None, ge=0)
    last_envelope_sequence: int | None = Field(default=None, ge=0)
    last_heartbeat_counter: int | None = Field(default=None, ge=0)
    seconds_since_last_message: float | None = Field(default=None, ge=0)
    seconds_since_last_heartbeat: float | None = Field(default=None, ge=0)

    envelopes_observed: int = Field(default=0, ge=0)
    trades_observed: int = Field(default=0, ge=0)
    heartbeats_observed: int = Field(default=0, ge=0)
    sequence_gap_count: int = Field(default=0, ge=0)
    duplicate_sequence_count: int = Field(default=0, ge=0)
    out_of_order_sequence_count: int = Field(default=0, ge=0)
    malformed_message_count: int = Field(default=0, ge=0)

    reason: str | None = Field(default=None, max_length=500)
    message_excerpt: str | None = Field(default=None, max_length=1024)
    message_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    producer: Literal["apps.collector"] = "apps.collector"
    trace_id: UUID = Field(default_factory=uuid4)

    @field_validator("detected_at", "connection_started_at")
    @classmethod
    def normalize_quality_timestamp_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @field_validator("affected_symbols")
    @classmethod
    def normalize_quality_symbols(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(value.strip().upper() for value in values))
