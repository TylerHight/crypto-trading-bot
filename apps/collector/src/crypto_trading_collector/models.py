import json
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
)

NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


def utc_now() -> datetime:
    """Return a timezone-aware current timestamp in UTC."""

    return datetime.now(UTC)


def event_id_for_trade(exchange: str, symbol: str, source_event_id: str) -> UUID:
    """Return the same UUID whenever the same source trade is received."""

    identity = json.dumps(
        [
            "market.trade.raw",
            "v1",
            exchange.strip().lower(),
            symbol.strip().upper(),
            source_event_id.strip(),
        ],
        separators=(",", ":"),
    )
    return uuid5(NAMESPACE_URL, identity)


class MarketTradeRawEvent(BaseModel):
    """Validated Kafka envelope for one raw market trade."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["market.trade.raw"] = "market.trade.raw"
    schema_version: Literal["v1"] = "v1"

    exchange: NonEmptyString
    symbol: NonEmptyString
    event_time: AwareDatetime
    ingested_at: AwareDatetime = Field(default_factory=utc_now)

    source_event_id: NonEmptyString
    source_sequence: int | None = Field(default=None, ge=0)
    producer: Literal["apps.collector"] = "apps.collector"

    trace_id: UUID = Field(default_factory=uuid4)
    correlation_id: UUID | None = None
    causation_id: UUID | None = None

    payload: dict[str, JsonValue]

    @field_validator("event_time", "ingested_at")
    @classmethod
    def normalize_to_utc(cls, value: datetime) -> datetime:
        """Represent every accepted timestamp in UTC."""

        return value.astimezone(UTC)


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
