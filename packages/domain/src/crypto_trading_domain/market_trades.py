import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
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

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def utc_now() -> datetime:
    """Return a timezone-aware current timestamp in UTC."""

    return datetime.now(UTC)


def event_id_for_trade(exchange: str, symbol: str, source_event_id: str) -> UUID:
    """Return the stable logical event ID for an exchange trade."""

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
    """Canonical Kafka envelope shared by live collection and backfill."""

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
    producer: Literal["apps.collector", "apps.historical_backfill"] = "apps.collector"
    trace_id: UUID = Field(default_factory=uuid4)
    correlation_id: UUID | None = None
    causation_id: UUID | None = None
    payload: dict[str, JsonValue]

    @field_validator("event_time", "ingested_at")
    @classmethod
    def normalize_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


def market_trade_event(
    *,
    exchange: str,
    symbol: str,
    source_event_id: str,
    event_time: datetime,
    payload: Mapping[str, Any],
    source_sequence: int | None = None,
    producer: Literal["apps.collector", "apps.historical_backfill"] = "apps.collector",
    correlation_id: UUID | None = None,
    causation_id: UUID | None = None,
) -> MarketTradeRawEvent:
    """Construct a canonical trade envelope with its stable logical identity."""

    return MarketTradeRawEvent(
        event_id=event_id_for_trade(exchange, symbol, source_event_id),
        exchange=exchange,
        symbol=symbol,
        event_time=event_time,
        source_event_id=source_event_id,
        source_sequence=source_sequence,
        producer=producer,
        correlation_id=correlation_id,
        causation_id=causation_id,
        payload=dict(payload),
    )
