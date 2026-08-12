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
