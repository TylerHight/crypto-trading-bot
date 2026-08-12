from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ExchangeTrade:
    """One trade extracted from an exchange WebSocket message."""

    exchange: str
    symbol: str
    source_event_id: str
    source_sequence: int | None
    event_time: datetime
    raw_payload: dict[str, Any]
