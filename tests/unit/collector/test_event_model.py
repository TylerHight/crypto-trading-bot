import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from crypto_trading_collector.models import MarketTradeRawEvent
from pydantic import ValidationError


def valid_values() -> dict[str, Any]:
    return {
        "event_id": uuid4(),
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "event_time": datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc),
        "source_event_id": "123",
        "source_sequence": 42,
        "correlation_id": None,
        "causation_id": None,
        "payload": {"trade_id": "123", "price": "65000.10"},
    }


def test_rejects_empty_symbol() -> None:
    values = valid_values()
    values["symbol"] = "   "

    with pytest.raises(ValidationError):
        MarketTradeRawEvent(**values)


def test_rejects_naive_event_time() -> None:
    values = valid_values()
    # A naive timestamp is deliberately constructed to prove validation rejects it.
    values["event_time"] = datetime(2026, 8, 11, 10, 0)  # noqa: DTZ001

    with pytest.raises(ValidationError):
        MarketTradeRawEvent(**values)


def test_rejects_unknown_envelope_field() -> None:
    values = valid_values()
    values["unexpected"] = "mistake"

    with pytest.raises(ValidationError):
        MarketTradeRawEvent(**values)


def test_rejects_non_json_payload_value() -> None:
    values = valid_values()
    values["payload"] = {"not_json": {1, 2, 3}}

    with pytest.raises(ValidationError):
        MarketTradeRawEvent(**values)


def test_normalizes_timestamp_to_utc() -> None:
    values = valid_values()
    central_time = timezone(timedelta(hours=-5))
    values["event_time"] = datetime(2026, 8, 11, 10, 0, tzinfo=central_time)

    event = MarketTradeRawEvent(**values)

    assert event.event_time.utcoffset() == timedelta(0)


def test_serialized_event_is_json() -> None:
    event = MarketTradeRawEvent(**valid_values())

    encoded = event.model_dump_json()

    assert json.loads(encoded)["symbol"] == "BTC-USD"
