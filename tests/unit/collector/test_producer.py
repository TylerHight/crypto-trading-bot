from collections.abc import Callable
from typing import Any

import pytest
from crypto_trading_collector.config import CollectorSettings
from crypto_trading_collector.models import MarketTradeRawEvent, event_id_for_trade
from crypto_trading_collector.producer import KafkaEventPublisher


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.flush_result = 0

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[..., None],
    ) -> None:
        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return self.flush_result


def build_event() -> MarketTradeRawEvent:
    return MarketTradeRawEvent(
        event_id=event_id_for_trade("coinbase", "BTC-USD", "123"),
        exchange="coinbase",
        symbol="BTC-USD",
        event_time="2026-08-11T15:30:00Z",
        source_event_id="123",
        source_sequence=42,
        correlation_id=None,
        causation_id=None,
        payload={"trade_id": "123"},
    )


def test_uses_stable_exchange_and_symbol_key() -> None:
    settings = CollectorSettings()
    fake = FakeProducer()
    publisher = KafkaEventPublisher(settings, producer=fake)

    publisher.publish(build_event())

    assert fake.messages[0]["topic"] == "market.trades.raw.v1"
    assert fake.messages[0]["key"] == b"coinbase:BTC-USD"


def test_shutdown_reports_undelivered_messages() -> None:
    settings = CollectorSettings()
    fake = FakeProducer()
    fake.flush_result = 1
    publisher = KafkaEventPublisher(settings, producer=fake)

    with pytest.raises(RuntimeError, match="1 undelivered"):
        publisher.close()
