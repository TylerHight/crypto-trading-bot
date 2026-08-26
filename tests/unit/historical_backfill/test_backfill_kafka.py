import json
from datetime import UTC, datetime
from typing import Any

import crypto_historical_backfill.kafka as kafka_module
from crypto_historical_backfill.kafka import AcknowledgedKafkaPublisher
from crypto_trading_domain import market_trade_event


class Message:
    def topic(self) -> str:
        return "market.trades.raw.v1"

    def partition(self) -> int:
        return 3

    def offset(self) -> int:
        return 41


class Producer:
    def __init__(self) -> None:
        self.call: dict[str, Any] = {}
        self.callback: Any = None

    def produce(self, **kwargs: Any) -> None:
        self.call = kwargs
        self.callback = kwargs["on_delivery"]

    def poll(self, timeout: float) -> int:
        self.callback(None, Message())
        return 1

    def flush(self, timeout: float) -> int:
        return 0


def test_publisher_uses_canonical_key_headers_and_returns_acknowledged_position() -> None:
    producer = Producer()
    event = market_trade_event(
        exchange="coinbase",
        symbol="BTC-USD",
        source_event_id="one",
        event_time=datetime(2026, 8, 25, tzinfo=UTC),
        payload={"trade_id": "one"},
        producer="apps.historical_backfill",
    )
    publisher = AcknowledgedKafkaPublisher(
        bootstrap_servers="unused",
        topic="market.trades.raw.v1",
        client_id="test",
        producer=producer,
    )

    receipt = publisher.publish(event)

    assert producer.call["key"] == b"coinbase:BTC-USD"
    assert dict(producer.call["headers"])["event_type"] == b"market.trade.raw"
    assert json.loads(producer.call["value"])["producer"] == "apps.historical_backfill"
    assert (receipt.topic, receipt.partition, receipt.offset) == (
        "market.trades.raw.v1",
        3,
        41,
    )


def test_constructed_producer_enables_idempotence_and_all_acks(monkeypatch: Any) -> None:
    configs: list[dict[str, Any]] = []

    class ConfigProducer(Producer):
        def __init__(self, config: dict[str, Any]) -> None:
            super().__init__()
            configs.append(config)

    monkeypatch.setattr(kafka_module, "Producer", ConfigProducer)

    AcknowledgedKafkaPublisher(
        bootstrap_servers="kafka:9092",
        topic="market.trades.raw.v1",
        client_id="test",
    )

    assert configs[0]["enable.idempotence"] is True
    assert configs[0]["acks"] == "all"
