import asyncio
import json
from typing import ClassVar

import crypto_exchange_adapters.coinbase as coinbase_module
import crypto_trading_collector.main as collector_main
import pytest
from crypto_trading_collector.config import CollectorSettings
from crypto_trading_collector.models import (
    MarketDataQualityEvent,
    MarketTradeRawEvent,
)


class OneTradeThenCancelWebSocket:
    def __init__(self) -> None:
        self._trade_sent = False

    async def send(self, _message: str) -> None:
        return

    def __aiter__(self) -> "OneTradeThenCancelWebSocket":
        return self

    async def __anext__(self) -> str:
        if self._trade_sent:
            raise asyncio.CancelledError

        self._trade_sent = True
        return json.dumps(
            {
                "channel": "market_trades",
                "sequence_num": 0,
                "events": [
                    {
                        "trades": [
                            {
                                "product_id": "BTC-USD",
                                "trade_id": "full-path",
                                "price": "68000.00",
                                "size": "0.001",
                                "time": "2026-08-20T16:00:00Z",
                                "side": "BUY",
                            }
                        ]
                    }
                ],
            }
        )


class FakeConnection:
    def __init__(self, websocket: OneTradeThenCancelWebSocket) -> None:
        self._websocket = websocket

    async def __aenter__(self) -> OneTradeThenCancelWebSocket:
        return self._websocket

    async def __aexit__(self, *_args: object) -> None:
        return


class RecordingPublisher:
    instances: ClassVar[list["RecordingPublisher"]] = []

    def __init__(
        self,
        settings: CollectorSettings,
        producer: object | None = None,
        topic: str | None = None,
        client_id: str | None = None,
    ) -> None:
        del producer, client_id
        self.topic = topic or settings.kafka_topic
        self.events: list[MarketTradeRawEvent | MarketDataQualityEvent] = []
        self.closed = False
        self.instances.append(self)

    def publish(self, event: MarketTradeRawEvent | MarketDataQualityEvent) -> None:
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_fake_websocket_reaches_both_collector_publishers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = OneTradeThenCancelWebSocket()
    RecordingPublisher.instances = []
    monkeypatch.setattr(
        coinbase_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(websocket),
    )
    monkeypatch.setattr(
        collector_main,
        "KafkaEventPublisher",
        RecordingPublisher,
    )

    with pytest.raises(asyncio.CancelledError):
        await collector_main.run(CollectorSettings(symbols=["BTC-USD"]))

    publishers: dict[str, RecordingPublisher] = {
        publisher.topic: publisher for publisher in RecordingPublisher.instances
    }
    trade_events = publishers["market.trades.raw.v1"].events
    quality_events = publishers["market.data.quality.v1"].events

    assert len(trade_events) == 1
    assert isinstance(trade_events[0], MarketTradeRawEvent)
    assert trade_events[0].source_event_id == "full-path"
    assert [event.observation_type for event in quality_events] == [
        "connection_opened",
        "connection_closed",
    ]
    assert all(publisher.closed for publisher in RecordingPublisher.instances)
