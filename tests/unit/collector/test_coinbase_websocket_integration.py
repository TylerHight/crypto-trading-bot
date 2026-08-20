import json
import logging

import crypto_exchange_adapters.coinbase as coinbase_module
import pytest
from crypto_exchange_adapters.coinbase import CoinbasePublicTradeClient


class FakeWebSocket:
    """Return predefined JSON messages instead of using the network."""

    def __init__(self, messages: list[dict[str, object]]) -> None:
        # iter() lets __anext__ retrieve one message at a time.
        self._messages = iter(messages)

        # The collector sends two subscription requests after connecting.
        # Saving them lets the test prove both requests occurred.
        self.sent_messages: list[str] = []

    async def send(self, message: str) -> None:
        """Pretend to send a subscription request to Coinbase."""

        self.sent_messages.append(message)

    def __aiter__(self) -> "FakeWebSocket":
        """Support the ``async for raw_message in websocket`` protocol."""

        return self

    async def __anext__(self) -> str:
        """Return the next fake Coinbase envelope as JSON text."""

        try:
            message = next(self._messages)
        except StopIteration as error:
            # Async iterators use StopAsyncIteration to signal completion.
            raise StopAsyncIteration from error

        return json.dumps(message)


class FakeConnection:
    """Mimic the asynchronous context manager returned by connect()."""

    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        """Return the fake WebSocket when the connection is entered."""

        return self.websocket

    async def __aexit__(
        self,
        exception_type: object,
        exception: object,
        traceback: object,
    ) -> None:
        """Leave the fake connection; there is no real socket to close."""

        return


def market_trade_message(sequence: int, trade_id: str) -> dict[str, object]:
    """Build one valid Coinbase market-trades envelope."""

    return {
        "channel": "market_trades",
        "sequence_num": sequence,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "product_id": "BTC-USD",
                        "trade_id": trade_id,
                        "price": "68000.00",
                        "size": "0.001",
                        "time": "2026-08-19T20:01:41Z",
                        "side": "BUY",
                    }
                ],
            }
        ],
    }


@pytest.mark.asyncio
async def test_trade_stream_logs_sequence_gap_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Send a gap through the real client loop without opening a network socket."""

    websocket = FakeWebSocket(
        [
            market_trade_message(sequence=10, trade_id="100"),
            market_trade_message(sequence=12, trade_id="101"),
        ]
    )

    def fake_connect(*_args: object, **_kwargs: object) -> FakeConnection:
        """Return the fake connection instead of contacting Coinbase."""

        return FakeConnection(websocket)

    # Patch the name used by coinbase.py, not the original library function.
    monkeypatch.setattr(coinbase_module, "connect", fake_connect)

    client = CoinbasePublicTradeClient(
        # This address is deliberately invalid and will never be contacted.
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=1,
        reconnect_max_seconds=2,
    )
    trade_stream = client.trades()

    try:
        # INFO includes the startup confirmation. ERROR includes the gap report.
        with caplog.at_level(logging.INFO):
            first_trade = await anext(trade_stream)
            second_trade = await anext(trade_stream)
    finally:
        # The real stream reconnects forever, so explicitly stop this test stream.
        await trade_stream.aclose()

    # Both valid trades must survive normal parsing, including the post-gap trade.
    assert first_trade.source_event_id == "100"
    assert second_trade.source_event_id == "101"

    # This confirms that the running connection path created its monitor.
    assert "continuity_monitor=enabled" in caplog.text

    # A jump from 10 to 12 means that sequence 11 is the exact missing range.
    assert "Coinbase sequence gap" in caplog.text
    assert "previous=10" in caplog.text
    assert "current=12" in caplog.text
    assert "missing_from=11" in caplog.text
    assert "missing_to=11" in caplog.text

    # Check the meaning of the requests, not only the number sent.
    subscriptions = [json.loads(message) for message in websocket.sent_messages]
    assert subscriptions == [
        {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channel": "market_trades",
        },
        {"type": "subscribe", "channel": "heartbeats"},
    ]
