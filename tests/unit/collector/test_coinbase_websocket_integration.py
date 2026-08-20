import json
import logging
from collections.abc import Iterator

import crypto_exchange_adapters.coinbase as coinbase_module
import pytest
from crypto_exchange_adapters.coinbase import (
    CoinbasePublicTradeClient,
    FeedQualityObservation,
)


class FakeWebSocket:
    """Return predefined JSON messages instead of using the network."""

    def __init__(self, messages: list[dict[str, object] | str]) -> None:
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

        return message if isinstance(message, str) else json.dumps(message)


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
    observations: list[FeedQualityObservation] = []

    client = CoinbasePublicTradeClient(
        # This address is deliberately invalid and will never be contacted.
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=1,
        reconnect_max_seconds=2,
        observation_callback=observations.append,
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

    gap = next(
        observation
        for observation in observations
        if observation.observation_type == "sequence_gap"
    )
    assert gap.previous_sequence == 10
    assert gap.current_sequence == 12
    assert gap.missing_from == 11
    assert gap.missing_to == 11

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


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TimedFakeWebSocket(FakeWebSocket):
    """Advance an injected monotonic clock before returning each message."""

    def __init__(
        self,
        messages: list[tuple[float, dict[str, object]]],
        clock: FakeClock,
    ) -> None:
        self._timed_messages: Iterator[tuple[float, dict[str, object]]] = iter(messages)
        self._clock = clock
        self.sent_messages = []

    async def __anext__(self) -> str:
        try:
            advance, message = next(self._timed_messages)
        except StopIteration as error:
            raise StopAsyncIteration from error

        self._clock.advance(advance)
        return json.dumps(message)


class SilentFakeWebSocket(FakeWebSocket):
    """Move time past the heartbeat deadline without real sleeping."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__([])
        self._clock = clock

    async def __anext__(self) -> str:
        self._clock.advance(11)
        raise TimeoutError


def heartbeat_message(sequence: int, counter: int) -> dict[str, object]:
    return {
        "channel": "heartbeats",
        "sequence_num": sequence,
        "events": [{"heartbeat_counter": str(counter)}],
    }


@pytest.mark.asyncio
async def test_heartbeat_silence_emits_once_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    first_websocket = SilentFakeWebSocket(clock)
    second_websocket = FakeWebSocket(
        [market_trade_message(sequence=0, trade_id="after-reconnect")]
    )
    connections = iter([first_websocket, second_websocket])

    def fake_connect(*_args: object, **_kwargs: object) -> FakeConnection:
        return FakeConnection(next(connections))

    monkeypatch.setattr(coinbase_module, "connect", fake_connect)
    monkeypatch.setattr(coinbase_module.random, "uniform", lambda *_args: 0.0)
    observations: list[FeedQualityObservation] = []
    client = CoinbasePublicTradeClient(
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=0,
        reconnect_max_seconds=0,
        heartbeat_timeout_seconds=10,
        observation_callback=observations.append,
        monotonic=clock,
    )
    trade_stream = client.trades()

    try:
        trade = await anext(trade_stream)
    finally:
        await trade_stream.aclose()

    assert trade.source_event_id == "after-reconnect"
    silence_events = [
        event for event in observations if event.observation_type == "heartbeat_silence"
    ]
    assert len(silence_events) == 1
    opened = [
        event for event in observations if event.observation_type == "connection_opened"
    ]
    assert len(opened) == 2
    assert opened[0].connection_id != opened[1].connection_id
    assert any(
        event.observation_type == "connection_recovered" for event in observations
    )
    assert any(event.observation_type == "reconnect_attempt" for event in observations)


@pytest.mark.asyncio
async def test_periodic_summary_reports_increasing_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    websocket = TimedFakeWebSocket(
        [
            (0, heartbeat_message(sequence=0, counter=100)),
            (61, heartbeat_message(sequence=1, counter=101)),
            (60, heartbeat_message(sequence=2, counter=102)),
            (0, market_trade_message(sequence=3, trade_id="summary-proof")),
        ],
        clock,
    )
    monkeypatch.setattr(
        coinbase_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(websocket),
    )
    observations: list[FeedQualityObservation] = []
    client = CoinbasePublicTradeClient(
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=1,
        reconnect_max_seconds=2,
        heartbeat_timeout_seconds=120,
        health_summary_interval_seconds=60,
        observation_callback=observations.append,
        monotonic=clock,
    )
    trade_stream = client.trades()

    try:
        trade = await anext(trade_stream)
    finally:
        await trade_stream.aclose()

    assert trade.source_event_id == "summary-proof"
    summaries = [
        event for event in observations if event.observation_type == "health_summary"
    ]
    assert [summary.envelopes_observed for summary in summaries] == [2, 3]
    assert [summary.heartbeats_observed for summary in summaries] == [2, 3]
    assert summaries[-1].last_envelope_sequence == 2
    assert summaries[-1].last_heartbeat_counter == 102


@pytest.mark.asyncio
async def test_all_anomalies_are_durable_and_later_trade_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed = "{" + ("x" * 100)
    websocket = FakeWebSocket(
        [
            heartbeat_message(sequence=0, counter=10),
            heartbeat_message(sequence=1, counter=12),
            {"channel": "subscriptions", "sequence_num": 1, "events": []},
            {"channel": "subscriptions", "sequence_num": 0, "events": []},
            malformed,
            {
                "channel": "heartbeats",
                "sequence_num": 2,
                "events": [{"heartbeat_counter": "not-a-number"}],
            },
            market_trade_message(sequence=3, trade_id="still-valid"),
        ]
    )
    monkeypatch.setattr(
        coinbase_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(websocket),
    )
    observations: list[FeedQualityObservation] = []
    client = CoinbasePublicTradeClient(
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=1,
        reconnect_max_seconds=2,
        malformed_message_excerpt_length=16,
        observation_callback=observations.append,
    )
    trade_stream = client.trades()

    try:
        trade = await anext(trade_stream)
    finally:
        await trade_stream.aclose()

    assert trade.source_event_id == "still-valid"
    observation_types = {event.observation_type for event in observations}
    assert "heartbeat_gap" in observation_types
    assert "duplicate_sequence" in observation_types
    assert "out_of_order_sequence" in observation_types
    assert "malformed_message" in observation_types
    heartbeat_gap = next(
        event for event in observations if event.observation_type == "heartbeat_gap"
    )
    assert heartbeat_gap.missing_from == 11
    assert heartbeat_gap.missing_to == 11
    malformed_events = [
        event for event in observations if event.observation_type == "malformed_message"
    ]
    assert len(malformed_events) == 2
    assert malformed_events[0].message_excerpt == malformed[:16]
    assert all(event.message_sha256 is not None for event in malformed_events)
    assert all(len(event.message_sha256 or "") == 64 for event in malformed_events)


@pytest.mark.asyncio
async def test_quality_publish_failure_is_logged_without_losing_trade(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    websocket = FakeWebSocket(
        [market_trade_message(sequence=0, trade_id="publisher-failed")]
    )
    monkeypatch.setattr(
        coinbase_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(websocket),
    )

    def fail_to_publish(_observation: FeedQualityObservation) -> None:
        raise RuntimeError("quality broker unavailable")

    client = CoinbasePublicTradeClient(
        websocket_url="wss://example.invalid",
        symbols=["BTC-USD"],
        reconnect_initial_seconds=1,
        reconnect_max_seconds=2,
        observation_callback=fail_to_publish,
    )
    trade_stream = client.trades()

    try:
        with caplog.at_level(logging.ERROR):
            trade = await anext(trade_stream)
    finally:
        await trade_stream.aclose()

    assert trade.source_event_id == "publisher-failed"
    assert "Failed to publish Coinbase data-quality observation" in caplog.text
