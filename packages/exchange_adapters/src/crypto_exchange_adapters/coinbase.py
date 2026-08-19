import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .models import ExchangeTrade
from .sequence_tracker import SequenceResult, SequenceTracker

LOGGER = logging.getLogger(__name__)

ENVELOPE_SEQUENCE_STREAM = "coinbase:advanced-trade-websocket"
HEARTBEAT_COUNTER_STREAM = "coinbase:heartbeat-counter"


class CoinbaseFeedContinuityMonitor:
    """Check message-envelope and heartbeat continuity for one connection."""

    def __init__(self, connection_id: UUID) -> None:
        self.connection_id = connection_id
        self._tracker = SequenceTracker()

    def observe_message(
        self,
        message: dict[str, Any],
    ) -> list[tuple[str, SequenceResult]]:
        """Observe one complete WebSocket envelope exactly once.

        Coinbase's outer ``sequence_num`` advances across every envelope on a
        connection, including market trades, heartbeats, and subscription
        messages. It must therefore be checked before non-trade messages are
        filtered and before a trade batch is expanded into individual trades.
        """

        observations: list[tuple[str, SequenceResult]] = []
        sequence = message.get("sequence_num")

        if sequence is not None:
            if not isinstance(sequence, int) or isinstance(sequence, bool):
                raise TypeError("Coinbase sequence_num must be an integer or null")
            if sequence < 0:
                raise ValueError("Coinbase sequence_num must not be negative")

            observations.append(
                (
                    ENVELOPE_SEQUENCE_STREAM,
                    self._tracker.observe(ENVELOPE_SEQUENCE_STREAM, sequence),
                )
            )

        if message.get("channel") == "heartbeats":
            events = message.get("events", [])
            if not isinstance(events, list):
                raise TypeError("Coinbase heartbeat events must be a list")

            for event in events:
                if not isinstance(event, dict):
                    raise TypeError("Coinbase heartbeat event must be an object")

                counter_value = event.get("heartbeat_counter")
                if counter_value is None:
                    raise KeyError("Coinbase heartbeat event has no heartbeat_counter")

                if isinstance(counter_value, bool) or not isinstance(
                    counter_value, (int, str)
                ):
                    raise TypeError(
                        "Coinbase heartbeat_counter must contain an integer"
                    )

                try:
                    counter = int(counter_value)
                except ValueError as error:
                    raise TypeError(
                        "Coinbase heartbeat_counter must contain an integer"
                    ) from error

                if counter < 0:
                    raise ValueError(
                        "Coinbase heartbeat_counter must not be negative"
                    )

                observations.append(
                    (
                        HEARTBEAT_COUNTER_STREAM,
                        self._tracker.observe(HEARTBEAT_COUNTER_STREAM, counter),
                    )
                )

        return observations


def report_continuity_observation(
    connection_id: UUID,
    stream: str,
    result: SequenceResult,
) -> None:
    """Log anomalous observations while leaving normal ingestion running."""

    if result.status == "gap":
        LOGGER.error(
            "Coinbase sequence gap connection_id=%s stream=%s previous=%s "
            "current=%s missing_from=%s missing_to=%s",
            connection_id,
            stream,
            result.previous,
            result.current,
            result.missing_from,
            result.missing_to,
        )
    elif result.status == "duplicate":
        LOGGER.warning(
            "Duplicate Coinbase sequence connection_id=%s stream=%s sequence=%s",
            connection_id,
            stream,
            result.current,
        )
    elif result.status == "out_of_order":
        LOGGER.warning(
            "Out-of-order Coinbase sequence connection_id=%s stream=%s "
            "previous=%s current=%s",
            connection_id,
            stream,
            result.previous,
            result.current,
        )


class CoinbasePublicTradeClient:
    """Read Coinbase public trades and normalize provider-specific fields."""

    def __init__(
        self,
        websocket_url: str,
        symbols: list[str],
        reconnect_initial_seconds: float,
        reconnect_max_seconds: float,
    ) -> None:
        self._websocket_url = websocket_url
        self._symbols = symbols
        self._reconnect_initial_seconds = reconnect_initial_seconds
        self._reconnect_max_seconds = reconnect_max_seconds

    async def trades(self) -> AsyncIterator[ExchangeTrade]:
        """Keep connecting and yield trades until the task is cancelled."""

        delay = self._reconnect_initial_seconds

        while True:
            try:
                async with connect(
                    self._websocket_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as websocket:
                    # Coinbase restarts sequence_num at zero for a new WebSocket
                    # connection. A fresh monitor prevents the new connection
                    # from being compared with the previous connection.
                    connection_id = uuid4()
                    continuity_monitor = CoinbaseFeedContinuityMonitor(connection_id)

                    await websocket.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "product_ids": self._symbols,
                                "channel": "market_trades",
                            }
                        )
                    )
                    # Coinbase recommends heartbeats because quiet subscriptions may
                    # otherwise close after 60-90 seconds without updates.
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "channel": "heartbeats",
                            }
                        )
                    )

                    LOGGER.info(
                        "Connected to Coinbase public WebSocket connection_id=%s for %s",
                        connection_id,
                        ",".join(self._symbols),
                    )
                    delay = self._reconnect_initial_seconds

                    async for raw_message in websocket:
                        try:
                            message = json.loads(raw_message)
                            if not isinstance(message, dict):
                                raise TypeError(
                                    "WebSocket message must be a JSON object"
                                )

                            # Observe every envelope before filtering channels or
                            # expanding a batched message into multiple trades.
                            for stream, result in continuity_monitor.observe_message(
                                message
                            ):
                                report_continuity_observation(
                                    connection_id,
                                    stream,
                                    result,
                                )

                            for trade in self.parse_message(message):
                                yield trade
                        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                            LOGGER.exception("Ignoring malformed Coinbase message")

            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, TimeoutError):
                LOGGER.exception("Coinbase WebSocket disconnected")

            jitter = random.uniform(0, min(1.0, delay * 0.25))
            wait_seconds = delay + jitter
            LOGGER.warning("Reconnecting in %.2f seconds", wait_seconds)
            await asyncio.sleep(wait_seconds)
            delay = min(delay * 2, self._reconnect_max_seconds)

    @staticmethod
    def parse_message(message: dict[str, Any]) -> list[ExchangeTrade]:
        """Extract every trade from one Coinbase market-trades message."""

        if message.get("channel") != "market_trades":
            return []

        sequence_value = message.get("sequence_num")
        if sequence_value is None:
            source_sequence = None
        elif isinstance(sequence_value, int) and not isinstance(sequence_value, bool):
            source_sequence = sequence_value
        else:
            raise TypeError("Coinbase sequence_num must be an integer or null")
        normalized: list[ExchangeTrade] = []

        for event in message.get("events", []):
            if not isinstance(event, dict):
                raise TypeError("Coinbase event must be a JSON object")

            for trade in event.get("trades", []):
                if not isinstance(trade, dict):
                    raise TypeError("Coinbase trade must be a JSON object")

                source_event_id = str(trade["trade_id"])
                symbol = str(trade["product_id"])
                event_time = datetime.fromisoformat(
                    str(trade["time"]).replace("Z", "+00:00")
                )

                if event_time.tzinfo is None:
                    raise ValueError("Exchange timestamp does not contain a timezone")

                normalized.append(
                    ExchangeTrade(
                        exchange="coinbase",
                        symbol=symbol,
                        source_event_id=source_event_id,
                        source_sequence=source_sequence,
                        event_time=event_time,
                        raw_payload=dict(trade),
                    )
                )

        return normalized
