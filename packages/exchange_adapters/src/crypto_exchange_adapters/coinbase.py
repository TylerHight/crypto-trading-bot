import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .models import ExchangeTrade

LOGGER = logging.getLogger(__name__)


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
                        "Connected to Coinbase public WebSocket for %s",
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
