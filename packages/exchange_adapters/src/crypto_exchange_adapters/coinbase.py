import asyncio
import hashlib
import json
import logging
import random
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .models import ExchangeTrade
from .sequence_tracker import SequenceResult, SequenceTracker

LOGGER = logging.getLogger(__name__)

ENVELOPE_SEQUENCE_STREAM = "coinbase:advanced-trade-websocket"
HEARTBEAT_COUNTER_STREAM = "coinbase:heartbeat-counter"

QualityObservationType = Literal[
    "connection_opened",
    "connection_closed",
    "connection_recovered",
    "reconnect_scheduled",
    "reconnect_attempt",
    "sequence_gap",
    "duplicate_sequence",
    "out_of_order_sequence",
    "heartbeat_gap",
    "heartbeat_silence",
    "malformed_message",
    "health_summary",
]


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class FeedQualityObservation:
    """Exchange-neutral details for one durable feed-quality event."""

    exchange: str
    connection_id: UUID
    observation_type: QualityObservationType
    detected_at: datetime
    affected_symbols: tuple[str, ...]
    connection_started_at: datetime
    channel: str | None = None
    stream: str | None = None
    previous_sequence: int | None = None
    current_sequence: int | None = None
    missing_from: int | None = None
    missing_to: int | None = None
    last_envelope_sequence: int | None = None
    last_heartbeat_counter: int | None = None
    seconds_since_last_message: float | None = None
    seconds_since_last_heartbeat: float | None = None
    envelopes_observed: int = 0
    trades_observed: int = 0
    heartbeats_observed: int = 0
    sequence_gap_count: int = 0
    duplicate_sequence_count: int = 0
    out_of_order_sequence_count: int = 0
    malformed_message_count: int = 0
    reason: str | None = None
    message_excerpt: str | None = None
    message_sha256: str | None = None


class HeartbeatSilenceError(TimeoutError):
    """Signal that a connection must be replaced after heartbeat silence."""


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
                    raise ValueError("Coinbase heartbeat_counter must not be negative")

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


class CoinbaseConnectionHealth:
    """Collect counters and timing state for one WebSocket connection."""

    def __init__(
        self,
        connection_id: UUID,
        symbols: list[str],
        started_at: datetime,
        started_monotonic: float,
    ) -> None:
        self.connection_id = connection_id
        self.symbols = tuple(symbols)
        self.started_at = started_at
        self._continuity = CoinbaseFeedContinuityMonitor(connection_id)
        self._last_message_monotonic = started_monotonic
        self._last_heartbeat_monotonic = started_monotonic

        self.envelopes_observed = 0
        self.trades_observed = 0
        self.heartbeats_observed = 0
        self.sequence_gap_count = 0
        self.duplicate_sequence_count = 0
        self.out_of_order_sequence_count = 0
        self.malformed_message_count = 0
        self.last_envelope_sequence: int | None = None
        self.last_heartbeat_counter: int | None = None

    def record_message(self, observed_monotonic: float) -> None:
        """Count a received envelope even if its JSON or payload is malformed."""

        self.envelopes_observed += 1
        self._last_message_monotonic = observed_monotonic

    def observe_continuity(
        self,
        message: dict[str, Any],
        observed_monotonic: float,
    ) -> list[tuple[str, SequenceResult]]:
        observations = self._continuity.observe_message(message)

        for stream, result in observations:
            if stream == ENVELOPE_SEQUENCE_STREAM:
                self.last_envelope_sequence = result.current
            elif stream == HEARTBEAT_COUNTER_STREAM:
                self.last_heartbeat_counter = result.current
                self.heartbeats_observed += 1
                self._last_heartbeat_monotonic = observed_monotonic

            if result.status == "gap":
                self.sequence_gap_count += 1
            elif result.status == "duplicate":
                self.duplicate_sequence_count += 1
            elif result.status == "out_of_order":
                self.out_of_order_sequence_count += 1

        return observations

    def record_trades(self, count: int) -> None:
        self.trades_observed += count

    def record_malformed_message(self) -> None:
        self.malformed_message_count += 1

    def seconds_since_last_heartbeat(self, now_monotonic: float) -> float:
        return max(0.0, now_monotonic - self._last_heartbeat_monotonic)

    def observation(
        self,
        observation_type: QualityObservationType,
        detected_at: datetime,
        now_monotonic: float,
        **details: Any,
    ) -> FeedQualityObservation:
        """Build an immutable snapshot containing the current counters."""

        return FeedQualityObservation(
            exchange="coinbase",
            connection_id=self.connection_id,
            observation_type=observation_type,
            detected_at=detected_at,
            affected_symbols=self.symbols,
            connection_started_at=self.started_at,
            last_envelope_sequence=self.last_envelope_sequence,
            last_heartbeat_counter=self.last_heartbeat_counter,
            seconds_since_last_message=max(
                0.0,
                now_monotonic - self._last_message_monotonic,
            ),
            seconds_since_last_heartbeat=self.seconds_since_last_heartbeat(
                now_monotonic
            ),
            envelopes_observed=self.envelopes_observed,
            trades_observed=self.trades_observed,
            heartbeats_observed=self.heartbeats_observed,
            sequence_gap_count=self.sequence_gap_count,
            duplicate_sequence_count=self.duplicate_sequence_count,
            out_of_order_sequence_count=self.out_of_order_sequence_count,
            malformed_message_count=self.malformed_message_count,
            **details,
        )


def _safe_message_fingerprint(
    raw_message: str | bytes,
    excerpt_limit: int,
) -> tuple[str, str]:
    """Return a bounded diagnostic excerpt and a stable hash of the raw bytes."""

    raw_bytes = (
        raw_message.encode("utf-8", errors="replace")
        if isinstance(raw_message, str)
        else raw_message
    )
    safe_text = raw_bytes.decode("utf-8", errors="replace")
    return safe_text[:excerpt_limit], hashlib.sha256(raw_bytes).hexdigest()


class CoinbasePublicTradeClient:
    """Read Coinbase public trades and normalize provider-specific fields."""

    def __init__(
        self,
        websocket_url: str,
        symbols: list[str],
        reconnect_initial_seconds: float,
        reconnect_max_seconds: float,
        heartbeat_timeout_seconds: float = 10.0,
        health_summary_interval_seconds: float = 60.0,
        malformed_message_excerpt_length: int = 256,
        observation_callback: Callable[[FeedQualityObservation], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._websocket_url = websocket_url
        self._symbols = symbols
        self._reconnect_initial_seconds = reconnect_initial_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._health_summary_interval_seconds = health_summary_interval_seconds
        self._malformed_message_excerpt_length = malformed_message_excerpt_length
        self._observation_callback = observation_callback
        self._monotonic = monotonic
        self._utc_now = utc_now

    async def trades(self) -> AsyncIterator[ExchangeTrade]:
        """Keep connecting and yield trades until the task is cancelled."""

        delay = self._reconnect_initial_seconds
        has_connected = False

        while True:
            health: CoinbaseConnectionHealth | None = None
            close_reason = "connection_attempt_failed"

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
                    started_monotonic = self._monotonic()
                    health = CoinbaseConnectionHealth(
                        connection_id=connection_id,
                        symbols=self._symbols,
                        started_at=self._utc_now(),
                        started_monotonic=started_monotonic,
                    )

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
                        "Connected to Coinbase public WebSocket connection_id=%s "
                        "continuity_monitor=enabled for %s",
                        connection_id,
                        ",".join(self._symbols),
                    )
                    self._emit_observation(
                        health.observation(
                            "connection_opened",
                            self._utc_now(),
                            self._monotonic(),
                        )
                    )
                    if has_connected:
                        self._emit_observation(
                            health.observation(
                                "connection_recovered",
                                self._utc_now(),
                                self._monotonic(),
                            )
                        )
                    has_connected = True
                    close_reason = "connection_closed"
                    delay = self._reconnect_initial_seconds
                    next_summary = (
                        started_monotonic + self._health_summary_interval_seconds
                    )
                    websocket_iterator = aiter(websocket)

                    while True:
                        now_monotonic = self._monotonic()
                        heartbeat_age = health.seconds_since_last_heartbeat(
                            now_monotonic
                        )
                        if heartbeat_age >= self._heartbeat_timeout_seconds:
                            close_reason = "heartbeat_silence"
                            self._report_heartbeat_silence(
                                health,
                                now_monotonic,
                            )
                            raise HeartbeatSilenceError(
                                "Coinbase heartbeat timeout expired"
                            )

                        if now_monotonic >= next_summary:
                            self._emit_health_summary(health, now_monotonic)
                            next_summary = (
                                now_monotonic + self._health_summary_interval_seconds
                            )
                            continue

                        wait_seconds = min(
                            self._heartbeat_timeout_seconds - heartbeat_age,
                            next_summary - now_monotonic,
                        )

                        try:
                            raw_message = await asyncio.wait_for(
                                anext(websocket_iterator),
                                timeout=wait_seconds,
                            )
                        except StopAsyncIteration:
                            close_reason = "websocket_stream_ended"
                            break
                        except TimeoutError:
                            # Re-evaluate both monotonic deadlines at the top of
                            # the loop. This permits a health summary without
                            # resetting the heartbeat-silence deadline.
                            continue

                        observed_monotonic = self._monotonic()
                        health.record_message(observed_monotonic)

                        try:
                            message = json.loads(raw_message)
                            if not isinstance(message, dict):
                                raise TypeError(
                                    "WebSocket message must be a JSON object"
                                )

                            # Observe every envelope before filtering channels or
                            # expanding a batched message into multiple trades.
                            observations = health.observe_continuity(
                                message,
                                observed_monotonic,
                            )
                            channel_value = message.get("channel")
                            channel = (
                                channel_value
                                if isinstance(channel_value, str)
                                else None
                            )
                            for stream, result in observations:
                                report_continuity_observation(
                                    connection_id,
                                    stream,
                                    result,
                                )
                                self._emit_continuity_anomaly(
                                    health,
                                    channel,
                                    stream,
                                    result,
                                    observed_monotonic,
                                )

                            trades = self.parse_message(message)
                            health.record_trades(len(trades))
                            for trade in trades:
                                yield trade
                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                            json.JSONDecodeError,
                        ) as error:
                            health.record_malformed_message()
                            excerpt, message_hash = _safe_message_fingerprint(
                                raw_message,
                                self._malformed_message_excerpt_length,
                            )
                            LOGGER.exception("Ignoring malformed Coinbase message")
                            self._emit_observation(
                                health.observation(
                                    "malformed_message",
                                    self._utc_now(),
                                    observed_monotonic,
                                    reason=str(error),
                                    message_excerpt=excerpt,
                                    message_sha256=message_hash,
                                )
                            )

            except asyncio.CancelledError:
                close_reason = "collector_cancelled"
                raise
            except GeneratorExit:
                close_reason = "consumer_closed"
                raise
            except HeartbeatSilenceError:
                # The timeout was already logged with connection context.
                pass
            except (ConnectionClosed, OSError, TimeoutError) as error:
                close_reason = type(error).__name__
                LOGGER.exception("Coinbase WebSocket disconnected")
            finally:
                if health is not None:
                    self._emit_observation(
                        health.observation(
                            "connection_closed",
                            self._utc_now(),
                            self._monotonic(),
                            reason=close_reason,
                        )
                    )

            jitter = random.uniform(0, min(1.0, delay * 0.25))
            wait_seconds = delay + jitter
            LOGGER.warning("Reconnecting in %.2f seconds", wait_seconds)
            if health is not None:
                self._emit_observation(
                    health.observation(
                        "reconnect_scheduled",
                        self._utc_now(),
                        self._monotonic(),
                        reason=f"wait_seconds={wait_seconds:.2f}",
                    )
                )
            await asyncio.sleep(wait_seconds)
            if health is not None:
                self._emit_observation(
                    health.observation(
                        "reconnect_attempt",
                        self._utc_now(),
                        self._monotonic(),
                    )
                )
            delay = min(delay * 2, self._reconnect_max_seconds)

    def _emit_continuity_anomaly(
        self,
        health: CoinbaseConnectionHealth,
        channel: str | None,
        stream: str,
        result: SequenceResult,
        observed_monotonic: float,
    ) -> None:
        if result.status not in {"gap", "duplicate", "out_of_order"}:
            return

        if result.status == "gap":
            observation_type: QualityObservationType = (
                "heartbeat_gap"
                if stream == HEARTBEAT_COUNTER_STREAM
                else "sequence_gap"
            )
        elif result.status == "duplicate":
            observation_type = "duplicate_sequence"
        else:
            observation_type = "out_of_order_sequence"

        self._emit_observation(
            health.observation(
                observation_type,
                self._utc_now(),
                observed_monotonic,
                channel=channel,
                stream=stream,
                previous_sequence=result.previous,
                current_sequence=result.current,
                missing_from=result.missing_from,
                missing_to=result.missing_to,
            )
        )

    def _report_heartbeat_silence(
        self,
        health: CoinbaseConnectionHealth,
        now_monotonic: float,
    ) -> None:
        observation = health.observation(
            "heartbeat_silence",
            self._utc_now(),
            now_monotonic,
            channel="heartbeats",
            stream=HEARTBEAT_COUNTER_STREAM,
            reason=(
                f"No heartbeat received for at least "
                f"{self._heartbeat_timeout_seconds:.2f} seconds"
            ),
        )
        LOGGER.error(
            "Coinbase heartbeat silence connection_id=%s seconds_since_heartbeat=%.2f",
            health.connection_id,
            observation.seconds_since_last_heartbeat,
        )
        self._emit_observation(observation)

    def _emit_health_summary(
        self,
        health: CoinbaseConnectionHealth,
        now_monotonic: float,
    ) -> None:
        observation = health.observation(
            "health_summary",
            self._utc_now(),
            now_monotonic,
        )
        LOGGER.info(
            "Coinbase feed health connection_id=%s envelopes=%d trades=%d "
            "heartbeats=%d sequence_gaps=%d malformed=%d",
            health.connection_id,
            observation.envelopes_observed,
            observation.trades_observed,
            observation.heartbeats_observed,
            observation.sequence_gap_count,
            observation.malformed_message_count,
        )
        self._emit_observation(observation)

    def _emit_observation(self, observation: FeedQualityObservation) -> None:
        """Keep a quality-publisher failure from stopping valid trades."""

        if self._observation_callback is None:
            return

        try:
            self._observation_callback(observation)
        except Exception:
            LOGGER.exception(
                "Failed to publish Coinbase data-quality observation "
                "connection_id=%s observation_type=%s",
                observation.connection_id,
                observation.observation_type,
            )

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
