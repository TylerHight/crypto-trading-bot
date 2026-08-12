import logging
from collections import deque
from collections.abc import Callable
from typing import Any, Protocol, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from .config import CollectorSettings
from .models import MarketTradeRawEvent

LOGGER = logging.getLogger(__name__)


class ProducerLike(Protocol):
    """The small part of Confluent Producer used by this application."""

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class KafkaEventPublisher:
    """Serialize validated events and enqueue them for Kafka delivery."""

    def __init__(
        self,
        settings: CollectorSettings,
        producer: ProducerLike | None = None,
    ) -> None:
        self._topic = settings.kafka_topic
        self._poll_timeout = settings.kafka_poll_timeout_seconds
        self._flush_timeout = settings.kafka_flush_timeout_seconds
        self._queue_full_retries = settings.kafka_queue_full_retries
        self._delivery_errors: deque[KafkaError] = deque()

        config: dict[str, Any] = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
            "enable.idempotence": True,
            "acks": "all",
            "security.protocol": settings.kafka_security_protocol,
        }

        if settings.kafka_sasl_mechanism is not None:
            username = settings.kafka_sasl_username
            password = settings.kafka_sasl_password
            if username is None or password is None:
                raise ValueError("Validated SASL configuration is incomplete")

            config.update(
                {
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": username.get_secret_value(),
                    "sasl.password": password.get_secret_value(),
                }
            )

        self._producer = producer if producer is not None else cast(ProducerLike, Producer(config))

    def publish(self, event: MarketTradeRawEvent) -> None:
        """Queue an event and surface any earlier asynchronous delivery error."""

        self._producer.poll(0)
        self._raise_pending_delivery_error()

        key = f"{event.exchange.lower()}:{event.symbol.upper()}".encode()
        value = event.model_dump_json().encode()
        headers = [
            ("event_type", event.event_type.encode()),
            ("schema_version", event.schema_version.encode()),
            ("trace_id", str(event.trace_id).encode()),
        ]

        attempts = 0
        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=value,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                if attempts >= self._queue_full_retries:
                    raise

                attempts += 1
                LOGGER.warning(
                    "Kafka producer queue is full; retry %d of %d",
                    attempts,
                    self._queue_full_retries,
                )
                self._producer.poll(self._poll_timeout)
                self._raise_pending_delivery_error()

    def close(self) -> None:
        """Wait a bounded amount of time for queued events to finish."""

        remaining = self._producer.flush(self._flush_timeout)
        self._raise_pending_delivery_error()

        if remaining:
            raise RuntimeError(f"Kafka shutdown timed out with {remaining} undelivered message(s)")

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self._delivery_errors.append(error)
            LOGGER.error("Kafka delivery failed: %s", error)
            return

        LOGGER.debug(
            "Delivered event to %s partition %s offset %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def _raise_pending_delivery_error(self) -> None:
        if self._delivery_errors:
            raise KafkaException(self._delivery_errors.popleft())
