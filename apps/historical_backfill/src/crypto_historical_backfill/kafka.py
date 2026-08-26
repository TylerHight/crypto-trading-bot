from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from crypto_trading_domain import MarketTradeRawEvent


@dataclass(frozen=True)
class KafkaReceipt:
    topic: str
    partition: int
    offset: int

    def as_dict(self) -> dict[str, str | int]:
        return {
            "kafka_topic": self.topic,
            "kafka_partition": self.partition,
            "kafka_offset": self.offset,
        }


class RetryablePublicationError(RuntimeError):
    """Kafka explicitly rejected a publication attempt."""


class AmbiguousPublicationError(RuntimeError):
    """Kafka did not return an acknowledgement before the bounded deadline."""


class ProducerLike(Protocol):
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


class AcknowledgedKafkaPublisher:
    """Publish one canonical event and wait for its exact broker acknowledgement."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        client_id: str,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        acknowledgement_timeout_seconds: float = 10.0,
        poll_seconds: float = 0.1,
        queue_full_retries: int = 3,
        producer: ProducerLike | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if acknowledgement_timeout_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Kafka acknowledgement bounds must be positive")
        if queue_full_retries < 0:
            raise ValueError("Kafka queue retry count must not be negative")
        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
            "security.protocol": security_protocol,
        }
        if sasl_mechanism:
            if not sasl_username or not sasl_password:
                raise ValueError("SASL username and password are required")
            config.update(
                {
                    "sasl.mechanism": sasl_mechanism,
                    "sasl.username": sasl_username,
                    "sasl.password": sasl_password,
                }
            )
        self._producer = producer or cast(ProducerLike, Producer(config))
        self._topic = topic
        self._ack_timeout = acknowledgement_timeout_seconds
        self._poll_seconds = poll_seconds
        self._queue_full_retries = queue_full_retries
        self._monotonic = monotonic

    def publish(self, event: MarketTradeRawEvent) -> KafkaReceipt:
        delivered: list[tuple[KafkaError | None, Message]] = []

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            delivered.append((error, message))

        attempts = 0
        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=f"{event.exchange.lower()}:{event.symbol.upper()}".encode(),
                    value=event.model_dump_json().encode(),
                    headers=[
                        ("event_type", event.event_type.encode()),
                        ("schema_version", event.schema_version.encode()),
                        ("trace_id", str(event.trace_id).encode()),
                    ],
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                if attempts >= self._queue_full_retries:
                    raise RetryablePublicationError("Kafka producer queue remained full") from None
                attempts += 1
                self._producer.poll(self._poll_seconds)
            except KafkaException as publication_error:
                raise RetryablePublicationError(
                    "Kafka rejected the publication"
                ) from publication_error

        deadline = self._monotonic() + self._ack_timeout
        while not delivered and self._monotonic() < deadline:
            remaining = deadline - self._monotonic()
            self._producer.poll(min(self._poll_seconds, max(0.0, remaining)))
        if not delivered:
            raise AmbiguousPublicationError(
                "Kafka acknowledgement was not observed before the deadline"
            )
        error, message = delivered[0]
        if error is not None:
            raise RetryablePublicationError("Kafka rejected the publication")
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        if topic is None or partition is None or offset is None:
            raise AmbiguousPublicationError("Kafka acknowledgement lacked a complete position")
        return KafkaReceipt(topic, partition, offset)

    def close(self, timeout_seconds: float = 10.0) -> None:
        if self._producer.flush(timeout_seconds):
            raise AmbiguousPublicationError("Kafka publisher close left undelivered records")
