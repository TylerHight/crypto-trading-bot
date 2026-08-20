import asyncio
import logging
from collections.abc import Callable
from dataclasses import asdict

from crypto_exchange_adapters.coinbase import (
    CoinbasePublicTradeClient,
    FeedQualityObservation,
)
from crypto_exchange_adapters.models import ExchangeTrade

from .config import CollectorSettings
from .exchange_client import TradeSource
from .models import (
    MarketDataQualityEvent,
    MarketTradeRawEvent,
    event_id_for_trade,
)
from .producer import KafkaEventPublisher

LOGGER = logging.getLogger(__name__)


def build_event(trade: ExchangeTrade) -> MarketTradeRawEvent:
    """Convert an exchange-neutral trade into the canonical Kafka event."""

    return MarketTradeRawEvent(
        event_id=event_id_for_trade(
            exchange=trade.exchange,
            symbol=trade.symbol,
            source_event_id=trade.source_event_id,
        ),
        exchange=trade.exchange,
        symbol=trade.symbol,
        event_time=trade.event_time,
        source_event_id=trade.source_event_id,
        source_sequence=trade.source_sequence,
        correlation_id=None,
        causation_id=None,
        payload=trade.raw_payload,
    )


def build_quality_event(
    observation: FeedQualityObservation,
) -> MarketDataQualityEvent:
    """Convert an adapter observation into the durable Kafka contract."""

    return MarketDataQualityEvent.model_validate(asdict(observation))


def build_trade_source(
    settings: CollectorSettings,
    observation_callback: Callable[[FeedQualityObservation], None] | None = None,
) -> TradeSource:
    """Construct the configured exchange adapter."""

    if settings.exchange == "coinbase":
        return CoinbasePublicTradeClient(
            websocket_url=str(settings.websocket_url),
            symbols=settings.symbols,
            reconnect_initial_seconds=settings.reconnect_initial_seconds,
            reconnect_max_seconds=settings.reconnect_max_seconds,
            heartbeat_timeout_seconds=settings.heartbeat_timeout_seconds,
            health_summary_interval_seconds=(settings.health_summary_interval_seconds),
            malformed_message_excerpt_length=(settings.malformed_message_excerpt_length),
            observation_callback=observation_callback,
        )

    raise ValueError(f"Unsupported exchange: {settings.exchange}")


async def run(settings: CollectorSettings) -> None:
    """Run the collector until cancelled or a fatal publication error occurs."""

    trade_publisher = KafkaEventPublisher(settings)
    quality_publisher = KafkaEventPublisher(
        settings,
        topic=settings.kafka_quality_topic,
        client_id=f"{settings.kafka_client_id}-quality",
    )
    source = build_trade_source(
        settings,
        observation_callback=lambda observation: quality_publisher.publish(
            build_quality_event(observation)
        ),
    )

    try:
        async for trade in source.trades():
            trade_publisher.publish(build_event(trade))
    except asyncio.CancelledError:
        LOGGER.info("Collector shutdown requested")
        raise
    finally:
        try:
            quality_publisher.close()
        except Exception:
            LOGGER.exception("Failed to flush the data-quality Kafka publisher")
        trade_publisher.close()


def main() -> None:
    """Load settings and start the asynchronous application."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = CollectorSettings()

    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        LOGGER.info("Collector stopped")


if __name__ == "__main__":
    main()
