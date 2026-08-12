import asyncio
import logging

from crypto_exchange_adapters.coinbase import CoinbasePublicTradeClient
from crypto_exchange_adapters.models import ExchangeTrade

from .config import CollectorSettings
from .exchange_client import TradeSource
from .models import MarketTradeRawEvent, event_id_for_trade
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


def build_trade_source(settings: CollectorSettings) -> TradeSource:
    """Construct the configured exchange adapter."""

    if settings.exchange == "coinbase":
        return CoinbasePublicTradeClient(
            websocket_url=str(settings.websocket_url),
            symbols=settings.symbols,
            reconnect_initial_seconds=settings.reconnect_initial_seconds,
            reconnect_max_seconds=settings.reconnect_max_seconds,
        )

    raise ValueError(f"Unsupported exchange: {settings.exchange}")


async def run(settings: CollectorSettings) -> None:
    """Run the collector until cancelled or a fatal publication error occurs."""

    source = build_trade_source(settings)
    publisher = KafkaEventPublisher(settings)

    try:
        async for trade in source.trades():
            publisher.publish(build_event(trade))
    except asyncio.CancelledError:
        LOGGER.info("Collector shutdown requested")
        raise
    finally:
        publisher.close()


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
