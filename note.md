from collections.abc import AsyncIterator
from typing import Protocol

from crypto_exchange_adapters.models import ExchangeTrade


class TradeSource(Protocol):
    """Anything that can asynchronously provide exchange trades."""

    def trades(self) -> AsyncIterator[ExchangeTrade]:
        """Yield normalized exchange trades until cancelled."""

        ...
