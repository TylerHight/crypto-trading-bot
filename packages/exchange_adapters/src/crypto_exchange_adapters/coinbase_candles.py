"""Coinbase Exchange public OHLCV history; no account or trading operations."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from urllib.parse import urlencode

from .coinbase_rest import (
    RETRYABLE_HTTP_STATUSES,
    HttpTransport,
    PermanentCoinbaseRestError,
    RetryableCoinbaseRestError,
    UrllibHttpTransport,
)

ENDPOINT = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
SCHEMA_VERSION = "exchange-ohlcv-v1"
MINUTE = timedelta(minutes=1)


def _decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise PermanentCoinbaseRestError(
            "candle prices and volume must be JSON numbers"
        )
    result = Decimal(value)
    try:
        with localcontext() as context:
            context.prec = 60
            valid = result.is_finite() and result == result.quantize(Decimal("1e-18"))
    except InvalidOperation:
        valid = False
    if not valid or (result and result.adjusted() >= 20):
        raise PermanentCoinbaseRestError("candle decimal exceeds decimal(38,18)")
    return result


def decode_candles(body: bytes) -> list[dict[str, object]]:
    try:
        payload = json.loads(body, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PermanentCoinbaseRestError("invalid candle JSON") from error
    if not isinstance(payload, list) or len(payload) > 300:
        raise PermanentCoinbaseRestError(
            "candle response must contain at most 300 buckets"
        )
    candles = []
    for item in payload:
        if not isinstance(item, list) or len(item) != 6:
            raise PermanentCoinbaseRestError(
                "candle must contain time, low, high, open, close, volume"
            )
        stamp = item[0]
        if isinstance(stamp, bool) or not isinstance(stamp, int) or stamp % 60:
            raise PermanentCoinbaseRestError(
                "candle timestamp must be an aligned Unix minute"
            )
        try:
            start = datetime.fromtimestamp(stamp, UTC)
        except (ValueError, OSError, OverflowError) as error:
            raise PermanentCoinbaseRestError(
                "candle timestamp is out of range"
            ) from error
        low, high, opening, close, volume = map(_decimal, item[1:])
        if not (0 < low <= opening <= high and low <= close <= high and volume > 0):
            raise PermanentCoinbaseRestError("candle OHLCV bounds are invalid")
        candles.append(
            {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "window_start": start,
                "window_end": start + MINUTE,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "base_volume": volume,
                "candle_schema_version": SCHEMA_VERSION,
            }
        )
    return candles


class CoinbaseCandlesClient:
    def __init__(
        self,
        *,
        transport: HttpTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_requests: int = 1800,
        max_retries: int = 3,
        request_interval: float = 0.15,
    ) -> None:
        if (
            not 1 <= max_requests <= 10000
            or not 0 <= max_retries <= 5
            or request_interval < 0.1
        ):
            raise ValueError("invalid historical candle request limits")
        self.transport = transport or UrllibHttpTransport()
        self.sleep = sleep
        self.max_requests = max_requests
        self.max_retries = max_retries
        self.request_interval = request_interval
        self.requests = 0

    def fetch_page(self, start: datetime, end: datetime) -> bytes:
        if (
            start.tzinfo is None
            or end.tzinfo is None
            or start.utcoffset() != timedelta(0)
            or end.utcoffset() != timedelta(0)
            or start.second
            or start.microsecond
            or end.second
            or end.microsecond
            or not MINUTE <= end - start <= 300 * MINUTE
        ):
            raise ValueError("candle page must be 1..300 aligned UTC minutes")
        # Coinbase includes the end bucket: request the last desired minute.
        query = urlencode(
            {
                "granularity": 60,
                "start": start.isoformat(),
                "end": (end - MINUTE).isoformat(),
            }
        )
        for attempt in range(self.max_retries + 1):
            if self.requests >= self.max_requests:
                raise RetryableCoinbaseRestError(
                    "historical candle request budget exhausted"
                )
            self.sleep(self.request_interval)
            self.requests += 1
            try:
                response = self.transport.request(
                    ENDPOINT + "?" + query,
                    {
                        "User-Agent": "crypto-history-research/1.0",
                        "Accept": "application/json",
                    },
                    10.0,
                )
            except RetryableCoinbaseRestError:
                if attempt == self.max_retries:
                    raise
                self.sleep(min(2**attempt, 5))
                continue
            if response.status == 200:
                decode_candles(response.body)
                return response.body
            if response.status not in RETRYABLE_HTTP_STATUSES:
                raise PermanentCoinbaseRestError(
                    f"historical candles returned HTTP {response.status}"
                )
            if attempt == self.max_retries:
                raise RetryableCoinbaseRestError(
                    f"historical candles returned HTTP {response.status} after bounded retries"
                )
            retry_after = next(
                (v for k, v in response.headers.items() if k.lower() == "retry-after"),
                "0",
            )
            try:
                delay = float(retry_after)
            except ValueError:
                delay = 0.0
            self.sleep(min(max(delay, 2**attempt), 5))
        raise AssertionError("unreachable retry loop")
