import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from crypto_exchange_adapters.coinbase_candles import (
    CoinbaseCandlesClient,
    decode_candles,
)
from crypto_exchange_adapters.coinbase_rest import (
    HttpResponse,
    PermanentCoinbaseRestError,
    RetryableCoinbaseRestError,
)

START = datetime(2026, 6, 10, tzinfo=UTC)


class Transport:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.urls = []

    def request(self, url, headers, timeout):
        self.urls.append(url)
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value


def body(*rows):
    return json.dumps(rows).encode()


def test_decodes_exact_ohlcv_and_validates_bounds():
    rows = decode_candles(body([1781049600, 1, 4, 2, 3, 5]))
    assert rows[0]["open"] == Decimal(2)
    assert rows[0]["window_end"] == START + timedelta(minutes=1)
    for invalid in (
        b"{}",
        body([1, 2]),
        body([1781049601, 1, 2, 1, 2, 3]),
        body([1781049600, 2, 1, 2, 1, 3]),
        body([1781049600, 1, 2, 1, 2, 0]),
    ):
        with pytest.raises(PermanentCoinbaseRestError):
            decode_candles(invalid)


def test_page_is_bounded_and_retries_transient_responses():
    transport = Transport(
        [
            HttpResponse(429, {"Retry-After": "0"}, b""),
            HttpResponse(200, {}, body([1781049600, 1, 2, 1, 2, 3])),
        ]
    )
    sleeps = []
    client = CoinbaseCandlesClient(
        transport=transport, sleep=sleeps.append, request_interval=0.1, max_retries=1
    )
    assert client.fetch_page(START, START + timedelta(minutes=1))
    assert client.requests == 2
    assert all("granularity=60" in url for url in transport.urls)
    assert sleeps == [0.1, 1, 0.1]
    with pytest.raises(ValueError):
        client.fetch_page(START, START + timedelta(minutes=301))


def test_retry_budget_is_bounded():
    transport = Transport([RetryableCoinbaseRestError("down")] * 2)
    client = CoinbaseCandlesClient(
        transport=transport, sleep=lambda _: None, request_interval=0.1, max_retries=1
    )
    with pytest.raises(RetryableCoinbaseRestError):
        client.fetch_page(START, START + timedelta(minutes=1))
