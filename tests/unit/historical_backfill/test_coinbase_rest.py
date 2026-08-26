import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest
from crypto_exchange_adapters.coinbase_rest import (
    CoinbaseMarketTradesClient,
    HttpResponse,
)

START = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)


def trade(trade_id: str, event_time: datetime) -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "product_id": "BTC-USD",
        "time": event_time.isoformat().replace("+00:00", "Z"),
        "price": "1",
        "size": "1",
    }


def response(trades: list[dict[str, object]], status: int = 200) -> HttpResponse:
    return HttpResponse(
        status=status,
        headers={},
        body=json.dumps({"trades": trades}).encode(),
    )


class QueueTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = responses
        self.urls: list[str] = []

    def request(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
    ) -> HttpResponse:
        self.urls.append(url)
        return self.responses.pop(0)


def test_full_result_window_is_split_until_subwindows_are_below_limit() -> None:
    transport = QueueTransport(
        [
            response([trade("one", START), trade("two", START + timedelta(seconds=8))]),
            response([trade("one", START)]),
            response([trade("two", START + timedelta(seconds=8))]),
        ]
    )
    client = CoinbaseMarketTradesClient(
        limit=2,
        minimum_split_duration=timedelta(seconds=1),
        transport=transport,
    )

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=10))

    assert result.coverage_complete
    assert [item.source_event_id for item in result.trades] == ["one", "two"]
    assert [window.outcome for window in result.requests] == [
        "split",
        "complete",
        "complete",
    ]
    assert len(transport.urls) == 3


def test_duplicate_and_boundary_results_are_normalized_deterministically() -> None:
    transport = QueueTransport(
        [
            response(
                [
                    trade("duplicate", START),
                    trade("duplicate", START),
                    trade("end-exclusive", START + timedelta(seconds=10)),
                ]
            )
        ]
    )
    client = CoinbaseMarketTradesClient(limit=10, transport=transport)

    result = client.fetch_interval("btc-usd", START, START + timedelta(seconds=10))

    assert [item.source_event_id for item in result.trades] == ["duplicate"]
    assert result.raw_trade_count == 2
    assert result.duplicate_identities == 1


def test_overlapping_split_responses_are_deduplicated_by_identity() -> None:
    midpoint = START + timedelta(seconds=5)
    transport = QueueTransport(
        [
            response(
                [
                    trade("left", START),
                    trade("middle", midpoint),
                    trade("right", START + timedelta(seconds=8)),
                ]
            ),
            response([trade("left", START), trade("middle", midpoint)]),
            response(
                [
                    trade("middle", midpoint),
                    trade("right", START + timedelta(seconds=8)),
                ]
            ),
        ]
    )
    client = CoinbaseMarketTradesClient(limit=3, transport=transport)

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=10))

    assert result.coverage_complete
    assert [item.source_event_id for item in result.trades] == [
        "left",
        "middle",
        "right",
    ]
    assert result.raw_trade_count == 3


def test_full_minimum_window_is_unresolved_instead_of_truncated() -> None:
    transport = QueueTransport([response([trade("one", START), trade("two", START)])])
    client = CoinbaseMarketTradesClient(
        limit=2,
        minimum_split_duration=timedelta(seconds=1),
        transport=transport,
    )

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=1))

    assert not result.coverage_complete
    assert result.unresolved_reason == "source_result_truncated"
    assert result.requests[0].outcome == "truncated"


def test_empty_response_is_not_considered_complete() -> None:
    client = CoinbaseMarketTradesClient(
        transport=QueueTransport([response([])]),
    )

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=10))

    assert not result.coverage_complete
    assert result.unresolved_reason == "source_history_unavailable"


def test_rate_limit_retry_honors_bounded_retry_after() -> None:
    rate_limited = HttpResponse(
        status=429,
        headers={"Retry-After": "2"},
        body=b"{}",
    )
    transport = QueueTransport([rate_limited, response([trade("one", START)])])
    sleeps: list[float] = []
    client = CoinbaseMarketTradesClient(
        transport=transport,
        max_retries=1,
        max_retry_delay_seconds=5,
        sleep=sleeps.append,
    )

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=10))

    assert result.coverage_complete
    assert sleeps == [2]
    query = parse_qs(urlparse(transport.urls[-1]).query)
    assert query["limit"] == ["1000"]


def test_request_cap_returns_an_unresolved_result() -> None:
    client = CoinbaseMarketTradesClient(
        limit=1,
        max_requests=1,
        transport=QueueTransport([response([trade("one", START)])]),
    )

    result = client.fetch_interval("BTC-USD", START, START + timedelta(seconds=10))

    assert not result.coverage_complete
    assert result.unresolved_reason == "source_request_limit_exceeded"


@pytest.mark.parametrize("start,end", [(START, START), (START + timedelta(1), START)])
def test_invalid_intervals_are_rejected(start: datetime, end: datetime) -> None:
    client = CoinbaseMarketTradesClient(transport=QueueTransport([]))

    with pytest.raises(ValueError, match="start_at"):
        client.fetch_interval("BTC-USD", start, end)


def test_invalid_product_id_is_rejected_before_http() -> None:
    client = CoinbaseMarketTradesClient(transport=QueueTransport([]))

    with pytest.raises(ValueError, match="product ID"):
        client.fetch_interval("../secrets", START, START + timedelta(seconds=1))
