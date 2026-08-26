import json
import re
import time
from collections import Counter, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import ExchangeTrade

RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
COINBASE_PRODUCT_PATTERN = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)+$")


class CoinbaseRestError(RuntimeError):
    """Base error for bounded Coinbase REST reads."""


class RetryableCoinbaseRestError(CoinbaseRestError):
    """The request may succeed if the whole reconciliation is retried."""


class PermanentCoinbaseRestError(CoinbaseRestError):
    """The request or response cannot be safely retried as-is."""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class HttpTransport(Protocol):
    def request(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        """Perform one HTTP GET without applying retry policy."""


class UrllibHttpTransport:
    """Small synchronous transport that keeps retry policy in the adapter."""

    def request(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        request = Request(url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout) as response:
                return HttpResponse(
                    status=int(response.status),
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except HTTPError as error:
            return HttpResponse(
                status=error.code,
                headers=dict(error.headers.items()) if error.headers else {},
                body=error.read(),
            )
        except (TimeoutError, URLError, OSError) as error:
            raise RetryableCoinbaseRestError(str(error)) from error


@dataclass(frozen=True)
class RestWindow:
    start_at: datetime
    end_at: datetime
    returned_results: int
    outcome: str

    def as_dict(self) -> dict[str, object]:
        return {
            "start_at": _utc_text(self.start_at),
            "end_at_exclusive": _utc_text(self.end_at),
            "returned_results": self.returned_results,
            "outcome": self.outcome,
        }


@dataclass(frozen=True)
class CoinbaseTradeCoverage:
    trades: tuple[ExchangeTrade, ...]
    requests: tuple[RestWindow, ...]
    raw_trade_count: int
    duplicate_identities: int
    coverage_complete: bool
    unresolved_reason: str | None = None


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _require_aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(UTC)


class CoinbaseMarketTradesClient:
    """Fetch complete bounded trade windows from Coinbase Advanced Trade REST."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.coinbase.com/api/v3/brokerage/market",
        bearer_token: str | None = None,
        limit: int = 1000,
        max_requests: int = 100,
        minimum_split_duration: timedelta = timedelta(seconds=1),
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        max_retry_delay_seconds: float = 30.0,
        transport: HttpTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be greater than zero")
        if max_requests <= 0:
            raise ValueError("max_requests must be greater than zero")
        if minimum_split_duration <= timedelta(0):
            raise ValueError("minimum_split_duration must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")
        if max_retry_delay_seconds < 0:
            raise ValueError("max_retry_delay_seconds must not be negative")

        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token.strip() if bearer_token else None
        self._limit = limit
        self._max_requests = max_requests
        self._minimum_split_duration = minimum_split_duration
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._max_retry_delay_seconds = max_retry_delay_seconds
        self._transport = transport or UrllibHttpTransport()
        self._sleep = sleep

    def fetch_interval(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> CoinbaseTradeCoverage:
        """Fetch and normalize an interval, splitting possibly truncated windows."""

        normalized_symbol = symbol.strip().upper()
        if not COINBASE_PRODUCT_PATTERN.fullmatch(normalized_symbol):
            raise ValueError("symbol must be a Coinbase product ID")
        start = _require_aware(start_at, "start_at")
        end = _require_aware(end_at, "end_at")
        if start >= end:
            raise ValueError("start_at must be before end_at")

        pending = deque([(start, end)])
        request_windows: list[RestWindow] = []
        accepted: list[ExchangeTrade] = []
        saw_empty_window = False

        while pending:
            if len(request_windows) >= self._max_requests:
                return self._coverage(
                    accepted,
                    request_windows,
                    coverage_complete=False,
                    unresolved_reason="source_request_limit_exceeded",
                )

            window_start, window_end = pending.popleft()
            payload_trades = self._request_window(
                normalized_symbol,
                window_start,
                window_end,
            )
            returned_results = len(payload_trades)

            if returned_results >= self._limit:
                duration = window_end - window_start
                if duration <= self._minimum_split_duration:
                    request_windows.append(
                        RestWindow(
                            window_start,
                            window_end,
                            returned_results,
                            "truncated",
                        )
                    )
                    return self._coverage(
                        accepted,
                        request_windows,
                        coverage_complete=False,
                        unresolved_reason="source_result_truncated",
                    )

                midpoint = window_start + duration / 2
                request_windows.append(
                    RestWindow(window_start, window_end, returned_results, "split")
                )
                pending.appendleft((midpoint, window_end))
                pending.appendleft((window_start, midpoint))
                continue

            request_windows.append(
                RestWindow(window_start, window_end, returned_results, "complete")
            )
            if not payload_trades:
                saw_empty_window = True

            for payload in payload_trades:
                trade = self.normalize_trade(payload, expected_symbol=normalized_symbol)
                if window_start <= trade.event_time < window_end:
                    accepted.append(trade)

        return self._coverage(
            accepted,
            request_windows,
            coverage_complete=not saw_empty_window,
            unresolved_reason=(
                "source_history_unavailable" if saw_empty_window else None
            ),
        )

    def _request_window(
        self,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[Mapping[str, object]]:
        query = urlencode(
            {
                "limit": self._limit,
                "start": f"{start_at.timestamp():.6f}",
                "end": f"{end_at.timestamp():.6f}",
            }
        )
        url = f"{self._base_url}/products/{symbol}/ticker?{query}"
        headers = {"Accept": "application/json", "Cache-Control": "no-cache"}
        if self._bearer_token:
            headers["Authorization"] = f"Bearer {self._bearer_token}"

        for attempt in range(self._max_retries + 1):
            try:
                response = self._transport.request(
                    url,
                    headers,
                    self._timeout_seconds,
                )
            except RetryableCoinbaseRestError:
                if attempt >= self._max_retries:
                    raise
                self._sleep(self._retry_delay({}, attempt))
                continue

            if 200 <= response.status < 300:
                return self._decode_trades(response.body)
            if response.status not in RETRYABLE_HTTP_STATUSES:
                raise PermanentCoinbaseRestError(
                    f"Coinbase returned HTTP {response.status}"
                )
            if attempt >= self._max_retries:
                raise RetryableCoinbaseRestError(
                    f"Coinbase returned HTTP {response.status} after bounded retries"
                )
            self._sleep(self._retry_delay(response.headers, attempt))

        raise AssertionError("bounded retry loop exited unexpectedly")

    def _retry_delay(self, headers: Mapping[str, str], attempt: int) -> float:
        retry_after = next(
            (value for key, value in headers.items() if key.lower() == "retry-after"),
            None,
        )
        if retry_after is not None:
            try:
                return min(max(0.0, float(retry_after)), self._max_retry_delay_seconds)
            except ValueError:
                pass
        return min(float(2**attempt), self._max_retry_delay_seconds)

    @staticmethod
    def _decode_trades(body: bytes) -> list[Mapping[str, object]]:
        try:
            payload = json.loads(body.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PermanentCoinbaseRestError(
                "Coinbase returned invalid JSON"
            ) from error
        if not isinstance(payload, Mapping) or not isinstance(
            payload.get("trades"), list
        ):
            raise PermanentCoinbaseRestError(
                "Coinbase response does not contain a trades array"
            )
        trades = payload["trades"]
        if not all(isinstance(trade, Mapping) for trade in trades):
            raise PermanentCoinbaseRestError("Coinbase trades must be JSON objects")
        return trades

    @staticmethod
    def normalize_trade(
        payload: Mapping[str, object],
        *,
        expected_symbol: str,
    ) -> ExchangeTrade:
        try:
            source_event_id = str(payload["trade_id"]).strip()
            symbol = str(payload["product_id"]).strip().upper()
            event_time = datetime.fromisoformat(
                str(payload["time"]).replace("Z", "+00:00")
            )
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentCoinbaseRestError("Coinbase trade is malformed") from error
        if not source_event_id:
            raise PermanentCoinbaseRestError("Coinbase trade_id must not be empty")
        if symbol != expected_symbol:
            raise PermanentCoinbaseRestError(
                f"Coinbase returned unexpected product_id {symbol!r}"
            )
        if event_time.tzinfo is None or event_time.utcoffset() is None:
            raise PermanentCoinbaseRestError("Coinbase trade time lacks a timezone")
        return ExchangeTrade(
            exchange="coinbase",
            symbol=symbol,
            source_event_id=source_event_id,
            source_sequence=None,
            event_time=event_time.astimezone(UTC),
            raw_payload=dict(payload),
        )

    @staticmethod
    def _coverage(
        trades: list[ExchangeTrade],
        requests: list[RestWindow],
        *,
        coverage_complete: bool,
        unresolved_reason: str | None,
    ) -> CoinbaseTradeCoverage:
        counts = Counter((trade.symbol, trade.source_event_id) for trade in trades)
        unique = {(trade.symbol, trade.source_event_id): trade for trade in trades}
        ordered = tuple(unique[identity] for identity in sorted(unique))
        return CoinbaseTradeCoverage(
            trades=ordered,
            requests=tuple(requests),
            raw_trade_count=len(trades),
            duplicate_identities=sum(count > 1 for count in counts.values()),
            coverage_complete=coverage_complete,
            unresolved_reason=unresolved_reason,
        )
