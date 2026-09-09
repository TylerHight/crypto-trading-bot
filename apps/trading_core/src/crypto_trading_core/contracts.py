from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any
from urllib.parse import urlparse

CANDLE_SCHEMA_VERSION = "v1"
HISTORICAL_CANDLE_SCHEMA_VERSION = "exchange-ohlcv-v1"
SUPPORTED_INTERVAL = "1m"
STRATEGY_VERSION = "sma-crossover-long-only-v1"
BACKTEST_ENGINE_VERSION = "candle-backtest-engine-v1"
RESULT_SCHEMA_VERSION = "v1"
SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
EXCHANGE_PATTERN = re.compile(r"^[a-z0-9_-]+$")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
DECIMAL_QUANTUM = Decimal("0.000000000000000001")


class InvalidBacktestInput(ValueError):
    """A source manifest, result manifest, or publication setting is unsafe."""


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise TypeError("naive datetime is not canonical")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        default=_json_default,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def is_local_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    return parsed.scheme in {"", "file"} or (
        len(uri) >= 3 and uri[1] == ":" and uri[2] in {"/", "\\"}
    )


def normalize_uri(value: str) -> str:
    return value.strip().replace("\\", "/").rstrip("/")


def validate_distinct_prefixes(source_output: str, result_output: str) -> None:
    source = normalize_uri(source_output)
    output = normalize_uri(result_output)
    if not source or not output or source == output:
        raise InvalidBacktestInput("candle source and backtest output must be distinct")
    if source.startswith(output + "/") or output.startswith(source + "/"):
        raise InvalidBacktestInput("candle source and backtest output must not be nested")


@dataclass(frozen=True)
class PublishedCandleSnapshot:
    manifest: dict[str, Any]
    manifest_uri: str
    manifest_sha256: str
    snapshot_key: str
    output_uri: str
    candle_count: int


def load_candle_snapshot(
    manifest_bytes: bytes,
    *,
    manifest_uri: str,
    expected_sha256: str,
    allowed_manifest_prefix: str,
    local_development: bool,
) -> PublishedCandleSnapshot:
    if is_local_uri(manifest_uri):
        if not local_development:
            raise InvalidBacktestInput(
                "local candle manifests require explicit local-development mode"
            )
    elif not normalize_uri(manifest_uri).startswith(normalize_uri(allowed_manifest_prefix) + "/"):
        raise InvalidBacktestInput("candle manifest is outside the allowed prefix")

    digest = hashlib.sha256(manifest_bytes).hexdigest()
    expected = expected_sha256.strip().lower()
    if not SHA256_PATTERN.fullmatch(expected) or digest != expected:
        raise InvalidBacktestInput("candle manifest SHA-256 digest does not match")
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("candle manifest is not valid UTF-8 JSON") from error
    if not isinstance(manifest, dict):
        raise InvalidBacktestInput("candle manifest must be a JSON object")
    if manifest.get("status") != "published" or manifest.get("mode") != "apply":
        raise InvalidBacktestInput("candle manifest is not a published apply run")
    if manifest.get("candle_schema_version") not in {
        CANDLE_SCHEMA_VERSION,
        HISTORICAL_CANDLE_SCHEMA_VERSION,
    }:
        raise InvalidBacktestInput("unsupported candle schema version")
    if manifest.get("candle_schema_version") == HISTORICAL_CANDLE_SCHEMA_VERSION and (
        manifest.get("source_kind") != "exchange_ohlcv"
        or not isinstance(manifest.get("source_archive_manifest_uri"), str)
        or not SHA256_PATTERN.fullmatch(str(manifest.get("source_archive_manifest_sha256", "")))
        or not SHA256_PATTERN.fullmatch(str(manifest.get("source_archive_key", "")))
        or not isinstance(manifest.get("files"), list)
    ):
        raise InvalidBacktestInput(
            "historical candles require explicit archive lineage and file hashes"
        )
    if manifest.get("interval") != SUPPORTED_INTERVAL:
        raise InvalidBacktestInput("unsupported candle interval")

    snapshot_key = manifest.get("snapshot_key")
    output_uri = manifest.get("candle_output_uri")
    candle_count = manifest.get("candle_count")
    if not isinstance(snapshot_key, str) or not SHA256_PATTERN.fullmatch(snapshot_key):
        raise InvalidBacktestInput("candle snapshot key is invalid")
    if (
        not isinstance(output_uri, str)
        or not output_uri.strip()
        or "/runs/" not in normalize_uri(output_uri)
    ):
        raise InvalidBacktestInput("candle output URI is not an immutable run directory")
    if isinstance(candle_count, bool) or not isinstance(candle_count, int) or candle_count < 0:
        raise InvalidBacktestInput("candle count is invalid")
    return PublishedCandleSnapshot(
        manifest=manifest,
        manifest_uri=manifest_uri,
        manifest_sha256=digest,
        snapshot_key=snapshot_key,
        output_uri=output_uri,
        candle_count=candle_count,
    )


def parse_utc_minute(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise InvalidBacktestInput(f"{field} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise InvalidBacktestInput(f"{field} must be UTC")
    parsed = parsed.astimezone(UTC)
    if parsed.second or parsed.microsecond:
        raise InvalidBacktestInput(f"{field} must align to a minute boundary")
    return parsed


@dataclass(frozen=True)
class BacktestSpec:
    candle_snapshot_key: str
    candle_manifest_sha256: str
    exchange: str
    symbol: str
    start: datetime
    end: datetime
    starting_cash: Decimal
    fast_period: int
    slow_period: int
    fee_bps: Decimal
    slippage_bps: Decimal
    strategy_version: str = STRATEGY_VERSION
    backtest_engine_version: str = BACKTEST_ENGINE_VERSION
    result_schema_version: str = RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not SHA256_PATTERN.fullmatch(self.candle_snapshot_key):
            raise InvalidBacktestInput("candle snapshot key is invalid")
        if not SHA256_PATTERN.fullmatch(self.candle_manifest_sha256):
            raise InvalidBacktestInput("candle manifest digest is invalid")
        if not EXCHANGE_PATTERN.fullmatch(self.exchange):
            raise InvalidBacktestInput("exchange has an invalid format")
        if not SYMBOL_PATTERN.fullmatch(self.symbol):
            raise InvalidBacktestInput("symbol has an invalid format")
        for value, field in ((self.start, "start"), (self.end, "end")):
            if (
                value.tzinfo is None
                or value.utcoffset() != timedelta(0)
                or value.second
                or value.microsecond
            ):
                raise InvalidBacktestInput(f"{field} must be an aligned UTC minute")
        if self.start >= self.end or self.end - self.start < timedelta(minutes=2):
            raise InvalidBacktestInput("backtest requires at least two evaluation minutes")
        if isinstance(self.fast_period, bool) or self.fast_period <= 0:
            raise InvalidBacktestInput("fast_period must be a positive integer")
        if isinstance(self.slow_period, bool) or self.slow_period <= self.fast_period:
            raise InvalidBacktestInput("slow_period must be greater than fast_period")
        self._validate_decimal(self.starting_cash, "starting_cash", positive=True)
        self._validate_decimal(self.fee_bps, "fee_bps")
        self._validate_decimal(self.slippage_bps, "slippage_bps")
        if self.fee_bps >= 10_000 or self.slippage_bps >= 10_000:
            raise InvalidBacktestInput("fee_bps and slippage_bps must be below 10000")

    @staticmethod
    def _validate_decimal(value: Decimal, field: str, *, positive: bool = False) -> None:
        if not value.is_finite() or value < 0 or (positive and value <= 0):
            qualifier = "positive" if positive else "nonnegative"
            raise InvalidBacktestInput(f"{field} must be finite and {qualifier}")
        try:
            with localcontext() as context:
                context.prec = 114
                quantized = value.quantize(DECIMAL_QUANTUM)
        except InvalidOperation as error:
            raise InvalidBacktestInput(f"{field} exceeds decimal(38,18)") from error
        if quantized != value or (quantized and quantized.adjusted() >= 20):
            raise InvalidBacktestInput(f"{field} exceeds decimal(38,18)")

    @staticmethod
    def _identity_decimal(value: Decimal) -> str:
        if value == 0:
            return "0"
        return format(value.normalize(), "f")

    def identity(self) -> dict[str, Any]:
        return {
            "backtest_engine_version": self.backtest_engine_version,
            "candle_manifest_sha256": self.candle_manifest_sha256,
            "candle_snapshot_key": self.candle_snapshot_key,
            "end": self.end,
            "exchange": self.exchange,
            "fast_period": self.fast_period,
            "fee_bps": self._identity_decimal(self.fee_bps),
            "result_schema_version": self.result_schema_version,
            "slow_period": self.slow_period,
            "slippage_bps": self._identity_decimal(self.slippage_bps),
            "start": self.start,
            "starting_cash": self._identity_decimal(self.starting_cash),
            "strategy_version": self.strategy_version,
            "symbol": self.symbol,
        }

    @property
    def key(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.identity())).hexdigest()
