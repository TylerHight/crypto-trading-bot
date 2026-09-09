import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from jobs.spark.candles import (
    InvalidCandleInput,
    candle_snapshot_key,
    load_curated_snapshot,
    validate_output_prefix,
)
from jobs.spark.schemas.market_trades_v1 import CURATED_MARKET_TRADE_SCHEMA
from jobs.spark.transforms.market_candles import (
    aggregate_market_candles,
    exact_vwap,
)

CREATED_AT = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
SNAPSHOT_KEY = "a" * 64


def manifest_bytes(**changes) -> bytes:
    value = {
        "status": "published",
        "mode": "apply",
        "snapshot_key": SNAPSHOT_KEY,
        "curated_schema_version": "v1",
        "curated_output_uri": "s3a://crypto-data/curated/market_trades/v1/runs/run-1",
        "curated_logical_trades": 3,
    }
    value.update(changes)
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def load(value: bytes, *, digest: str | None = None, local: bool = True):
    return load_curated_snapshot(
        value,
        manifest_uri="manifest.json",
        expected_sha256=digest or hashlib.sha256(value).hexdigest(),
        allowed_manifest_prefix="s3a://crypto-data/curated/market_trades/v1/manifests",
        local_development=local,
    )


def test_curated_manifest_is_pinned_by_digest_and_deterministic_snapshot_key() -> None:
    value = manifest_bytes()
    source = load(value)

    assert source.expected_trades == 3
    assert source.manifest_sha256 == hashlib.sha256(value).hexdigest()
    assert candle_snapshot_key(source) == candle_snapshot_key(source)


def test_new_curated_manifest_digest_creates_a_new_candle_snapshot_key() -> None:
    first = load(manifest_bytes(curated_logical_trades=3))
    second = load(manifest_bytes(curated_logical_trades=4))

    assert candle_snapshot_key(first) != candle_snapshot_key(second)


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "dry_run_ready"},
        {"mode": "dry_run"},
        {"curated_schema_version": "v2"},
        {"snapshot_key": "bad"},
        {"curated_output_uri": "s3a://crypto-data/raw/market_trade_raw/v1"},
        {"curated_logical_trades": -1},
    ],
)
def test_invalid_curated_manifests_are_rejected(changes) -> None:
    with pytest.raises(InvalidCandleInput):
        load(manifest_bytes(**changes))


def test_manifest_digest_mismatch_is_rejected() -> None:
    with pytest.raises(InvalidCandleInput, match="digest"):
        load(manifest_bytes(), digest="0" * 64)


def test_local_manifest_requires_explicit_development_mode() -> None:
    with pytest.raises(InvalidCandleInput, match="local-development"):
        load(manifest_bytes(), local=False)


def test_remote_manifest_must_be_under_the_allowed_prefix() -> None:
    value = manifest_bytes()
    with pytest.raises(InvalidCandleInput, match="outside"):
        load_curated_snapshot(
            value,
            manifest_uri="s3a://other/manifest.json",
            expected_sha256=hashlib.sha256(value).hexdigest(),
            allowed_manifest_prefix="s3a://crypto-data/curated/manifests",
            local_development=False,
        )


@pytest.mark.parametrize(
    ("source", "output"),
    [
        ("s3a://bucket/same", "s3a://bucket/same/"),
        ("s3a://bucket/source", "s3a://bucket/source/candles"),
        ("s3a://bucket/source/candles", "s3a://bucket/source"),
    ],
)
def test_source_and_output_must_be_disjoint(source: str, output: str) -> None:
    with pytest.raises(InvalidCandleInput):
        validate_output_prefix(source, output)


def test_exact_vwap_uses_decimal_half_even_at_scale_18() -> None:
    rounds_to_even = exact_vwap(
        Decimal("2.000000000000000001"), Decimal("2.000000000000000000")
    )
    rounds_up_to_even = exact_vwap(
        Decimal("2.000000000000000003"), Decimal("2.000000000000000000")
    )

    assert rounds_to_even == Decimal("1.000000000000000000")
    assert rounds_up_to_even == Decimal("1.000000000000000002")
    assert exact_vwap(Decimal(1), Decimal(0)) is None
    assert exact_vwap(Decimal("1e38"), Decimal(1)) is None


def curated_row(
    *,
    symbol: str,
    event_time: datetime,
    price: str,
    size: str,
    offset: int,
    producer: str = "apps.collector",
) -> tuple[object, ...]:
    price_value = Decimal(price).quantize(Decimal("0.000000000000000001"))
    size_value = Decimal(size).quantize(Decimal("0.000000000000000001"))
    event_id = str(uuid4())
    return (
        event_id,
        "coinbase",
        symbol,
        f"trade-{offset}-{symbol}",
        event_time,
        datetime(2026, 8, 28, 12, offset, tzinfo=UTC),
        datetime(2026, 8, 28, 12, offset, tzinfo=UTC),
        price_value,
        size_value,
        (price_value * size_value).quantize(Decimal("0.000000000000000001")),
        "BUY",
        offset,
        producer,
        str(uuid4()),
        None,
        None,
        "market.trades.raw.v1",
        0,
        offset,
        "v1",
        CREATED_AT,
        event_time.date(),
    )


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    previous_python = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("market-candle-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    if previous_python is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = previous_python


def test_one_trade_produces_one_flat_candle(spark) -> None:
    row = curated_row(
        symbol="BTC-USD",
        event_time=datetime(2026, 8, 25, 14, 0, 1, tzinfo=UTC),
        price="100",
        size="2",
        offset=0,
    )
    frame = spark.createDataFrame([row], CURATED_MARKET_TRADE_SCHEMA)

    candle = aggregate_market_candles(
        frame,
        interval="1m",
        source_snapshot_key=SNAPSHOT_KEY,
        created_at=CREATED_AT,
    ).first()

    assert candle["open"] == candle["high"] == candle["low"] == candle["close"]
    assert candle["vwap"] == Decimal("100.000000000000000000")
    assert candle["base_volume"] == Decimal("2.000000000000000000")
    assert candle["trade_count"] == 1


def test_candles_use_event_time_and_deterministic_kafka_tie_breaker(spark) -> None:
    shared_time = datetime(2026, 8, 25, 14, 0, 10, tzinfo=UTC)
    rows = [
        curated_row(
            symbol="BTC-USD", event_time=shared_time, price="100", size="1", offset=2
        ),
        curated_row(
            symbol="BTC-USD", event_time=shared_time, price="90", size="2", offset=1
        ),
        curated_row(
            symbol="BTC-USD",
            event_time=datetime(2026, 8, 25, 14, 1, tzinfo=UTC),
            price="110",
            size="1",
            offset=3,
            producer="apps.historical_backfill",
        ),
        curated_row(
            symbol="ETH-USD", event_time=shared_time, price="50", size="1", offset=4
        ),
    ]
    frame = spark.createDataFrame(list(reversed(rows)), CURATED_MARKET_TRADE_SCHEMA)

    candles = (
        aggregate_market_candles(
            frame.repartition(3),
            interval="1m",
            source_snapshot_key=SNAPSHOT_KEY,
            created_at=CREATED_AT,
        )
        .orderBy("symbol", "window_start")
        .collect()
    )

    assert len(candles) == 3
    btc_first = next(
        row
        for row in candles
        if row["symbol"] == "BTC-USD" and row["window_start"].minute == 0
    )
    assert btc_first["open"] == Decimal("90.000000000000000000")
    assert btc_first["close"] == Decimal("100.000000000000000000")
    assert btc_first["high"] == Decimal("100.000000000000000000")
    assert btc_first["low"] == Decimal("90.000000000000000000")
    assert btc_first["base_volume"] == Decimal("3.000000000000000000")
    assert btc_first["quote_volume"] == Decimal("280.000000000000000000")
    assert btc_first["vwap"] == Decimal("93.333333333333333333")
    historical = next(row for row in candles if row["window_start"].minute == 1)
    assert historical["historical_backfill_trade_count"] == 1
    assert historical["event_date"] == date(2026, 8, 25)
    assert historical["event_hour"] == "14"


def test_only_one_minute_interval_is_supported() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        aggregate_market_candles(
            object(),  # type: ignore[arg-type]
            interval="5m",
            source_snapshot_key=SNAPSHOT_KEY,
            created_at=CREATED_AT,
        )
