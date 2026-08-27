import json
import os
import sys
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from jobs.spark.transforms.curated_market_trades import (
    curate_market_trades,
    validate_raw_record,
)

NOW = datetime(2026, 8, 26, 17, 0, tzinfo=UTC)
EVENT_TIME = "2026-08-25T14:00:01Z"
WINDOWS_PYSPARK_WORKER_UNSUPPORTED = pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info >= (3, 13),
    reason="Spark 3.5's Python worker is incompatible with Windows Python 3.13",
)


def event_id(symbol: str, trade_id: str) -> str:
    identity = json.dumps(
        ["market.trade.raw", "v1", "coinbase", symbol, trade_id],
        separators=(",", ":"),
    )
    return str(uuid5(NAMESPACE_URL, identity))


def event(
    trade_id: str = "trade-1",
    *,
    symbol: str = "BTC-USD",
    producer: str = "apps.collector",
    price: str = "65000.10",
    size: str = "0.001",
    side: str = "BUY",
) -> dict[str, object]:
    trace_id = str(uuid4())
    return {
        "event_id": event_id(symbol, trade_id),
        "event_type": "market.trade.raw",
        "schema_version": "v1",
        "exchange": "coinbase",
        "symbol": symbol,
        "event_time": EVENT_TIME,
        "ingested_at": "2026-08-26T16:59:59Z",
        "source_event_id": trade_id,
        "source_sequence": 42 if producer == "apps.collector" else None,
        "producer": producer,
        "trace_id": trace_id,
        "correlation_id": None,
        "causation_id": str(uuid4()) if producer == "apps.historical_backfill" else None,
        "payload": {
            "trade_id": trade_id,
            "product_id": symbol,
            "price": price,
            "size": size,
            "side": side,
            "time": EVENT_TIME,
        },
    }


def raw(document: dict[str, object], offset: int = 0) -> dict[str, object]:
    trace_id = str(document["trace_id"])
    return {
        "kafka_topic": "market.trades.raw.v1",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": datetime(2026, 8, 26, 17, 0, tzinfo=UTC).replace(
            tzinfo=None
        ),
        "kafka_key": f"coinbase:{document['symbol']}".encode(),
        "kafka_value": json.dumps(document, separators=(",", ":")).encode(),
        "kafka_headers": [
            {"key": "event_type", "value": b"market.trade.raw"},
            {"key": "schema_version", "value": b"v1"},
            {"key": "trace_id", "value": trace_id.encode()},
        ],
    }


def validate(document: dict[str, object], offset: int = 0):
    return validate_raw_record(raw(document, offset), run_id="run-1", curated_at=NOW)


@pytest.mark.parametrize("producer", ["apps.collector", "apps.historical_backfill"])
def test_valid_producers_create_the_same_fixed_precision_schema(producer: str) -> None:
    result = validate(event(producer=producer))

    assert result.quarantine is None
    assert result.candidate is not None
    assert result.candidate["price"] == Decimal("65000.100000000000000000")
    assert result.candidate["size"] == Decimal("0.001000000000000000")
    assert result.candidate["notional"] == Decimal("65.000100000000000000")
    assert result.candidate["event_date"].isoformat() == "2026-08-25"
    assert result.candidate["kafka_timestamp"].tzinfo == UTC


def test_notional_uses_scale_18_round_half_even() -> None:
    result = validate(event(price="1.000000000000000001", size="1.000000000000000001"))

    assert result.candidate is not None
    assert result.candidate["notional"] == Decimal("1.000000000000000002")


@pytest.mark.parametrize(
    ("field", "value", "failure"),
    [
        ("price", "0", "invalid_price"),
        ("price", "-1", "invalid_price"),
        ("price", "not-a-number", "invalid_price"),
        ("price", "NaN", "invalid_price"),
        ("price", "100000000000000000000", "decimal_overflow"),
        ("price", "0.0000000000000000001", "decimal_overflow"),
        ("size", "0", "invalid_size"),
        ("size", "Infinity", "invalid_size"),
    ],
)
def test_invalid_numeric_values_have_one_primary_quarantine_reason(
    field: str, value: str, failure: str
) -> None:
    document = event()
    document["payload"][field] = value  # type: ignore[index]
    result = validate(document)

    assert result.candidate is None
    assert result.quarantine is not None
    assert result.quarantine["failure_code"] == failure
    encoded = json.dumps(result.quarantine, default=str)
    assert "65000.10" not in encoded and "0.001" not in encoded
    assert "payload" not in encoded


@pytest.mark.parametrize("side", ["", "HOLD", None])
def test_invalid_side_is_quarantined(side: object) -> None:
    document = event()
    document["payload"]["side"] = side  # type: ignore[index]
    assert validate(document).quarantine["failure_code"] == "invalid_side"  # type: ignore[index]


@pytest.mark.parametrize(
    ("mutation", "failure"),
    [
        (lambda value: value["payload"].update(trade_id="other"), "payload_identity_mismatch"),
        (lambda value: value["payload"].update(product_id="ETH-USD"), "payload_identity_mismatch"),
        (
            lambda value: value["payload"].update(time="2026-08-25T14:00:02Z"),
            "payload_time_mismatch",
        ),
        (lambda value: value.update(event_time="2026-08-25T14:00:01"), "invalid_envelope_contract"),
        (lambda value: value.update(producer="unknown"), "invalid_envelope_contract"),
    ],
)
def test_payload_envelope_and_producer_mismatches_quarantine(mutation, failure: str) -> None:
    document = event()
    mutation(document)
    assert validate(document).quarantine["failure_code"] == failure  # type: ignore[index]


def test_kafka_key_and_headers_must_match() -> None:
    document = event()
    key_record = raw(document)
    key_record["kafka_key"] = b"coinbase:ETH-USD"
    header_record = raw(document)
    header_record["kafka_headers"][1]["value"] = b"v2"  # type: ignore[index]

    assert validate_raw_record(
        key_record, run_id="run", curated_at=NOW
    ).quarantine["failure_code"] == "invalid_kafka_key"  # type: ignore[index]
    assert validate_raw_record(
        header_record, run_id="run", curated_at=NOW
    ).quarantine["failure_code"] == "invalid_headers"  # type: ignore[index]


def test_invalid_utf8_and_json_are_hashed_without_payload_copy() -> None:
    record = raw(event())
    record["kafka_value"] = b"\xffsecret-price"
    invalid_utf8 = validate_raw_record(record, run_id="run", curated_at=NOW)
    record["kafka_value"] = b"{bad-json"
    invalid_json = validate_raw_record(record, run_id="run", curated_at=NOW)

    assert invalid_utf8.quarantine["failure_code"] == "invalid_utf8"  # type: ignore[index]
    assert invalid_json.quarantine["failure_code"] == "invalid_json"  # type: ignore[index]
    assert len(invalid_utf8.quarantine["value_sha256"]) == 64  # type: ignore[arg-type,index]
    assert "secret" not in json.dumps(invalid_utf8.quarantine, default=str)


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    previous_python = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("curated-market-trades-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    if previous_python is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = previous_python


@WINDOWS_PYSPARK_WORKER_UNSUPPORTED
def test_exact_cross_producer_duplicates_select_lowest_position(spark) -> None:
    collector = event("same")
    backfill = event("same", producer="apps.historical_backfill")
    records = spark.createDataFrame([raw(backfill, 8), raw(collector, 4)])

    frames = curate_market_trades(records, run_id="run", curated_at=NOW)
    selected = frames.curated.collect()

    assert len(selected) == 1
    assert selected[0]["kafka_offset"] == 4
    assert selected[0]["producer"] == "apps.collector"
    assert frames.quarantine.count() == 0


@WINDOWS_PYSPARK_WORKER_UNSUPPORTED
def test_conflicting_duplicate_has_no_winner_and_safe_quarantine(spark) -> None:
    first = event("conflict", price="10")
    second = deepcopy(first)
    second["payload"]["price"] = "11"  # type: ignore[index]
    records = spark.createDataFrame([raw(second, 2), raw(first, 1)])

    frames = curate_market_trades(records, run_id="run", curated_at=NOW)
    quarantine = [row.asDict(recursive=True) for row in frames.quarantine.collect()]

    assert frames.curated.count() == 0
    assert len(quarantine) == 2
    assert {row["failure_code"] for row in quarantine} == {"conflicting_duplicate"}
    assert {row["failure_field"] for row in quarantine} == {"price,notional"}
    assert "10.000" not in json.dumps(quarantine, default=str)


@WINDOWS_PYSPARK_WORKER_UNSUPPORTED
def test_symbols_are_independent_and_input_order_is_deterministic(spark) -> None:
    values = [raw(event("btc", symbol="BTC-USD"), 3), raw(event("eth", symbol="ETH-USD"), 2)]
    forward = curate_market_trades(
        spark.createDataFrame(values), run_id="run", curated_at=NOW
    ).curated.orderBy("event_id").collect()
    reverse = curate_market_trades(
        spark.createDataFrame(list(reversed(values))), run_id="run", curated_at=NOW
    ).curated.orderBy("event_id").collect()

    assert forward == reverse
    assert {row["symbol"] for row in forward} == {"BTC-USD", "ETH-USD"}
