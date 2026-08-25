import json
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from jobs.spark.entrypoints.audit_raw_market_trades import (
    KafkaPartitionBound,
    kafka_batch_options,
)
from jobs.spark.transforms.raw_integrity import (
    build_integrity_report,
    parse_event_value,
    select_kafka_audit_records,
)

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("topic", T.StringType(), nullable=False),
        T.StructField("partition", T.IntegerType(), nullable=False),
        T.StructField("offset", T.LongType(), nullable=False),
        T.StructField("key", T.BinaryType(), nullable=True),
        T.StructField("value", T.BinaryType(), nullable=True),
        T.StructField(
            "headers",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("key", T.StringType(), nullable=False),
                        T.StructField("value", T.BinaryType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

PARQUET_SCHEMA = T.StructType(
    [
        T.StructField("kafka_topic", T.StringType(), nullable=False),
        T.StructField("kafka_partition", T.IntegerType(), nullable=False),
        T.StructField("kafka_offset", T.LongType(), nullable=False),
        T.StructField("kafka_key", T.BinaryType(), nullable=True),
        T.StructField("kafka_value", T.BinaryType(), nullable=True),
        T.StructField(
            "kafka_headers",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("key", T.StringType(), nullable=False),
                        T.StructField("value", T.BinaryType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

PARTITION_BOUND_SCHEMA = T.StructType(
    [
        T.StructField("kafka_topic", T.StringType(), nullable=False),
        T.StructField("kafka_partition", T.IntegerType(), nullable=False),
        T.StructField("earliest_offset", T.LongType(), nullable=False),
        T.StructField("ending_offset_exclusive", T.LongType(), nullable=False),
    ]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("raw-integrity-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def event_value(event_id: str) -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "market.trade.raw",
            "schema_version": "v1",
        }
    ).encode()


def kafka_row(
    offset: int,
    event_id: str,
    partition: int = 0,
) -> tuple[object, ...]:
    return (
        "market.trades.raw.v1",
        partition,
        offset,
        b"coinbase:BTC-USD",
        event_value(event_id),
        [],
    )


def parquet_row(
    offset: int,
    event_id: str,
    partition: int = 0,
) -> tuple[object, ...]:
    return (
        "market.trades.raw.v1",
        partition,
        offset,
        b"coinbase:BTC-USD",
        event_value(event_id),
        [],
    )


def report_for(
    spark: SparkSession,
    kafka_rows: list[tuple[object, ...]],
    parquet_rows: list[tuple[object, ...]],
    sample_limit: int = 20,
    partition_bounds: list[tuple[object, ...]] | None = None,
) -> dict[str, object]:
    kafka = select_kafka_audit_records(
        spark.createDataFrame(kafka_rows, schema=KAFKA_SCHEMA)
    )
    parquet = spark.createDataFrame(parquet_rows, schema=PARQUET_SCHEMA)
    if partition_bounds is None:
        offsets_by_partition: dict[tuple[str, int], list[int]] = {}
        for row in kafka_rows:
            offsets_by_partition.setdefault((str(row[0]), int(row[1])), []).append(
                int(row[2])
            )
        partition_bounds = [
            (topic, partition, min(offsets), max(offsets) + 1)
            for (topic, partition), offsets in offsets_by_partition.items()
        ]
    bounds = spark.createDataFrame(partition_bounds, schema=PARTITION_BOUND_SCHEMA)
    return build_integrity_report(
        kafka,
        parquet,
        bounds,
        sample_limit,
        datetime(2026, 8, 20, tzinfo=timezone.utc),
    )


def test_identical_kafka_and_parquet_positions_pass(spark: SparkSession) -> None:
    report = report_for(
        spark,
        [kafka_row(0, "event-0"), kafka_row(1, "event-1")],
        [parquet_row(0, "event-0"), parquet_row(1, "event-1")],
    )

    assert report["status"] == "passed"
    assert report["partitions"] == [
        {
            "kafka_topic": "market.trades.raw.v1",
            "kafka_partition": 0,
            "earliest_offset": 0,
            "ending_offset_exclusive": 2,
            "kafka_records": 2,
            "parquet_records_in_range": 2,
            "minimum_archived_offset": 0,
            "maximum_archived_offset": 1,
            "missing_from_parquet": 0,
            "duplicate_parquet_positions": 0,
        }
    ]


def test_equal_counts_still_find_one_missing_and_one_duplicate(
    spark: SparkSession,
) -> None:
    report = report_for(
        spark,
        [kafka_row(0, "event-0"), kafka_row(1, "event-1")],
        [parquet_row(0, "event-0"), parquet_row(0, "event-0-copy")],
    )

    partition = report["partitions"][0]
    assert report["status"] == "failed"
    assert partition["kafka_records"] == partition["parquet_records_in_range"] == 2
    assert partition["missing_from_parquet"] == 1
    assert partition["duplicate_parquet_positions"] == 1


def test_missing_position_has_a_bounded_safe_sample(spark: SparkSession) -> None:
    report = report_for(
        spark,
        [kafka_row(0, "event-0"), kafka_row(1, "event-1")],
        [],
        sample_limit=1,
    )

    assert report["status"] == "failed"
    assert report["partitions"][0]["missing_from_parquet"] == 2
    assert report["samples"]["missing_from_parquet"] == [
        {
            "kafka_topic": "market.trades.raw.v1",
            "kafka_partition": 0,
            "kafka_offset": 0,
        }
    ]


def test_duplicate_position_has_a_bounded_safe_sample(spark: SparkSession) -> None:
    report = report_for(
        spark,
        [kafka_row(0, "event-0"), kafka_row(1, "event-1")],
        [
            parquet_row(0, "event-0"),
            parquet_row(0, "event-0-copy"),
            parquet_row(1, "event-1"),
            parquet_row(1, "event-1-copy"),
        ],
        sample_limit=1,
    )

    assert report["status"] == "failed"
    assert report["partitions"][0]["duplicate_parquet_positions"] == 2
    assert len(report["samples"]["duplicate_parquet_positions"]) == 1


def test_partitions_are_compared_independently(spark: SparkSession) -> None:
    report = report_for(
        spark,
        [kafka_row(5, "p0", partition=0), kafka_row(9, "p1", partition=1)],
        [parquet_row(5, "p0", partition=0)],
    )

    partitions = {
        row["kafka_partition"]: row for row in report["partitions"]
    }
    assert partitions[0]["missing_from_parquet"] == 0
    assert partitions[1]["missing_from_parquet"] == 1


def test_empty_kafka_partition_is_included_from_captured_bounds(
    spark: SparkSession,
) -> None:
    report = report_for(
        spark,
        [kafka_row(5, "p1", partition=1)],
        [parquet_row(5, "p1", partition=1)],
        partition_bounds=[
            ("market.trades.raw.v1", 0, 0, 0),
            ("market.trades.raw.v1", 1, 5, 6),
        ],
    )

    partitions = {
        row["kafka_partition"]: row for row in report["partitions"]
    }
    assert report["status"] == "passed"
    assert partitions[0]["kafka_records"] == 0
    assert partitions[0]["parquet_records_in_range"] == 0
    assert partitions[0]["minimum_archived_offset"] is None
    assert partitions[0]["maximum_archived_offset"] is None


def test_parquet_history_before_retained_kafka_range_is_ignored(
    spark: SparkSession,
) -> None:
    report = report_for(
        spark,
        [kafka_row(10, "current")],
        [parquet_row(2, "old"), parquet_row(10, "current")],
    )

    assert report["status"] == "passed"
    assert report["partitions"][0]["parquet_records_in_range"] == 1


def test_invalid_values_are_counted_and_samples_are_capped(
    spark: SparkSession,
) -> None:
    kafka_rows = [kafka_row(index, f"event-{index}") for index in range(3)]
    parquet_rows = [
        (
            "market.trades.raw.v1",
            0,
            index,
            b"coinbase:BTC-USD",
            b"not-json",
            [],
        )
        for index in range(3)
    ]

    report = report_for(spark, kafka_rows, parquet_rows, sample_limit=2)

    assert report["status"] == "failed"
    assert report["invalid_event_values"] == 3
    assert len(report["samples"]["invalid_event_values"]) == 2


def test_duplicate_event_ids_are_warning_only(spark: SparkSession) -> None:
    report = report_for(
        spark,
        [kafka_row(0, "same"), kafka_row(1, "same")],
        [parquet_row(0, "same"), parquet_row(1, "same")],
    )

    assert report["status"] == "passed"
    assert report["duplicate_event_ids"] == 1
    assert report["samples"]["duplicate_event_ids"] == [
        {"event_id": "same", "occurrences": 2}
    ]


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (b"\xff", "invalid_utf8"),
        (b"not-json", "invalid_json"),
        (b"[]", "not_object"),
        (b'{"event_id":"one"}', "missing_or_invalid_event_type"),
    ],
)
def test_parse_event_value_explains_malformed_values(
    value: bytes,
    reason: str,
) -> None:
    assert parse_event_value(value) == (None, reason)


def test_captured_bounds_are_encoded_as_explicit_kafka_options() -> None:
    options = kafka_batch_options(
        [
            KafkaPartitionBound("market.trades.raw.v1", 1, 8, 13),
            KafkaPartitionBound("market.trades.raw.v1", 0, 3, 10),
        ]
    )

    assert json.loads(options["assign"]) == {"market.trades.raw.v1": [0, 1]}
    assert json.loads(options["startingOffsets"]) == {
        "market.trades.raw.v1": {"0": 3, "1": 8}
    }
    assert json.loads(options["endingOffsets"]) == {
        "market.trades.raw.v1": {"0": 10, "1": 13}
    }
