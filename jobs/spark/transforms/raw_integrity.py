import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

POSITION_COLUMNS = ["kafka_topic", "kafka_partition", "kafka_offset"]
REQUIRED_EVENT_FIELDS = ("event_id", "event_type", "schema_version")

EVENT_VALIDATION_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("invalid_reason", T.StringType(), nullable=True),
    ]
)


def parse_event_value(
    value: bytes | bytearray | memoryview | None,
) -> tuple[str | None, str | None]:
    """Extract an event ID or explain why an archived value is malformed.

    This deliberately uses strict UTF-8 decoding. A permissive decoder could
    replace invalid bytes and make a corrupted Kafka value look valid.
    """

    if value is None:
        return None, "null_value"

    try:
        text = bytes(value).decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None, "invalid_utf8"

    try:
        event = json.loads(text)
    except json.JSONDecodeError:
        return None, "invalid_json"

    if not isinstance(event, Mapping):
        return None, "not_object"

    for field in REQUIRED_EVENT_FIELDS:
        field_value = event.get(field)
        if not isinstance(field_value, str) or not field_value.strip():
            return None, f"missing_or_invalid_{field}"

    return event["event_id"], None


def select_kafka_audit_records(records: DataFrame) -> DataFrame:
    """Keep only the Kafka identity needed for the archive comparison.

    Values and headers can be large. The audit validates the archived value
    from Parquet, so caching Kafka payloads would waste memory without making
    the position comparison stronger.
    """

    return records.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )


def calculate_partition_bounds(kafka_records: DataFrame) -> DataFrame:
    """Describe the exact offsets materialized by the bounded Kafka read."""

    return kafka_records.groupBy("kafka_topic", "kafka_partition").agg(
        F.min("kafka_offset").alias("earliest_offset"),
        (F.max("kafka_offset") + F.lit(1)).alias("ending_offset_exclusive"),
        F.count(F.lit(1)).alias("kafka_records"),
    )


def filter_parquet_to_bounds(
    parquet_records: DataFrame,
    partition_bounds: DataFrame,
) -> DataFrame:
    """Keep only Parquet rows inside Kafka's currently retained audit range."""

    parquet = parquet_records.alias("parquet")
    bounds = partition_bounds.alias("bounds")
    return parquet.join(
        bounds,
        on=(
            (F.col("parquet.kafka_topic") == F.col("bounds.kafka_topic"))
            & (
                F.col("parquet.kafka_partition")
                == F.col("bounds.kafka_partition")
            )
            & (F.col("parquet.kafka_offset") >= F.col("bounds.earliest_offset"))
            & (
                F.col("parquet.kafka_offset")
                < F.col("bounds.ending_offset_exclusive")
            )
        ),
        how="inner",
    ).select(
        F.col("parquet.kafka_topic").alias("kafka_topic"),
        F.col("parquet.kafka_partition").alias("kafka_partition"),
        F.col("parquet.kafka_offset").alias("kafka_offset"),
        F.col("parquet.kafka_value").alias("kafka_value"),
    )


def find_missing_parquet_positions(
    kafka_records: DataFrame,
    parquet_records: DataFrame,
) -> DataFrame:
    """Return Kafka log positions with no corresponding Parquet row."""

    kafka_positions = kafka_records.select(*POSITION_COLUMNS).distinct()
    parquet_positions = parquet_records.select(*POSITION_COLUMNS).distinct()
    return kafka_positions.join(
        parquet_positions,
        on=POSITION_COLUMNS,
        how="left_anti",
    )


def find_duplicate_parquet_positions(parquet_records: DataFrame) -> DataFrame:
    """Return Kafka log positions archived more than once."""

    return (
        parquet_records.groupBy(*POSITION_COLUMNS)
        .agg(F.count(F.lit(1)).alias("occurrences"))
        .where(F.col("occurrences") > 1)
    )


def add_event_validation(parquet_records: DataFrame) -> DataFrame:
    """Add parsed event identity and a nullable malformed-value reason."""

    validation_udf = F.udf(parse_event_value, EVENT_VALIDATION_SCHEMA)
    return (
        parquet_records.withColumn(
            "_event_validation",
            validation_udf(F.col("kafka_value")),
        )
        .withColumn("event_id", F.col("_event_validation.event_id"))
        .withColumn("invalid_reason", F.col("_event_validation.invalid_reason"))
        .drop("_event_validation")
    )


def find_duplicate_event_ids(validated_records: DataFrame) -> DataFrame:
    """Return valid event IDs that occur in more than one archived row."""

    return (
        validated_records.where(F.col("invalid_reason").isNull())
        .groupBy("event_id")
        .agg(F.count(F.lit(1)).alias("occurrences"))
        .where(F.col("occurrences") > 1)
    )


def safe_sample(
    records: DataFrame,
    columns: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    """Collect only bounded, explicitly selected identifiers for a report."""

    return [
        row.asDict(recursive=True)
        for row in records.select(*columns).orderBy(*columns).limit(limit).collect()
    ]


def build_integrity_report(
    kafka_records: DataFrame,
    parquet_records: DataFrame,
    sample_limit: int,
    started_at: datetime,
) -> dict[str, Any]:
    """Run all bounded checks and return a JSON-serializable audit report."""

    partition_bounds = calculate_partition_bounds(kafka_records).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    parquet_in_range = filter_parquet_to_bounds(
        parquet_records,
        partition_bounds,
    )
    parquet_positions = parquet_in_range.select(*POSITION_COLUMNS).persist(
        StorageLevel.DISK_ONLY
    )
    missing_positions = find_missing_parquet_positions(
        kafka_records,
        parquet_positions,
    ).persist(StorageLevel.DISK_ONLY)
    duplicate_positions = find_duplicate_parquet_positions(
        parquet_positions
    ).persist(StorageLevel.DISK_ONLY)
    validated_records = (
        add_event_validation(parquet_in_range)
        .select(*POSITION_COLUMNS, "event_id", "invalid_reason")
        .persist(StorageLevel.DISK_ONLY)
    )
    invalid_records = validated_records.where(F.col("invalid_reason").isNotNull())
    duplicate_event_ids = find_duplicate_event_ids(validated_records).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:
        parquet_counts = parquet_positions.groupBy(
            "kafka_topic",
            "kafka_partition",
        ).agg(F.count(F.lit(1)).alias("parquet_records_in_range"))
        missing_counts = missing_positions.groupBy(
            "kafka_topic",
            "kafka_partition",
        ).agg(F.count(F.lit(1)).alias("missing_from_parquet"))
        duplicate_counts = duplicate_positions.groupBy(
            "kafka_topic",
            "kafka_partition",
        ).agg(F.count(F.lit(1)).alias("duplicate_parquet_positions"))

        partition_rows = (
            partition_bounds.join(
                parquet_counts,
                on=["kafka_topic", "kafka_partition"],
                how="left",
            )
            .join(
                missing_counts,
                on=["kafka_topic", "kafka_partition"],
                how="left",
            )
            .join(
                duplicate_counts,
                on=["kafka_topic", "kafka_partition"],
                how="left",
            )
            .fillna(
                0,
                subset=[
                    "parquet_records_in_range",
                    "missing_from_parquet",
                    "duplicate_parquet_positions",
                ],
            )
            .orderBy("kafka_topic", "kafka_partition")
            .collect()
        )

        partitions = [row.asDict(recursive=True) for row in partition_rows]
        missing_count = sum(row["missing_from_parquet"] for row in partitions)
        duplicate_position_count = sum(
            row["duplicate_parquet_positions"] for row in partitions
        )
        invalid_count = invalid_records.count()
        duplicate_event_id_count = duplicate_event_ids.count()
        failed = bool(missing_count or duplicate_position_count or invalid_count)

        return {
            "topic": (
                partitions[0]["kafka_topic"] if partitions else "unknown"
            ),
            "started_at": started_at.astimezone(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "completed_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "status": "failed" if failed else "passed",
            "partitions": partitions,
            "invalid_event_values": invalid_count,
            "duplicate_event_ids": duplicate_event_id_count,
            "samples": {
                "missing_from_parquet": safe_sample(
                    missing_positions,
                    POSITION_COLUMNS,
                    sample_limit,
                ),
                "duplicate_parquet_positions": safe_sample(
                    duplicate_positions,
                    [*POSITION_COLUMNS, "occurrences"],
                    sample_limit,
                ),
                "invalid_event_values": safe_sample(
                    invalid_records,
                    [*POSITION_COLUMNS, "invalid_reason"],
                    sample_limit,
                ),
                "duplicate_event_ids": safe_sample(
                    duplicate_event_ids,
                    ["event_id", "occurrences"],
                    sample_limit,
                ),
            },
        }
    finally:
        duplicate_event_ids.unpersist()
        validated_records.unpersist()
        duplicate_positions.unpersist()
        missing_positions.unpersist()
        parquet_positions.unpersist()
        partition_bounds.unpersist()
