import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlsplit

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from jobs.spark.config import RawAuditSettings
from jobs.spark.entrypoints.raw_market_trades import configure_s3a
from jobs.spark.object_storage import HadoopObjectStore
from jobs.spark.transforms.raw_integrity import (
    build_integrity_report,
    select_kafka_audit_records,
)

LOGGER = logging.getLogger(__name__)
INTEGRITY_FAILURE_EXIT_CODE = 2

PARTITION_BOUND_SCHEMA = T.StructType(
    [
        T.StructField("kafka_topic", T.StringType(), nullable=False),
        T.StructField("kafka_partition", T.IntegerType(), nullable=False),
        T.StructField("earliest_offset", T.LongType(), nullable=False),
        T.StructField("ending_offset_exclusive", T.LongType(), nullable=False),
    ]
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit a fixed Kafka range against immutable raw Parquet."
    )
    parser.add_argument(
        "--report-output",
        help=(
            "Optional append-only JSON evidence URI. Use an s3a:// URI beneath "
            "the curation evidence prefix when the report will feed curation."
        ),
    )
    return parser


def validate_report_output(uri: str, allowed_prefix: str) -> str:
    """Require a credential-free S3A object below the evidence prefix."""

    parsed = urlsplit(uri)
    prefix = urlsplit(allowed_prefix)
    invalid = (
        parsed.scheme != "s3a"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or prefix.scheme != "s3a"
        or not prefix.netloc
        or prefix.username is not None
        or prefix.password is not None
        or prefix.query
        or prefix.fragment
        or parsed.netloc != prefix.netloc
    )
    decoded_path = unquote(parsed.path)
    decoded_prefix_path = unquote(prefix.path).rstrip("/")
    path_segments = decoded_path.split("/")
    if (
        invalid
        or not decoded_prefix_path
        or any(segment in {".", ".."} for segment in path_segments)
        or decoded_path == decoded_prefix_path
        or not decoded_path.startswith(decoded_prefix_path + "/")
        or decoded_path.endswith("/")
    ):
        raise ValueError(
            "report output must be a credential-free s3a:// JSON object beneath "
            "RAW_AUDIT_REPORT_PREFIX"
        )
    return uri


@dataclass(frozen=True)
class KafkaPartitionBound:
    """A fixed retained Kafka offset range captured before the audit read."""

    topic: str
    partition: int
    earliest_offset: int
    ending_offset_exclusive: int


def capture_kafka_partition_bounds(
    spark: SparkSession,
    settings: RawAuditSettings,
) -> list[KafkaPartitionBound]:
    """Capture broker beginning/end offsets for every current topic partition."""

    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM is unavailable for Kafka offset capture")
    properties = jvm.java.util.Properties()
    properties.setProperty(
        "bootstrap.servers",
        settings.kafka_bootstrap_servers,
    )
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)

    try:
        topic_names = jvm.java.util.Collections.singleton(settings.kafka_topic)
        descriptions = admin.describeTopics(topic_names).allTopicNames().get()
        description = descriptions.get(settings.kafka_topic)
        partition_ids = sorted(
            int(partition.partition()) for partition in description.partitions()
        )
        if not partition_ids:
            raise RuntimeError(
                f"Kafka topic {settings.kafka_topic!r} has no partitions"
            )

        topic_partitions: dict[int, Any] = {}
        earliest_specs = jvm.java.util.HashMap()
        ending_specs = jvm.java.util.HashMap()
        for partition_id in partition_ids:
            topic_partition = jvm.org.apache.kafka.common.TopicPartition(
                settings.kafka_topic,
                partition_id,
            )
            topic_partitions[partition_id] = topic_partition
            earliest_specs.put(
                topic_partition,
                jvm.org.apache.kafka.clients.admin.OffsetSpec.earliest(),
            )
            ending_specs.put(
                topic_partition,
                jvm.org.apache.kafka.clients.admin.OffsetSpec.latest(),
            )

        # Capture the upper boundary first so messages arriving afterward are
        # excluded even while the remaining audit metadata is being collected.
        endings = admin.listOffsets(ending_specs).all().get()
        beginnings = admin.listOffsets(earliest_specs).all().get()

        return [
            KafkaPartitionBound(
                topic=settings.kafka_topic,
                partition=partition_id,
                earliest_offset=int(
                    beginnings.get(topic_partitions[partition_id]).offset()
                ),
                ending_offset_exclusive=int(
                    endings.get(topic_partitions[partition_id]).offset()
                ),
            )
            for partition_id in partition_ids
        ]
    finally:
        admin.close()


def kafka_batch_options(
    bounds: list[KafkaPartitionBound],
) -> dict[str, str]:
    """Encode captured bounds as explicit Spark Kafka batch options."""

    if not bounds:
        raise ValueError("At least one Kafka partition bound is required")

    assignments: dict[str, list[int]] = {}
    starting_offsets: dict[str, dict[str, int]] = {}
    ending_offsets: dict[str, dict[str, int]] = {}
    for bound in bounds:
        assignments.setdefault(bound.topic, []).append(bound.partition)
        starting_offsets.setdefault(bound.topic, {})[str(bound.partition)] = (
            bound.earliest_offset
        )
        ending_offsets.setdefault(bound.topic, {})[str(bound.partition)] = (
            bound.ending_offset_exclusive
        )

    for partitions in assignments.values():
        partitions.sort()

    return {
        "assign": json.dumps(assignments, separators=(",", ":"), sort_keys=True),
        "startingOffsets": json.dumps(
            starting_offsets,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "endingOffsets": json.dumps(
            ending_offsets,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }


def read_bounded_kafka_batch(
    spark: SparkSession,
    settings: RawAuditSettings,
    bounds: list[KafkaPartitionBound],
) -> DataFrame:
    """Create a non-streaming Kafka read using already captured offsets."""

    options = kafka_batch_options(bounds)
    reader = spark.read.format("kafka").option(
        "kafka.bootstrap.servers",
        settings.kafka_bootstrap_servers,
    )
    for name, value in options.items():
        reader = reader.option(name, value)

    return (
        reader
        .option("failOnDataLoss", "true")
        .option("includeHeaders", "true")
        .load()
    )


def print_human_summary(report: dict[str, Any]) -> None:
    """Print a short operator-friendly summary before the machine-readable line."""

    print(f"Raw integrity audit: {str(report['status']).upper()}")
    for partition in report["partitions"]:
        print(
            "  "
            f"{partition['kafka_topic']} partition={partition['kafka_partition']} "
            f"range=[{partition['earliest_offset']},"
            f"{partition['ending_offset_exclusive']}) "
            f"kafka={partition['kafka_records']} "
            f"parquet={partition['parquet_records_in_range']} "
            f"archived_range=[{partition['minimum_archived_offset']},"
            f"{partition['maximum_archived_offset']}] "
            f"missing={partition['missing_from_parquet']} "
            f"duplicate_positions={partition['duplicate_parquet_positions']}"
        )
    print(f"  invalid_event_values={report['invalid_event_values']}")
    print(f"  duplicate_event_ids={report['duplicate_event_ids']} (warning only)")


def main() -> None:
    """Run the bounded, read-only Kafka-to-Parquet integrity audit."""

    logging.basicConfig(level=logging.INFO)
    arguments = build_parser().parse_args()
    settings = RawAuditSettings.from_env()
    report_output = (
        validate_report_output(arguments.report_output, settings.report_prefix)
        if arguments.report_output
        else None
    )
    started_at = datetime.now(timezone.utc)
    spark = (
        SparkSession.builder.appName("audit-raw-market-trades")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, settings)

    LOGGER.info(
        "Auditing retained Kafka records from %s/%s against %s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.input_path,
    )

    captured_bounds = capture_kafka_partition_bounds(spark, settings)
    partition_bounds = spark.createDataFrame(
        [
            (
                bound.topic,
                bound.partition,
                bound.earliest_offset,
                bound.ending_offset_exclusive,
            )
            for bound in captured_bounds
        ],
        schema=PARTITION_BOUND_SCHEMA,
    )
    kafka_records = select_kafka_audit_records(
        read_bounded_kafka_batch(spark, settings, captured_bounds)
    ).persist(StorageLevel.DISK_ONLY)

    try:
        # Materializing this bounded DataFrame freezes the earliest-to-latest
        # Kafka snapshot before Parquet is loaded and compared.
        kafka_record_count = kafka_records.count()
        LOGGER.info("Frozen Kafka audit snapshot contains %d records", kafka_record_count)

        parquet_records = spark.read.parquet(settings.input_path)
        report = build_integrity_report(
            kafka_records,
            parquet_records,
            partition_bounds,
            settings.sample_limit,
            started_at,
        )
        if report_output:
            report["report_uri"] = report_output
            HadoopObjectStore(spark).write_json_append_only(
                report_output,
                report,
            )
        print_human_summary(report)
        print(
            "AUDIT_REPORT_JSON="
            + json.dumps(report, separators=(",", ":"), sort_keys=True)
        )
    finally:
        kafka_records.unpersist()
        spark.stop()

    if report["status"] != "passed":
        raise SystemExit(INTEGRITY_FAILURE_EXIT_CODE)


if __name__ == "__main__":
    main()
