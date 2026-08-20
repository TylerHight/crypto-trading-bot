import json
import logging
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from jobs.spark.config import RawAuditSettings
from jobs.spark.entrypoints.raw_market_trades import configure_s3a
from jobs.spark.transforms.raw_integrity import (
    build_integrity_report,
    select_kafka_audit_records,
)

LOGGER = logging.getLogger(__name__)
INTEGRITY_FAILURE_EXIT_CODE = 2


def read_bounded_kafka_batch(
    spark: SparkSession,
    settings: RawAuditSettings,
) -> DataFrame:
    """Create a non-streaming Kafka read bounded at the source's latest offsets."""

    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
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
            f"missing={partition['missing_from_parquet']} "
            f"duplicate_positions={partition['duplicate_parquet_positions']}"
        )
    print(f"  invalid_event_values={report['invalid_event_values']}")
    print(f"  duplicate_event_ids={report['duplicate_event_ids']} (warning only)")


def main() -> None:
    """Run the bounded, read-only Kafka-to-Parquet integrity audit."""

    logging.basicConfig(level=logging.INFO)
    settings = RawAuditSettings.from_env()
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

    kafka_records = select_kafka_audit_records(
        read_bounded_kafka_batch(spark, settings)
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
            settings.sample_limit,
            started_at,
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
