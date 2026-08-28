from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from jobs.spark.config import CurationSettings
from jobs.spark.curation import (
    FrozenRawSnapshot,
    InvalidCurationInput,
    canonical_json_bytes,
    load_frozen_snapshot,
    validate_distinct_prefixes,
)
from jobs.spark.entrypoints.raw_market_trades import configure_s3a
from jobs.spark.object_storage import HadoopObjectStore
from jobs.spark.schemas.market_trades_v1 import (
    CURATED_MARKET_TRADE_SCHEMA,
    CURATED_SCHEMA_VERSION,
    QUARANTINE_SCHEMA,
)
from jobs.spark.transforms.curated_market_trades import (
    CANONICAL_TOPIC,
    TRANSFORM_VERSION,
    curate_market_trades,
    decimal_policy,
)

LOGGER = logging.getLogger(__name__)
DRY_RUN_EXIT_CODE = 2
UNRESOLVED_EXIT_CODE = 3
INVALID_INPUT_EXIT_CODE = 4

RAW_COLUMNS = {
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "kafka_value",
    "kafka_headers",
}

BOUND_SCHEMA = T.StructType(
    [
        T.StructField("kafka_topic", T.StringType(), nullable=False),
        T.StructField("kafka_partition", T.IntegerType(), nullable=False),
        T.StructField("earliest_offset", T.LongType(), nullable=False),
        T.StructField("ending_offset_exclusive", T.LongType(), nullable=False),
    ]
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build an immutable curated market-trade snapshot."
    )
    parser.add_argument("--raw-integrity-report", required=True)
    parser.add_argument(
        "--raw-input",
        default="s3a://crypto-data/raw/market_trade_raw/v1",
    )
    parser.add_argument(
        "--output",
        default="s3a://crypto-data/curated/market_trades/v1",
    )
    parser.add_argument(
        "--quarantine-output",
        default="s3a://crypto-data/quarantine/market_trades/v1",
    )
    parser.add_argument(
        "--evidence-prefix",
        help="Override CURATION_EVIDENCE_PREFIX.",
    )
    parser.add_argument(
        "--local-development",
        action="store_true",
        help="Allow local audit evidence and local filesystem paths.",
    )
    parser.add_argument("--apply", action="store_true")
    return parser


def filter_to_snapshot(
    raw_records: DataFrame,
    snapshot: FrozenRawSnapshot,
) -> DataFrame:
    missing = RAW_COLUMNS.difference(raw_records.columns)
    if missing:
        raise InvalidCurationInput(
            "raw input is missing required columns: " + ", ".join(sorted(missing))
        )
    bounds = raw_records.sparkSession.createDataFrame(
        [
            (
                item.kafka_topic,
                item.kafka_partition,
                item.earliest_offset,
                item.ending_offset_exclusive,
            )
            for item in snapshot.bounds
        ],
        BOUND_SCHEMA,
    )
    return (
        raw_records.alias("raw")
        .join(
            bounds.alias("bounds"),
            on=(
                (F.col("raw.kafka_topic") == F.col("bounds.kafka_topic"))
                & (
                    F.col("raw.kafka_partition")
                    == F.col("bounds.kafka_partition")
                )
                & (F.col("raw.kafka_offset") >= F.col("bounds.earliest_offset"))
                & (
                    F.col("raw.kafka_offset")
                    < F.col("bounds.ending_offset_exclusive")
                )
            ),
            how="inner",
        )
        .select(*[F.col(f"raw.{column}").alias(column) for column in RAW_COLUMNS])
    )


def _timestamp_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time_bounds(records: DataFrame, column: str) -> dict[str, str | None]:
    row = records.agg(F.min(column).alias("minimum"), F.max(column).alias("maximum")).first()
    if row is None:
        return {"minimum": None, "maximum": None}
    return {
        "minimum": _timestamp_text(row["minimum"]),
        "maximum": _timestamp_text(row["maximum"]),
    }


def _bounded_counts(
    records: DataFrame,
    columns: list[str],
    limit: int,
) -> dict[str, Any]:
    grouped = records.groupBy(*columns).agg(F.count(F.lit(1)).alias("rows"))
    rows = grouped.orderBy(*columns).limit(limit + 1).collect()
    return {
        "values": [row.asDict(recursive=True) for row in rows[:limit]],
        "truncated": len(rows) > limit,
    }


def _schema_signature(schema: T.StructType) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in schema.fields]


def validate_published_outputs(
    spark: SparkSession,
    curated_uri: str,
    quarantine_uri: str,
    expected_curated: int,
    expected_quarantine: int,
) -> tuple[DataFrame, DataFrame]:
    curated = spark.read.parquet(curated_uri)
    quarantine = spark.read.parquet(quarantine_uri)
    if _schema_signature(curated.schema) != _schema_signature(CURATED_MARKET_TRADE_SCHEMA):
        raise RuntimeError("published curated schema does not match v1")
    if _schema_signature(quarantine.schema) != _schema_signature(QUARANTINE_SCHEMA):
        raise RuntimeError("published quarantine schema does not match v1")
    if curated.count() != expected_curated or quarantine.count() != expected_quarantine:
        raise RuntimeError("published output row counts do not reconcile")
    duplicates = curated.groupBy("event_id").count().where(F.col("count") != 1).limit(1)
    invalid = curated.where(
        F.col("event_id").isNull()
        | (F.col("price") <= 0)
        | (F.col("size") <= 0)
        | (F.col("notional") <= 0)
        | (~F.col("source_side").isin("BUY", "SELL"))
        | (F.col("event_date") != F.to_date("event_time"))
    ).limit(1)
    if duplicates.count() or invalid.count():
        raise RuntimeError("published curated invariants failed")
    return curated, quarantine


def _safe_samples(quarantine: DataFrame, limit: int) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True)
        for row in quarantine.select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "failure_code",
            "failure_field",
            "event_id_when_safe",
            "value_sha256",
        )
        .orderBy("kafka_topic", "kafka_partition", "kafka_offset")
        .limit(limit)
        .collect()
    ]


def _metrics(
    raw_count: int,
    frames: Any,
    sample_limit: int,
) -> dict[str, Any]:
    valid_deliveries = frames.candidates.count()
    curated_count = frames.curated.count()
    invalid_input = frames.quarantine.where(
        F.col("failure_code") != "conflicting_duplicate"
    ).count()
    conflict_deliveries_row = frames.duplicate_groups.where(
        F.col("logical_variants") > 1
    ).agg(F.coalesce(F.sum("deliveries"), F.lit(0)).alias("value")).first()
    conflict_deliveries = int(conflict_deliveries_row["value"])
    exact_duplicate_row = frames.duplicate_groups.where(
        F.col("logical_variants") == 1
    ).agg(
        F.coalesce(F.sum(F.col("deliveries") - F.lit(1)), F.lit(0)).alias("value")
    ).first()
    exact_duplicates = int(exact_duplicate_row["value"])
    quarantine_count = frames.quarantine.count()
    if raw_count != valid_deliveries + invalid_input:
        raise RuntimeError("raw-to-validation count reconciliation failed")
    if valid_deliveries != curated_count + exact_duplicates + conflict_deliveries:
        raise RuntimeError("logical deduplication count reconciliation failed")

    latency = frames.curated.select(
        (
            F.unix_millis("kafka_timestamp") - F.unix_millis("event_time")
        ).alias("latency_ms")
    ).agg(
        F.percentile_approx("latency_ms", [0.5, 0.95, 0.99], 1000).alias(
            "percentiles"
        )
    ).first()["percentiles"]
    return {
        "raw_rows_in_snapshot": raw_count,
        "valid_deliveries": valid_deliveries,
        "curated_logical_trades": curated_count,
        "unique_event_ids_examined": frames.duplicate_groups.count(),
        "exact_duplicate_deliveries": exact_duplicates,
        "conflicting_duplicate_deliveries": conflict_deliveries,
        "quarantined_input_rows": invalid_input,
        "quarantine_records": quarantine_count,
        "quarantine_counts": _bounded_counts(
            frames.quarantine, ["failure_code"], sample_limit
        ),
        "curated_rows_by_exchange_symbol": _bounded_counts(
            frames.curated, ["exchange", "symbol"], sample_limit
        ),
        "curated_rows_by_producer": _bounded_counts(
            frames.curated, ["producer"], sample_limit
        ),
        "curated_rows_by_event_date": _bounded_counts(
            frames.curated, ["event_date"], sample_limit
        ),
        "event_time_bounds": _time_bounds(frames.curated, "event_time"),
        "ingestion_time_bounds": _time_bounds(frames.curated, "ingested_at"),
        "kafka_time_bounds": _time_bounds(frames.curated, "kafka_timestamp"),
        "late_arrival_latency_ms": {
            "p50": latency[0] if latency else None,
            "p95": latency[1] if latency else None,
            "p99": latency[2] if latency else None,
        },
        "samples": _safe_samples(frames.quarantine, sample_limit),
    }


def _existing_manifest(
    store: HadoopObjectStore,
    manifest_uri: str,
    snapshot_key: str,
) -> dict[str, Any] | None:
    if not store.exists(manifest_uri):
        return None
    try:
        manifest = json.loads(store.read_bytes(manifest_uri))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidCurationInput("existing manifest is malformed") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("snapshot_key") != snapshot_key
        or manifest.get("status") != "published"
    ):
        raise InvalidCurationInput("existing snapshot manifest is not a valid publication")
    return manifest


def run_curation(
    spark: SparkSession,
    arguments: argparse.Namespace,
    settings: CurationSettings,
    *,
    store: HadoopObjectStore | None = None,
    started_at: datetime | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    validate_distinct_prefixes(
        arguments.raw_input, arguments.output, arguments.quarantine_output
    )
    object_store = store or HadoopObjectStore(spark)
    started = started_at or datetime.now(timezone.utc)
    curation_run_id = run_id or str(uuid4())
    report_bytes = object_store.read_bytes(arguments.raw_integrity_report)
    snapshot = load_frozen_snapshot(
        report_bytes,
        report_uri=arguments.raw_integrity_report,
        allowed_evidence_prefix=arguments.evidence_prefix or settings.evidence_prefix,
        local_development=arguments.local_development,
    )
    manifest_uri = (
        f"{arguments.output.rstrip('/')}/manifests/"
        f"{snapshot.snapshot_key}/manifest.json"
    )
    existing = _existing_manifest(object_store, manifest_uri, snapshot.snapshot_key)
    if existing is not None:
        return {
            **existing,
            "status": "resolved_existing_snapshot",
            "published_status": "published",
            "manifest_uri": manifest_uri,
        }
    if (
        settings.maximum_input_rows is not None
        and snapshot.expected_raw_rows > settings.maximum_input_rows
    ):
        raise InvalidCurationInput("frozen snapshot exceeds CURATION_MAXIMUM_INPUT_ROWS")

    raw_records = spark.read.parquet(arguments.raw_input)
    bounded = filter_to_snapshot(raw_records, snapshot).persist(StorageLevel.DISK_ONLY)
    try:
        raw_count = bounded.count()
        if raw_count != snapshot.expected_raw_rows:
            raise InvalidCurationInput(
                "raw Parquet no longer matches the frozen audit partition bounds"
            )
        frames = curate_market_trades(
            bounded,
            run_id=curation_run_id,
            curated_at=started,
            canonical_topic=CANONICAL_TOPIC,
        )
        frames.candidates.persist(StorageLevel.DISK_ONLY)
        frames.curated.persist(StorageLevel.DISK_ONLY)
        frames.quarantine.persist(StorageLevel.DISK_ONLY)
        frames.duplicate_groups.persist(StorageLevel.DISK_ONLY)
        try:
            metrics = _metrics(raw_count, frames, settings.sample_limit)
            conflict_count = metrics["conflicting_duplicate_deliveries"]
            status = (
                "unresolved_conflicting_duplicates"
                if conflict_count
                else "dry_run_ready"
            )
            report: dict[str, Any] = {
                "curation_run_id": curation_run_id,
                "snapshot_key": snapshot.snapshot_key,
                "curated_schema_version": CURATED_SCHEMA_VERSION,
                "transform_version": TRANSFORM_VERSION,
                "mode": "apply" if arguments.apply else "dry_run",
                "started_at": _timestamp_text(started),
                "completed_at": _timestamp_text(datetime.now(timezone.utc)),
                "status": status,
                "raw_integrity_report_uri": snapshot.report_uri,
                "raw_integrity_report_sha256": snapshot.report_sha256,
                "input_topic": snapshot.topic,
                "partition_offset_bounds": [
                    item.as_dict() for item in snapshot.bounds
                ],
                "decimal_policy": decimal_policy(),
                **metrics,
                "manifest_uri": manifest_uri,
            }
            if not arguments.apply:
                return report

            curated_uri = (
                f"{arguments.output.rstrip('/')}/runs/{curation_run_id}"
            )
            quarantine_uri = (
                f"{arguments.quarantine_output.rstrip('/')}/runs/{curation_run_id}"
            )
            curated_writer = frames.curated.write.mode("errorifexists")
            if metrics["curated_logical_trades"]:
                curated_writer.partitionBy("event_date").parquet(curated_uri)
            else:
                # A root-level empty Parquet file preserves the checked-in schema;
                # partitioned writes otherwise create no readable files.
                curated_writer.parquet(curated_uri)
            quarantine_writer = frames.quarantine.write.mode("errorifexists")
            quarantine_writer.parquet(quarantine_uri)
            validated_curated, validated_quarantine = validate_published_outputs(
                spark,
                curated_uri,
                quarantine_uri,
                metrics["curated_logical_trades"],
                metrics["quarantine_records"],
            )
            curated_files = object_store.parquet_metrics(curated_uri)
            quarantine_files = object_store.parquet_metrics(quarantine_uri)
            report.update(
                {
                    "curated_output_uri": curated_uri,
                    "quarantine_output_uri": quarantine_uri,
                    "output_files": {
                        "curated": curated_files.files,
                        "quarantine": quarantine_files.files,
                    },
                    "output_bytes": curated_files.bytes + quarantine_files.bytes,
                }
            )
            # Force the verified reads before publication; no consumer discovers
            # run-scoped data until this manifest exists.
            validated_curated.unpersist(blocking=False)
            validated_quarantine.unpersist(blocking=False)
            if conflict_count:
                report["status"] = "unresolved_conflicting_duplicates"
                report["completed_at"] = _timestamp_text(datetime.now(timezone.utc))
                return report

            report["status"] = "published"
            report["completed_at"] = _timestamp_text(datetime.now(timezone.utc))
            object_store.write_json_append_only(manifest_uri, report)
            return report
        finally:
            frames.duplicate_groups.unpersist()
            frames.quarantine.unpersist()
            frames.curated.unpersist()
            frames.candidates.unpersist()
    finally:
        bounded.unpersist()


def print_summary(report: dict[str, Any]) -> None:
    print(f"Curated market trades: {str(report['status']).upper()}")
    print(f"  snapshot_key={report['snapshot_key']}")
    if "raw_rows_in_snapshot" in report:
        print(f"  raw_rows={report['raw_rows_in_snapshot']}")
        print(f"  curated_rows={report['curated_logical_trades']}")
        print(f"  exact_duplicate_deliveries={report['exact_duplicate_deliveries']}")
        print(
            "  conflicting_duplicate_deliveries="
            f"{report['conflicting_duplicate_deliveries']}"
        )
        print(f"  quarantined_input_rows={report['quarantined_input_rows']}")
    print("CURATION_REPORT_JSON=" + canonical_json_bytes(report).decode("ascii"))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    arguments = build_parser().parse_args()
    settings = CurationSettings.from_env()
    spark = (
        SparkSession.builder.appName("curate-market-trades")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, settings)
    try:
        try:
            report = run_curation(spark, arguments, settings)
        except InvalidCurationInput as error:
            LOGGER.error("Curation input rejected: %s", error)
            raise SystemExit(INVALID_INPUT_EXIT_CODE) from error
        print_summary(report)
        if report["status"] == "dry_run_ready":
            raise SystemExit(DRY_RUN_EXIT_CODE)
        if report["status"] == "unresolved_conflicting_duplicates":
            raise SystemExit(UNRESOLVED_EXIT_CODE)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
