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

from jobs.spark.candles import (
    InvalidCandleInput,
    PublishedCuratedSnapshot,
    candle_snapshot_key,
    load_curated_snapshot,
    validate_output_prefix,
)
from jobs.spark.config import CandleSettings
from jobs.spark.curation import canonical_json_bytes
from jobs.spark.entrypoints.raw_market_trades import configure_s3a
from jobs.spark.object_storage import HadoopObjectStore
from jobs.spark.schemas.market_candles_v1 import (
    CANDLE_KEY_COLUMNS,
    CANDLE_SCHEMA_VERSION,
    CANDLE_TRANSFORM_VERSION,
    MARKET_CANDLE_SCHEMA,
    SUPPORTED_INTERVAL,
)
from jobs.spark.schemas.market_trades_v1 import (
    CURATED_MARKET_TRADE_SCHEMA,
    CURATED_SCHEMA_VERSION,
)
from jobs.spark.transforms.market_candles import aggregate_market_candles

LOGGER = logging.getLogger(__name__)
DRY_RUN_EXIT_CODE = 2
INVALID_INPUT_EXIT_CODE = 4


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build immutable one-minute candles from one curated manifest."
    )
    parser.add_argument("--curated-manifest", required=True)
    parser.add_argument("--curated-manifest-sha256", required=True)
    parser.add_argument(
        "--output",
        default="s3a://crypto-data/analytics/market_candles/v1",
    )
    parser.add_argument("--interval", default=SUPPORTED_INTERVAL, choices=["1m"])
    parser.add_argument(
        "--source-manifest-prefix",
        help="Override CANDLE_SOURCE_MANIFEST_PREFIX.",
    )
    parser.add_argument(
        "--known-backfill-event-id",
        help="Optionally prove this historical event contributes to its UTC minute.",
    )
    parser.add_argument(
        "--local-development",
        action="store_true",
        help="Allow local manifest and output paths.",
    )
    parser.add_argument("--apply", action="store_true")
    return parser


def _timestamp_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _schema_signature(schema: T.StructType) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def _require_curated_schema(records: DataFrame) -> None:
    if _schema_signature(records.schema) != _schema_signature(CURATED_MARKET_TRADE_SCHEMA):
        raise InvalidCandleInput("curated output schema does not match v1")


def _validate_curated_rows(records: DataFrame, expected_rows: int) -> int:
    actual_rows = records.count()
    if actual_rows != expected_rows:
        raise InvalidCandleInput("curated row count does not match its manifest")
    duplicate = records.groupBy("event_id").count().where(F.col("count") != 1).limit(1)
    invalid = records.where(
        F.col("event_id").isNull()
        | (F.col("price") <= 0)
        | (F.col("size") <= 0)
        | (F.col("notional") <= 0)
        | (F.col("curated_schema_version") != CURATED_SCHEMA_VERSION)
        | (F.col("event_date") != F.to_date("event_time"))
    ).limit(1)
    if duplicate.count() or invalid.count():
        raise InvalidCandleInput("curated source invariants failed")
    return actual_rows


def _bounded_counts(
    records: DataFrame,
    columns: list[str],
    limit: int,
) -> dict[str, Any]:
    rows = (
        records.groupBy(*columns)
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy(*columns)
        .limit(limit + 1)
        .collect()
    )
    return {
        "values": [row.asDict(recursive=True) for row in rows[:limit]],
        "truncated": len(rows) > limit,
    }


def _window_bounds(candles: DataFrame) -> dict[str, str | None]:
    row = candles.agg(
        F.min("window_start").alias("minimum"),
        F.max("window_end").alias("maximum"),
    ).first()
    if row is None:
        return {"minimum": None, "maximum": None}
    return {
        "minimum": _timestamp_text(row["minimum"]),
        "maximum": _timestamp_text(row["maximum"]),
    }


def _safe_samples(candles: DataFrame, limit: int) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True)
        for row in candles.select(
            "exchange",
            "symbol",
            "interval",
            "window_start",
            "window_end",
            "trade_count",
            "historical_backfill_trade_count",
        )
        .orderBy("exchange", "symbol", "window_start")
        .limit(limit)
        .collect()
    ]


def _validate_candles(candles: DataFrame, source_trade_count: int) -> dict[str, int]:
    candle_count = candles.count()
    duplicate = (
        candles.groupBy(*CANDLE_KEY_COLUMNS)
        .count()
        .where(F.col("count") != 1)
        .limit(1)
    )
    invalid = candles.where(
        F.col("exchange").isNull()
        | F.col("symbol").isNull()
        | F.col("window_start").isNull()
        | F.col("open").isNull()
        | F.col("high").isNull()
        | F.col("low").isNull()
        | F.col("close").isNull()
        | F.col("base_volume").isNull()
        | F.col("quote_volume").isNull()
        | F.col("vwap").isNull()
        | (F.col("interval") != SUPPORTED_INTERVAL)
        | (F.col("window_end") != F.expr("window_start + INTERVAL 1 MINUTE"))
        | (F.col("low") > F.col("open"))
        | (F.col("open") > F.col("high"))
        | (F.col("low") > F.col("close"))
        | (F.col("close") > F.col("high"))
        | (F.col("base_volume") <= 0)
        | (F.col("quote_volume") <= 0)
        | (F.col("vwap") <= 0)
        | (F.col("trade_count") <= 0)
        | (F.col("historical_backfill_trade_count") < 0)
        | (F.col("historical_backfill_trade_count") > F.col("trade_count"))
        | (F.col("event_date") != F.to_date("window_start"))
        | (F.col("event_hour") != F.date_format("window_start", "HH"))
        | (F.col("candle_schema_version") != CANDLE_SCHEMA_VERSION)
        | (F.col("candle_transform_version") != CANDLE_TRANSFORM_VERSION)
    ).limit(1)
    count_row = candles.agg(
        F.coalesce(F.sum("trade_count"), F.lit(0)).alias("trade_count_sum")
    ).first()
    trade_count_sum = int(count_row["trade_count_sum"] if count_row else 0)
    if duplicate.count() or invalid.count():
        raise InvalidCandleInput("candle invariants or decimal bounds failed")
    if trade_count_sum != source_trade_count:
        raise InvalidCandleInput("candle trade counts do not reconcile to curated input")
    return {"candle_count": candle_count, "trade_count_sum": trade_count_sum}


def _known_backfill_identity(
    records: DataFrame,
    event_id: str | None,
) -> tuple[str, str, datetime] | None:
    if event_id is None:
        return None
    rows = (
        records.where(F.col("event_id") == event_id)
        .select(
            "exchange",
            "symbol",
            "producer",
            F.date_trunc("minute", "event_time").alias("window_start"),
        )
        .limit(2)
        .collect()
    )
    if len(rows) != 1 or rows[0]["producer"] != "apps.historical_backfill":
        raise InvalidCandleInput("known backfill event is missing or has wrong provenance")
    return rows[0]["exchange"], rows[0]["symbol"], rows[0]["window_start"]


def _verify_known_backfill(
    candles: DataFrame,
    identity: tuple[str, str, datetime] | None,
) -> None:
    if identity is None:
        return
    exchange, symbol, window_start = identity
    count = candles.where(
        (F.col("exchange") == exchange)
        & (F.col("symbol") == symbol)
        & (F.col("window_start") == window_start)
        & (F.col("historical_backfill_trade_count") > 0)
    ).count()
    if count != 1:
        raise InvalidCandleInput("known backfill event did not contribute to one candle")


def _report_metrics(candles: DataFrame, sample_limit: int) -> dict[str, Any]:
    stats = candles.agg(
        F.min("trade_count").alias("minimum"),
        F.max("trade_count").alias("maximum"),
        F.percentile_approx("trade_count", [0.5, 0.95, 0.99], 1000).alias(
            "percentiles"
        ),
    ).first()
    percentiles = stats["percentiles"] if stats else None
    return {
        "candles_by_exchange_symbol": _bounded_counts(
            candles, ["exchange", "symbol"], sample_limit
        ),
        "candles_by_event_date_hour": _bounded_counts(
            candles, ["event_date", "event_hour"], sample_limit
        ),
        "window_time_bounds": _window_bounds(candles),
        "trade_count_per_candle": {
            "minimum": stats["minimum"] if stats else None,
            "maximum": stats["maximum"] if stats else None,
            "p50": percentiles[0] if percentiles else None,
            "p95": percentiles[1] if percentiles else None,
            "p99": percentiles[2] if percentiles else None,
        },
        "samples": _safe_samples(candles, sample_limit),
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
        raise InvalidCandleInput("existing candle manifest is malformed") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("snapshot_key") != snapshot_key
        or manifest.get("status") != "published"
    ):
        raise InvalidCandleInput("existing candle manifest is not a valid publication")
    return manifest


def _read_source(
    spark: SparkSession,
    source: PublishedCuratedSnapshot,
) -> DataFrame:
    records = spark.read.parquet(source.output_uri)
    _require_curated_schema(records)
    return records


def validate_published_output(
    spark: SparkSession,
    output_uri: str,
    expected_candles: int,
    expected_trades: int,
) -> DataFrame:
    physical = spark.read.parquet(output_uri)
    expected_types = _schema_signature(MARKET_CANDLE_SCHEMA)
    physical_types = _schema_signature(physical.schema)
    # Numeric-looking Hive partition values may be inferred as integers. Read
    # with the checked-in schema below, but still validate every physical field.
    for name, expected_type in expected_types.items():
        actual_type = physical_types.get(name)
        if name == "event_hour" and actual_type in {"int", "string"}:
            continue
        if actual_type != expected_type:
            raise InvalidCandleInput("published candle schema does not match v1")
    candles = spark.read.schema(MARKET_CANDLE_SCHEMA).parquet(output_uri)
    if _schema_signature(candles.schema) != _schema_signature(MARKET_CANDLE_SCHEMA):
        raise InvalidCandleInput("published candle schema does not match v1")
    metrics = _validate_candles(candles, expected_trades)
    if metrics["candle_count"] != expected_candles:
        raise InvalidCandleInput("published candle row count changed during read-back")
    return candles


def run_candles(
    spark: SparkSession,
    arguments: argparse.Namespace,
    settings: CandleSettings,
    *,
    store: HadoopObjectStore | None = None,
    started_at: datetime | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    object_store = store or HadoopObjectStore(spark)
    started = started_at or datetime.now(timezone.utc)
    candle_run_id = run_id or str(uuid4())
    manifest_bytes = object_store.read_bytes(arguments.curated_manifest)
    source = load_curated_snapshot(
        manifest_bytes,
        manifest_uri=arguments.curated_manifest,
        expected_sha256=arguments.curated_manifest_sha256,
        allowed_manifest_prefix=(
            arguments.source_manifest_prefix or settings.source_manifest_prefix
        ),
        local_development=arguments.local_development,
    )
    validate_output_prefix(source.output_uri, arguments.output)
    if (
        settings.maximum_input_rows is not None
        and source.expected_trades > settings.maximum_input_rows
    ):
        raise InvalidCandleInput("curated snapshot exceeds CANDLE_MAXIMUM_INPUT_ROWS")
    snapshot_key = candle_snapshot_key(source, interval=arguments.interval)
    manifest_uri = (
        f"{arguments.output.rstrip('/')}/manifests/{snapshot_key}/manifest.json"
    )
    existing = _existing_manifest(object_store, manifest_uri, snapshot_key)
    if existing is not None:
        return {
            **existing,
            "status": "resolved_existing_snapshot",
            "published_status": "published",
            "manifest_uri": manifest_uri,
        }

    source_records = _read_source(spark, source).persist(StorageLevel.DISK_ONLY)
    try:
        source_count = _validate_curated_rows(source_records, source.expected_trades)
        known_identity = _known_backfill_identity(
            source_records, arguments.known_backfill_event_id
        )
        candles = aggregate_market_candles(
            source_records,
            interval=arguments.interval,
            source_snapshot_key=source.snapshot_key,
            transform_version=CANDLE_TRANSFORM_VERSION,
            created_at=started,
        ).persist(StorageLevel.DISK_ONLY)
        try:
            counts = _validate_candles(candles, source_count)
            _verify_known_backfill(candles, known_identity)
            report: dict[str, Any] = {
                "candle_run_id": candle_run_id,
                "snapshot_key": snapshot_key,
                "candle_schema_version": CANDLE_SCHEMA_VERSION,
                "candle_transform_version": CANDLE_TRANSFORM_VERSION,
                "interval": arguments.interval,
                "mode": "apply" if arguments.apply else "dry_run",
                "started_at": _timestamp_text(started),
                "completed_at": _timestamp_text(datetime.now(timezone.utc)),
                "status": "dry_run_ready",
                "source_curated_manifest_uri": source.manifest_uri,
                "source_curated_manifest_sha256": source.manifest_sha256,
                "source_curated_snapshot_key": source.snapshot_key,
                "source_curated_schema_version": CURATED_SCHEMA_VERSION,
                "source_curated_trades": source_count,
                **counts,
                **_report_metrics(candles, settings.sample_limit),
                "partition_columns": ["interval", "event_date", "event_hour"],
                "manifest_uri": manifest_uri,
            }
            if not arguments.apply:
                return report

            output_uri = f"{arguments.output.rstrip('/')}/runs/{candle_run_id}"
            writer = candles.write.mode("errorifexists")
            if counts["candle_count"]:
                writer.partitionBy("interval", "event_date", "event_hour").parquet(
                    output_uri
                )
            else:
                writer.parquet(output_uri)
            published = validate_published_output(
                spark,
                output_uri,
                counts["candle_count"],
                source_count,
            )
            _verify_known_backfill(published, known_identity)
            file_metrics = object_store.parquet_metrics(output_uri)
            if file_metrics.files <= 0 or file_metrics.bytes <= 0:
                raise InvalidCandleInput("published candle output has no Parquet files")
            report.update(
                {
                    "status": "published",
                    "completed_at": _timestamp_text(datetime.now(timezone.utc)),
                    "candle_output_uri": output_uri,
                    "output_files": file_metrics.files,
                    "output_bytes": file_metrics.bytes,
                }
            )
            object_store.write_json_append_only(manifest_uri, report)
            return report
        finally:
            candles.unpersist()
    finally:
        source_records.unpersist()


def print_summary(report: dict[str, Any]) -> None:
    print(f"One-minute market candles: {str(report['status']).upper()}")
    print(f"  snapshot_key={report['snapshot_key']}")
    if "candle_count" in report:
        print(f"  source_curated_trades={report['source_curated_trades']}")
        print(f"  candles={report['candle_count']}")
        print(f"  trade_count_sum={report['trade_count_sum']}")
    print("CANDLE_REPORT_JSON=" + canonical_json_bytes(report).decode("ascii"))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    arguments = build_parser().parse_args()
    settings = CandleSettings.from_env()
    spark = (
        SparkSession.builder.appName("build-market-candles")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, settings)
    try:
        try:
            report = run_candles(spark, arguments, settings)
        except InvalidCandleInput as error:
            LOGGER.error("Candle input or validation rejected: %s", error)
            raise SystemExit(INVALID_INPUT_EXIT_CODE) from error
        print_summary(report)
        if report["status"] == "dry_run_ready":
            raise SystemExit(DRY_RUN_EXIT_CODE)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
