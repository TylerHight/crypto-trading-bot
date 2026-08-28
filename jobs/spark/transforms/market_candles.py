from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs.spark.schemas.market_candles_v1 import (
    CANDLE_SCHEMA_VERSION,
    CANDLE_TRANSFORM_VERSION,
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    MARKET_CANDLE_SCHEMA,
    SUPPORTED_INTERVAL,
)

REQUIRED_CURATED_COLUMNS = {
    "event_id",
    "exchange",
    "symbol",
    "event_time",
    "ingested_at",
    "kafka_timestamp",
    "price",
    "size",
    "notional",
    "producer",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
}
QUANTUM = Decimal(1).scaleb(-DECIMAL_SCALE)
DECIMAL_TYPE = T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)


def exact_vwap(
    quote_volume: Decimal | None,
    base_volume: Decimal | None,
) -> Decimal | None:
    """Return scale-18 half-even VWAP, or none when publication must fail."""

    if quote_volume is None or base_volume is None or base_volume <= 0:
        return None
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION * 3
            value = (quote_volume / base_volume).quantize(
                QUANTUM, rounding=ROUND_HALF_EVEN
            )
    except (InvalidOperation, ZeroDivisionError):
        return None
    if not value.is_finite() or value <= 0:
        return None
    integer_digits = max(value.adjusted() + 1, 0)
    if integer_digits > DECIMAL_PRECISION - DECIMAL_SCALE:
        return None
    return value


def aggregate_market_candles(
    curated_trades: DataFrame,
    *,
    interval: str,
    source_snapshot_key: str,
    transform_version: str = CANDLE_TRANSFORM_VERSION,
    created_at: datetime,
) -> DataFrame:
    """Aggregate curated trades into deterministic, non-empty UTC minute candles."""

    if interval != SUPPORTED_INTERVAL:
        raise ValueError(f"Unsupported candle interval: {interval!r}")
    missing = REQUIRED_CURATED_COLUMNS.difference(curated_trades.columns)
    if missing:
        raise ValueError(
            "Curated trades are missing required columns: "
            + ", ".join(sorted(missing))
        )
    if not source_snapshot_key or not transform_version:
        raise ValueError("Snapshot key and transform version must not be empty")

    window_start = F.date_trunc("minute", F.col("event_time"))
    order_key = F.struct(
        F.col("event_time"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("event_id"),
    )
    ordered_value = F.struct(F.col("price"), F.col("event_id"))
    aggregates = (
        curated_trades.withColumn("window_start", window_start)
        .groupBy("exchange", "symbol", "window_start")
        .agg(
            F.min_by(ordered_value, order_key).alias("_first"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by(ordered_value, order_key).alias("_last"),
            F.sum("size").cast(DECIMAL_TYPE).alias("base_volume"),
            F.sum("notional").cast(DECIMAL_TYPE).alias("quote_volume"),
            F.count(F.lit(1)).cast("long").alias("trade_count"),
            F.sum(
                F.when(
                    F.col("producer") == "apps.historical_backfill", F.lit(1)
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("historical_backfill_trade_count"),
            F.min("kafka_timestamp").alias("minimum_kafka_timestamp"),
            F.max("kafka_timestamp").alias("maximum_kafka_timestamp"),
            F.min("ingested_at").alias("minimum_ingested_at"),
            F.max("ingested_at").alias("maximum_ingested_at"),
        )
    )
    vwap_udf = F.udf(exact_vwap, DECIMAL_TYPE)
    return (
        aggregates.withColumn("interval", F.lit(interval))
        .withColumn("window_end", F.expr("window_start + INTERVAL 1 MINUTE"))
        .withColumn("open", F.col("_first.price").cast(DECIMAL_TYPE))
        .withColumn("close", F.col("_last.price").cast(DECIMAL_TYPE))
        .withColumn("first_event_id", F.col("_first.event_id"))
        .withColumn("last_event_id", F.col("_last.event_id"))
        .withColumn(
            "vwap", vwap_udf(F.col("quote_volume"), F.col("base_volume"))
        )
        .withColumn("source_curated_snapshot_key", F.lit(source_snapshot_key))
        .withColumn("candle_schema_version", F.lit(CANDLE_SCHEMA_VERSION))
        .withColumn("candle_transform_version", F.lit(transform_version))
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .withColumn("event_date", F.to_date("window_start"))
        .withColumn("event_hour", F.date_format("window_start", "HH"))
        .select(*MARKET_CANDLE_SCHEMA.fieldNames())
    )
