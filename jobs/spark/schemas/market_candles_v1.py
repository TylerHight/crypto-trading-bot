from pyspark.sql import types as T

CANDLE_SCHEMA_VERSION = "v1"
CANDLE_TRANSFORM_VERSION = "market-candles-1m-v1"
SUPPORTED_INTERVAL = "1m"
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18

CANDLE_KEY_COLUMNS = ["exchange", "symbol", "interval", "window_start"]

MARKET_CANDLE_SCHEMA = T.StructType(
    [
        T.StructField("exchange", T.StringType(), nullable=False),
        T.StructField("symbol", T.StringType(), nullable=False),
        T.StructField("interval", T.StringType(), nullable=False),
        T.StructField("window_start", T.TimestampType(), nullable=False),
        T.StructField("window_end", T.TimestampType(), nullable=False),
        T.StructField(
            "open", T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False
        ),
        T.StructField(
            "high", T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False
        ),
        T.StructField(
            "low", T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False
        ),
        T.StructField(
            "close", T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False
        ),
        T.StructField(
            "base_volume",
            T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable=False,
        ),
        T.StructField(
            "quote_volume",
            T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable=False,
        ),
        T.StructField(
            "vwap", T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE), nullable=False
        ),
        T.StructField("trade_count", T.LongType(), nullable=False),
        T.StructField("historical_backfill_trade_count", T.LongType(), nullable=False),
        T.StructField("first_event_id", T.StringType(), nullable=False),
        T.StructField("last_event_id", T.StringType(), nullable=False),
        T.StructField("minimum_kafka_timestamp", T.TimestampType(), nullable=False),
        T.StructField("maximum_kafka_timestamp", T.TimestampType(), nullable=False),
        T.StructField("minimum_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("maximum_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("source_curated_snapshot_key", T.StringType(), nullable=False),
        T.StructField("candle_schema_version", T.StringType(), nullable=False),
        T.StructField("candle_transform_version", T.StringType(), nullable=False),
        T.StructField("created_at", T.TimestampType(), nullable=False),
        T.StructField("event_date", T.DateType(), nullable=False),
        T.StructField("event_hour", T.StringType(), nullable=False),
    ]
)
