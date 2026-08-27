from pyspark.sql import types as T

CURATED_SCHEMA_VERSION = "v1"
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18

CURATED_MARKET_TRADE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("exchange", T.StringType(), nullable=False),
        T.StructField("symbol", T.StringType(), nullable=False),
        T.StructField("source_event_id", T.StringType(), nullable=False),
        T.StructField("event_time", T.TimestampType(), nullable=False),
        T.StructField("ingested_at", T.TimestampType(), nullable=False),
        T.StructField("kafka_timestamp", T.TimestampType(), nullable=False),
        T.StructField(
            "price",
            T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable=False,
        ),
        T.StructField(
            "size",
            T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable=False,
        ),
        T.StructField(
            "notional",
            T.DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable=False,
        ),
        T.StructField("source_side", T.StringType(), nullable=False),
        T.StructField("source_sequence", T.LongType(), nullable=True),
        T.StructField("producer", T.StringType(), nullable=False),
        T.StructField("trace_id", T.StringType(), nullable=False),
        T.StructField("correlation_id", T.StringType(), nullable=True),
        T.StructField("causation_id", T.StringType(), nullable=True),
        T.StructField("kafka_topic", T.StringType(), nullable=False),
        T.StructField("kafka_partition", T.IntegerType(), nullable=False),
        T.StructField("kafka_offset", T.LongType(), nullable=False),
        T.StructField("curated_schema_version", T.StringType(), nullable=False),
        T.StructField("curated_at", T.TimestampType(), nullable=False),
        T.StructField("event_date", T.DateType(), nullable=False),
    ]
)

QUARANTINE_SCHEMA = T.StructType(
    [
        T.StructField("curation_run_id", T.StringType(), nullable=False),
        T.StructField("kafka_topic", T.StringType(), nullable=True),
        T.StructField("kafka_partition", T.IntegerType(), nullable=True),
        T.StructField("kafka_offset", T.LongType(), nullable=True),
        T.StructField("kafka_timestamp", T.TimestampType(), nullable=True),
        T.StructField("failure_code", T.StringType(), nullable=False),
        T.StructField("failure_field", T.StringType(), nullable=True),
        T.StructField("event_id_when_safe", T.StringType(), nullable=True),
        T.StructField("value_sha256", T.StringType(), nullable=True),
        T.StructField("quarantined_at", T.TimestampType(), nullable=False),
    ]
)
