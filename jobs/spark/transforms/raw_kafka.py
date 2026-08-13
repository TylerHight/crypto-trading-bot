from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def select_raw_kafka_record(records: DataFrame) -> DataFrame:
    """Preserve a Kafka record exactly and add UTC storage partitions.

    The key, value, and header values intentionally remain binary. Parsing the event
    envelope belongs in the curated layer, not in the immutable raw archive.
    """

    return records.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").alias("kafka_key"),
        F.col("value").alias("kafka_value"),
        F.col("headers").alias("kafka_headers"),
        F.to_date(F.col("timestamp")).alias("event_date"),
        F.date_format(F.col("timestamp"), "HH").alias("event_hour"),
    )
