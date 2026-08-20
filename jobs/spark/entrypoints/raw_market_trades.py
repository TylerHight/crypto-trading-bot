import logging
from typing import Protocol

from pyspark.sql import SparkSession

from jobs.spark.config import RawSinkSettings
from jobs.spark.transforms.raw_kafka import select_raw_kafka_record

LOGGER = logging.getLogger(__name__)


class S3Settings(Protocol):
    """Minimum object-storage settings shared by Spark entry points."""

    @property
    def s3_endpoint(self) -> str: ...

    @property
    def s3_access_key(self) -> str: ...

    @property
    def s3_secret_key(self) -> str: ...


def configure_s3a(spark: SparkSession, settings: S3Settings) -> None:
    """Configure Hadoop's S3A client for local MinIO or an S3-compatible endpoint."""

    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("fs.s3a.endpoint", settings.s3_endpoint)
    hadoop.set("fs.s3a.access.key", settings.s3_access_key)
    hadoop.set("fs.s3a.secret.key", settings.s3_secret_key)
    hadoop.set("fs.s3a.path.style.access", "true")
    hadoop.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def main() -> None:
    settings = RawSinkSettings.from_env()
    spark = (
        SparkSession.builder.appName("raw-market-trades-sink")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark, settings)

    LOGGER.info(
        "Starting raw sink from %s/%s to %s with checkpoint %s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.output_path,
        settings.checkpoint_path,
    )

    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("includeHeaders", "true")
        .load()
    )

    query = (
        select_raw_kafka_record(kafka_records)
        .writeStream.format("parquet")
        .outputMode("append")
        .option("path", settings.output_path)
        .option("checkpointLocation", settings.checkpoint_path)
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime=settings.trigger_interval)
        .queryName("raw-market-trades-v1")
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
