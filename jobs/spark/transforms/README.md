# Spark Transformations

`transforms/` contains pure, reusable PySpark DataFrame transformations.

Expected behavior includes parsing, normalization, event-time handling, watermarking, deduplication, quarantine routing, candle aggregation, and shared projection logic. Functions receive DataFrames and configuration values and return DataFrames; they do not create Spark sessions, open Kafka connections, choose storage endpoints, or schedule themselves.

Live and replay tests must exercise these same functions so a fixed raw dataset produces equivalent curated results.

`raw_integrity.py` compares retained Kafka identities with raw Parquet,
restricts Parquet to the retained Kafka range, validates archived event JSON,
and returns bounded samples for an integrity report. Duplicate event IDs are a
warning because the immutable raw layer intentionally preserves source
redelivery; duplicate Kafka positions are an archive failure.

`curated_market_trades.py` is the source-independent raw-to-curated transform.
It performs strict envelope/payload/provenance validation, fixed-precision
normalization, deterministic exact deduplication, and safe quarantine routing.
Producer and ingestion metadata are deliberately not part of logical equality,
allowing a live and reviewed backfill delivery of the same source trade to
collapse to one fact while retaining the lowest Kafka-position provenance.

`market_candles.py` contains the source-independent one-minute aggregation.
Opening and closing trades use a total event/Kafka order, empty windows are
absent, volumes are decimal sums, and VWAP is calculated by an exact decimal
half-even function. The transform creates no session and opens no storage.
