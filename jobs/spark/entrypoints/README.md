# Spark Entry Points

`entrypoints/` contains thin `spark-submit` targets for live streaming, bounded backfill processing, compaction, and deterministic replay.

An entry point may parse arguments, configure sources and sinks, establish checkpoint or run-manifest paths, call shared transformations, and record run metadata. It must not duplicate transformation logic. All inputs must be explicit so the same command can run against local fixtures, MinIO, or S3 and can be submitted locally or through EMR Serverless.

`audit_raw_market_trades.py` is a bounded batch entry point. It freezes an
earliest-to-latest Kafka read before comparing that snapshot with raw Parquet.
It exits with `0` when archive integrity passes and `2` when records are
missing, Kafka positions are duplicated, or archived values are malformed.
