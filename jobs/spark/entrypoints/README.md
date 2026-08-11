# Spark Entry Points

`entrypoints/` contains thin `spark-submit` targets for live streaming, bounded backfill processing, compaction, and deterministic replay.

An entry point may parse arguments, configure sources and sinks, establish checkpoint or run-manifest paths, call shared transformations, and record run metadata. It must not duplicate transformation logic. All inputs must be explicit so the same command can run against local fixtures, MinIO, or S3 and can be submitted locally or through EMR Serverless.
