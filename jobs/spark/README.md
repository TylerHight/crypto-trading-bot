# Spark Jobs

This directory owns PySpark Structured Streaming, batch, and deterministic replay implementations.

The same DataFrame transformation functions should serve live and replay entry points. Environment differences—local Spark versus EMR Serverless, MinIO versus S3, and local Kafka versus MSK—are supplied through configuration rather than transformation branches.

Keep Spark entry-point wiring in `entrypoints/`, reusable DataFrame logic in `transforms/`, and Spark-facing schema definitions in `schemas/`. Exchange clients, Airflow DAGs, dbt models, and trading decisions do not belong here.

## Implemented raw sink

`entrypoints/raw_market_trades.py` consumes `market.trades.raw.v1` with Structured Streaming and writes immutable Parquet to `s3a://crypto-data/raw/market_trade_raw/v1`. It uses `s3a://crypto-data/checkpoints/raw-market-trades-v1` for durable Kafka offset and commit state.

`transforms/raw_kafka.py` projects Kafka metadata without parsing the event envelope. Keys, values, and header values remain binary so the raw layer can reproduce the consumed record exactly. See the [local pipeline guide](../../infra/compose/README.md) for runtime commands.

## Implemented raw integrity audit

`entrypoints/audit_raw_market_trades.py` captures broker-reported beginning and
ending offsets for every current topic partition, then performs a bounded,
non-streaming read of retained Kafka data and compares it with raw Parquet by
topic, partition, and offset. It reports the captured boundaries, Kafka and
Parquet counts, archived offset extrema, missing archive records, duplicate
archive positions, malformed event values, and duplicate event IDs without
changing source data or Spark checkpoints.

`transforms/raw_integrity.py` contains the reusable DataFrame comparisons and
strict event-value validation. Run the local audit through
`scripts/run_raw_integrity_audit.ps1`; the final `AUDIT_REPORT_JSON=` line is the
machine-readable report.

## Curated market trades v1

`entrypoints/curate_market_trades.py` consumes a passing raw-integrity report,
filters immutable raw Parquet to its exact topic/partition offset bounds, and
calls the shared `transforms/curated_market_trades.py` transformation. The
transformation strictly validates Coinbase envelopes, Kafka keys and headers,
uses `decimal(38,18)`, selects exact duplicates by the lowest Kafka position,
and sends invalid or conflicting rows to a safe quarantine projection.

Dry run is the default and exits `2` after reporting counts without writing.
Add `--apply` to write unique run-scoped Parquet. A successful publication is
discoverable only through the append-only manifest at
`curated/market_trades/v1/manifests/<snapshot-key>/manifest.json`; rerunning the
same frozen snapshot returns that manifest without another write. Any conflict
exits `3`, writes run-scoped evidence in apply mode, and publishes no manifest.
Invalid evidence or configuration exits `4`.

Example inside the Spark image:

```powershell
podman compose run --rm --no-deps -T raw-sink `
  /opt/spark/bin/spark-submit --master 'local[2]' `
  --conf spark.jars.ivy=/opt/spark/.ivy2 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  /opt/spark/work-dir/jobs/spark/entrypoints/curate_market_trades.py `
  --raw-integrity-report s3a://crypto-data/reconciliation/raw-integrity/audit.json
```

Review the terminal `CURATION_REPORT_JSON` line, then repeat with `--apply`.
Credentials come from `CURATION_S3_*` environment variables and are never
included in reports. Local evidence and filesystem outputs require the explicit
`--local-development` option. Unreferenced run directories left by a failed
apply are safe to retain for investigation; retry with the same frozen report,
and do not overwrite or expose them manually.

Download the published manifest and validate the snapshot with DuckDB:

```powershell
python -m jobs.spark.entrypoints.validate_curated_market_trades `
  --manifest .\curated-manifest.json `
  --known-event-id <known-backfill-event-id>
```

The validator checks the v1 schema, UUID/uniqueness, positive decimals,
normalization, event-date partition semantics, and the optional known backfill
fixture. It reads S3 credentials from the same environment variables.
