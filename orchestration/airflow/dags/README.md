# Airflow DAGs

`dags/` contains bounded workflows for historical backfill, Spark replay, Parquet compaction, schema and partition publication, dbt builds, and data-quality checks.

Every DAG must define explicit time bounds, retries, timeouts, concurrency limits, alerts, and idempotent task inputs. DAG parsing must not make network calls. Continuous collectors, streaming consumers, the trading core, and exchange reconciliation are services—not scheduled DAG tasks.
