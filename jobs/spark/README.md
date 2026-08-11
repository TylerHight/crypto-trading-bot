# Spark Jobs

This directory owns PySpark Structured Streaming, batch, and deterministic replay implementations.

The same DataFrame transformation functions should serve live and replay entry points. Environment differences—local Spark versus EMR Serverless, MinIO versus S3, and local Kafka versus MSK—are supplied through configuration rather than transformation branches.

Keep Spark entry-point wiring in `entrypoints/`, reusable DataFrame logic in `transforms/`, and Spark-facing schema definitions in `schemas/`. Exchange clients, Airflow DAGs, dbt models, and trading decisions do not belong here.
