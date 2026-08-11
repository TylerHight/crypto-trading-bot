# Integration Tests

Integration tests verify real component interaction using disposable local dependencies, preferably through Docker Compose or Testcontainers.

Examples include collector-to-Kafka publication, Spark reads and writes against Kafka and MinIO, PostgreSQL inbox/outbox behavior, dbt against DuckDB, and safe restart recovery. Tests must create isolated topics, schemas, buckets or prefixes, and database namespaces and must clean them up without touching developer or shared environments.
