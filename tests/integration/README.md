# Integration Tests

Integration tests verify real component interaction using disposable local dependencies, preferably through Docker Compose or Testcontainers.

Examples include collector-to-Kafka publication, Spark reads and writes against Kafka and MinIO, PostgreSQL inbox/outbox behavior, dbt against DuckDB, and safe restart recovery. Tests must create isolated topics, schemas, buckets or prefixes, and database namespaces and must clean them up without touching developer or shared environments.

The implemented raw-pipeline test publishes a uniquely identifiable record to local Kafka and waits for Spark to archive it in MinIO. It verifies the Kafka identity, key, value, and headers from the resulting Parquet row. Run it only against the local Compose environment:

```powershell
$env:RUN_INTEGRATION_TESTS = "1"
.\.venv\Scripts\python.exe -m pytest -q tests\integration\test_raw_market_trades_pipeline.py
Remove-Item Env:RUN_INTEGRATION_TESTS
```
