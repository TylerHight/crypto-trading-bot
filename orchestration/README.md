# Orchestration

`orchestration/` coordinates bounded work across existing applications, Spark jobs, dbt commands, and quality checks.

Orchestration code declares dependencies, schedules, retries, alerts, and run metadata. It must not implement market transformations, continuously supervise Kafka consumers, or contain trading reconciliation logic. Every invoked operation must already be safe to retry independently.
