# Historical Backfill

This application retrieves bounded ranges of historical public market data and lands replayable raw records.

Responsibilities:

- Accept explicit exchange, instrument, and time-range arguments.
- Respect exchange pagination and rate limits.
- Write deterministic manifests and resume incomplete ranges safely.
- Produce the same raw contract used by live ingestion.
- Exit with an unambiguous status suitable for Airflow retries.

Backfills are finite and idempotent. Scheduling belongs in `orchestration/airflow/`, storage configuration belongs in environment adapters, and shared exchange behavior belongs in `packages/exchange_adapters/`.
