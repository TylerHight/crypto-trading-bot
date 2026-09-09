# Operator dashboard

`operator-dashboard` is a local, read-only explanation layer over the running
market-data and paper-pilot system. It presents health, trusted data lineage,
research evidence, a draft or registered paper pilot, and the next safe action.
It contains no exchange authentication, order placement, or state-changing
controls.

## Run locally

Start the local Compose services first, from the repository root:

```powershell
podman compose up -d
uv run operator-dashboard
```

Open <http://127.0.0.1:8090>. The server accepts only loopback hosts and exposes
two read-only routes:

- `GET /` renders the dashboard.
- `GET /api/status` returns the same safe normalized state as JSON.

The page refreshes every 30 seconds by default. Stop it with `Ctrl+C`; it does
not stop or alter the Compose services.

## Configuration

The defaults target the local Compose stack. Override only the documented
`OPERATOR_DASHBOARD_*` settings when needed:

```text
OPERATOR_DASHBOARD_HOST
OPERATOR_DASHBOARD_PORT
OPERATOR_DASHBOARD_REFRESH_SECONDS
OPERATOR_DASHBOARD_STALE_AFTER_SECONDS
OPERATOR_DASHBOARD_REQUEST_TIMEOUT_SECONDS
OPERATOR_DASHBOARD_KAFKA_BOOTSTRAP_SERVERS
OPERATOR_DASHBOARD_KAFKA_QUALITY_TOPIC
OPERATOR_DASHBOARD_S3_ENDPOINT
OPERATOR_DASHBOARD_S3_ACCESS_KEY
OPERATOR_DASHBOARD_S3_SECRET_KEY
OPERATOR_DASHBOARD_S3_REGION
OPERATOR_DASHBOARD_S3_BUCKET
OPERATOR_DASHBOARD_DATABASE_URL
OPERATOR_DASHBOARD_HISTORICAL_MANIFEST_PREFIX
OPERATOR_DASHBOARD_RESEARCH_REPORT_PREFIX
OPERATOR_DASHBOARD_PILOT_PLAN
```

Artifact-prefix settings are also available for isolated local tests. The
dashboard never renders the configured endpoint, credentials, or database URL.

## What the status means

The top Research card prefers the latest longer-research report. **Not enough
data** means the 90-day input check failed; prepare complete history before a new
experiment. **No strategy selected** means none passed selection. **Test complete**
shows the selected strategy and its net return versus buy-and-hold. Only a positive
difference supports considering a paper trial. A saved research result does not
become invalid simply because time has passed. Publication details stay collapsed.

The underlying report is read from
`analytics/strategy_experiments/v1/longer_research/reports/`. The dashboard reads its
summary without scanning candle data. Older short-experiment results remain the
fallback only when no longer-research publication exists.

- `healthy` / `current`: the source responded recently enough for the configured
  freshness threshold.
- `stale`: the last available evidence is older than the freshness threshold;
  it remains visible but is not presented as current.
- `missing`: the source responded but has no relevant artifact or health event.
- `unavailable`: the dashboard could not read that source.
- `not registered`: PostgreSQL has no real pilot, so no balance, fills, or
  assessment are invented.

The raw archive status is derived from bounded MinIO object metadata; it is not
a substitute for the raw Kafka-to-Parquet integrity audit.

## Verify read-only behavior

Run the dashboard tests, then note the pilot/session counts before and after a
browsing session:

```powershell
python -m pytest -q tests/unit/operator_dashboard
podman exec crypto-trading-bot_postgres_1 psql -U paper_app -d crypto_trading -Atc "SELECT count(*) FROM paper_pilots;"
```

Only `GET` and `OPTIONS` routes exist. The database adapter opens an explicit
read-only transaction; Kafka auto-commit is disabled; MinIO calls use only list
and get operations. The integration test also verifies these assumptions against
local PostgreSQL and MinIO.
