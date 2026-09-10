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
these read-only routes:

- `GET /` renders the dashboard.
- `GET /api/status` returns the same safe normalized state as JSON.
- `GET /assets/dashboard.js` serves the bundled chart and navigation code.

Use **Refresh snapshot** when you want new status. There is no timed page reload:
dropdowns and open details stay put while you inspect a result. The selected
tab, strategy, period, segment, and trade filters also survive a manual refresh
in the same browser tab (when session storage is enabled).
Stop the server with `Ctrl+C`; it does not stop or alter the Compose services.

## Find your way around

- **Research** opens first: study result, account value, return, drawdown,
  fill count, fees, and a strategy comparison. Hover account points for values.
- **Trades**: larger green buy and pink sell triangles plotted at execution
  prices. Hover or keyboard-focus a marker for UTC time, price, fee, and segment.
  Filter by buy/sell, search a date or price, and page through the trade table.
- **System**: live feed, archive, data publications, and maintenance actions.
- **Evidence**: source files, checksums, approval, and selection details.
- **Paper trial**: draft or registered forward simulation, separate from backtests.

Strategy, period, and segment controls are shared by Research and Trades.
Arrow keys move between focused tabs. Small screens scroll charts horizontally
without widening the page. The trade table has its own scroll area.

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
OPERATOR_DASHBOARD_GAP_AWARE_RESEARCH_PREFIX
OPERATOR_DASHBOARD_GAP_AWARE_SELECTION_PREFIX
OPERATOR_DASHBOARD_PILOT_PLAN
```

Artifact-prefix settings are also available for isolated local tests. The
dashboard never renders the configured endpoint, credentials, or database URL.
`OPERATOR_DASHBOARD_REFRESH_SECONDS` remains accepted for compatibility with
existing configuration checks, but no longer schedules browser refreshes.

## What the status means

The Research tab prefers the latest gap-aware sealed report, then falls
back to the older longer-research report. **No strategy selected** means none
passed the fixed train/validation rule. **Segmented research complete** means a
sealed winner was tested independently in each continuous source segment;
that result is research-only and cannot start a paper trial. A saved research
result does not become invalid simply because time has passed. Publication
details live in the Evidence tab.

Select a strategy, **Training** or **Validation**, and an optional segment.
Account-value lines are separate for each continuous source segment; they do
not form one tradable account balance. The four headline metrics always refer
to the whole selected period: average segment return, maximum segment drawdown,
and summed fills and fees. The segment selector narrows the account and trade
charts, not those period totals.

The saved evidence contains at most 240 account points and 400 trade markers
per segment. Trades shows the published-marker count versus the actual fill
count; its table and filters cover only those published markers. Unpublished
trades cannot be inspected here. No candle prices or trades are invented, and
changing a control does not rerun research.

## Verify the interface

1. Open Research, change strategy/period, and choose a segment.
2. Open Trades, hover a marker, filter to buys, and search a date.
3. Leave a dropdown open for over 30 seconds: the page should not reload.
4. Click Refresh snapshot: your tab and selections should remain.
5. Open System or Evidence: research charts should no longer clutter that page.

Optional automated Chrome checks (requires Playwright in your test environment):

```powershell
$env:PYTHONPATH = 'apps/operator_dashboard/src'
$env:DASHBOARD_CHROME = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
$env:DASHBOARD_LIVE_URL = 'http://127.0.0.1:8090'
python -m pytest -q tests/browser/test_dashboard_workspace.py
```

The browser suite checks navigation, filters, paging, tooltips, a real 32-second
wait, refresh persistence, narrow screens, missing evidence, and all candidates
and periods in a read-only live snapshot. Without `DASHBOARD_CHROME`, these
optional tests skip.

The underlying reports are read from
`analytics/strategy_experiments/v1/gap_aware_research/reports/` and, if absent,
`analytics/strategy_experiments/v1/longer_research/reports/`. The dashboard
reads their summaries without scanning candle data. Older short-experiment
results remain the fallback only when neither report exists.

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
