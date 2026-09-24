# Historical research workspace

The local DuckDB workspace lets you query the historical candle and completed
backtest Parquet files in MinIO with SQL. It does not modify source data. The
local database holds source views, source-manifest identity, and a
relationship model with complete published result columns for DBeaver.

## Create or refresh the workspace

Start the local MinIO service, then run this from the repository root:

```powershell
$env:PYTHONPATH = 'apps/trading_core/src;packages/domain/src'
$env:RESEARCH_WORKSPACE_S3_ENDPOINT = 'http://127.0.0.1:9000'
$env:RESEARCH_WORKSPACE_S3_ACCESS_KEY = 'minioadmin'
$env:RESEARCH_WORKSPACE_S3_SECRET_KEY = 'minioadmin'
.venv311\Scripts\python.exe -m crypto_trading_core.research_workspace `
  --local-development --replace `
  --output analytics/workspaces/crypto_research_complete.duckdb
```

This creates these local, ignored files:

- `analytics/workspaces/crypto_research_complete.duckdb`: DuckDB views and full
  research-model result tables. The older `crypto_research.duckdb` remains in
  use by another process and was not replaced.
- `analytics/workspaces/crypto_research_complete.dbeaver-init.sql`: one-session MinIO
  connection settings for DBeaver. It includes local-development credentials;
  do not commit or share it.
- `analytics/workspaces/crypto_research_complete.starters.sql`: ready-to-run queries.

`--replace` changes only those local workspace files. It never rewrites MinIO
objects, research publications, or PostgreSQL.

## Open it in DBeaver

1. Choose **Database → New Database Connection → DuckDB**.
2. Select the existing database file:
   `C:\Development\Projects\crypto-trading-bot\analytics\workspaces\crypto_research_complete.duckdb`
   for the completed model. The older `crypto_research.duckdb` remains a
   compatibility copy while another process has it open.
3. Open an SQL editor and run the whole generated
   `crypto_research_complete.dbeaver-init.sql` file once for that session.
4. Refresh the navigator. The views appear under the `research_data` schema.
5. Open `crypto_research_complete.starters.sql`, run one query, then use DBeaver's chart
   view on the result grid for a line or bar graph.

The initialization file is needed because a DuckDB connection must load the
S3-compatible MinIO extension and credentials before it can read remote
Parquet. The database itself intentionally does not retain those credentials.

## Available views

| View | What it contains |
|---|---|
| `candles` | Full pinned historical one-minute candle publication. |
| `backtest_fills`, `backtest_equity`, `backtest_decisions` | Every fill, account point, and decision in complete published backtest artifacts. |
| `backtest_runs` | Distinct backtest identifiers; use as the parent view for the three backtest views in an EER diagram. |
| `candidate_results`, `baseline_results` | Candidate and baseline metrics from sealed experiment artifacts. |
| `baseline_fills`, `baseline_equity` | Full baseline result artifacts where they were published. |
| `experiment_runs` | Distinct experiment identifiers; use as the parent view for the four experiment views in an EER diagram. |
| `gap_aware_reports` | Status and recommendation of sealed gap-aware studies. |
| `gap_aware_trade_samples`, `gap_aware_equity_samples` | The bounded trade and equity samples published with the gap-aware selection. |
| `workspace_info` | The exact historical-candle manifest and SHA-256 used by this workspace. |

All views are in the `research_data` schema. For example:

```sql
SELECT window_start, close
FROM research_data.candles
WHERE symbol = 'BTC-USD'
ORDER BY window_start;
```

## Show relationships in DBeaver diagrams

Open the **research_model** schema in DBeaver and select **View Diagram**. It
contains two parent tables and seven child tables with real DuckDB foreign
keys, so DBeaver can draw the relationship lines. Refresh the connection after
rebuilding the workspace. Each child table includes all published source
columns plus a local `source_row` identifier. The parent tables contain run IDs.
The `research_data` views remain available for direct queries against Parquet.
Four additional `research_model` views expose candles, gap-aware reports, and
saved gap-aware trade/equity samples. The samples are bounded publications,
not all simulated trades or account points.

The relationships are:

| Child view | Child column | Parent view | Parent column |
|---|---|---|---|
| `backtest_fills` | `backtest_run_id` | `backtest_runs` | `backtest_run_id` |
| `backtest_equity` | `backtest_run_id` | `backtest_runs` | `backtest_run_id` |
| `backtest_decisions` | `backtest_run_id` | `backtest_runs` | `backtest_run_id` |
| `candidate_results` | `experiment_run_id` | `experiment_runs` | `experiment_run_id` |
| `baseline_results` | `experiment_run_id` | `experiment_runs` | `experiment_run_id` |
| `baseline_fills` | `experiment_run_id` | `experiment_runs` | `experiment_run_id` |
| `baseline_equity` | `experiment_run_id` | `experiment_runs` | `experiment_run_id` |

The model is a local snapshot rebuilt from the source views whenever you
recreate the workspace. Newly published source rows appear in the
`research_data` views immediately, but require a rebuild to appear in
`research_model`.

If DBeaver has the database open, disconnect it before running `--replace`.
DuckDB prevents a second process from replacing or opening that file for a
write while it is in use. You can build to another `--output` path if you need
to keep the current connection open.

## Important limits

The full historical candle files and complete ordinary-backtest artifacts can
be queried. The current gap-aware study intentionally published at most 400
trade markers and 240 account points per source segment for visual review.
Its two `gap_aware_*_samples` views expose those saved samples, not omitted
fills or minute-by-minute account values. The workspace does not rerun the
study or fabricate those omitted records.
