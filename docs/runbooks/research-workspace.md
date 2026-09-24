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
  --local-development `
  --output analytics/workspaces/crypto_research_relational.duckdb
```

This creates these local, ignored files:

- `analytics/workspaces/crypto_research_relational.duckdb`: source views and a
  relational snapshot of published research rows. Older workspace files remain
  untouched if DBeaver has them open.
- `analytics/workspaces/crypto_research_relational.dbeaver-init.sql`: one-session MinIO
  connection settings for DBeaver. It includes local-development credentials;
  do not commit or share it.
- `analytics/workspaces/crypto_research_relational.starters.sql`: ready-to-run queries.

If rebuilding this exact output later, add `--replace` after disconnecting it
in DBeaver. That changes only these local workspace files. It never rewrites MinIO
objects, research publications, or PostgreSQL.

## Open it in DBeaver

1. Choose **Database → New Database Connection → DuckDB**.
2. Select the existing database file:
   `C:\Development\Projects\crypto-trading-bot\analytics\workspaces\crypto_research_relational.duckdb`.
3. Open an SQL editor and run the whole generated
   `crypto_research_relational.dbeaver-init.sql` file once for that session.
4. Refresh the navigator. The views appear under the `research_data` schema.
5. Open `crypto_research_relational.starters.sql`, run one query, then use DBeaver's chart
   view on the result grid for a line or bar graph.

The initialization file is needed because a DuckDB connection must load the
S3-compatible MinIO extension and credentials before it can read remote
Parquet. The database itself intentionally does not retain those credentials.

## Available views

| View | What it contains |
|---|---|
| `candles` | Full pinned historical one-minute candle publication. |
| `backtest_fills`, `backtest_equity`, `backtest_decisions` | Every fill, account point, and decision in complete published backtest artifacts. |
| `backtest_manifests`, `backtest_runs` | Published content keys and run identifiers; the manifest maps a candidate result to its actual backtest run. |
| `candidate_results`, `baseline_results` | Candidate and baseline metrics from sealed experiment artifacts. |
| `baseline_fills`, `baseline_equity` | Full baseline result artifacts where they were published. |
| `experiment_runs` | Distinct experiment identifiers. |
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

Open the **research_model** schema in DBeaver and select **View Diagram**. Its
15 enforced foreign keys represent real publication identities, not just a
display-only run hub. The most useful paths are:

- `experiment_runs` -> `candidates` -> `candidate_results` -> `backtest_runs`
  (via the manifest's `backtest_key`) -> `backtest_decisions` -> `backtest_fills`.
- `experiment_runs` -> `baseline_results` <- `candidate_results` (same
  experiment and comparison range).
- `experiment_runs` -> `experiment_evaluations` -> `baseline_fills` and
  `baseline_equity`. Evaluation artifacts are a different stage from the
  train/validation baseline results.
- `gap_aware_selections` -> `gap_aware_candidates` -> `gap_aware_segments` ->
  saved trade/equity samples.

`backtest_equity` belongs to a backtest run but does not point to one specific
decision. The raw `candles` and `gap_aware_reports` remain views because the
publications do not provide safe row-level keys to the other components.
The gap-aware study likewise has no verified key to an experiment run, so it is
intentionally a separate connected component. A line between these would
suggest a join the source data cannot prove.

Source views stay in `research_data`; complete published result columns are
copied into `research_model` with a local `source_row` where needed. The model
is a local snapshot. New publications appear in the source views immediately,
but require a rebuild to appear in the relational tables. Gap-aware samples
remain bounded publications, not every simulated fill or account point.

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
