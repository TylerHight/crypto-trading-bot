# Queryable historical research workspace

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed stories 0003, 0004, 0006, and 0008

## User story

As a strategy owner, I want a simple SQL workspace over historical prices and
published backtest artifacts so that I can inspect and graph the evidence
without manually downloading Parquet files or modifying immutable research.

## Problem

The dashboard is deliberately a bounded visual summary. The underlying
historical candles and complete ordinary-backtest artifacts are Parquet files,
which were awkward to inspect and graph without a reusable local query layer.

## Scope

- Build a local DuckDB database containing views over MinIO-backed Parquet.
- Pin the workspace to the latest historical candle manifest and record its
  SHA-256 identity.
- Include candles, complete backtest fills/equity/decisions, experiment
  comparisons, and saved gap-aware chart samples.
- Provide DBeaver initialization and starter-query files.

## Non-goals

- Rerunning strategies, recovering unpublished per-minute results, writing to
  MinIO, or changing a selection result.
- Using PostgreSQL as a duplicate analytical store.

## Acceptance criteria

1. A generated local DuckDB file exposes documented views over the pinned
   historical candle publication and published research artifacts.
2. DBeaver can initialize a local MinIO-backed session and query those views.
3. Starter SQL supports a price line, complete ordinary-backtest fills/equity,
   and candidate comparison graphs.
4. The workspace records the source manifest identity and does not modify
   source Parquet, MinIO, PostgreSQL, strategy selection, or research results.
5. The current gap-aware publication is labelled as sampled rather than
   presented as complete fill-level data.

## Validation plan

- Test local Parquet and JSON sources, workspace rebuilding behavior, and a
  read-only connection.
- Query each production view through a fresh DuckDB connection initialized with
  its generated MinIO SQL.
- Run formatting, linting, typing, and diff checks.

## Rollout constraints

The generated workspace and its MinIO initialization SQL are local ignored
files. Refreshing it updates only local views and source identity; it cannot
produce a new strategy result or authorize paper trading.

## Delivery and validation — September 10, 2026

Implemented `create-research-workspace` and generated
`analytics/workspaces/crypto_research.duckdb`. It is pinned to historical candle
manifest SHA-256
`0463be6b320b4ba207c65aebf01dab2d4a3ff2e275a227dd6c67d4c259ca1072`.
The generated workspace has views for 129,834 historical candles, published
backtest fills/equity, candidate results, and the sealed gap-aware samples.

The workspace was queried after a new DuckDB connection loaded MinIO settings:
129,834 candles, 10 complete backtest fills, 160 complete backtest equity
points, 4 candidate-result rows, 3,577 saved gap-aware trade markers, and
2,880 saved gap-aware equity points returned successfully. Those last two are
bounded samples rather than a claim that every gap-aware fill or account point
was retained.

Three unit tests cover local Parquet views, saved JSON samples, source-file
immutability, explicit replacement, and a read-only local connection. The
operator runbook documents creation, DBeaver connection setup, starter graphs,
and limits.

## September 24, 2026 model correction

The first `research_model` child tables contained only `source_row` and a run ID.
That made the DBeaver relationship diagram possible but left those tables
unhelpful for exploring trades, equity, and candidate results. Workspace
rebuilds now copy the complete published columns into each child table while
retaining foreign keys to the two run tables. The `research_data` views still
read the source files directly. The existing DuckDB file must be rebuilt to
gain those columns; an open DBeaver connection prevents in-place replacement.
