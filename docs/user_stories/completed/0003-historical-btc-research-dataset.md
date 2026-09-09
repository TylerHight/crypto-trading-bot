# Historical BTC dataset for strategy research

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed story 0002, public historical candle access, local MinIO

## User story

As a strategy owner, I want a complete, reproducible 90-day BTC-USD candle
dataset so that strategy research can use a meaningful historical period.

## Problem

The existing verified live archive contains only 924 BTC minutes in the fixed
90-day research window. Research correctly stops before strategy selection.
Downloading exchange-published historical candles can fill this specific need.

## Scope

- Fetch one fixed historical BTC-USD period plus SMA warmup from a documented
  public source with bounded requests, retries, and resumable response caching.
- Preserve source responses and their hashes; publish normalized one-minute
  OHLCV candles with explicit exchange-aggregate provenance.
- Report every missing minute, duplicate, and conflict. Never synthesize prices,
  trade records, trade counts, VWAP, or volume to conceal missing source data.
- Allow the research reader to consume this explicit candle source without
  presenting it as the independently audited live-trade pipeline.
- Verify coverage through the research runner without running strategy selection.
- Show historical coverage and readiness in the dashboard's top summary.

## Non-goals

- Running or tuning strategies, selecting a winner, or starting a paper trial.
- Changing the previous experiment configuration or its sealed reports.
- Replacing the live collector, raw archive, curated trades, or their manifests.
- Paid data subscriptions, exchange credentials, order placement, or database cleanup.

## Acceptance criteria

1. A checked-in acquisition plan fixes the source, BTC-USD, UTC start/end,
   one-minute interval, and warmup; acquisition and response lineage are published.
2. Requests stay within the source's page limits. Transient failures retry within
   explicit limits, and interrupted downloads resume without re-fetching saved pages.
3. Parsing validates timestamps, exact-decimal OHLCV, price bounds and duplicate
   identity. Identical duplicates are counted and collapsed; conflicts prevent readiness.
4. Publications contain complete gap ranges, source/normalized file hashes, and
   truthful origin metadata. Partial source history remains explicitly incomplete.
5. A repeat run resolves the same immutable dataset or rejects changed output.
6. The requested 90-day period plus warmup is acquired. The coverage-only
   research check reports exact source gaps and does not access test prices or
   select a strategy. Research remains blocked while gaps exist.
7. The read-only dashboard shows available/required history and the next action;
   ready data is not represented as positive strategy evidence.

## Validation plan

- Unit-test paging, UTC boundaries, decimals, retry limits, malformed responses,
  duplicate/conflict handling, gap reporting, interrupted resume and digest conflicts.
- Use deterministic local fixtures to verify the new candle contract, legacy
  compatibility, and the research runner's coverage-only path.
- Download the actual historical range, validate its publications by read-back,
  repeat the acquisition, and run coverage-only research on the pinned manifest.
- Verify the dashboard API and HTML agree with the saved readiness result.
- Run relevant backfill, trading-core, dashboard, lint, and type checks.

## Rollout constraints

Source unavailability or genuine missing minutes must remain visible. The story
is not complete merely because fixture data passes: real coverage must pass, or
the remaining external limitation and unfinished acceptance criteria must be recorded.

## Delivery and validation — September 9, 2026

Implemented the public Coinbase client, resumable acquisition command, explicit
`exchange-ohlcv-v1` contract, research-reader support, coverage-only mode, and
dashboard history status. The
[runbook](../../../apps/historical_backfill/README.md#historical-research-candles)
documents operation and source limitations.

The fixed acquisition completed all **433** source requests and published
**129,834** candles including warmup. Coinbase published no candles for five
minutes: four from `2026-06-29T13:10:00Z` through `13:14:00Z`, and one from
`2026-07-06T01:38:00Z` through `01:39:00Z`. Targeted requests to both Coinbase
public candle APIs confirmed the absence. No prices or volumes were invented.

The coverage-only research check found **129,595 of 129,600** research minutes,
published an inconclusive result, selected no strategy, and accessed no test
prices. Acquisition is complete; strategy research requires an explicit gap
policy next.

```text
Dataset key: 178b044cef946af509c7d9b97245d0ac0884787747e673ab99acd6bd6b47f7b3
Dataset manifest SHA-256: 0463be6b320b4ba207c65aebf01dab2d4a3ff2e275a227dd6c67d4c259ca1072
Readiness key: 7ce8024647508ce645bbca1809794b1cfd71080111476db5bd827a7ad5660cfb
Readiness coverage SHA-256: d8dcb391c31e7a845fb75f57a33ba6022124cfa8f83674b8d01d858dc38bf2d7
Readiness manifest SHA-256: 35634523815d61cfb8d67a636a090fc42b74cb29a0cc87d6b2244d83c5b7
```

Validation covers paging, retries, exact decimals, OHLCV bounds, malformed
responses, resume without refetching, gap reporting, immutable cache conflicts,
historical and legacy candle contracts, coverage-only behavior, and dashboard
rendering. The final focused regression run passed all 31 tests; Ruff and mypy
also passed across the application, package, and test source trees.
