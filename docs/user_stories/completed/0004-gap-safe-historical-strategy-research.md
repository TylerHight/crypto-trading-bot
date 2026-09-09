# Gap-safe historical strategy research

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed story 0003 and its immutable BTC-USD historical-candle dataset

## User story

As a strategy owner, I want research to handle documented exchange candle gaps
explicitly so that I can evaluate a fixed strategy without silently filling,
bridging, or trading through unavailable source data.

## Problem

The approved historical acquisition contains 129,595 of 129,600 requested
research minutes. Coinbase published no candle for five minutes in two short
ranges. The research runner correctly blocks selection today, but it needs a
pre-registered, testable rule for what a missing candle means before the
existing fixed strategy can be evaluated responsibly.

## Scope

- Add a versioned, checked-in gap policy to the existing research specification.
- Treat every missing minute as unavailable data: do not interpolate prices,
  volume, or trades, and do not create a synthetic candle.
- Reset continuity at each gap. A signal, simulated fill, return, or indicator
  window is valid only when all source minutes it needs are present and
  contiguous after the most recent gap.
- Publish the excluded ranges, invalidated warmup windows, valid minute count,
  and policy digest in the immutable research report.
- Permit the existing fixed strategy to run only on policy-valid data; retain
  the fixed candidates, dates, costs, and selection/test split from story 0002.
- Make the dashboard say whether historical research is blocked, ready under
  the policy, or inconclusive, with one short next action.

## Non-goals

- Changing strategy parameters, candidates, date boundaries, cost assumptions,
  or the sealed pre-registration after results are seen.
- Filling gaps from another exchange, estimating missing prices, or treating a
  missing minute as zero volume.
- Declaring the strategy profitable, selecting a winner, starting a paper
  pilot, or placing orders.
- Replacing the live collector or independently audited live-trade pipeline.

## Acceptance criteria

1. The policy is checked in, versioned, and includes the exact missing ranges
   and the rule that no synthetic candle may be produced.
2. The research runner rejects a report if its policy, source manifest,
   strategy specification, or source-gap inventory changes after publication.
3. Tests prove that indicators and simulated trades cannot span a gap, and that
   each gap invalidates only the documented dependent window.
4. The immutable report records source coverage, exclusions, policy digest,
   valid selection/test minutes, and a clear reason when results remain
   inconclusive.
5. The runner may evaluate the already fixed candidates only after the policy
   validates the data. It must not access the test split while selecting a
   candidate.
6. The dashboard summarizes the result and next action without claiming missing
   source data is healthy or complete.
7. Unit and end-to-end fixture tests cover gaps at the start, middle, and end
   of a window; all relevant lint, type, and regression checks pass.

## Validation plan

- Use deterministic one-minute fixtures with known gaps to verify continuity,
  indicator warmup, simulated fills, and exact exclusion accounting.
- Run the policy against the published 90-day Coinbase dataset and confirm the
  five documented missing minutes remain visible.
- Verify report read-back and conflict rejection from object storage.
- Confirm dashboard API and page show the same status and action.
- Run relevant historical-backfill, trading-core, and dashboard tests plus lint
  and type checks.

## Rollout constraints

Human review of the policy is still required before it enables candidate selection. If valid
coverage is insufficient after excluding dependent windows, publish an
inconclusive report and leave research blocked. This story never authorizes a
paper trial; that remains a separate decision after sealed out-of-sample
evidence.

## Delivery and validation — September 9, 2026

Implemented the pinned `contiguous-source-minutes-v1` policy, exact source-gap
matching, dependent SMA-window accounting, immutable report lineage, and
dashboard status. The runner remains intentionally blocked at
`pending_human_review`; it does not select a strategy, read test prices, or
recommend a paper trial.

The real report preserved all five Coinbase gaps and recorded **107,517**
selection-valid minutes and **21,600** test-valid minutes. Its report SHA-256 is
`edc14ea22c7ee0a63d2651a59b18471b7cdb8a6629e4b9cba70395bee5fb46fe`.
Focused policy, research, and dashboard tests passed, along with lint and type
checks. A future story must be separately reviewed before it can implement a
gap-aware selection engine.
