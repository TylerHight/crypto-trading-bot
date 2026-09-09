# Human review of the historical gap policy

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed stories 0003 and 0004; immutable gap-safe report
  `edc14ea22c7ee0a63d2651a59b18471b7cdb8a6629e4b9cba70395bee5fb46fe`

## User story

As the strategy owner, I want to explicitly approve or reject the documented
historical-data gap policy so that no one can enable strategy selection merely
by changing configuration after seeing the historical coverage result.

## Problem

The five genuine Coinbase gaps are documented and the runner safely blocks
research at `pending_human_review`. The remaining decision is a human one:
whether excluding the gap minutes and their dependent SMA windows is an
acceptable basis for future historical selection. Software must not make that
judgment silently.

## Scope

- Present the fixed source manifest, exact five missing minutes, policy actions,
  and valid-minute counts for a named reviewer.
- Record one immutable approval or rejection with reviewer identity, UTC time,
  policy digest, source manifest digest, report digest, and a short rationale.
- Reject an approval record whose pinned policy, source, or report differs from
  the reviewed evidence.
- Show the recorded decision and its consequence in the read-only dashboard.
- If rejected, keep strategy research blocked and document the chosen next step.

## Non-goals

- Selecting a strategy, accessing test prices, evaluating out-of-sample
  performance, or starting a paper trial.
- Editing the policy, gaps, candidates, dates, costs, or source data as part of
  approval.
- Treating silence, a dashboard visit, or an automated test as approval.
- Implementing the future gap-aware selection engine.

## Acceptance criteria

1. The review material shows the two source-gap ranges, the policy digest,
   107,517 selection-valid minutes, and 21,600 test-valid minutes.
2. A reviewer can record exactly one explicit decision for the pinned evidence:
   `approved` or `rejected`, never an implied approval.
3. The decision publication is append-only, hash-pinned, and rejects a changed
   policy, dataset, coverage report, or duplicate conflicting decision.
4. A rejection leaves all research, selection, evaluation, and paper-pilot
   workflows blocked.
5. An approval only authorizes a separate, reviewed implementation of a
   gap-aware selection engine; it does not itself run that engine or access the
   test split.
6. The dashboard states the decision, reviewer, evidence identity, and one
   plain-language next action.
7. Tests cover approval, rejection, tampering, duplicate decision conflicts,
   and dashboard read-only behavior.

## Validation plan

- Create deterministic fixtures for the published report, policy, and source
  manifest; prove all three hashes must match the review record.
- Verify that approval and rejection are mutually exclusive and append-only.
- Confirm the dashboard renders a decision without making PostgreSQL, MinIO, or
  strategy-state changes.
- Run relevant research, dashboard, lint, and type checks.

## Rollout constraints

Only the strategy owner (or a delegated named reviewer) may make this decision.
Until then, the current `pending_human_review` state is the correct operational
state. A positive decision does not claim profitability and does not authorize a
paper trial.

## Decision record — September 9, 2026

The strategy owner explicitly approved the documented policy in this story.
The approval applies only to the following pinned evidence:

- Policy SHA-256: `871e038984899c21834a9ce630509a713596ea3997d4238c875d20f9d6a50ea4`
- Historical dataset manifest SHA-256:
  `0463be6b320b4ba207c65aebf01dab2d4a3ff2e275a227dd6c67d4c259ca1072`
- Gap-safe research report SHA-256:
  `edc14ea22c7ee0a63d2651a59b18471b7cdb8a6629e4b9cba70395bee5fb46fe`

It approves the no-invention, reset-after-gap rule for future research only.
It does not approve candidate selection, test-period access, paper trading, or
live trading.

## Delivery and validation — September 9, 2026

Implemented `record-gap-policy-review`, which verifies the pinned policy,
research report, coverage report, source-manifest hash, and exact source gaps
before publishing a review. It uses append-only registrations to reject a
conflicting later decision for the same evidence. The dashboard verifies and
shows the recorded decision without unlocking selection, OOS access, paper
trading, or live trading.

The strategy-owner approval was published at:

```text
s3a://crypto-data/analytics/strategy_experiments/v1/gap_policy_reviews/reviews/15803b4cb7a9eb99fa378678ffba5e52a490ac8f5b85d60e3155bc7d0396b6c4/manifest.json
SHA-256: b646dbc787b758838135d9a258cbf6ff4c45ba970c9e7bc17eed5a56c3cd4afa
```

The approval-record, research-runner, and dashboard checks passed, along with
Ruff and mypy across 36 source files.

Story 0006 is the separately reviewed implementation of the gap-aware,
train/validation/test-sealed research engine. Its result remains research-only.
