# Research decision and hypothesis reset

## Status

- Status: superseded
- Priority: high
- Owner: strategy owner
- Dependencies: completed stories 0003 through 0006

## User story

As a strategy owner, I want an immutable decision record for the rejected SMA
research attempt and a controlled way to register a new hypothesis so that we
do not quietly tune the losing strategy or start a paper pilot from obsolete
evidence.

## Problem

The approved 90-day BTC-USD segmented run completed on September 9, 2026. None
of its three fixed SMA candidates passed the unchanged train/validation
drawdown limits. Therefore it produced no sealed winner, did not access test
prices, and cannot justify the existing paper-pilot draft. Continuing to build
or operate a pilot around that strategy would spend time on evidence that has
already failed its selection gate.

## Scope

- Publish one append-only, SHA-pinned research decision that links the approved
  review, gap-aware report, selection evidence, and named owner decision.
- Support only `retire_hypothesis` or `authorize_new_hypothesis` decisions;
  the latter must name a separate future specification and contain no test
  performance claim.
- Make paper-pilot registration reject the retired SMA experiment and its
  obsolete draft plan with a clear explanation.
- Update the dashboard and operator documentation to show that the current
  result is a selection failure, not a paper-pilot opportunity.
- Provide a strict new-hypothesis template that fixes the asset, time ranges,
  candidate family, costs, segment aggregation, and selection rule before any
  new train/validation/test prices are read.

## Non-goals

- Altering the rejected SMA candidates, dates, thresholds, costs, gap policy,
  or test boundary to obtain a better result.
- Picking a new strategy, optimizing parameters, accessing a test split, or
  running another backtest as part of this story.
- Registering a paper session, starting a paper pilot, placing an order, or
  using exchange credentials.

## Acceptance criteria

1. The decision record rejects changed report, selection, policy-review, or
   source hashes and cannot be overwritten by a conflicting later decision.
2. The record states that the fixed SMA family was not selected and that its
   result is research-only; a future paper pilot is blocked for this attempt.
3. Paper-pilot registration rejects the retired evaluation/plan before it
   creates PostgreSQL state or writes a pilot artifact.
4. The dashboard presents one plain-language next action: review and register
   a separate hypothesis, rather than tune or forward-test the rejected one.
5. The new-hypothesis template has strict schema validation and an immutable
   identity that changes for every strategy, range, cost, or candidate change.
6. Tests cover tampering, repeat/conflicting decisions, paper-pilot blocking,
   dashboard rendering, and the template's test-isolation fields.

## Validation plan

- Use deterministic fixture evidence from story 0006 to publish and read back
  a `retire_hypothesis` decision.
- Attempt to reuse the retired pilot plan and verify zero pilot/session rows
  and no new object publication.
- Attempt changed evidence and a conflicting decision; both must fail closed.
- Run trading-core, paper-pilot, dashboard, lint, and type checks.

## Rollout constraints

This is a governance and safety boundary, not a profit claim. The decision
record does not itself select a replacement strategy. Any future hypothesis
needs its own story, fixed specification, historical evidence, and explicit
forward-testing decision.

## Superseded note

The strategy owner requested visual access to the sealed study evidence before
making this decision. Story 0008 delivers that visibility first. This story
remains the recommended decision boundary after the visual review.
