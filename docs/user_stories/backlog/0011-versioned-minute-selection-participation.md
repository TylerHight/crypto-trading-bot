# Version the legacy minute selection participation policy

## Status

- Status: recommended
- Priority: medium
- Owner: strategy research
- Dependencies: the daily promotion correction and prospective study in story 0010

## User story and problem

As a research operator, I want a minute-strategy candidate with no validation
trades to be ineligible under a new explicit policy, while historical v1
artifacts remain replayable under their original rules.

The September 24 audit found that `experiments.select_candidate` under
`validation-return-selection-v1` checks training fills and drawdowns only.
A zero-fill, zero-return validation can outrank an active losing candidate.
`gap_aware_research` reuses the selector. No selected candidate emerged from
the rejected long historical study, and no new minute research is authorized
by the daily breakout experiment. The separate daily engine now closes this
loophole under its own new policy.

## Scope and non-goals

Introduce a new minute policy/specification version with required validation
participation, explicit research promotion checks, and operator-facing reasons.
Retain v1 validation for archived evidence, but do not let it create newly
approved trials using absent participation evidence. Do not rewrite original
manifests or reinterpret historical selection outcomes. Do not rerun the
rejected SMA grid, launch a pilot, or change execution behavior in this story.

## Acceptance criteria

1. New selection rejects zero validation fills and records the reason.
2. Existing v1 fixtures and sealed results still replay with v1 semantics.
3. Registration checks participation evidence before writing any session or
   pilot state; cached legacy reports cannot bypass the operational gate.
4. Research summaries and dashboard messaging agree with the new gate.
5. Schema/version changes have documented migration and compatibility behavior.

## Validation and rollout

Use synthetic candidate comparisons and explicit old/new schema fixtures.
Test archived replay, cached artifacts, no-trade rejection, and zero state
writes on registration failure. Run minute experiment, gap-aware research,
paper-session/pilot, and dashboard suites. This is a separate versioned change
because silently altering `select_candidate` would invalidate old evidence.
