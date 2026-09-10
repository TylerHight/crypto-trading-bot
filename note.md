# User story: Launch and assess the first production-like paper-trading pilot

## Implementation status

### Current decision — September 9, 2026

This draft pilot must not be registered. The later, approved segmented
historical run rejected all fixed SMA candidates at the train/validation gate,
so there is no sealed winner or OOS result that could support this pilot. The
draft and its earlier short evaluation remain historical evidence only. Story
0007 in `docs/user_stories/backlog/` will formally retire this attempt and
require a separate pre-registered hypothesis before any new pilot is considered.

The software workflow is implemented and covered by accelerated tests. A real
input lineage and sealed evaluation were prepared on 2026-09-03, but the pilot
has **not** been registered: explicit operator approval and the real 30-day
observation period intentionally remain open. Replayed or backdated fixtures
are not represented as real pilot evidence.

The reproducible development/CI interpreter is Python 3.11. Python 3.13 is
excluded; the complete Spark-inclusive suite runs on the pinned interpreter.
The collector image is also based on Python 3.11, so its deployable runtime now
matches the declared package support instead of failing to build on Python 3.13.

### Current operational evidence

- A passing raw-integrity report proves 508,105 retained Kafka positions match
  raw Parquet exactly, with zero missing positions, duplicate positions, or
  invalid event values. A preceding 440-record sink-lag failure is also retained
  as immutable evidence rather than deleted.
- Curated snapshot `2b51f585...7081d` contains 380,429 logical Coinbase trades,
  removes 76,898 exact redeliveries, has zero conflicting duplicates, and
  quarantines 50,778 older `payload_time_mismatch` deliveries.
- Candle snapshot `0e751f28...82163` contains 1,029 BTC-USD/ETH-USD one-minute
  candles. Independent DuckDB validation reconciles their trade counts to all
  380,429 curated trades.
- The SHA-pinned experiment in
  `experiments/btc-usd-sma-pilot-candidate-v1.json` selected SMA 5/20 without
  test access, then opened its 30-minute OOS slice once. The strategy returned
  -0.4302258725% versus buy-and-hold -0.3729505735%, an excess return of
  -0.0572752990 percentage points. The evaluation manifest is independently
  valid at digest
  `2a560696c84c3400aecb91e753ecfcc8d7edc0ebebc2737ac1b47c664373981b`.
- `pilots/btc-usd-sma-forward-v1.json` is a valid draft 30-day plan starting
  2026-09-07T00:00:00Z, with digest
  `6a1303f375d88fcd389f0414bb5c35acd614d808bcf45b024540045b094e7e8c`.
  Its pilot ID would be
  `247f97da17afa01eec5f94912486ac59b0db66a54e8e4fbd17d6cb4a82bc8230`.
- The final verification run passed 255 tests with 10 environment-gated tests
  skipped, plus Ruff, trading-core mypy, PowerShell parsing, diff checks, and
  the enabled PostgreSQL/MinIO pilot lifecycle integration test.
- The rebuilt live collector retained one Coinbase connection across nine
  consecutive 60-second health summaries with zero sequence gaps and no
  reconnect. Kafka, MinIO, PostgreSQL, the collector, and the raw sink remain
  running; the stateful services are healthy.

The short evaluation and its slight OOS underperformance are material review
facts. Registration requires a named operator to approve that evidence and the
precommitted criteria before 2026-09-07T00:00:00Z. If approved and registered,
the earliest normal final assessment is 2026-10-07T00:00:00Z. Until then,
PostgreSQL correctly contains zero real pilot and paper-session rows.

## Story

As a strategy owner,
I want to run one pre-registered paper-trading pilot on real forward market data
and publish an immutable pass, fail, or inconclusive assessment,
so that we can decide whether the strategy merits execution-system design
without changing success criteria after seeing the results.

## Why this is next

The project can now:

```text
collect and repair public trades
    -> publish immutable one-minute candles
    -> select an SMA candidate without test leakage
    -> evaluate it once out of sample
    -> advance a durable, restart-safe paper portfolio
```

The next risk is no longer missing infrastructure. It is spending more time
building around a strategy before observing how the complete system behaves on
real, forward-only data.

This story operationalizes the existing paper trader. It precommits the pilot
duration and decision thresholds, runs bounded processing cycles, publishes
daily evidence, and produces a final review. It does not introduce live orders,
private exchange access, or automatic promotion.

## Outcome

Support one reproducible pilot workflow:

1. Register a SHA-pinned pilot plan before its first forward candle.
2. Create exactly one paper session from the plan's validated OOS evaluation.
3. Process real, newly published candle manifests through bounded idempotent
   cycles.
4. Publish immutable daily operational/performance snapshots.
5. Finalize only after the precommitted observation requirements are satisfied
   or a terminal failure occurs.
6. Produce a deterministic `pass`, `fail`, or `inconclusive` assessment.

A passing pilot means only `eligible_for_execution_design_review`. It must
never enable live trading or claim future profitability.

## Prerequisite: reproducible supported runtime

Before launching the pilot, make the supported development runtime explicit:

- Pin project development and CI to Python 3.11 or 3.12.
- Exclude Python 3.13 until the pinned PySpark version supports it.
- Configure local Spark driver and workers to use the same pinned interpreter.
- Check in the version declaration and setup instructions.
- Run the complete unit and contract suite, including
  `test_raw_integrity_audit.py`, without environment-specific exclusions.

This is a prerequisite to trustworthy pilot operations, not a separate
refactoring project.

## Pilot plan v1

Add a strict UTF-8 JSON contract and JSON Schema. The exact plan bytes are
SHA-256 pinned.

The plan contains at least:

```json
{
  "pilot_plan_version": "v1",
  "name": "btc-usd-sma-forward-v1",
  "evaluation_manifest_uri": "s3a://crypto-data/analytics/strategy_experiments/v1/evaluations/<key>/manifest.json",
  "evaluation_manifest_sha256": "<64-lowercase-hex>",
  "start_not_before": "2026-09-15T00:00:00Z",
  "minimum_calendar_days": 30,
  "minimum_processed_candles": 40000,
  "minimum_fills": 10,
  "maximum_drawdown": "0.200000000000000000",
  "minimum_excess_return_over_buy_and_hold": "0.000000000000000000",
  "maximum_data_gap_events": 0,
  "maximum_conflict_events": 0,
  "maximum_unplanned_pauses": 0
}
```

Validation rules:

- Reject unknown top-level and nested fields.
- Require `pilot_plan_version = "v1"`.
- Require a conservative stable lowercase name.
- Verify the raw plan digest before parsing.
- Require a future or present aligned UTC `start_not_before` at registration.
- Bound `minimum_calendar_days` to 7 through 90.
- Bound `minimum_processed_candles` and `minimum_fills` by configured
  operational limits.
- Require finite scale-18 decimal thresholds.
- Require maximum drawdown in `(0, 1]`.
- Require nonnegative event thresholds.
- Require the paper-session drawdown limit to be equal to or stricter than the
  plan threshold.

The pilot identity includes the raw and canonical plan digests, evaluation
digest, paper engine/schema versions, assessment-policy version, and all
thresholds. Any change creates a new pilot.

## Commands

Register and launch the pilot:

```powershell
$plan = Resolve-Path .\pilots\btc-usd-sma-forward-v1.json
$digest = (Get-FileHash $plan -Algorithm SHA256).Hash.ToLowerInvariant()

start-paper-pilot `
  --plan $plan `
  --plan-sha256 $digest `
  --approved-by research-operator `
  --approval-note "Reviewed sealed OOS evidence and precommitted pilot criteria" `
  --local-development
```

Process one real candle publication:

```powershell
run-paper-pilot-cycle `
  --pilot-id <pilot-id> `
  --candle-manifest <manifest-uri> `
  --candle-manifest-sha256 <64-lowercase-hex> `
  --command-id <stable-cycle-id>
```

Publish an immutable daily snapshot:

```powershell
report-paper-pilot `
  --pilot-id <pilot-id> `
  --as-of 2026-09-16T00:00:00Z
```

Finalize after the observation window or a terminal failure:

```powershell
finalize-paper-pilot `
  --pilot-id <pilot-id> `
  --reviewed-by research-operator `
  --review-note "Reviewed operational and performance evidence"
```

Provide equivalent Python module entry points. Commands use
`PAPER_DATABASE_URL` and the existing object-storage settings. Credentials
must never appear in output, plans, snapshots, reports, or database audit text.

## Registration behavior

`start-paper-pilot` must:

- Validate the complete evaluation lineage with the existing validator.
- Require a published simulation-only evaluation with a selected candidate.
- Verify and parse the pinned pilot plan.
- Refuse registration after processing any candle at or after
  `start_not_before`.
- Create the existing durable paper session using the plan's drawdown limit.
- Persist an immutable pilot row linking the plan, evaluation, session, and
  approval.
- Return the existing pilot on an identical retry.
- Reject a reused command ID with changed arguments.

Registration does not decide whether the OOS result is “good enough.” The
operator explicitly approves the forward experiment after reviewing it, and
the precommitted pilot thresholds govern the final assessment.

## Bounded pilot cycles

`run-paper-pilot-cycle` is a thin operational boundary around the implemented
`process-paper-candles` behavior:

- Accept exactly one SHA-pinned immutable candle publication.
- Require its first relevant minute to be at or after `start_not_before`.
- Use only real published Coinbase candle data for an actual pilot.
- Reject synthetic fixtures outside tests.
- Preserve exact-once, gap, conflict, ordering, cost, and drawdown behavior.
- Reuse the pilot's paper-session ID; never create an implicit replacement.
- Record cycle start/end, input digest, counts, session state, and safe failure
  reason.
- Be safe to retry with the same command ID.
- Remain bounded and exit; do not add an unbounded polling daemon.

Scheduling the command externally is allowed, but the orchestration layer may
only invoke existing bounded commands. It must not contain trading logic.

## Daily immutable snapshots

At most one canonical snapshot may be published per pilot and UTC date.
Identical reruns resolve the existing snapshot.

Each snapshot includes:

- Pilot, plan, evaluation, selection, and paper-session identities.
- Observation start and as-of times.
- Expected, discovered, processed, rejected, and missing candle counts.
- Data gap and conflicting-candle event counts.
- Planned and unplanned pause durations and reasons.
- Processing-cycle success/failure counts and latest successful cycle time.
- Cash, position, marked equity, pending target, fills, fees, and drawdown.
- Strategy return, forward buy-and-hold return, and excess return.
- Whether each precommitted criterion currently passes.
- A prominent `interim_only = true` marker before finalization.

Publish canonical JSON and a digest-pinned manifest beneath:

```text
analytics/paper_pilots/v1/
  pilots/<pilot-id>/manifest.json
  snapshots/<pilot-id>/event_date=YYYY-MM-DD/manifest.json
  assessments/<pilot-id>/manifest.json
```

Write artifacts first, validate them by read-back, and publish the discoverable
manifest last. Existing conflicting artifacts fail closed.

## Final assessment

`finalize-paper-pilot` uses only stored plan criteria and immutable pilot
evidence. It accepts no threshold overrides.

Return:

- `pass` when the minimum duration, candles, and fills are met; drawdown and
  all operational event counts remain within bounds; and strategy excess return
  meets the precommitted threshold.
- `fail` when a terminal stop, drawdown breach, data conflict, or other
  criterion is definitively outside its bound.
- `inconclusive` when the requested end is reached without enough valid
  candles or fills, or required evidence is unavailable.

Finalization must:

- Recalculate every metric from PostgreSQL state and immutable event rows.
- Verify all referenced candle manifests and daily snapshots.
- Record the exact assessment-policy version.
- Include criterion-by-criterion actual, threshold, and result values.
- Include operator identity, bounded review note, and timestamp.
- Be idempotent for identical input.
- Make the pilot terminal; no later cycle may advance its session.
- Never start another pilot or approve live execution automatically.

## Pilot lifecycle

```text
registered -> running -> completed
                      -> failed
                      -> inconclusive
registered/running -> cancelled
```

`completed`, `failed`, `inconclusive`, and `cancelled` are terminal.
Paper-session automatic pauses do not automatically finalize the pilot. An
operator may investigate and resume within the plan's event limits; every
transition remains audited.

## Operational walkthrough

Document a production-like local walkthrough:

1. Start Kafka, MinIO, PostgreSQL, collector, and raw sink.
2. Verify component health and raw-data progress.
3. Build and validate a new curated snapshot and one-minute candle publication.
4. Run or select an existing sealed OOS evaluation.
5. Register the pinned pilot plan before forward processing begins.
6. Run at least two paper cycles with distinct real candle publications.
7. Retry one cycle and prove no duplicate candle, decision, fill, or equity row.
8. Restart the process between cycles and prove checkpoint continuity.
9. Publish and validate a daily snapshot.
10. Exercise a planned pause and resume.
11. Use an accelerated test fixture to demonstrate each final verdict.

The real 30-day observation period continues after the software story is
deployed. Synthetic or replayed data can validate mechanics but cannot satisfy
the real pilot's performance evidence.

## Required tests

### Unit tests

- Strict plan fields, bounds, decimal handling, and raw digest verification.
- Stable pilot identity and identity changes for every relevant input.
- Registration before/after start boundary.
- Evaluation and session lineage mismatch rejection.
- Cycle idempotency and changed-command-argument rejection.
- Daily metric and criterion calculations.
- Snapshot identity, append-only publication, and conflicting rerun rejection.
- Deterministic pass, fail, and inconclusive assessment cases.
- Finalization with no threshold override path.
- Terminal pilot behavior.
- Credential redaction and simulation-only enforcement.

### Integration tests

- Run the complete pilot lifecycle against PostgreSQL and MinIO.
- Use validator-compatible evaluation and candle fixtures.
- Prove restart recovery and concurrent-cycle serialization.
- Prove an exact retry creates no duplicate durable rows.
- Publish, read back, and independently validate a daily snapshot.
- Finalize accelerated pass, fail, and inconclusive pilots.
- Confirm finalization stops subsequent paper processing.

### Regression tests

- Run the complete suite on the pinned supported Python version, including the
  Spark raw-integrity tests.
- Existing collection, curation, backtest, experiment, and paper-trading tests
  remain green.
- Static dependency tests continue to prove no private exchange or
  order-placement path exists.

## Acceptance criteria

The software story is complete when:

1. A strict pinned plan launches exactly one durable pilot and paper session.
2. Bounded real-candle cycles are retry-safe, restart-safe, and auditable.
3. Daily immutable snapshots expose both data health and strategy performance.
4. Final assessment is deterministic from precommitted thresholds and accepts
   no overrides.
5. PostgreSQL and MinIO integration tests cover the full accelerated lifecycle.
6. The complete suite passes on the documented supported Python runtime.
7. No command can authenticate to an exchange, place an order, or promote
   itself to live trading.

The operational milestone is complete only after the registered pilot runs for
its real precommitted observation period and its final immutable assessment is
reviewed.

## Definition of done

- Python runtime support is pinned and reproducible.
- Pilot-plan and assessment schemas are checked in and documented.
- Registration, cycle, snapshot, finalization, and validation commands are
  executable.
- Database migration and immutable object-storage layouts are documented.
- Unit, PostgreSQL, MinIO, Spark, formatting, typing, schema, and lockfile
  checks pass.
- The operator walkthrough is complete with accelerated fixtures.
- A real pilot plan is registered, with its ongoing duration clearly reported
  rather than simulated or backdated.
