# Daily research participation checks and prospective data capture

September 24 update: a fixed 2020-2025 historical screen rejected the
breakout rule before its prospective observation period. The Windows task was
disabled. Its registration and first 19 warmup days remain sealed. See the
[research decision](../../research/btc-usd-breakout-historical-screen-2026-09-24.md).

## Status

- Status: complete (software and initial capture; breakout study retired before its test)
- Priority: high
- Owner: strategy research
- Dependencies: story 0009 and the September 23 daily momentum review

## User story and problem

As a research operator, I want inactive daily candidates rejected before test
access and a separately registered future study with auditable daily capture.
The old daily SMA selection chose a zero-fill validation candidate, and a
single-entry test could set its numerical paper-eligibility flag. Its frozen
follow-up failed. Generated local research output also cluttered Git status.

## Delivered scope and acceptance criteria

The daily SMA engine now uses v2 calculation and promotion identities. It
requires positive training/validation returns and validation participation,
and checks actual completed test trades, positive return, excess return, and
drawdown at 1x and 2x costs. Buy-and-hold fees use the slipped execution price.
Cached legacy selections and promotion results are rejected before fresh price
access. Every daily result remains operationally ineligible for paper trading.

The [fixed breakout protocol](../../research/btc-usd-daily-breakout-protocol-2026-09-24.md)
registers a single 20/10-day rule before a 180-day future observation period.
Its runner seals the exact specification, raw daily responses, and chained
capture manifests. Evaluation is offline and waits for complete settled data.
An OS lock prevents overlapping registration, capture, or evaluation; process
death releases it without deleting the persistent lock file.

The Windows task `CryptoResearch-BtcDailyBreakoutV1` collects at 21:15 Central
under the signed-in user's limited interactive token. It needs the computer on
and connected, catches up on the next run, and expires March 25, 2027. The
installer verifies configuration before reusing an existing task. The
[runbook](../../runbooks/daily-breakout-study.md) describes operation and logs.

Git now ignores `/artifacts/`, `/scratch.py`, and `/note2.md`; the latter two
were removed from the index and retained byte-for-byte locally. Artifact data
was already untracked: 1,643 files, approximately 53 MiB at audit time.
Container contexts also exclude artifacts, analytical workspaces, environment
files, and local virtual environments. Specifications, lockfiles, fixtures,
and linked project documentation remain tracked inputs.

## Validation evidence

- 19 daily-momentum tests passed, covering participation, completed trades,
  cached legacy evidence, versioned decisions, and exact costs.
- 24 breakout tests passed, covering future boundaries, tamper detection,
  no-lookahead signals, fees, deterministic evaluation, independent-process
  lock contention, and recovery after process death.
- 63 existing experiment, paper, gap-aware, and public-data adapter tests
  passed. One pytest cache-write permission warning did not affect results.
- Repository Ruff passed; full trading-core mypy passed for 35 source files.
  Both PowerShell scripts parse, and Git diff checks passed.
- Registered September 24, 2026 at 14:55:00 UTC with spec SHA-256
  `41551c83ec1e42a5dcb4be61ad6ffc889afb171a94300245c27daa55d4e18084`.
  Registration SHA-256:
  `c17c91ee8cbd2ba58157579ccf76007fdd6037c231cdf5001e54fab8e3ad61ab`.
- Captured 19 warmup days (September 5 through September 23); source SHA-256:
  `ac8a41a3f9f1ac9ae9ce46b34d1c3f4520a2931c63328c3157ac7419d414531f`.
  Repeated collection added zero days and kept one capture manifest.
- Task test run September 24 at 09:56 Central completed with `LastTaskResult=0`.
  The updated installer then verified the existing owner, enabled state,
  schedule, settings, and expiration without replacing it.
- Early evaluation returned `waiting`, without a performance report, with
  earliest evaluation March 24, 2027 at 01:00 UTC.

## Non-goals and rollout constraints

No profitable strategy has been established. The future test has not completed
and cannot be accelerated using its synthetic fixtures. No order or paper
session was created. Next-open execution is a simulation assumption; actual
signal latency, intraday risk, and execution require a separate implementation.

The legacy minute engine also lacks a validation participation minimum. Its
sealed v1 replay semantics were not changed by the daily fix. A compatible
versioned migration is documented in
[story 0011](../backlog/0011-versioned-minute-selection-participation.md).
Historical daily specifications and raw v1 artifacts remain unchanged.
