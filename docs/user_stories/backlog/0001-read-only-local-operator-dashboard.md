# US-0001: Read-only local operator dashboard

**Status:** recommended  
**Priority:** next  
**Created:** 2026-09-03  
**Owner:** project operator  
**Dependencies:** existing collector health events, raw-integrity reports,
immutable curation/candle/experiment artifacts, and paper-pilot PostgreSQL
state.

## User story

As a project operator,
I want one local, read-only dashboard that explains the health, data lineage,
research result, and paper-pilot status in plain language,
so that I can decide what needs attention without assembling information from
container logs, Kafka, MinIO manifests, PostgreSQL, and several CLI commands.

## Why this is next

The pipeline and paper-pilot workflow now have meaningful operational evidence,
but it is spread across infrastructure views and immutable artifacts. The
current Kafka UI and MinIO console show individual components; neither explains
the project-level state or the next safe operator action. A read-only dashboard
reduces that cognitive load before any UI is allowed to initiate pilot actions.

## MVP scope

Provide a local-only dashboard, bound to loopback by default, with these views:

1. **Pipeline health** — collector connection and latest health-summary time,
   Kafka/raw-sink progress where available, and MinIO/PostgreSQL availability.
2. **Trusted data** — latest raw-integrity result, latest curated and candle
   publications, their timestamps, record counts, SHA-256 digests, and clear
   stale/unavailable indicators.
3. **Research** — selected candidate, its train/validation/test windows, OOS
   strategy return, buy-and-hold return, excess return, and a plain-language
   explanation that an OOS result is evidence rather than a profitability claim.
4. **Paper pilot** — plan identity and precommitted criteria; either the real
   pilot state and forward metrics or an explicit `not registered` state. The
   view must distinguish a draft plan, a registered pilot, and a terminal
   assessment.
5. **Next safe action** — contextual links to the documented command or
   runbook step, such as investigate a stale collector, create a candle
   publication, review a pilot approval, or wait for forward evidence.

The dashboard may use a narrow local read API or server-rendered application,
but presentation code must not embed trading, portfolio, curation, or audit
rules. Reuse existing validators and domain/application services.

## Non-goals

- Registering, pausing, resuming, finalizing, or cancelling a pilot from the
  dashboard.
- Any exchange authentication, account access, order placement, or live-trading
  controls.
- Replacing Kafka UI, MinIO console, database administration tools, or logs.
- Inventing health or performance values when a dependency or artifact is
  unavailable.
- Building a cloud-hosted multi-user product, alerting system, or mobile app.

## Acceptance criteria

1. An operator can start the dashboard locally with one documented command and
   view it only through a loopback address by default.
2. The home view presents the five MVP areas without exposing credentials,
   private URLs, raw trade payloads, or database connection strings.
3. Every displayed artifact identity links to, or copies as, its canonical URI
   and SHA-256 digest; a missing, invalid, or stale artifact is shown as such
   rather than represented as current.
4. Pipeline health identifies the source and observation time for every status.
   A failure to reach a dependency is visibly `unavailable` and does not make
   another component look unhealthy by inference.
5. The research view shows the current OOS comparison and explicitly states
   that it is not a live-performance or profitability claim.
6. The pilot view shows `not registered` while no pilot exists, and displays
   no invented balance, fills, or assessment. Once a pilot exists, it reads
   durable PostgreSQL state and immutable publications only.
7. The application performs no state-changing database, object-storage, Kafka,
   or exchange operation. Automated tests prove that all configured routes are
   read-only.
8. Unit tests cover formatting and stale/unavailable states; integration tests
   cover data from local PostgreSQL and MinIO fixtures; a browser-level or
   equivalent rendered-view test covers the `not registered` pilot state.
9. The project documentation explains setup, local access, data sources,
   refresh behavior, and how to verify that the dashboard did not mutate state.

## Validation and rollout

Run the complete Python suite plus dashboard-specific unit, integration, and
rendered-view tests. Start it against the local Compose stack, compare each
displayed value with its source manifest or PostgreSQL row, and verify the
database/object counts are unchanged before and after a read-only browsing
session. Keep the dashboard local and opt-in until those checks pass.

## Definition of done

The story is complete when a new operator can open one local page and accurately
answer: “Is data collection healthy?”, “What is the newest trusted dataset?”,
“What did the strategy’s OOS test show?”, “Is a paper pilot running?”, and
“What is the next safe action?”—without using a database client or manually
parsing a manifest.
