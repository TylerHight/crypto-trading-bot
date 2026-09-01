# User story: Forward-test a sealed strategy in paper trading

## Story

As a strategy operator,
I want to run one sealed, out-of-sample-evaluated SMA strategy against newly
published market candles in a durable paper portfolio,
so that I can observe forward performance and operational behavior without
placing real orders or risking funds.

## Why this is next

The project now has a trustworthy research path:

```text
raw trades
    -> curated trades
    -> immutable one-minute candles
    -> deterministic backtests
    -> sealed train/validation selection
    -> one-time out-of-sample evaluation against buy-and-hold
```

That is enough offline assurance for this stage. More research validators would
have diminishing value before the strategy is exercised as a long-running
process.

The next unknowns are operational: whether the strategy behaves correctly as
candles arrive over time, survives restarts, avoids duplicate decisions and
fills, carries pending actions across processing batches, and exposes enough
evidence for an operator to understand its state.

This story moves into paper trading while retaining a hard safety boundary. It
must not add exchange authentication, balance access, order submission, or live
execution code.

## Outcome

Given a SHA-pinned successful strategy-evaluation manifest, create one durable
paper-trading session and incrementally process SHA-pinned
`market_candles.v1` publications occurring strictly after the evaluation test
interval.

The workflow must:

1. Create an immutable session identity from the evaluation, engine versions,
   cost model, starting portfolio, safety limit, and operator approval.
2. Persist cash, position, pending target, processed-candle checkpoint,
   decisions, simulated fills, equity, and session status in PostgreSQL.
3. Process each accepted candle exactly once across retries and restarts.
4. Preserve the existing SMA close-decision/next-open-fill semantics.
5. Expose a read-only status command explaining portfolio state and forward
   performance.
6. Refuse any path that could contact a private exchange endpoint or place an
   order.

## Scope

In scope:

- One sealed `sma-crossover-long-only-v1` candidate per session.
- One exchange and symbol per session.
- SHA-pinned immutable candle manifests.
- Bounded processing invoked by an operator or test.
- PostgreSQL locally, using a schema suitable for RDS later.
- The exact fees and slippage inherited from the sealed experiment.
- Durable audit records and read-only status reporting.

Out of scope:

- Private exchange credentials or authenticated exchange endpoints.
- Real or testnet order placement.
- An execution gateway, broker adapter, or balance lookup.
- Multiple strategies in one portfolio.
- Shorting, leverage, derivatives, or margin.
- Dynamic parameters or automatic strategy promotion.
- Airflow scheduling, dashboards, alert delivery, or a control API.
- Re-optimizing a strategy while its paper session is running.

## User-visible commands

Create a session from a validated evaluation manifest pinned by its exact
SHA-256 digest:

```powershell
create-paper-session `
  --evaluation-manifest s3a://crypto-data/analytics/strategy_experiments/v1/evaluations/<evaluation-key>/manifest.json `
  --evaluation-manifest-sha256 <64-lowercase-hex-digest> `
  --approved-by <nonempty-operator-identifier> `
  --approval-note "Forward test approved after reviewing OOS results" `
  --maximum-drawdown 0.20
```

Process one bounded candle publication:

```powershell
process-paper-candles `
  --session-id <paper-session-id> `
  --candle-manifest s3a://crypto-data/analytics/market_candles/v1/manifests/<snapshot-key>/manifest.json `
  --candle-manifest-sha256 <64-lowercase-hex-digest>
```

Inspect current state without mutating it:

```powershell
show-paper-session --session-id <paper-session-id>
```

Pause, resume, or permanently stop a session:

```powershell
set-paper-session-state `
  --session-id <paper-session-id> `
  --state paused `
  --actor <operator-identifier> `
  --reason "Investigating an input discontinuity"
```

Provide equivalent Python module entry points. Database credentials must come
from `PAPER_DATABASE_URL` or a supported secret source and must never appear
in logs, reports, database audit text, or errors.

## Session creation

Before writing state, creation must:

- Verify the evaluation manifest's exact digest before parsing.
- Run the existing evaluation validator, including selection, source, strategy,
  baseline, artifact, and comparison replay checks.
- Require `execution_mode = "simulation"` throughout the lineage.
- Require a selected candidate and completed test evaluation.
- Recover the exact strategy version, parameters, exchange, symbol, fee,
  slippage, and final test boundary from the sealed artifacts.
- Reject unknown versions and malformed or unsafe URIs.
- Require explicit local-development mode for local artifacts.

Identical logical inputs must resolve the same session identity and existing
row. Changing the evaluation digest, portfolio seed, engine version, maximum
drawdown, or approval must produce a different identity.

Record the operator identifier, timestamp, bounded approval note, and evaluation
digest. Approval is audit evidence; it is not a claim that the strategy is
profitable.

## Durable PostgreSQL state

Add a forward-only migration for at least:

- `paper_sessions`
- `paper_candle_inputs`
- `paper_decisions`
- `paper_fills`
- `paper_equity`
- `paper_session_events`

Use database constraints to enforce:

- A stable unique session identity.
- One processed candle per session, exchange, symbol, and open time.
- One decision per session and decision candle.
- At most one fill for a pending decision.
- Nonnegative cash, quantity, fees, and notional.
- UTC timestamps and explicit version fields.
- Append-only decisions, fills, equity, and lifecycle events.

The session row may hold the current portfolio and checkpoint for efficient
reads, but all mutations must be reconstructable from immutable event rows.
Rows must never contain object-storage secrets, database passwords, private
exchange credentials, or complete raw exchange payloads.

## Incremental processing

`process-paper-candles` processes one pinned immutable manifest and exits. It
does not poll forever.

For each invocation:

1. Lock and validate the session in a database transaction.
2. Verify the candle manifest digest and published artifacts with existing
   candle safety rules.
3. Select only the session exchange and symbol.
4. Reject candles at or before the evaluation test end.
5. Reject a gap after the session has started; never silently skip a minute.
6. Treat already committed candles as idempotent only when source identity and
   values match exactly.
7. Reject conflicting data for an already processed candle.
8. Apply unseen candles in ascending open-time order.
9. Commit input identity, decision, possible next-open fill, equity, and
   checkpoint atomically.

Report counts for discovered, already processed, newly processed, decided,
filled, and rejected candles. Retry after an ambiguous client failure must
converge on the same database state.

## Strategy continuity

Paper mode must reuse domain strategy and broker rules:

- Seed SMA history with the last `slow_period - 1` candles ending at the
  evaluation boundary from the evaluation's pinned source snapshot.
- A target decided from candle `t` may fill only at candle `t+1` open.
- A pending target must survive process exit and restart.
- Fees, slippage, rounding, and affordable quantity must exactly match the
  backtest engine.
- Mark open positions to every accepted close.
- Do not force liquidation at a batch boundary.
- Do not invent decisions for gaps or partial candles.

Parity tests must prove that one-candle, multi-batch, and single-batch processing
produce the same decisions, fills, cash, position, and equity as the existing
deterministic domain rules.

## Safety and lifecycle

Session states are:

```text
active -> paused
active -> stopped
paused -> active
paused -> stopped
```

`stopped` is terminal. Every state change requires an explicit command,
actor, and append-only reason. Processing a paused or stopped session must not
advance its checkpoint.

Automatically pause before processing more candles when:

- Input timestamps are discontinuous or move backward.
- A pinned artifact conflicts with an existing candle.
- Portfolio invariants fail.
- Current drawdown exceeds the immutable session maximum.

An automatic pause records a machine-readable reason and safe diagnostic
context. It never automatically resumes. These controls protect the paper
experiment; they do not replace the independent risk controls needed for live
execution.

## Read-only status

`show-paper-session` returns deterministic JSON containing:

- Session ID, state, and engine versions.
- Evaluation and selection manifest URIs and digests.
- Exchange, symbol, strategy, and sealed parameters.
- Starting cash, cash, position quantity, and marked equity.
- Pending target and decision time, if any.
- First and last processed candle times.
- Candle, decision, buy-fill, and sell-fill counts.
- Gross return, net return, fees, and maximum drawdown.
- A forward buy-and-hold baseline using the same first candle and costs.
- Last state-change reason and time.

It must use a read-only transaction and cannot repair, advance, or resume a
session.

## Audit and concurrency

- Use structured logs containing session IDs and safe digests.
- Never log credential-bearing database URLs.
- Every state-changing command records command ID, actor, timestamp, action,
  reason, and resulting state.
- Accept an optional command ID for retry correlation; otherwise generate one.
- Reusing a command ID with different arguments is an error.
- Database transactions must prevent concurrent advancement of one session.

## Configuration

Provide bounded settings for:

```text
PAPER_DATABASE_URL
PAPER_CANDLE_MANIFEST_PREFIX
PAPER_EVALUATION_MANIFEST_PREFIX
PAPER_MAXIMUM_CANDLES_PER_RUN
PAPER_TRANSACTION_TIMEOUT_SECONDS
```

Environment settings cannot weaken sealed strategy parameters or costs.
Maximum drawdown belongs to the immutable session identity.

## Required tests

### Unit tests

- Stable and changed-input session identities.
- Evaluation digest and lineage validation failures.
- Rejection of unevaluated, unselected, or non-simulation input.
- Warm-up and first forward decision boundary.
- Decision-at-close and fill-at-next-open across invocations.
- Fee, slippage, rounding, and equity parity with the domain engine.
- Idempotent retries and conflicting-candle rejection.
- Missing, duplicate, out-of-order, and pre-test candle rejection.
- Paused, resumed, and stopped behavior.
- Automatic drawdown pause.
- Credential redaction.
- Status calculations and forward buy-and-hold comparison.

### PostgreSQL integration tests

- Apply migrations to a clean local database.
- Create a session from a validator-compatible evaluation fixture.
- Process candles and verify durable state.
- Retry without duplicate decisions, fills, or equity.
- Carry and fill a pending target across publication boundaries.
- Simulate a client failure after commit and retry safely.
- Run two processors concurrently and prove serialized results.
- Restart the application and continue from its checkpoint.
- Prove read-only status does not mutate tables.

### Regression tests

- Existing collection, integrity, reconciliation, curation, candle, backtest,
  and sealed-experiment tests remain green.
- Paper-trading code does not import a private exchange client or expose an
  order-placement interface.

## Acceptance criteria

This story is complete when:

1. A validated, digest-pinned evaluation creates one idempotent paper session
   with auditable operator approval.
2. Repeated bounded publications advance the durable portfolio exactly once
   and preserve strategy semantics across restarts.
3. Fixed input produces the same outcome regardless of batch size.
4. Gaps, conflicts, bad lineage, paused state, and drawdown breaches fail
   closed without advancing the portfolio.
5. Operators can inspect paper performance and its forward buy-and-hold
   baseline without mutation.
6. PostgreSQL tests prove transactionality, retry safety, concurrency safety,
   and restart recovery.
7. No code in this increment can authenticate to an exchange or place an
   order.
8. Documentation states that paper results are simulations and do not
   establish future profitability.

## Definition of done

- PostgreSQL migration and storage implementation are checked in.
- Session creation, bounded candle processing, lifecycle, and status commands
  are documented and executable.
- Domain behavior is reused or factored cleanly; broker math is not duplicated
  with different semantics.
- Unit and PostgreSQL integration tests pass.
- Existing tests, formatting, typing, schemas, and lockfile checks pass.
- A local walkthrough demonstrates create, process, retry, inspect, pause,
  resume, and stop using simulation-only inputs.
