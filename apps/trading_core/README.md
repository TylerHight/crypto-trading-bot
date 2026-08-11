# Trading Core

The trading core owns strategy evaluation, portfolio accounting, synchronous risk checks, and order intent creation within one transactional boundary.

Responsibilities:

- Consume trusted candle and order/fill events.
- Run the selected strategy using the injected clock and execution mode.
- Lock account state, evaluate risk, reserve exposure, and persist decisions.
- Commit approved order intents and outbox records atomically in PostgreSQL.
- Fail closed when data is stale, reconciliation is incomplete, or the kill switch is active.

It never calls an exchange directly. Private exchange effects belong to `apps/execution_gateway/`. Reusable trading rules and state transitions belong in `packages/domain/`.
