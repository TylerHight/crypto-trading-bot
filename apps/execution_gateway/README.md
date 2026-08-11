# Execution Gateway

The execution gateway owns private exchange communication and the ambiguity of external order side effects.

Responsibilities:

- Consume approved order commands and use stable client order identifiers.
- Submit, cancel, and query orders through the configured exchange adapter.
- Consume private user-stream events and persist exchange facts.
- Reconcile orders, fills, balances, and positions at startup, after reconnects, and on schedule.
- Halt safely and resolve unknown submissions before any retry.

The gateway does not choose trades or relax risk limits. Exchange protocol implementations belong in `packages/exchange_adapters/`; order-state rules belong in `packages/domain/`.
