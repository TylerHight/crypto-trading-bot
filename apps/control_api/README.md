# Control API

The control API is a small operator-facing boundary for safe configuration and lifecycle controls.

Responsibilities:

- Read current mode, health, limits, and reconciliation status.
- Activate the global kill switch immediately.
- Accept validated configuration changes with authentication and audit records.
- Require explicit operator action for promotion between backtest, paper, testnet, and live modes.

It does not contain strategy logic, submit orders, or bypass the trading core. A custom frontend is outside the initial scope; this directory owns only the API and its authorization boundary.
