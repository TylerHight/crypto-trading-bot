# Exchange Adapters

This package isolates public and private exchange APIs behind application-owned interfaces.

Expected contents include authentication and signing, REST and WebSocket clients, request/response normalization, rate-limit behavior, stable client-order-ID support, and recorded fixtures for contract tests. Public market-data and private execution concerns may share transport primitives but should expose separate capabilities.

Adapters translate exchange-specific payloads into canonical contracts. They do not decide strategy actions, own portfolio state, or weaken domain validation.
