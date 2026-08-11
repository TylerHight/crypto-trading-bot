# Domain Package

The domain package contains infrastructure-neutral trading concepts and rules.

Expected contents include money and quantity value objects, signals, order intents, order-state transitions, portfolio calculations, risk limits, strategy and clock interfaces, execution-mode abstractions, and reconciliation invariants.

This package must remain deterministic and easy to unit test. It must not import Kafka, Spark, Airflow, AWS SDKs, database drivers, HTTP frameworks, or exchange SDKs. Persistence and transport layers translate to and from domain types at their boundaries.
