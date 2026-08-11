# Schemas and Event Contracts

This directory is the language-neutral source of truth for versioned Kafka events and persisted data contracts.

Start with checked-in JSON Schema and representative fixtures; add Avro and Schema Registry compatibility metadata when the pipeline reaches that phase. Contracts define envelopes, payloads, field meaning, precision, nullability, and compatibility rules. Breaking changes require a new version and an architecture decision when migration is nontrivial.

Generated Python models and Spark schemas may live near their consumers, but they must be validated against these canonical definitions.
