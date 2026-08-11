# Contract Tests

Contract tests verify boundaries whose shape is controlled outside a single function.

Examples include exchange payload fixtures, JSON Schema or Avro compatibility, Kafka event envelopes, PostgreSQL migration expectations, dbt source contracts, and adapter request/response mappings. Tests should clearly distinguish an intentional contract version change from an accidental breaking change.
