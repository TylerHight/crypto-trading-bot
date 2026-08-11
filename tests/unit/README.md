# Unit Tests

Unit tests verify deterministic behavior without Kafka, Spark clusters, object storage, databases, networks, or AWS.

Priorities include domain state transitions, risk calculations, fixed-precision arithmetic, normalization, configuration validation, pure Spark transformation helpers where practical, and failure-policy decisions. Unit tests should be fast enough to run on every edit and should use explicit clocks and random seeds.
