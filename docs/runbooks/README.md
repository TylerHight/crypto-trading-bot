# Runbooks

Runbooks contain tested, step-by-step operational procedures for detection, containment, recovery, and verification.

Initial runbooks should cover collector reconnect storms, Kafka or MSK unavailability, Spark checkpoint recovery, failed replay or dbt runs, stale market data, reconciliation divergence, ambiguous order submission, kill-switch activation, credential rotation, and AWS teardown.

Every procedure should state prerequisites, safety warnings, exact commands or links, expected evidence, rollback conditions, and the point at which manual escalation is required.
