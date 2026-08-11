# Data Processing Jobs

`jobs/` contains distributed data-processing workloads rather than network-facing application services. Jobs must be runnable locally and on AWS with the same transformation code and environment-specific submission configuration.

Each job has explicit inputs, outputs, checkpoints or run manifests, and idempotency behavior. Batch jobs must be bounded. Streaming jobs may be long-running but must recover from durable checkpoints. Workflow scheduling belongs in `orchestration/`, not in job implementations.
