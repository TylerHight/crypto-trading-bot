# Infrastructure

`infra/` owns reproducible runtime infrastructure for local development and AWS. It contains configuration for services and platforms, not application or transformation logic.

Local and cloud definitions should expose the same logical contracts: Kafka bootstrap configuration, S3-style prefixes, PostgreSQL connection behavior, container images, secrets by reference, and observability endpoints. Never commit credentials, Terraform state, private keys, or account-specific generated files.
