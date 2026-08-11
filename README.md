# Crypto Trading and Data Platform

This repository is a local-first, AWS-portable crypto market-data and automated-trading platform. The current scaffold defines ownership boundaries before implementation begins. The proposed system and delivery phases are documented in [the architecture](docs/architecture/architecture.md).

## Repository map

| Path | Responsibility |
|---|---|
| `apps/` | Independently runnable Python application processes |
| `jobs/` | Distributed streaming, batch, and replay jobs |
| `packages/` | Reusable domain and infrastructure-neutral Python libraries |
| `analytics/` | dbt transformations, tests, documentation, and marts |
| `orchestration/` | Airflow DAGs for bounded workflow coordination |
| `schemas/` | Versioned event and data-contract definitions |
| `infra/` | Local Compose and AWS Terraform definitions |
| `tests/` | Cross-repository unit, contract, integration, and replay suites |
| `docs/` | Architecture decisions, runbooks, and incident learning |

Every scaffold directory contains a README that defines its scope. Add implementation files beside the relevant README as each delivery phase begins.

## Dependency direction

```text
apps  jobs  analytics  orchestration
  \     |       |          /
       packages + schemas
             |
       external systems
```

- Applications and jobs may depend on `packages/` and generated or validated artifacts from `schemas/`.
- One application must not import another application. Shared behavior moves into a package.
- `packages/domain/` contains business rules and must not import Kafka, Spark, Airflow, AWS, a web framework, or an exchange SDK.
- Orchestration coordinates existing commands and jobs; it does not implement transformations or trading rules.
- Infrastructure provisions runtime dependencies; it does not contain application behavior.
- Analytics reads trusted data and produces analytical models; it never becomes the transactional source of trading truth.

## Local-to-AWS contract

The same code and container images should work in both environments. Configuration maps local Kafka, MinIO, DuckDB, and PostgreSQL to Amazon MSK, S3, Athena/Glue, and RDS. Cloud-specific SDK calls belong in adapters or infrastructure, not in domain or transformation code.

## Repository conventions

- Store no credentials, account identifiers, or private market data in Git.
- Keep executable entry points thin and delegate behavior to testable modules.
- Version externally visible schemas and test backward compatibility.
- Make replay, backfill, and orchestration operations bounded and idempotent.
- Add a package only when behavior is shared by more than one runtime unit or represents an explicit architectural boundary.
