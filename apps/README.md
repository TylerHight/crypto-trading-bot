# Applications

`apps/` contains independently runnable, long-lived or operator-facing Python processes. Each child directory owns one deployment unit, its entry point, runtime configuration, health checks, and container definition.

Applications may use shared libraries from `packages/` and contracts from `schemas/`, but they must not import code from another application directory. If two applications need the same behavior, extract it into the narrowest appropriate shared package.

Application code should remain thin around domain behavior: receive input, validate configuration, call shared logic, persist or publish results, expose health, and shut down cleanly. Deployment topology belongs in `infra/`, while multi-application workflow scheduling belongs in `orchestration/`.
