# Shared Packages

`packages/` contains reusable Python code shared across deployment units. Packages should expose narrow, documented interfaces and avoid depending on an application entry point.

The dependency direction is inward: applications, jobs, and orchestration may depend on packages; packages must not import from those outer layers. Prefer a little duplication over creating a vague utility package. Promote code here only when it represents stable domain behavior, an explicit external adapter, or cross-cutting observability.
