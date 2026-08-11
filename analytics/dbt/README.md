# dbt Project

This directory will contain the dbt project shared by local DuckDB and AWS Athena targets.

Expected contents include sources, staging models, facts, dimensions, marts, generic and singular tests, snapshots where justified, macros, seeds, exposures, and generated documentation configuration. Models should use portable SQL when practical. Engine-specific external locations, materializations, and functions belong in dispatched macros.

The initial model inventory and quality expectations are defined in the architecture document. Generated build artifacts and local credentials must not be committed.
