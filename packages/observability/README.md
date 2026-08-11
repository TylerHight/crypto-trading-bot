# Observability Package

This package provides consistent structured logging, metrics, tracing context, health reporting, and correlation identifiers across applications and jobs.

It may define metric names and small adapters for local output or CloudWatch, but it must not contain business decisions or make domain behavior depend on a monitoring backend. Avoid logging secrets, private credentials, raw authentication headers, or unnecessarily sensitive account data.
