# Analytics

`analytics/` owns analytical transformations and their documentation. It consumes curated, contract-compliant datasets and produces core models and marts for quality, market, strategy, and execution analysis.

Analytics is not the transactional source of orders, fills, positions, limits, or kill-switch state. Operational correctness remains in PostgreSQL and event streams. Keep query-engine portability visible and isolate DuckDB- or Athena-specific behavior behind explicit adapter macros.
