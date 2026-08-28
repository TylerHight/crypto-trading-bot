# Spark Schemas

This directory contains Spark-specific representations of canonical contracts: `StructType` definitions, parsing helpers, and explicit mappings between serialized events and DataFrame columns.

The language-neutral source of truth remains in the repository-level `schemas/` directory. Spark schemas must preserve field meaning, nullability, precision, timestamps, and schema versions. Do not place business transformations or unversioned ad hoc inference here.

`market_trades_v1.py` defines curated logical trades and safe quarantine rows.
`market_candles_v1.py` defines deterministic one-minute OHLCV/VWAP output and
its partition/key metadata.
