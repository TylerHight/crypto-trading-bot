# Spark Schemas

This directory contains Spark-specific representations of canonical contracts: `StructType` definitions, parsing helpers, and explicit mappings between serialized events and DataFrame columns.

The language-neutral source of truth remains in the repository-level `schemas/` directory. Spark schemas must preserve field meaning, nullability, precision, timestamps, and schema versions. Do not place business transformations or unversioned ad hoc inference here.
