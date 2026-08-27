from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

EXPECTED_TYPES = {
    "event_id": "VARCHAR",
    "exchange": "VARCHAR",
    "symbol": "VARCHAR",
    "source_event_id": "VARCHAR",
    "event_time": "TIMESTAMP",
    "ingested_at": "TIMESTAMP",
    "kafka_timestamp": "TIMESTAMP",
    "price": "DECIMAL(38,18)",
    "size": "DECIMAL(38,18)",
    "notional": "DECIMAL(38,18)",
    "source_side": "VARCHAR",
    "source_sequence": "BIGINT",
    "producer": "VARCHAR",
    "trace_id": "VARCHAR",
    "correlation_id": "VARCHAR",
    "causation_id": "VARCHAR",
    "kafka_topic": "VARCHAR",
    "kafka_partition": "INTEGER",
    "kafka_offset": "BIGINT",
    "curated_schema_version": "VARCHAR",
    "curated_at": "TIMESTAMP",
    "event_date": "DATE",
}


def _configure_s3(connection: Any, uri: str) -> str:
    if not uri.startswith("s3a://"):
        return uri
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    endpoint = os.environ.get("CURATION_S3_ENDPOINT", "http://127.0.0.1:9000")
    secure = endpoint.startswith("https://")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    connection.execute("SET s3_endpoint = ?", [endpoint])
    connection.execute("SET s3_use_ssl = ?", [secure])
    connection.execute("SET s3_url_style = 'path'")
    connection.execute(
        "SET s3_access_key_id = ?",
        [os.environ.get("CURATION_S3_ACCESS_KEY", "minioadmin")],
    )
    connection.execute(
        "SET s3_secret_access_key = ?",
        [os.environ.get("CURATION_S3_SECRET_KEY", "minioadmin")],
    )
    return "s3://" + uri.removeprefix("s3a://")


def validate_manifest(manifest_path: Path, known_event_id: str | None = None) -> None:
    try:
        import duckdb
    except ImportError as error:
        raise SystemExit(
            "DuckDB is required; install it in the active environment with `pip install duckdb`."
        ) from error

    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("status") != "published":
        raise SystemExit("Manifest is not a published curated snapshot.")
    output_uri = manifest.get("curated_output_uri")
    if not isinstance(output_uri, str):
        raise SystemExit("Manifest has no curated output URI.")

    connection = duckdb.connect()
    parquet_uri = _configure_s3(connection, output_uri).rstrip("/") + "/**/*.parquet"
    relation = f"read_parquet({json.dumps(parquet_uri)}, hive_partitioning=true)"
    described = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    actual_types = {row[0]: row[1] for row in described}
    if actual_types != EXPECTED_TYPES:
        raise SystemExit(f"Curated schema mismatch: {actual_types}")

    invalid_row = connection.execute(
        f"""
        SELECT count(*)
        FROM {relation}
        WHERE event_id IS NULL
           OR try_cast(event_id AS UUID) IS NULL
           OR price <= 0 OR size <= 0 OR notional <= 0
           OR exchange <> lower(exchange)
           OR symbol <> upper(symbol)
           OR source_side NOT IN ('BUY', 'SELL')
           OR event_date <> cast(event_time AS DATE)
           OR curated_schema_version <> 'v1'
        """
    ).fetchone()
    if invalid_row is None:
        raise SystemExit("DuckDB did not return validation counts.")
    invalid = invalid_row[0]
    count_row = connection.execute(
        f"SELECT count(*), count(DISTINCT event_id) FROM {relation}"
    ).fetchone()
    if count_row is None:
        raise SystemExit("DuckDB did not return row counts.")
    total, unique_ids = count_row
    if invalid or total != unique_ids:
        raise SystemExit(
            f"Curated invariants failed: invalid={invalid}, rows={total}, unique={unique_ids}"
        )
    if known_event_id is not None:
        known_row = connection.execute(
            f"""
            SELECT count(*)
            FROM {relation}
            WHERE event_id = ?
              AND producer = 'apps.historical_backfill'
              AND kafka_timestamp > event_time
            """,
            [known_event_id],
        ).fetchone()
        if known_row is None:
            raise SystemExit("DuckDB did not return the fixture count.")
        known = known_row[0]
        if known != 1:
            raise SystemExit("Known backfilled event was not present exactly once.")
    print(f"Curated snapshot valid: rows={total}, unique_event_ids={unique_ids}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate one curated manifest with DuckDB.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--known-event-id")
    arguments = parser.parse_args()
    validate_manifest(arguments.manifest, arguments.known_event_id)


if __name__ == "__main__":
    main()
