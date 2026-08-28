from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

EXPECTED_TYPES = {
    "exchange": "VARCHAR",
    "symbol": "VARCHAR",
    "window_start": "TIMESTAMP",
    "window_end": "TIMESTAMP",
    "open": "DECIMAL(38,18)",
    "high": "DECIMAL(38,18)",
    "low": "DECIMAL(38,18)",
    "close": "DECIMAL(38,18)",
    "base_volume": "DECIMAL(38,18)",
    "quote_volume": "DECIMAL(38,18)",
    "vwap": "DECIMAL(38,18)",
    "trade_count": "BIGINT",
    "historical_backfill_trade_count": "BIGINT",
    "first_event_id": "VARCHAR",
    "last_event_id": "VARCHAR",
    "minimum_kafka_timestamp": "TIMESTAMP",
    "maximum_kafka_timestamp": "TIMESTAMP",
    "minimum_ingested_at": "TIMESTAMP",
    "maximum_ingested_at": "TIMESTAMP",
    "source_curated_snapshot_key": "VARCHAR",
    "candle_schema_version": "VARCHAR",
    "candle_transform_version": "VARCHAR",
    "created_at": "TIMESTAMP",
    "interval": "VARCHAR",
    "event_date": "DATE",
    "event_hour": "VARCHAR",
}


def _configure_s3(connection: Any, uri: str) -> str:
    if not uri.startswith("s3a://"):
        return uri
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    endpoint = os.environ.get("CANDLE_S3_ENDPOINT", "http://127.0.0.1:9000")
    secure = endpoint.startswith("https://")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    connection.execute("SET s3_endpoint = ?", [endpoint])
    connection.execute("SET s3_use_ssl = ?", [secure])
    connection.execute("SET s3_url_style = 'path'")
    connection.execute(
        "SET s3_access_key_id = ?",
        [os.environ.get("CANDLE_S3_ACCESS_KEY", "minioadmin")],
    )
    connection.execute(
        "SET s3_secret_access_key = ?",
        [os.environ.get("CANDLE_S3_SECRET_KEY", "minioadmin")],
    )
    return "s3://" + uri.removeprefix("s3a://")


def validate_manifest(
    manifest_path: Path,
    *,
    known_backfill_symbol: str | None = None,
    known_backfill_window_start: str | None = None,
) -> None:
    try:
        import duckdb
    except ImportError as error:
        raise SystemExit(
            "DuckDB is required; install it in the active environment with `pip install duckdb`."
        ) from error

    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("status") != "published":
        raise SystemExit("Manifest is not a published candle snapshot.")
    output_uri = manifest.get("candle_output_uri")
    source_trades = manifest.get("source_curated_trades")
    if not isinstance(output_uri, str) or not isinstance(source_trades, int):
        raise SystemExit("Manifest is missing its candle output or source count.")
    if (known_backfill_symbol is None) != (known_backfill_window_start is None):
        raise SystemExit("Known backfill symbol and window start must be supplied together.")

    connection = duckdb.connect()
    parquet_uri = _configure_s3(connection, output_uri).rstrip("/") + "/**/*.parquet"
    relation = (
        f"read_parquet({json.dumps(parquet_uri)}, hive_partitioning=true, "
        "hive_types={'interval': VARCHAR, 'event_date': DATE, 'event_hour': VARCHAR})"
    )
    described = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    actual_types = {row[0]: row[1] for row in described}
    if actual_types != EXPECTED_TYPES:
        raise SystemExit(f"Candle schema mismatch: {actual_types}")

    invalid_row = connection.execute(
        f"""
        SELECT count(*)
        FROM {relation}
        WHERE exchange IS NULL OR symbol IS NULL OR window_start IS NULL
           OR interval <> '1m'
           OR window_end <> window_start + INTERVAL 1 MINUTE
           OR low > open OR open > high OR low > close OR close > high
           OR base_volume <= 0 OR quote_volume <= 0 OR vwap <= 0
           OR trade_count <= 0
           OR historical_backfill_trade_count < 0
           OR historical_backfill_trade_count > trade_count
           OR event_date <> cast(window_start AS DATE)
           OR event_hour <> strftime(window_start, '%H')
           OR candle_schema_version <> 'v1'
           OR try_cast(first_event_id AS UUID) IS NULL
           OR try_cast(last_event_id AS UUID) IS NULL
        """
    ).fetchone()
    count_row = connection.execute(
        f"""
        SELECT count(*),
               count(DISTINCT (exchange, symbol, interval, window_start)),
               coalesce(sum(trade_count), 0)
        FROM {relation}
        """
    ).fetchone()
    if invalid_row is None or count_row is None:
        raise SystemExit("DuckDB did not return candle validation counts.")
    invalid = invalid_row[0]
    candle_count, unique_keys, trade_count_sum = count_row
    if invalid or candle_count != unique_keys or trade_count_sum != source_trades:
        raise SystemExit(
            "Candle invariants failed: "
            f"invalid={invalid}, candles={candle_count}, unique={unique_keys}, "
            f"trades={trade_count_sum}/{source_trades}"
        )
    if known_backfill_symbol is not None and known_backfill_window_start is not None:
        known_row = connection.execute(
            f"""
            SELECT count(*)
            FROM {relation}
            WHERE symbol = ?
              AND window_start = cast(? AS TIMESTAMP)
              AND historical_backfill_trade_count > 0
            """,
            [known_backfill_symbol.upper(), known_backfill_window_start],
        ).fetchone()
        if known_row is None or known_row[0] != 1:
            raise SystemExit("Known historical-backfill candle was not present exactly once.")
    print(
        f"Candle snapshot valid: candles={candle_count}, source_trades={trade_count_sum}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate one candle manifest with DuckDB.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--known-backfill-symbol")
    parser.add_argument("--known-backfill-window-start")
    arguments = parser.parse_args()
    validate_manifest(
        arguments.manifest,
        known_backfill_symbol=arguments.known_backfill_symbol,
        known_backfill_window_start=arguments.known_backfill_window_start,
    )


if __name__ == "__main__":
    main()
