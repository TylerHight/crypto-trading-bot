"""Create a local DuckDB workspace over immutable historical research data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from crypto_trading_core.contracts import InvalidBacktestInput, is_local_uri
from crypto_trading_core.storage import ObjectStorage, StorageSettings, child_uri, parse_location

DEFAULT_CANDLE_MANIFEST_PREFIX = "s3a://crypto-data/analytics/historical_candles/v1/manifests/"
DEFAULT_BACKTEST_PREFIX = "s3a://crypto-data/analytics/strategy_experiments/v1/backtests/runs/"
DEFAULT_EXPERIMENT_PREFIX = "s3a://crypto-data/analytics/strategy_experiments/v1/runs/"
DEFAULT_GAP_AWARE_PREFIX = "s3a://crypto-data/analytics/strategy_experiments/v1/gap_aware_research/"


@dataclass(frozen=True)
class ResearchWorkspaceSettings:
    workspace_path: Path
    candle_manifest_prefix: str
    backtest_prefix: str
    experiment_prefix: str
    gap_aware_prefix: str
    storage: StorageSettings

    @classmethod
    def from_env(cls, workspace_path: Path) -> ResearchWorkspaceSettings:
        return cls(
            workspace_path=workspace_path,
            candle_manifest_prefix=os.getenv(
                "RESEARCH_WORKSPACE_CANDLE_MANIFEST_PREFIX", DEFAULT_CANDLE_MANIFEST_PREFIX
            ),
            backtest_prefix=os.getenv(
                "RESEARCH_WORKSPACE_BACKTEST_PREFIX", DEFAULT_BACKTEST_PREFIX
            ),
            experiment_prefix=os.getenv(
                "RESEARCH_WORKSPACE_EXPERIMENT_PREFIX", DEFAULT_EXPERIMENT_PREFIX
            ),
            gap_aware_prefix=os.getenv(
                "RESEARCH_WORKSPACE_GAP_AWARE_PREFIX", DEFAULT_GAP_AWARE_PREFIX
            ),
            storage=StorageSettings(
                endpoint_url=os.getenv("RESEARCH_WORKSPACE_S3_ENDPOINT"),
                access_key=os.getenv("RESEARCH_WORKSPACE_S3_ACCESS_KEY"),
                secret_key=os.getenv("RESEARCH_WORKSPACE_S3_SECRET_KEY"),
                region=os.getenv("RESEARCH_WORKSPACE_S3_REGION", "us-east-1"),
            ),
        )


def _s3_uri(uri: str) -> str:
    return "s3://" + uri.split("://", 1)[1] if uri.startswith(("s3://", "s3a://")) else uri


def _parquet_glob(uri: str) -> str:
    return child_uri(uri, "**", "*.parquet")


def _json_glob(uri: str) -> str:
    return child_uri(uri, "*", "manifest.json")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _configure_s3(connection: duckdb.DuckDBPyConnection, settings: StorageSettings) -> None:
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    if settings.endpoint_url:
        endpoint = settings.endpoint_url.removeprefix("http://").removeprefix("https://")
        connection.execute("SET s3_endpoint = ?", [endpoint])
        connection.execute("SET s3_use_ssl = ?", [settings.endpoint_url.startswith("https://")])
        connection.execute("SET s3_url_style = 'path'")
    if settings.access_key:
        connection.execute("SET s3_access_key_id = ?", [settings.access_key])
    if settings.secret_key:
        connection.execute("SET s3_secret_access_key = ?", [settings.secret_key])
    connection.execute("SET s3_region = ?", [settings.region])


def _dbeaver_init_sql(settings: StorageSettings) -> str:
    """Return one-session setup SQL without changing remote data."""

    statements = ["INSTALL httpfs", "LOAD httpfs", "SET TimeZone = 'UTC'"]
    if settings.endpoint_url:
        endpoint = settings.endpoint_url.removeprefix("http://").removeprefix("https://")
        statements.extend(
            [
                f"SET s3_endpoint = {_quote(endpoint)}",
                f"SET s3_use_ssl = {'true' if settings.endpoint_url.startswith('https://') else 'false'}",
                "SET s3_url_style = 'path'",
            ]
        )
    if settings.access_key:
        statements.append(f"SET s3_access_key_id = {_quote(settings.access_key)}")
    if settings.secret_key:
        statements.append(f"SET s3_secret_access_key = {_quote(settings.secret_key)}")
    statements.append(f"SET s3_region = {_quote(settings.region)}")
    return ";\n".join(statements) + ";\n"


def _starter_queries() -> str:
    return """-- Choose one query, run it, then graph the result in DBeaver.
-- All timestamps are UTC. Source data stays in MinIO; this database contains views.

-- Daily BTC close-price line
SELECT date_trunc('day', window_start) AS day, avg(close) AS average_close
FROM research_data.candles
WHERE symbol = 'BTC-USD'
GROUP BY day
ORDER BY day;

-- Every complete backtest fill that was published as Parquet
SELECT backtest_run_id, fill_time, side, execution_price, fee
FROM research_data.backtest_fills
ORDER BY backtest_run_id, fill_time;

-- Account value through each complete published backtest
SELECT backtest_run_id, window_start AS point_time, equity, drawdown, position
FROM research_data.backtest_equity
ORDER BY backtest_run_id, window_start;

-- Candidate comparison from the sealed selection publication
SELECT *
FROM research_data.candidate_results
ORDER BY experiment_run_id, candidate_id;

-- Follow one candidate result through its baseline comparison and published fills.
SELECT r.candidate_id, r.range_name, r.percentage_return AS candidate_return,
       baseline.percentage_return AS baseline_return,
       run.backtest_run_id, fill.fill_time, fill.side, fill.execution_price
FROM research_model.candidate_results r
JOIN research_model.baseline_results baseline
  USING (experiment_run_id, range_name)
JOIN research_model.backtest_runs run USING (backtest_key)
LEFT JOIN research_model.backtest_fills fill USING (backtest_run_id)
ORDER BY r.candidate_id, r.range_name, fill.fill_time;

-- Saved samples from the gap-aware study. These are not every simulated fill.
SELECT candidate_id, period, segment_number, fill_time, side, execution_price, fee
FROM research_data.gap_aware_trade_samples
ORDER BY candidate_id, period, segment_number, fill_time;

-- Saved account-value samples from the gap-aware study.
SELECT candidate_id, period, segment_number, point_time, equity, drawdown, position
FROM research_data.gap_aware_equity_samples
ORDER BY candidate_id, period, segment_number, point_time;
"""


def _latest_candle_manifest(settings: ResearchWorkspaceSettings) -> tuple[str, bytes]:
    location = parse_location(settings.candle_manifest_prefix)
    if location.scheme == "file":
        candidates = sorted(Path(location.key).glob("*/manifest.json"))
        if not candidates:
            raise InvalidBacktestInput("No historical candle manifest was found")
        path = max(candidates, key=lambda candidate: candidate.stat().st_mtime)
        return str(path), path.read_bytes()

    storage = ObjectStorage(settings.storage)
    client: Any = storage._s3()  # The workspace only uses the existing read-only client.
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=location.bucket, Prefix=location.key.rstrip("/") + "/"
    )
    items = [
        item
        for page in pages
        for item in page.get("Contents", [])
        if str(item.get("Key", "")).endswith("/manifest.json")
    ]
    if not items:
        raise InvalidBacktestInput("No historical candle manifest was found")
    latest = max(items, key=lambda item: item["LastModified"])
    uri = f"s3a://{location.bucket}/{latest['Key']}"
    return uri, storage.read_bytes(uri)


def _read_candle_source(settings: ResearchWorkspaceSettings) -> tuple[str, Mapping[str, Any], str]:
    manifest_uri, manifest_bytes = _latest_candle_manifest(settings)
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as error:
        raise InvalidBacktestInput("Historical candle manifest is not JSON") from error
    if not isinstance(manifest, Mapping) or not isinstance(manifest.get("candle_output_uri"), str):
        raise InvalidBacktestInput("Historical candle manifest has no candle output URI")
    return (
        manifest_uri,
        manifest,
        hashlib.sha256(manifest_bytes).hexdigest(),
    )


def _source_table(uri: str, *, hive: bool = False) -> str:
    options = ", hive_partitioning=true" if hive else ""
    return f"read_parquet({_quote(_s3_uri(_parquet_glob(uri)))}, filename=true{options})"


def _create_views(
    connection: duckdb.DuckDBPyConnection,
    *,
    candles_uri: str,
    settings: ResearchWorkspaceSettings,
) -> None:
    connection.execute("CREATE SCHEMA research_data")
    connection.execute(
        f"""CREATE VIEW research_data.candles AS
        SELECT * FROM {_source_table(candles_uri, hive=True)}"""
    )
    backtest_root = settings.backtest_prefix.rstrip("/")
    if backtest_root.endswith("/runs"):
        backtest_root = backtest_root.removesuffix("/runs")
    manifests = _quote(_s3_uri(child_uri(backtest_root, "manifests", "*", "manifest.json")))
    connection.execute(
        f"""CREATE VIEW research_data.backtest_manifests AS
        SELECT filename AS manifest_file,
               json_extract_string(json, '$.backtest_run_id') AS backtest_run_id,
               json_extract_string(json, '$.backtest_key') AS backtest_key
        FROM read_json_objects({manifests}, filename=true)"""
    )
    for name, suffix in (
        ("backtest_fills", "fills.parquet"),
        ("backtest_equity", "equity_curve.parquet"),
        ("backtest_decisions", "decisions.parquet"),
    ):
        source = _quote(_s3_uri(child_uri(settings.backtest_prefix, "*", suffix)))
        connection.execute(
            f"""CREATE VIEW research_data.{name} AS
            SELECT *, regexp_extract(replace(filename, chr(92), '/'), '/(?:backtests/(?:runs/)?|runs/)([^/]+)/', 1) AS backtest_run_id
            FROM read_parquet({source}, filename=true)"""
        )
    connection.execute(
        """CREATE VIEW research_data.backtest_runs AS
        SELECT DISTINCT backtest_run_id
        FROM (
            SELECT backtest_run_id FROM research_data.backtest_fills
            UNION ALL
            SELECT backtest_run_id FROM research_data.backtest_equity
            UNION ALL
            SELECT backtest_run_id FROM research_data.backtest_decisions
        )"""
    )
    for name, suffix in (
        ("candidate_results", "selection/candidate_results.parquet"),
        ("baseline_results", "selection/baseline_results.parquet"),
        ("baseline_fills", "evaluation/baseline_fills.parquet"),
        ("baseline_equity", "evaluation/baseline_equity_curve.parquet"),
    ):
        source = _quote(_s3_uri(child_uri(settings.experiment_prefix, "*", suffix)))
        connection.execute(
            f"""CREATE VIEW research_data.{name} AS
            SELECT *, regexp_extract(replace(filename, chr(92), '/'), '/(?:experiments/(?:runs/)?|runs/)([^/]+)/', 1) AS experiment_run_id
            FROM read_parquet({source}, filename=true)"""
        )
    connection.execute(
        """CREATE VIEW research_data.experiment_runs AS
        SELECT DISTINCT experiment_run_id
        FROM (
            SELECT experiment_run_id FROM research_data.candidate_results
            UNION ALL
            SELECT experiment_run_id FROM research_data.baseline_results
            UNION ALL
            SELECT experiment_run_id FROM research_data.baseline_fills
            UNION ALL
            SELECT experiment_run_id FROM research_data.baseline_equity
        )"""
    )
    report_json = _quote(_s3_uri(_json_glob(child_uri(settings.gap_aware_prefix, "reports"))))
    connection.execute(
        f"""CREATE VIEW research_data.gap_aware_reports AS
        SELECT filename AS report_file,
               json_extract_string(json, '$.status') AS publication_status,
               json_extract_string(json, '$.summary.status') AS research_status,
               json_extract_string(json, '$.summary.message') AS message,
               json_extract_string(json, '$.summary.recommendation') AS recommendation
        FROM read_json_objects({report_json}, filename=true)"""
    )
    selection_json = _quote(_s3_uri(_json_glob(child_uri(settings.gap_aware_prefix, "selections"))))
    connection.execute(
        f"""CREATE VIEW research_data.gap_aware_trade_samples AS
        WITH documents AS (
          SELECT filename, json FROM read_json_objects({selection_json}, filename=true)
        ), candidates AS (
          SELECT filename, candidate.key AS candidate_id, candidate.value AS candidate_json
          FROM documents, json_each(json_extract(json, '$.evidence.candidates')) AS candidate
        ), periods AS (
          SELECT filename, candidate_id, period.key AS period, period.value AS period_json
          FROM candidates, json_each(candidate_json) AS period
          WHERE period.key IN ('train', 'validation')
        ), segments AS (
          SELECT filename, candidate_id, period, segment.key::INTEGER + 1 AS segment_number,
                 segment.value AS segment_json
          FROM periods, json_each(json_extract(period_json, '$.segments')) AS segment
        )
        SELECT filename AS selection_file, candidate_id, period, segment_number,
               json_extract_string(marker.value, '$.time')::TIMESTAMPTZ AS fill_time,
               json_extract_string(marker.value, '$.side') AS side,
               json_extract_string(marker.value, '$.execution_price')::DECIMAL(38,18)
                 AS execution_price,
               json_extract_string(marker.value, '$.fee')::DECIMAL(38,18) AS fee
        FROM segments,
             json_each(json_extract(segment_json, '$.chart.trade_markers')) AS marker"""
    )
    connection.execute(
        f"""CREATE VIEW research_data.gap_aware_equity_samples AS
        WITH documents AS (
          SELECT filename, json FROM read_json_objects({selection_json}, filename=true)
        ), candidates AS (
          SELECT filename, candidate.key AS candidate_id, candidate.value AS candidate_json
          FROM documents, json_each(json_extract(json, '$.evidence.candidates')) AS candidate
        ), periods AS (
          SELECT filename, candidate_id, period.key AS period, period.value AS period_json
          FROM candidates, json_each(candidate_json) AS period
          WHERE period.key IN ('train', 'validation')
        ), segments AS (
          SELECT filename, candidate_id, period, segment.key::INTEGER + 1 AS segment_number,
                 segment.value AS segment_json
          FROM periods, json_each(json_extract(period_json, '$.segments')) AS segment
        )
        SELECT filename AS selection_file, candidate_id, period, segment_number,
               json_extract_string(point.value, '$.time')::TIMESTAMPTZ AS point_time,
               json_extract_string(point.value, '$.equity')::DECIMAL(38,18) AS equity,
               json_extract_string(point.value, '$.drawdown')::DECIMAL(38,18) AS drawdown,
               json_extract_string(point.value, '$.position') AS position
        FROM segments,
             json_each(json_extract(segment_json, '$.chart.equity_points')) AS point"""
    )


def _create_relationship_model(connection: duckdb.DuckDBPyConnection) -> None:
    """Materialize source rows with foreign keys supported by publication identity."""

    connection.execute("CREATE SCHEMA research_model")
    connection.execute(
        """CREATE TABLE research_model.backtest_runs (
            backtest_run_id VARCHAR PRIMARY KEY,
            backtest_key VARCHAR UNIQUE,
            manifest_file VARCHAR
        )"""
    )
    connection.execute(
        """INSERT INTO research_model.backtest_runs
        SELECT r.backtest_run_id, m.backtest_key, m.manifest_file
        FROM research_data.backtest_runs r
        LEFT JOIN research_data.backtest_manifests m USING (backtest_run_id)
        WHERE r.backtest_run_id IS NOT NULL AND r.backtest_run_id <> ''"""
    )
    connection.execute(
        "CREATE TABLE research_model.experiment_runs (experiment_run_id VARCHAR PRIMARY KEY)"
    )
    connection.execute(
        """INSERT INTO research_model.experiment_runs
        SELECT experiment_run_id FROM research_data.experiment_runs
        WHERE experiment_run_id IS NOT NULL AND experiment_run_id <> ''"""
    )

    def materialize(name: str, *, required: tuple[str, ...], constraints: tuple[str, ...]) -> None:
        columns = connection.execute(f"DESCRIBE SELECT * FROM research_data.{name}").fetchall()
        names = [str(row[0]) for row in columns]
        if "source_row" in names or any(key not in names for key in required):
            raise InvalidBacktestInput(f"{name} has an incompatible source schema")
        definitions = ",\n                ".join(
            f"{_identifier(str(column))} {column_type}"
            + (" NOT NULL" if column in required else "")
            for column, column_type, *_ in columns
        )
        constraint_sql = ",\n                ".join(constraints)
        connection.execute(
            f"""CREATE TABLE research_model.{name} (
                source_row BIGINT PRIMARY KEY,
                {definitions},
                {constraint_sql}
            )"""
        )
        connection.execute(
            f"INSERT INTO research_model.{name} SELECT row_number() OVER (), * "
            f"FROM research_data.{name}"
        )

    materialize(
        "backtest_decisions",
        required=("backtest_run_id", "decision_time"),
        constraints=(
            "UNIQUE (backtest_run_id, decision_time)",
            "FOREIGN KEY (backtest_run_id) REFERENCES research_model.backtest_runs(backtest_run_id)",
        ),
    )
    materialize(
        "backtest_fills",
        required=("backtest_run_id", "decision_time"),
        constraints=(
            (
                "FOREIGN KEY (backtest_run_id, decision_time) "
                "REFERENCES research_model.backtest_decisions(backtest_run_id, decision_time)"
            ),
        ),
    )
    materialize(
        "backtest_equity",
        required=("backtest_run_id", "window_start"),
        constraints=(
            "FOREIGN KEY (backtest_run_id) REFERENCES research_model.backtest_runs(backtest_run_id)",
        ),
    )

    connection.execute(
        """CREATE TABLE research_model.candidates (
            experiment_run_id VARCHAR NOT NULL,
            candidate_id VARCHAR NOT NULL,
            fast_period INTEGER,
            slow_period INTEGER,
            PRIMARY KEY (experiment_run_id, candidate_id),
            FOREIGN KEY (experiment_run_id)
                REFERENCES research_model.experiment_runs(experiment_run_id)
        )"""
    )
    connection.execute(
        """INSERT INTO research_model.candidates
        SELECT DISTINCT experiment_run_id, candidate_id, fast_period, slow_period
        FROM research_data.candidate_results"""
    )
    materialize(
        "baseline_results",
        required=("experiment_run_id", "range_name"),
        constraints=(
            "UNIQUE (experiment_run_id, range_name)",
            (
                "FOREIGN KEY (experiment_run_id) "
                "REFERENCES research_model.experiment_runs(experiment_run_id)"
            ),
        ),
    )
    materialize(
        "candidate_results",
        required=("experiment_run_id", "candidate_id", "range_name", "backtest_key"),
        constraints=(
            "UNIQUE (experiment_run_id, candidate_id, range_name)",
            (
                "FOREIGN KEY (experiment_run_id, candidate_id) "
                "REFERENCES research_model.candidates(experiment_run_id, candidate_id)"
            ),
            (
                "FOREIGN KEY (experiment_run_id, range_name) "
                "REFERENCES research_model.baseline_results(experiment_run_id, range_name)"
            ),
            ("FOREIGN KEY (backtest_key) REFERENCES research_model.backtest_runs(backtest_key)"),
        ),
    )
    connection.execute(
        """CREATE TABLE research_model.experiment_evaluations (
            experiment_run_id VARCHAR PRIMARY KEY,
            FOREIGN KEY (experiment_run_id)
                REFERENCES research_model.experiment_runs(experiment_run_id)
        )"""
    )
    connection.execute(
        """INSERT INTO research_model.experiment_evaluations
        SELECT DISTINCT experiment_run_id FROM (
            SELECT experiment_run_id FROM research_data.baseline_fills
            UNION ALL
            SELECT experiment_run_id FROM research_data.baseline_equity
        )"""
    )
    for name in ("baseline_fills", "baseline_equity"):
        materialize(
            name,
            required=("experiment_run_id",),
            constraints=(
                (
                    "FOREIGN KEY (experiment_run_id) "
                    "REFERENCES research_model.experiment_evaluations(experiment_run_id)"
                ),
            ),
        )

    connection.execute(
        "CREATE TABLE research_model.gap_aware_selections (selection_file VARCHAR PRIMARY KEY)"
    )
    connection.execute(
        """INSERT INTO research_model.gap_aware_selections
        SELECT DISTINCT selection_file FROM (
            SELECT selection_file FROM research_data.gap_aware_trade_samples
            UNION ALL
            SELECT selection_file FROM research_data.gap_aware_equity_samples
        )"""
    )
    connection.execute(
        """CREATE TABLE research_model.gap_aware_candidates (
            selection_file VARCHAR NOT NULL,
            candidate_id VARCHAR NOT NULL,
            PRIMARY KEY (selection_file, candidate_id),
            FOREIGN KEY (selection_file)
                REFERENCES research_model.gap_aware_selections(selection_file)
        )"""
    )
    connection.execute(
        """INSERT INTO research_model.gap_aware_candidates
        SELECT DISTINCT selection_file, candidate_id FROM (
            SELECT selection_file, candidate_id FROM research_data.gap_aware_trade_samples
            UNION ALL
            SELECT selection_file, candidate_id FROM research_data.gap_aware_equity_samples
        )"""
    )
    connection.execute(
        """CREATE TABLE research_model.gap_aware_segments (
            selection_file VARCHAR NOT NULL,
            candidate_id VARCHAR NOT NULL,
            period VARCHAR NOT NULL,
            segment_number INTEGER NOT NULL,
            PRIMARY KEY (selection_file, candidate_id, period, segment_number),
            FOREIGN KEY (selection_file, candidate_id)
                REFERENCES research_model.gap_aware_candidates(selection_file, candidate_id)
        )"""
    )
    connection.execute(
        """INSERT INTO research_model.gap_aware_segments
        SELECT DISTINCT selection_file, candidate_id, period, segment_number FROM (
            SELECT selection_file, candidate_id, period, segment_number
            FROM research_data.gap_aware_trade_samples
            UNION ALL
            SELECT selection_file, candidate_id, period, segment_number
            FROM research_data.gap_aware_equity_samples
        )"""
    )
    for name in ("gap_aware_trade_samples", "gap_aware_equity_samples"):
        materialize(
            name,
            required=("selection_file", "candidate_id", "period", "segment_number"),
            constraints=(
                (
                    "FOREIGN KEY (selection_file, candidate_id, period, segment_number) "
                    "REFERENCES research_model.gap_aware_segments"
                    "(selection_file, candidate_id, period, segment_number)"
                ),
            ),
        )
    for name in ("candles", "gap_aware_reports"):
        connection.execute(
            f"CREATE VIEW research_model.{name} AS SELECT * FROM research_data.{name}"
        )


def create_workspace(
    settings: ResearchWorkspaceSettings, *, replace: bool = False
) -> dict[str, str]:
    """Create remote source views and a local snapshot with verified relationships."""

    path = settings.workspace_path.resolve()
    if path.exists() and not replace:
        raise InvalidBacktestInput(
            f"Workspace already exists: {path}. Use --replace to rebuild it."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest_uri, manifest, manifest_sha256 = _read_candle_source(settings)
    candles_uri = str(manifest["candle_output_uri"])
    staging_path = path.with_name(f".{path.name}.building")
    if staging_path.exists():
        staging_path.unlink()
    connection = duckdb.connect(str(staging_path))
    try:
        if not is_local_uri(candles_uri):
            _configure_s3(connection, settings.storage)
        _create_views(connection, candles_uri=candles_uri, settings=settings)
        _create_relationship_model(connection)
        connection.execute(
            """CREATE TABLE research_data.workspace_info (
                created_at TIMESTAMPTZ,
                candle_manifest_uri VARCHAR,
                candle_manifest_sha256 VARCHAR,
                candle_output_uri VARCHAR,
                notes VARCHAR
            )"""
        )
        connection.execute(
            "INSERT INTO research_data.workspace_info VALUES (now(), ?, ?, ?, ?)",
            [
                manifest_uri,
                manifest_sha256,
                candles_uri,
                "Source views query immutable publications directly. research_model is a relational snapshot with validated publication, decision, candidate, comparison, and sample relationships. Gap-aware samples are bounded publications.",
            ],
        )
    finally:
        connection.close()
    staging_path.replace(path)
    dbeaver_init_path = path.with_suffix(".dbeaver-init.sql")
    starter_queries_path = path.with_suffix(".starters.sql")
    starter_queries_path.write_text(_starter_queries(), encoding="utf-8")
    if not is_local_uri(candles_uri):
        dbeaver_init_path.write_text(_dbeaver_init_sql(settings.storage), encoding="utf-8")
    return {
        "workspace_path": str(path),
        "candle_manifest_uri": manifest_uri,
        "candle_manifest_sha256": manifest_sha256,
        "candle_output_uri": candles_uri,
        "dbeaver_init_sql": str(dbeaver_init_path) if dbeaver_init_path.exists() else "",
        "starter_queries_sql": str(starter_queries_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a local DuckDB research workspace over read-only historical data."
    )
    parser.add_argument(
        "--output",
        default="analytics/workspaces/crypto_research.duckdb",
        help="Local DuckDB file containing source views and a result snapshot.",
    )
    parser.add_argument(
        "--replace", action="store_true", help="Replace an existing local workspace."
    )
    parser.add_argument(
        "--local-development",
        action="store_true",
        help="Use the local MinIO defaults when dedicated workspace settings are absent.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = ResearchWorkspaceSettings.from_env(Path(args.output))
    if args.local_development:
        settings = ResearchWorkspaceSettings(
            **{
                **settings.__dict__,
                "storage": StorageSettings(
                    endpoint_url=settings.storage.endpoint_url or "http://127.0.0.1:9000",
                    access_key=settings.storage.access_key or "minioadmin",
                    secret_key=settings.storage.secret_key or "minioadmin",
                    region=settings.storage.region,
                ),
            }
        )
    result = create_workspace(settings, replace=args.replace)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
