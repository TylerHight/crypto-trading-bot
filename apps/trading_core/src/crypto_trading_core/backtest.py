from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from crypto_trading_domain.backtest import Candle, InvalidBacktest, run_backtest

from crypto_trading_core.contracts import (
    BACKTEST_ENGINE_VERSION,
    RESULT_SCHEMA_VERSION,
    STRATEGY_VERSION,
    BacktestSpec,
    InvalidBacktestInput,
    PublishedCandleSnapshot,
    canonical_json_bytes,
    is_local_uri,
    load_candle_snapshot,
    normalize_uri,
    parse_utc_minute,
    validate_distinct_prefixes,
)
from crypto_trading_core.schemas import result_tables
from crypto_trading_core.storage import (
    ObjectStorage,
    StorageSettings,
    child_uri,
    parse_location,
)


@dataclass(frozen=True)
class BacktestSettings:
    storage: StorageSettings
    source_manifest_prefix: str
    source_output_prefix: str
    output_prefix: str
    maximum_input_candles: int

    @classmethod
    def from_env(cls) -> BacktestSettings:
        maximum = int(os.getenv("BACKTEST_MAXIMUM_INPUT_CANDLES", "100000"))
        if maximum <= 0:
            raise InvalidBacktestInput("BACKTEST_MAXIMUM_INPUT_CANDLES must be positive")
        return cls(
            storage=StorageSettings(
                endpoint_url=os.getenv("BACKTEST_S3_ENDPOINT"),
                access_key=os.getenv("BACKTEST_S3_ACCESS_KEY"),
                secret_key=os.getenv("BACKTEST_S3_SECRET_KEY"),
                region=os.getenv("BACKTEST_S3_REGION", "us-east-1"),
            ),
            source_manifest_prefix=os.getenv(
                "BACKTEST_CANDLE_MANIFEST_PREFIX",
                "s3a://crypto-data/analytics/market_candles/v1/manifests",
            ),
            source_output_prefix=os.getenv(
                "BACKTEST_CANDLE_OUTPUT_PREFIX",
                "s3a://crypto-data/analytics/market_candles/v1/runs",
            ),
            output_prefix=os.getenv(
                "BACKTEST_OUTPUT_PREFIX",
                "s3a://crypto-data/analytics/backtests/v1",
            ),
            maximum_input_candles=maximum,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic long-only SMA backtest from pinned candles."
    )
    parser.add_argument("--candle-manifest", required=True)
    parser.add_argument("--candle-manifest-sha256", required=True)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--starting-cash", required=True)
    parser.add_argument("--fast-period", type=int, required=True)
    parser.add_argument("--slow-period", type=int, required=True)
    parser.add_argument("--fee-bps", required=True)
    parser.add_argument("--slippage-bps", required=True)
    parser.add_argument("--output")
    parser.add_argument("--source-manifest-prefix")
    parser.add_argument("--local-development", action="store_true")
    return parser


def _decimal(value: str, field: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InvalidBacktestInput(f"{field} must be an exact decimal") from error
    if not parsed.is_finite():
        raise InvalidBacktestInput(f"{field} must be finite")
    return parsed


def _configure_duckdb(
    connection: duckdb.DuckDBPyConnection,
    source_uri: str,
    settings: StorageSettings,
) -> str:
    connection.execute("SET TimeZone = 'UTC'")
    if not source_uri.startswith(("s3a://", "s3://")):
        path = Path(parse_location(source_uri).key)
        return str(path / "**" / "*.parquet").replace("\\", "/")
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    if settings.endpoint_url:
        endpoint = settings.endpoint_url
        secure = endpoint.startswith("https://")
        endpoint = endpoint.removeprefix("http://").removeprefix("https://")
        connection.execute("SET s3_endpoint = ?", [endpoint])
        connection.execute("SET s3_use_ssl = ?", [secure])
        connection.execute("SET s3_url_style = 'path'")
    if settings.access_key is not None:
        connection.execute("SET s3_access_key_id = ?", [settings.access_key])
    if settings.secret_key is not None:
        connection.execute("SET s3_secret_access_key = ?", [settings.secret_key])
    connection.execute("SET s3_region = ?", [settings.region])
    translated = "s3://" + source_uri.split("://", 1)[1]
    return translated.rstrip("/") + "/**/*.parquet"


def load_candle_range(
    source: PublishedCandleSnapshot,
    *,
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    warmup_candles: int,
    storage_settings: StorageSettings,
    maximum_input_candles: int,
) -> tuple[Candle, ...]:
    warmup_start = start - timedelta(minutes=warmup_candles)
    expected = warmup_candles + int((end - start) / timedelta(minutes=1))
    if expected > maximum_input_candles:
        raise InvalidBacktestInput("requested range exceeds BACKTEST_MAXIMUM_INPUT_CANDLES")
    if source.candle_count < expected:
        raise InvalidBacktestInput("candle snapshot cannot cover the requested range")

    connection = duckdb.connect()
    try:
        parquet_uri = _configure_duckdb(connection, source.output_uri, storage_settings)
        relation = (
            f"read_parquet({json.dumps(parquet_uri)}, hive_partitioning=true, "
            "hive_types={'interval': VARCHAR, 'event_date': DATE, 'event_hour': VARCHAR})"
        )
        rows = connection.execute(
            f"""
            SELECT exchange, symbol, window_start, window_end, open, high, low, close,
                   base_volume, quote_volume, vwap, trade_count,
                   source_curated_snapshot_key, candle_schema_version
            FROM {relation}
            WHERE interval = '1m'
              AND exchange = ?
              AND symbol = ?
              AND event_date BETWEEN ? AND ?
              AND window_start >= ?
              AND window_start < ?
            ORDER BY window_start
            """,
            [
                exchange,
                symbol,
                warmup_start.date(),
                (end - timedelta(minutes=1)).date(),
                warmup_start.replace(tzinfo=None),
                end.replace(tzinfo=None),
            ],
        ).fetchall()
    except duckdb.Error as error:
        raise InvalidBacktestInput(f"candle query failed: {error}") from error
    finally:
        connection.close()

    candles: list[Candle] = []
    for row in rows:
        start = row[2]
        end = row[3]
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        candle = Candle(
            exchange=row[0],
            symbol=row[1],
            window_start=start.astimezone(UTC),
            window_end=end.astimezone(UTC),
            open=row[4],
            high=row[5],
            low=row[6],
            close=row[7],
        )
        base_volume, quote_volume, vwap, trade_count = row[8:12]
        if (
            any(
                not value.is_finite() or value <= 0
                for value in (base_volume, quote_volume, vwap)
            )
            or vwap < candle.low
            or vwap > candle.high
            or isinstance(trade_count, bool)
            or not isinstance(trade_count, int)
            or trade_count < 1
            or row[12] != source.manifest.get("source_curated_snapshot_key")
            or row[13] != "v1"
        ):
            raise InvalidBacktestInput("selected candle violates the v1 data contract")
        candles.append(candle)
    return tuple(candles)


def load_candles(
    source: PublishedCandleSnapshot,
    spec: BacktestSpec,
    *,
    storage_settings: StorageSettings,
    maximum_input_candles: int,
) -> tuple[Candle, ...]:
    return load_candle_range(
        source,
        exchange=spec.exchange,
        symbol=spec.symbol,
        start=spec.start,
        end=spec.end,
        warmup_candles=spec.slow_period - 1,
        storage_settings=storage_settings,
        maximum_input_candles=maximum_input_candles,
    )


class CandleLoader(Protocol):
    def __call__(
        self,
        source: PublishedCandleSnapshot,
        spec: BacktestSpec,
        *,
        storage_settings: StorageSettings,
        maximum_input_candles: int,
    ) -> tuple[Candle, ...]: ...


def _summary_dict(summary: Any) -> dict[str, Any]:
    return asdict(summary)


def _parquet_bytes(table: Any) -> bytes:
    output = BytesIO()
    pq.write_table(table, output, compression="zstd", version="2.6")
    return output.getvalue()


def _existing_manifest(
    store: ObjectStorage,
    manifest_uri: str,
    spec: BacktestSpec,
) -> dict[str, Any] | None:
    body = store.try_read_bytes(manifest_uri)
    if body is None:
        return None
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("existing backtest manifest is invalid JSON") from error
    expected_identity = json.loads(canonical_json_bytes(spec.identity()))
    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "published"
        or manifest.get("backtest_key") != spec.key
        or manifest.get("identity") != expected_identity
        or not isinstance(manifest.get("artifacts"), dict)
    ):
        raise InvalidBacktestInput("existing backtest manifest conflicts with requested identity")
    return manifest


def run_application(
    arguments: argparse.Namespace,
    settings: BacktestSettings,
    *,
    store: ObjectStorage | None = None,
    run_id: str | None = None,
    started_at: datetime | None = None,
    candle_loader: CandleLoader = load_candles,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.storage)
    started = started_at or datetime.now(UTC)
    output = arguments.output or settings.output_prefix
    if (is_local_uri(arguments.candle_manifest) or is_local_uri(output)) and not (
        arguments.local_development
    ):
        raise InvalidBacktestInput("local input or output requires --local-development")
    manifest_body = storage.read_bytes(arguments.candle_manifest)
    source = load_candle_snapshot(
        manifest_body,
        manifest_uri=arguments.candle_manifest,
        expected_sha256=arguments.candle_manifest_sha256,
        allowed_manifest_prefix=(
            arguments.source_manifest_prefix or settings.source_manifest_prefix
        ),
        local_development=arguments.local_development,
    )
    if not is_local_uri(source.output_uri) and not normalize_uri(
        source.output_uri
    ).startswith(normalize_uri(settings.source_output_prefix) + "/"):
        raise InvalidBacktestInput("candle output is outside the allowed prefix")
    if not is_local_uri(output):
        allowed_output = normalize_uri(settings.output_prefix)
        normalized_output = normalize_uri(output)
        if normalized_output != allowed_output and not normalized_output.startswith(
            allowed_output + "/"
        ):
            raise InvalidBacktestInput("backtest output is outside the allowed prefix")
    validate_distinct_prefixes(source.output_uri, output)
    start = parse_utc_minute(arguments.start, "start")
    end = parse_utc_minute(arguments.end, "end")
    if start >= end:
        raise InvalidBacktestInput("start must be earlier than end")
    spec = BacktestSpec(
        candle_snapshot_key=source.snapshot_key,
        candle_manifest_sha256=source.manifest_sha256,
        exchange=arguments.exchange.strip(),
        symbol=arguments.symbol.strip(),
        start=start,
        end=end,
        starting_cash=_decimal(arguments.starting_cash, "starting_cash"),
        fast_period=arguments.fast_period,
        slow_period=arguments.slow_period,
        fee_bps=_decimal(arguments.fee_bps, "fee_bps"),
        slippage_bps=_decimal(arguments.slippage_bps, "slippage_bps"),
    )
    manifest_uri = child_uri(output, "manifests", spec.key, "manifest.json")
    existing = _existing_manifest(storage, manifest_uri, spec)
    if existing is not None:
        return {
            **existing,
            "status": "resolved_existing_backtest",
            "published_status": "published",
            "manifest_uri": manifest_uri,
        }

    candles = candle_loader(
        source,
        spec,
        storage_settings=settings.storage,
        maximum_input_candles=settings.maximum_input_candles,
    )
    try:
        result = run_backtest(
            candles,
            start=spec.start,
            end=spec.end,
            starting_cash=spec.starting_cash,
            fast_period=spec.fast_period,
            slow_period=spec.slow_period,
            fee_bps=spec.fee_bps,
            slippage_bps=spec.slippage_bps,
        )
    except InvalidBacktest as error:
        raise InvalidBacktestInput(str(error)) from error

    actual_run_id = run_id or str(uuid4())
    run_uri = child_uri(output, "runs", actual_run_id)
    artifacts: dict[str, dict[str, Any]] = {}
    tables = result_tables(result)
    for filename, table in tables.items():
        body = _parquet_bytes(table)
        uri = child_uri(run_uri, filename)
        storage.write_bytes_append_only(
            uri, body, content_type="application/vnd.apache.parquet"
        )
        artifacts[filename] = {
            "bytes": len(body),
            "rows": table.num_rows,
            "sha256": hashlib.sha256(body).hexdigest(),
            "uri": uri,
        }

    summary_document = {
        "backtest_key": spec.key,
        "execution_mode": "simulation",
        "summary": _summary_dict(result.summary),
    }
    summary_body = canonical_json_bytes(summary_document)
    summary_uri = child_uri(run_uri, "summary.json")
    storage.write_bytes_append_only(
        summary_uri, summary_body, content_type="application/json"
    )
    artifacts["summary.json"] = {
        "bytes": len(summary_body),
        "rows": 1,
        "sha256": hashlib.sha256(summary_body).hexdigest(),
        "uri": summary_uri,
    }

    for name, metadata in artifacts.items():
        published_body = storage.read_bytes(metadata["uri"])
        if (
            len(published_body) != metadata["bytes"]
            or hashlib.sha256(published_body).hexdigest() != metadata["sha256"]
        ):
            raise InvalidBacktestInput(
                f"published artifact failed read-back validation: {name}"
            )

    manifest: dict[str, Any] = {
        "artifacts": artifacts,
        "backtest_engine_version": BACKTEST_ENGINE_VERSION,
        "backtest_key": spec.key,
        "backtest_run_id": actual_run_id,
        "candle_manifest_sha256": source.manifest_sha256,
        "candle_manifest_uri": source.manifest_uri,
        "candle_output_uri": source.output_uri,
        "candle_snapshot_key": source.snapshot_key,
        "completed_at": datetime.now(UTC),
        "execution_mode": "simulation",
        "identity": spec.identity(),
        "effective_range": {
            "evaluation_end": candles[-1].window_end,
            "evaluation_start": spec.start,
            "warmup_start": candles[0].window_start,
        },
        "manifest_uri": manifest_uri,
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "requested_range": {"end": spec.end, "start": spec.start},
        "run_output_uri": run_uri,
        "started_at": started,
        "status": "published",
        "strategy_version": STRATEGY_VERSION,
        "summary": _summary_dict(result.summary),
    }
    if not storage.try_write_bytes_append_only(
        manifest_uri, canonical_json_bytes(manifest), content_type="application/json"
    ):
        existing = _existing_manifest(storage, manifest_uri, spec)
        if existing is None:
            raise InvalidBacktestInput("backtest manifest publication race was unresolved")
        return {
            **existing,
            "status": "resolved_existing_backtest",
            "published_status": "published",
            "manifest_uri": manifest_uri,
        }
    return json.loads(canonical_json_bytes(manifest))


def print_summary(report: dict[str, Any]) -> None:
    print(f"Candle strategy backtest: {str(report['status']).upper()}")
    print(f"  execution_mode={report['execution_mode']}")
    print(f"  backtest_key={report['backtest_key']}")
    if isinstance(report.get("summary"), dict):
        print(f"  ending_equity={report['summary']['ending_equity']}")
        print(f"  percentage_return={report['summary']['percentage_return']}")
    print("BACKTEST_REPORT_JSON=" + canonical_json_bytes(report).decode("ascii"))


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        report = run_application(arguments, BacktestSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Backtest rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print_summary(report)


if __name__ == "__main__":
    main()
