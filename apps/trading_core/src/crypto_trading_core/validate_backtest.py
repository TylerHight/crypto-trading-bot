from __future__ import annotations

import argparse
import hashlib
import json
import sys
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from crypto_trading_domain.backtest import InvalidBacktest, run_backtest

from crypto_trading_core.backtest import BacktestSettings, load_candles
from crypto_trading_core.contracts import (
    BACKTEST_ENGINE_VERSION,
    RESULT_SCHEMA_VERSION,
    STRATEGY_VERSION,
    BacktestSpec,
    InvalidBacktestInput,
    canonical_json_bytes,
    is_local_uri,
    load_candle_snapshot,
    normalize_uri,
    parse_utc_minute,
)
from crypto_trading_core.schemas import result_tables
from crypto_trading_core.storage import ObjectStorage

REQUIRED_ARTIFACTS = {
    "decisions.parquet",
    "fills.parquet",
    "equity_curve.parquet",
    "summary.json",
}


def _decimal(value: Any, field: str) -> Decimal:
    if not isinstance(value, str):
        raise InvalidBacktestInput(f"manifest {field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InvalidBacktestInput(f"manifest {field} is not a decimal") from error
    if not parsed.is_finite():
        raise InvalidBacktestInput(f"manifest {field} must be finite")
    return parsed


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidBacktestInput(f"manifest {field} must be an integer")
    return value


def _spec(manifest: dict[str, Any]) -> BacktestSpec:
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise InvalidBacktestInput("backtest manifest identity is missing")
    required_strings = (
        "candle_snapshot_key",
        "candle_manifest_sha256",
        "exchange",
        "symbol",
        "start",
        "end",
        "strategy_version",
        "backtest_engine_version",
        "result_schema_version",
    )
    if any(not isinstance(identity.get(field), str) for field in required_strings):
        raise InvalidBacktestInput("backtest identity contains an invalid string field")
    return BacktestSpec(
        candle_snapshot_key=identity["candle_snapshot_key"],
        candle_manifest_sha256=identity["candle_manifest_sha256"],
        exchange=identity["exchange"],
        symbol=identity["symbol"],
        start=parse_utc_minute(identity["start"], "identity.start"),
        end=parse_utc_minute(identity["end"], "identity.end"),
        starting_cash=_decimal(identity.get("starting_cash"), "starting_cash"),
        fast_period=_integer(identity.get("fast_period"), "fast_period"),
        slow_period=_integer(identity.get("slow_period"), "slow_period"),
        fee_bps=_decimal(identity.get("fee_bps"), "fee_bps"),
        slippage_bps=_decimal(identity.get("slippage_bps"), "slippage_bps"),
        strategy_version=identity["strategy_version"],
        backtest_engine_version=identity["backtest_engine_version"],
        result_schema_version=identity["result_schema_version"],
    )


def _validate_artifact_metadata(
    name: str,
    metadata: Any,
    body: bytes,
) -> None:
    if not isinstance(metadata, dict):
        raise InvalidBacktestInput(f"artifact metadata is invalid for {name}")
    if metadata.get("bytes") != len(body):
        raise InvalidBacktestInput(f"artifact byte count changed for {name}")
    if metadata.get("sha256") != hashlib.sha256(body).hexdigest():
        raise InvalidBacktestInput(f"artifact digest changed for {name}")
    if not isinstance(metadata.get("uri"), str):
        raise InvalidBacktestInput(f"artifact URI is invalid for {name}")


def validate_manifest(
    manifest_uri: str,
    *,
    settings: BacktestSettings,
    local_development: bool,
    store: ObjectStorage | None = None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.storage)
    if is_local_uri(manifest_uri):
        if not local_development:
            raise InvalidBacktestInput(
                "local backtest manifests require explicit local-development mode"
            )
    else:
        allowed_output = normalize_uri(settings.output_prefix)
        if not normalize_uri(manifest_uri).startswith(allowed_output + "/manifests/"):
            raise InvalidBacktestInput("backtest manifest is outside the allowed prefix")
    try:
        manifest = json.loads(storage.read_bytes(manifest_uri))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("backtest manifest is not valid UTF-8 JSON") from error
    if not isinstance(manifest, dict):
        raise InvalidBacktestInput("backtest manifest must be a JSON object")
    if manifest.get("status") != "published" or manifest.get("execution_mode") != "simulation":
        raise InvalidBacktestInput("backtest manifest is not a simulated publication")
    if (
        manifest.get("strategy_version") != STRATEGY_VERSION
        or manifest.get("backtest_engine_version") != BACKTEST_ENGINE_VERSION
        or manifest.get("result_schema_version") != RESULT_SCHEMA_VERSION
    ):
        raise InvalidBacktestInput("unsupported backtest result version")

    spec = _spec(manifest)
    if spec.key != manifest.get("backtest_key"):
        raise InvalidBacktestInput("backtest manifest identity does not match its key")
    if json.loads(canonical_json_bytes(spec.identity())) != manifest.get("identity"):
        raise InvalidBacktestInput("backtest manifest identity is not canonical")
    if normalize_uri(str(manifest.get("manifest_uri", ""))) != normalize_uri(manifest_uri):
        raise InvalidBacktestInput("backtest manifest URI does not identify itself")

    candle_manifest_uri = manifest.get("candle_manifest_uri")
    if not isinstance(candle_manifest_uri, str):
        raise InvalidBacktestInput("source candle manifest URI is missing")
    candle_manifest_body = storage.read_bytes(candle_manifest_uri)
    source = load_candle_snapshot(
        candle_manifest_body,
        manifest_uri=candle_manifest_uri,
        expected_sha256=spec.candle_manifest_sha256,
        allowed_manifest_prefix=settings.source_manifest_prefix,
        local_development=local_development,
    )
    if source.snapshot_key != spec.candle_snapshot_key:
        raise InvalidBacktestInput("source candle snapshot key changed")
    if manifest.get("candle_output_uri") != source.output_uri:
        raise InvalidBacktestInput("source candle output URI changed")
    if not is_local_uri(source.output_uri) and not normalize_uri(
        source.output_uri
    ).startswith(normalize_uri(settings.source_output_prefix) + "/"):
        raise InvalidBacktestInput("candle output is outside the allowed prefix")

    run_output_uri = manifest.get("run_output_uri")
    if not isinstance(run_output_uri, str):
        raise InvalidBacktestInput("backtest run output URI is missing")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != REQUIRED_ARTIFACTS:
        raise InvalidBacktestInput("backtest artifact inventory is incomplete")
    bodies: dict[str, bytes] = {}
    for name in sorted(REQUIRED_ARTIFACTS):
        metadata = artifacts[name]
        if not isinstance(metadata, dict) or not isinstance(metadata.get("uri"), str):
            raise InvalidBacktestInput(f"artifact URI is invalid for {name}")
        if not normalize_uri(metadata["uri"]).startswith(normalize_uri(run_output_uri) + "/"):
            raise InvalidBacktestInput(f"artifact is outside the immutable run for {name}")
        body = storage.read_bytes(metadata["uri"])
        _validate_artifact_metadata(name, metadata, body)
        bodies[name] = body

    candles = load_candles(
        source,
        spec,
        storage_settings=settings.storage,
        maximum_input_candles=settings.maximum_input_candles,
    )
    try:
        expected_result = run_backtest(
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
    expected_tables = result_tables(expected_result)
    for name, expected_table in expected_tables.items():
        try:
            actual_table = pq.read_table(BytesIO(bodies[name]))
        except (pa.ArrowInvalid, OSError) as error:
            raise InvalidBacktestInput(f"artifact is not valid Parquet: {name}") from error
        if not actual_table.schema.equals(expected_table.schema) or not actual_table.equals(
            expected_table
        ):
            raise InvalidBacktestInput(f"artifact does not reproduce exactly: {name}")
        if artifacts[name].get("rows") != actual_table.num_rows:
            raise InvalidBacktestInput(f"artifact row count changed for {name}")

    expected_summary = {
        "backtest_key": spec.key,
        "execution_mode": "simulation",
        "summary": {
            key: format(value, "f") if isinstance(value, Decimal) else value
            for key, value in vars(expected_result.summary).items()
        },
    }
    try:
        actual_summary = json.loads(bodies["summary.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("summary artifact is invalid JSON") from error
    if actual_summary != expected_summary or manifest.get("summary") != expected_summary["summary"]:
        raise InvalidBacktestInput("backtest summary does not reproduce exactly")
    if artifacts["summary.json"].get("rows") != 1:
        raise InvalidBacktestInput("summary artifact row count must be one")

    expected_requested_range = json.loads(
        canonical_json_bytes({"end": spec.end, "start": spec.start})
    )
    expected_effective_range = json.loads(
        canonical_json_bytes(
            {
                "evaluation_end": candles[-1].window_end,
                "evaluation_start": spec.start,
                "warmup_start": candles[0].window_start,
            }
        )
    )
    if (
        manifest.get("requested_range") != expected_requested_range
        or manifest.get("effective_range") != expected_effective_range
    ):
        raise InvalidBacktestInput("backtest requested or effective range changed")

    return {
        "backtest_key": spec.key,
        "decisions": len(expected_result.decisions),
        "execution_mode": "simulation",
        "fills": len(expected_result.fills),
        "status": "valid",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a published backtest result.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        report = validate_manifest(
            arguments.manifest,
            settings=BacktestSettings.from_env(),
            local_development=arguments.local_development,
        )
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Backtest validation failed: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Backtest result valid")
    print("BACKTEST_VALIDATION_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
