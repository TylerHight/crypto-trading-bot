from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from crypto_trading_core.backtest import load_candle_range
from crypto_trading_core.contracts import (
    SHA256_PATTERN,
    InvalidBacktestInput,
    canonical_json_bytes,
    is_local_uri,
    normalize_uri,
)
from crypto_trading_core.experiment_contracts import (
    SelectionPolicy,
    evaluation_key,
    load_experiment_spec,
    selection_key,
)
from crypto_trading_core.experiment_schemas import (
    BASELINE_RESULT_SCHEMA,
    CANDIDATE_RESULT_SCHEMA,
    baseline_result_table,
)
from crypto_trading_core.experiments import (
    ExperimentSettings,
    _artifact_body,
    _baseline_row,
    _source,
    load_selection_manifest,
    select_candidate,
)
from crypto_trading_core.schemas import result_tables
from crypto_trading_core.storage import ObjectStorage
from crypto_trading_core.validate_backtest import validate_manifest as validate_backtest


def _manifest(
    store: ObjectStorage, uri: str, expected_sha256: str, field: str
) -> tuple[dict[str, Any], str]:
    body = store.read_bytes(uri)
    digest = hashlib.sha256(body).hexdigest()
    if not SHA256_PATTERN.fullmatch(expected_sha256) or digest != expected_sha256:
        raise InvalidBacktestInput(f"{field} SHA-256 digest does not match")
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput(f"{field} is invalid UTF-8 JSON") from error
    if not isinstance(manifest, dict) or manifest.get("status") != "published":
        raise InvalidBacktestInput(f"{field} is not a published manifest")
    return manifest, digest


def _table(body: bytes, expected_schema: pa.Schema, field: str) -> pa.Table:
    try:
        table = pq.read_table(BytesIO(body))
    except (pa.ArrowInvalid, OSError) as error:
        raise InvalidBacktestInput(f"{field} is not valid Parquet") from error
    if not table.schema.equals(expected_schema):
        raise InvalidBacktestInput(f"{field} schema changed")
    return table


def _selection_location(
    manifest_uri: str, settings: ExperimentSettings, local_development: bool
) -> None:
    if is_local_uri(manifest_uri):
        if not local_development:
            raise InvalidBacktestInput(
                "local experiment validation requires local-development mode"
            )
    elif not normalize_uri(manifest_uri).startswith(normalize_uri(settings.output_prefix) + "/"):
        raise InvalidBacktestInput("experiment manifest is outside the allowed prefix")


def _backtest_settings_for(manifest_uri: str, settings: ExperimentSettings):
    prefix = normalize_uri(manifest_uri).split("/manifests/", 1)[0]
    return replace(settings.backtest, output_prefix=prefix)


def validate_selection(
    manifest_uri: str,
    expected_sha256: str,
    *,
    settings: ExperimentSettings,
    local_development: bool,
    store: ObjectStorage | None = None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    _selection_location(manifest_uri, settings, local_development)
    manifest, digest = load_selection_manifest(storage, manifest_uri, expected_sha256)
    if manifest.get("manifest_uri") != manifest_uri:
        raise InvalidBacktestInput("selection manifest URI does not identify itself")
    artifacts = manifest.get("artifacts")
    expected_names = {
        "baseline_results.parquet",
        "candidate_results.parquet",
        "selection_summary.json",
    }
    if not isinstance(artifacts, dict) or set(artifacts) != expected_names:
        raise InvalidBacktestInput("selection artifact inventory is incomplete")
    candidate_body = _artifact_body(
        storage, artifacts["candidate_results.parquet"], "candidate results"
    )
    baseline_body = _artifact_body(
        storage, artifacts["baseline_results.parquet"], "baseline results"
    )
    summary_body = _artifact_body(storage, artifacts["selection_summary.json"], "selection summary")
    candidate_table = _table(candidate_body, CANDIDATE_RESULT_SCHEMA, "candidate results")
    baseline_table = _table(baseline_body, BASELINE_RESULT_SCHEMA, "baseline results")
    if (
        artifacts["candidate_results.parquet"].get("rows") != candidate_table.num_rows
        or artifacts["baseline_results.parquet"].get("rows") != baseline_table.num_rows
    ):
        raise InvalidBacktestInput("selection artifact row count changed")
    if artifacts["selection_summary.json"].get("rows") != 1:
        raise InvalidBacktestInput("selection summary row count must be one")
    try:
        stored_summary = json.loads(summary_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("selection summary is invalid JSON") from error

    policy_value = manifest.get("selection_policy")
    if not isinstance(policy_value, dict):
        raise InvalidBacktestInput("selection policy is missing")
    policy = SelectionPolicy(
        minimum_train_fills=int(policy_value["minimum_train_fills"]),
        maximum_train_drawdown=_decimal(policy_value["maximum_train_drawdown"]),
        maximum_validation_drawdown=_decimal(policy_value["maximum_validation_drawdown"]),
    )
    candidate_rows = candidate_table.to_pylist()
    if any(row["range_name"] not in {"train", "validation"} for row in candidate_rows):
        raise InvalidBacktestInput("selection artifacts contain test-derived candidate rows")
    selected, evidence = select_candidate(candidate_rows, policy)
    expected_summary = json.loads(
        canonical_json_bytes(
            {
                "candidate_evidence": evidence,
                "selected_candidate_id": selected,
                "selection_status": (
                    "selected" if selected is not None else "no_candidate_selected"
                ),
                "test_data_accessed": False,
            }
        )
    )
    if stored_summary != expected_summary or manifest.get("summary") != expected_summary:
        raise InvalidBacktestInput("selection ranking or evidence does not reproduce")
    selected_value = manifest.get("selected_candidate")
    if (selected_value is None) != (selected is None) or (
        isinstance(selected_value, dict) and selected_value.get("candidate_id") != selected
    ):
        raise InvalidBacktestInput("sealed selected candidate does not match ranking")

    for row in candidate_rows:
        uri = row["backtest_manifest_uri"]
        body = storage.read_bytes(uri)
        if hashlib.sha256(body).hexdigest() != row["backtest_manifest_sha256"]:
            raise InvalidBacktestInput("underlying backtest manifest digest changed")
        validate_backtest(
            uri,
            settings=_backtest_settings_for(uri, settings),
            local_development=local_development,
            store=storage,
        )

    spec_uri = manifest.get("experiment_spec_uri")
    spec_digest = manifest.get("experiment_spec_raw_sha256")
    if not isinstance(spec_uri, str) or not isinstance(spec_digest, str):
        raise InvalidBacktestInput("selection does not pin its experiment specification")
    spec = load_experiment_spec(
        storage.read_bytes(spec_uri),
        spec_uri=spec_uri,
        expected_sha256=spec_digest,
        allowed_spec_prefix=settings.spec_prefix,
        local_development=local_development,
        maximum_candidates=settings.maximum_candidates,
        maximum_candidate_candle_evaluations=(settings.maximum_candidate_candle_evaluations),
    )
    source = _source(spec, settings, storage, local_development=local_development)
    if selection_key(spec, source) != manifest.get("selection_key"):
        raise InvalidBacktestInput("selection key does not match pinned inputs")
    expected_baselines = []
    for name, interval in (("train", spec.train), ("validation", spec.validation)):
        row, _ = _baseline_row(
            spec,
            source,
            name,
            interval,
            settings=settings,
            range_loader=load_candle_range,
        )
        expected_baselines.append(row)
    expected_baselines.sort(key=lambda row: row["range_name"])
    if not baseline_table.equals(baseline_result_table(expected_baselines)):
        raise InvalidBacktestInput("selection baselines do not reproduce")
    return {
        "candidates": len(spec.candidates),
        "selection_key": manifest["selection_key"],
        "selection_manifest_sha256": digest,
        "selection_status": manifest["selection_status"],
        "status": "valid",
        "test_data_accessed": False,
    }


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, str):
        raise InvalidBacktestInput("selection decimal is not a string")
    return Decimal(value)


def validate_evaluation(
    manifest_uri: str,
    expected_sha256: str,
    *,
    settings: ExperimentSettings,
    local_development: bool,
    store: ObjectStorage | None = None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    _selection_location(manifest_uri, settings, local_development)
    manifest, digest = _manifest(storage, manifest_uri, expected_sha256, "evaluation manifest")
    selection_uri = manifest.get("selection_manifest_uri")
    selection_digest = manifest.get("selection_manifest_sha256")
    if not isinstance(selection_uri, str) or not isinstance(selection_digest, str):
        raise InvalidBacktestInput("evaluation does not pin its selection")
    validate_selection(
        selection_uri,
        selection_digest,
        settings=settings,
        local_development=local_development,
        store=storage,
    )
    selection, _ = load_selection_manifest(storage, selection_uri, selection_digest)
    expected_key = evaluation_key(selection["selection_key"], selection_digest)
    if manifest.get("evaluation_key") != expected_key:
        raise InvalidBacktestInput("evaluation key does not match its sealed selection")

    strategy = manifest.get("strategy_backtest")
    if not isinstance(strategy, dict):
        raise InvalidBacktestInput("evaluation strategy backtest is missing")
    strategy_uri = strategy.get("manifest_uri")
    if not isinstance(strategy_uri, str):
        raise InvalidBacktestInput("evaluation strategy manifest URI is invalid")
    strategy_body = storage.read_bytes(strategy_uri)
    if hashlib.sha256(strategy_body).hexdigest() != strategy.get("manifest_sha256"):
        raise InvalidBacktestInput("evaluation strategy manifest digest changed")
    validate_backtest(
        strategy_uri,
        settings=_backtest_settings_for(strategy_uri, settings),
        local_development=local_development,
        store=storage,
    )

    artifacts = manifest.get("artifacts")
    expected_names = {
        "baseline_equity_curve.parquet",
        "baseline_fills.parquet",
        "comparison.json",
    }
    if not isinstance(artifacts, dict) or set(artifacts) != expected_names:
        raise InvalidBacktestInput("evaluation artifact inventory is incomplete")
    bodies = {name: _artifact_body(storage, artifacts[name], name) for name in expected_names}
    if artifacts["comparison.json"].get("rows") != 1:
        raise InvalidBacktestInput("evaluation comparison row count must be one")

    spec = load_experiment_spec(
        storage.read_bytes(selection["experiment_spec_uri"]),
        spec_uri=selection["experiment_spec_uri"],
        expected_sha256=selection["experiment_spec_raw_sha256"],
        allowed_spec_prefix=settings.spec_prefix,
        local_development=local_development,
        maximum_candidates=settings.maximum_candidates,
        maximum_candidate_candle_evaluations=(settings.maximum_candidate_candle_evaluations),
    )
    source = _source(spec, settings, storage, local_development=local_development)
    baseline_row, baseline_result = _baseline_row(
        spec,
        source,
        "test",
        spec.test,
        settings=settings,
        range_loader=load_candle_range,
    )
    expected_tables = result_tables(baseline_result)
    actual_fills = pq.read_table(BytesIO(bodies["baseline_fills.parquet"]))
    actual_equity = pq.read_table(BytesIO(bodies["baseline_equity_curve.parquet"]))
    if not actual_fills.equals(expected_tables["fills.parquet"]) or not actual_equity.equals(
        expected_tables["equity_curve.parquet"]
    ):
        raise InvalidBacktestInput("evaluation baseline artifacts do not reproduce")

    strategy_manifest = json.loads(strategy_body)
    strategy_summary = strategy_manifest["summary"]
    comparison = {
        "baseline": baseline_row,
        "evaluation_range": "out_of_sample",
        "execution_mode": "simulation",
        "selected_candidate": selection["selected_candidate"],
        "selection_basis": "train_and_validation_only",
        "strategy": strategy_summary,
        "strategy_backtest_key": strategy_manifest["backtest_key"],
        "strategy_backtest_manifest_sha256": hashlib.sha256(strategy_body).hexdigest(),
        "strategy_backtest_manifest_uri": strategy_uri,
        "strategy_excess_absolute_return": (
            _decimal(strategy_summary["absolute_return"]) - baseline_row["absolute_return"]
        ),
        "strategy_excess_percentage_return": (
            _decimal(strategy_summary["percentage_return"]) - baseline_row["percentage_return"]
        ),
        "test_range": spec.test.as_dict(),
    }
    expected_comparison = json.loads(canonical_json_bytes(comparison))
    if json.loads(bodies["comparison.json"]) != expected_comparison:
        raise InvalidBacktestInput("evaluation comparison does not reproduce")
    if manifest.get("summary") != expected_comparison:
        raise InvalidBacktestInput("evaluation manifest summary changed")
    return {
        "evaluation_key": expected_key,
        "evaluation_manifest_sha256": digest,
        "selection_key": selection["selection_key"],
        "status": "valid",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a strategy experiment stage.")
    subparsers = parser.add_subparsers(dest="stage", required=True)
    for name in ("selection", "evaluation"):
        stage = subparsers.add_parser(name)
        stage.add_argument("--manifest", required=True)
        stage.add_argument("--manifest-sha256", required=True)
        stage.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        validator = validate_selection if arguments.stage == "selection" else validate_evaluation
        report = validator(
            arguments.manifest,
            arguments.manifest_sha256,
            settings=ExperimentSettings.from_env(),
            local_development=arguments.local_development,
        )
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Experiment validation failed: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Strategy experiment valid")
    print("EXPERIMENT_VALIDATION_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
