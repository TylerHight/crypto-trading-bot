from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any, Protocol
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from crypto_trading_domain.backtest import (
    BacktestResult,
    Candle,
    InvalidBacktest,
    run_buy_and_hold,
)

from crypto_trading_core.backtest import (
    BacktestSettings,
    CandleLoader,
    load_candle_range,
    load_candles,
    run_application,
)
from crypto_trading_core.contracts import (
    SHA256_PATTERN,
    InvalidBacktestInput,
    PublishedCandleSnapshot,
    canonical_json_bytes,
    is_local_uri,
    load_candle_snapshot,
    normalize_uri,
    validate_distinct_prefixes,
)
from crypto_trading_core.experiment_contracts import (
    BASELINE_VERSION,
    EXPERIMENT_ENGINE_VERSION,
    EXPERIMENT_RESULT_SCHEMA_VERSION,
    EXPERIMENT_SPEC_VERSION,
    SELECTION_POLICY_VERSION,
    Candidate,
    ExperimentRange,
    ExperimentSpec,
    SelectionPolicy,
    evaluation_key,
    load_experiment_spec,
    selection_identity,
    selection_key,
)
from crypto_trading_core.experiment_schemas import (
    baseline_result_table,
    candidate_result_table,
)
from crypto_trading_core.schemas import result_tables
from crypto_trading_core.storage import ObjectStorage, StorageSettings, child_uri


@dataclass(frozen=True)
class ExperimentSettings:
    backtest: BacktestSettings
    spec_prefix: str
    output_prefix: str
    maximum_candidates: int
    maximum_candidate_candle_evaluations: int

    @classmethod
    def from_env(cls) -> ExperimentSettings:
        maximum_candidates = int(os.getenv("EXPERIMENT_MAXIMUM_CANDIDATES", "50"))
        maximum_evaluations = int(
            os.getenv("EXPERIMENT_MAXIMUM_CANDIDATE_CANDLE_EVALUATIONS", "5000000")
        )
        if not 2 <= maximum_candidates <= 50 or maximum_evaluations <= 0:
            raise InvalidBacktestInput("experiment resource caps are invalid")
        return cls(
            backtest=BacktestSettings.from_env(),
            spec_prefix=os.getenv(
                "EXPERIMENT_SPEC_PREFIX",
                "s3a://crypto-data/analytics/strategy_experiments/v1/specs",
            ),
            output_prefix=os.getenv(
                "EXPERIMENT_OUTPUT_PREFIX",
                "s3a://crypto-data/analytics/strategy_experiments/v1",
            ),
            maximum_candidates=maximum_candidates,
            maximum_candidate_candle_evaluations=maximum_evaluations,
        )


class RangeLoader(Protocol):
    def __call__(
        self,
        source: PublishedCandleSnapshot,
        *,
        exchange: str,
        symbol: str,
        start: datetime,
        end: datetime,
        warmup_candles: int,
        storage_settings: StorageSettings,
        maximum_input_candles: int,
    ) -> tuple[Candle, ...]: ...


def _safe_output(output: str, settings: ExperimentSettings, local_development: bool) -> None:
    if is_local_uri(output):
        if not local_development:
            raise InvalidBacktestInput("local experiment output requires local-development mode")
        return
    normalized = normalize_uri(output)
    allowed = normalize_uri(settings.output_prefix)
    if normalized != allowed and not normalized.startswith(allowed + "/"):
        raise InvalidBacktestInput("experiment output is outside the allowed prefix")


def _source(
    spec: ExperimentSpec,
    settings: ExperimentSettings,
    store: ObjectStorage,
    *,
    local_development: bool,
) -> PublishedCandleSnapshot:
    body = store.read_bytes(spec.candle_manifest_uri)
    source = load_candle_snapshot(
        body,
        manifest_uri=spec.candle_manifest_uri,
        expected_sha256=spec.candle_manifest_sha256,
        allowed_manifest_prefix=settings.backtest.source_manifest_prefix,
        local_development=local_development,
    )
    if not is_local_uri(source.output_uri) and not normalize_uri(
        source.output_uri
    ).startswith(normalize_uri(settings.backtest.source_output_prefix) + "/"):
        raise InvalidBacktestInput("candle output is outside the allowed prefix")
    return source


def _decimal(value: Any, field: str) -> Decimal:
    if not isinstance(value, str):
        raise InvalidBacktestInput(f"{field} must be a decimal string")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise InvalidBacktestInput(f"{field} must be finite")
    return parsed


def _backtest_arguments(
    spec: ExperimentSpec,
    candidate: Candidate,
    interval: ExperimentRange,
    *,
    output: str,
    local_development: bool,
) -> argparse.Namespace:
    return argparse.Namespace(
        candle_manifest=spec.candle_manifest_uri,
        candle_manifest_sha256=spec.candle_manifest_sha256,
        exchange=spec.exchange,
        symbol=spec.symbol,
        start=interval.start.isoformat().replace("+00:00", "Z"),
        end=interval.end.isoformat().replace("+00:00", "Z"),
        starting_cash=format(spec.starting_cash, "f"),
        fast_period=candidate.fast_period,
        slow_period=candidate.slow_period,
        fee_bps=format(spec.fee_bps, "f"),
        slippage_bps=format(spec.slippage_bps, "f"),
        output=output,
        source_manifest_prefix=None,
        local_development=local_development,
    )


def _baseline_key(
    spec: ExperimentSpec,
    source: PublishedCandleSnapshot,
    interval_name: str,
    interval: ExperimentRange,
) -> str:
    identity = {
        "baseline_version": BASELINE_VERSION,
        "candle_manifest_sha256": source.manifest_sha256,
        "candle_snapshot_key": source.snapshot_key,
        "end": interval.end,
        "exchange": spec.exchange,
        "fee_bps": spec.fee_bps,
        "range_name": interval_name,
        "slippage_bps": spec.slippage_bps,
        "start": interval.start,
        "starting_cash": spec.starting_cash,
        "symbol": spec.symbol,
    }
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


def _summary_values(result: BacktestResult) -> dict[str, Any]:
    summary = result.summary
    return {
        "absolute_return": summary.absolute_return,
        "ending_equity": summary.ending_equity,
        "evaluation_candles": summary.evaluation_candles,
        "fill_count": summary.buys + summary.sells,
        "gross_traded_notional": summary.gross_traded_notional,
        "maximum_drawdown": summary.maximum_drawdown,
        "percentage_candles_long": summary.percentage_candles_long,
        "percentage_return": summary.percentage_return,
        "starting_equity": summary.starting_equity,
        "total_fees": summary.total_fees,
    }


def _baseline_row(
    spec: ExperimentSpec,
    source: PublishedCandleSnapshot,
    interval_name: str,
    interval: ExperimentRange,
    *,
    settings: ExperimentSettings,
    range_loader: RangeLoader,
) -> tuple[dict[str, Any], BacktestResult]:
    candles = range_loader(
        source,
        exchange=spec.exchange,
        symbol=spec.symbol,
        start=interval.start,
        end=interval.end,
        warmup_candles=0,
        storage_settings=settings.backtest.storage,
        maximum_input_candles=settings.backtest.maximum_input_candles,
    )
    try:
        result = run_buy_and_hold(
            candles,
            start=interval.start,
            end=interval.end,
            starting_cash=spec.starting_cash,
            fee_bps=spec.fee_bps,
            slippage_bps=spec.slippage_bps,
        )
    except InvalidBacktest as error:
        raise InvalidBacktestInput(str(error)) from error
    return {
        "baseline_key": _baseline_key(spec, source, interval_name, interval),
        "baseline_version": BASELINE_VERSION,
        "range_name": interval_name,
        **_summary_values(result),
    }, result


def _candidate_row(
    candidate: Candidate,
    interval_name: str,
    report: dict[str, Any],
    manifest_sha256: str,
    baseline: dict[str, Any],
) -> dict[str, Any]:
    summary = report.get("summary")
    if not isinstance(summary, dict):
        raise InvalidBacktestInput("underlying backtest summary is missing")
    percentage_return = _decimal(summary.get("percentage_return"), "percentage_return")
    baseline_return = baseline["percentage_return"]
    return {
        "absolute_return": _decimal(summary.get("absolute_return"), "absolute_return"),
        "backtest_key": report["backtest_key"],
        "backtest_manifest_sha256": manifest_sha256,
        "backtest_manifest_uri": report["manifest_uri"],
        "baseline_percentage_return": baseline_return,
        "candidate_id": candidate.candidate_id,
        "ending_equity": _decimal(summary.get("ending_equity"), "ending_equity"),
        "evaluation_candles": int(summary["evaluation_candles"]),
        "excess_percentage_return": percentage_return - baseline_return,
        "fast_period": candidate.fast_period,
        "fill_count": int(summary["buys"]) + int(summary["sells"]),
        "gross_traded_notional": _decimal(
            summary.get("gross_traded_notional"), "gross_traded_notional"
        ),
        "maximum_drawdown": _decimal(
            summary.get("maximum_drawdown"), "maximum_drawdown"
        ),
        "percentage_candles_long": _decimal(
            summary.get("percentage_candles_long"), "percentage_candles_long"
        ),
        "percentage_return": percentage_return,
        "range_name": interval_name,
        "slow_period": candidate.slow_period,
        "starting_equity": _decimal(summary.get("starting_equity"), "starting_equity"),
        "total_fees": _decimal(summary.get("total_fees"), "total_fees"),
    }


def select_candidate(
    rows: list[dict[str, Any]], policy: SelectionPolicy
) -> tuple[str | None, list[dict[str, Any]]]:
    by_candidate: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        by_candidate.setdefault(str(row["candidate_id"]), {})[str(row["range_name"])] = row
    evidence: list[dict[str, Any]] = []
    eligible: list[tuple[str, dict[str, Any]]] = []
    for candidate_id in sorted(by_candidate):
        ranges = by_candidate[candidate_id]
        if set(ranges) != {"train", "validation"}:
            raise InvalidBacktestInput("candidate results are incomplete")
        train = ranges["train"]
        validation = ranges["validation"]
        reasons: list[str] = []
        if train["fill_count"] < policy.minimum_train_fills:
            reasons.append("minimum_train_fills")
        if train["maximum_drawdown"] > policy.maximum_train_drawdown:
            reasons.append("maximum_train_drawdown")
        if validation["maximum_drawdown"] > policy.maximum_validation_drawdown:
            reasons.append("maximum_validation_drawdown")
        item = {
            "candidate_id": candidate_id,
            "eligible": not reasons,
            "rejection_reasons": reasons,
            "validation_maximum_drawdown": validation["maximum_drawdown"],
            "validation_percentage_return": validation["percentage_return"],
            "validation_total_fees": validation["total_fees"],
        }
        evidence.append(item)
        if not reasons:
            eligible.append((candidate_id, validation))
    eligible.sort(
        key=lambda item: (
            -item[1]["percentage_return"],
            item[1]["maximum_drawdown"],
            item[1]["total_fees"],
            item[0],
        )
    )
    selected = eligible[0][0] if eligible else None
    return selected, evidence


def _parquet_bytes(table: pa.Table) -> bytes:
    output = BytesIO()
    pq.write_table(table, output, compression="zstd", version="2.6")
    return output.getvalue()


def _publish(
    store: ObjectStorage,
    uri: str,
    body: bytes,
    *,
    rows: int,
    content_type: str,
) -> dict[str, Any]:
    store.write_bytes_append_only(uri, body, content_type=content_type)
    read_back = store.read_bytes(uri)
    digest = hashlib.sha256(body).hexdigest()
    if read_back != body:
        raise InvalidBacktestInput("experiment artifact failed read-back validation")
    return {"bytes": len(body), "rows": rows, "sha256": digest, "uri": uri}


def _existing(
    store: ObjectStorage, manifest_uri: str, key_name: str, key: str
) -> dict[str, Any] | None:
    body = store.try_read_bytes(manifest_uri)
    if body is None:
        return None
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("existing experiment manifest is invalid JSON") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "published"
        or manifest.get(key_name) != key
        or not isinstance(manifest.get("artifacts"), dict)
    ):
        raise InvalidBacktestInput("existing experiment manifest conflicts with identity")
    return manifest


def prepare_experiment(
    arguments: argparse.Namespace,
    settings: ExperimentSettings,
    *,
    store: ObjectStorage | None = None,
    run_id: str | None = None,
    started_at: datetime | None = None,
    candle_loader: CandleLoader = load_candles,
    range_loader: RangeLoader = load_candle_range,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    output = arguments.output or settings.output_prefix
    _safe_output(output, settings, arguments.local_development)
    spec_body = storage.read_bytes(arguments.spec)
    spec = load_experiment_spec(
        spec_body,
        spec_uri=arguments.spec,
        expected_sha256=arguments.spec_sha256,
        allowed_spec_prefix=settings.spec_prefix,
        local_development=arguments.local_development,
        maximum_candidates=settings.maximum_candidates,
        maximum_candidate_candle_evaluations=(
            settings.maximum_candidate_candle_evaluations
        ),
    )
    source = _source(
        spec, settings, storage, local_development=arguments.local_development
    )
    validate_distinct_prefixes(source.output_uri, output)
    key = selection_key(spec, source)
    manifest_uri = child_uri(output, "selections", key, "manifest.json")
    existing = _existing(storage, manifest_uri, "selection_key", key)
    if existing is not None:
        return {
            **existing,
            "published_status": "published",
            "status": "resolved_existing_selection",
        }

    candidate_output = child_uri(output, "backtests")
    candidate_settings = replace(settings.backtest, output_prefix=candidate_output)
    baselines: dict[str, dict[str, Any]] = {}
    baseline_rows: list[dict[str, Any]] = []
    for name, interval in (("train", spec.train), ("validation", spec.validation)):
        row, _ = _baseline_row(
            spec,
            source,
            name,
            interval,
            settings=settings,
            range_loader=range_loader,
        )
        baselines[name] = row
        baseline_rows.append(row)

    candidate_rows: list[dict[str, Any]] = []
    for candidate in spec.candidates:
        for name, interval in (("train", spec.train), ("validation", spec.validation)):
            report = run_application(
                _backtest_arguments(
                    spec,
                    candidate,
                    interval,
                    output=candidate_output,
                    local_development=arguments.local_development,
                ),
                candidate_settings,
                store=storage,
                candle_loader=candle_loader,
            )
            manifest_body = storage.read_bytes(report["manifest_uri"])
            candidate_rows.append(
                _candidate_row(
                    candidate,
                    name,
                    report,
                    hashlib.sha256(manifest_body).hexdigest(),
                    baselines[name],
                )
            )

    candidate_rows.sort(key=lambda row: (row["candidate_id"], row["range_name"]))
    baseline_rows.sort(key=lambda row: row["range_name"])
    selected, evidence = select_candidate(candidate_rows, spec.selection_policy)
    selection_status = "selected" if selected is not None else "no_candidate_selected"
    summary = {
        "candidate_evidence": evidence,
        "selected_candidate_id": selected,
        "selection_status": selection_status,
        "test_data_accessed": False,
    }

    actual_run_id = run_id or str(uuid4())
    run_uri = child_uri(output, "runs", actual_run_id, "selection")
    candidate_table = candidate_result_table(candidate_rows)
    baseline_table = baseline_result_table(baseline_rows)
    artifacts = {
        "candidate_results.parquet": _publish(
            storage,
            child_uri(run_uri, "candidate_results.parquet"),
            _parquet_bytes(candidate_table),
            rows=candidate_table.num_rows,
            content_type="application/vnd.apache.parquet",
        ),
        "baseline_results.parquet": _publish(
            storage,
            child_uri(run_uri, "baseline_results.parquet"),
            _parquet_bytes(baseline_table),
            rows=baseline_table.num_rows,
            content_type="application/vnd.apache.parquet",
        ),
    }
    summary_body = canonical_json_bytes(summary)
    artifacts["selection_summary.json"] = _publish(
        storage,
        child_uri(run_uri, "selection_summary.json"),
        summary_body,
        rows=1,
        content_type="application/json",
    )
    started = started_at or datetime.now(UTC)
    manifest: dict[str, Any] = {
        "artifacts": artifacts,
        "candle_manifest_sha256": source.manifest_sha256,
        "candle_manifest_uri": source.manifest_uri,
        "candle_output_uri": source.output_uri,
        "candle_snapshot_key": source.snapshot_key,
        "completed_at": datetime.now(UTC),
        "execution_mode": "simulation",
        "experiment_engine_version": EXPERIMENT_ENGINE_VERSION,
        "experiment_result_schema_version": EXPERIMENT_RESULT_SCHEMA_VERSION,
        "experiment_spec_canonical_sha256": spec.canonical_sha256,
        "experiment_spec_raw_sha256": spec.raw_sha256,
        "experiment_spec_uri": spec.spec_uri,
        "identity": selection_identity(spec, source),
        "manifest_uri": manifest_uri,
        "ranges": {
            "test": spec.test.as_dict(),
            "train": spec.train.as_dict(),
            "validation": spec.validation.as_dict(),
        },
        "run_id": actual_run_id,
        "run_output_uri": run_uri,
        "selected_candidate": (
            next(item.as_dict() for item in spec.candidates if item.candidate_id == selected)
            if selected is not None
            else None
        ),
        "selection_key": key,
        "selection_policy": spec.selection_policy.as_dict(),
        "selection_policy_version": SELECTION_POLICY_VERSION,
        "selection_status": selection_status,
        "started_at": started,
        "status": "published",
        "summary": summary,
        "test_data_accessed": False,
        "versions": {
            "baseline": BASELINE_VERSION,
            "experiment_spec": EXPERIMENT_SPEC_VERSION,
        },
    }
    if not storage.try_write_bytes_append_only(
        manifest_uri, canonical_json_bytes(manifest), content_type="application/json"
    ):
        concurrent = _existing(storage, manifest_uri, "selection_key", key)
        if concurrent is None:
            raise InvalidBacktestInput("selection publication race was unresolved")
        return {
            **concurrent,
            "published_status": "published",
            "status": "resolved_existing_selection",
        }
    return json.loads(canonical_json_bytes(manifest))


def load_selection_manifest(
    store: ObjectStorage, uri: str, expected_sha256: str
) -> tuple[dict[str, Any], str]:
    body = store.read_bytes(uri)
    digest = hashlib.sha256(body).hexdigest()
    if (
        not SHA256_PATTERN.fullmatch(expected_sha256)
        or digest != expected_sha256
    ):
        raise InvalidBacktestInput("selection manifest SHA-256 digest does not match")
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("selection manifest is invalid UTF-8 JSON") from error
    if not isinstance(manifest, dict) or manifest.get("status") != "published":
        raise InvalidBacktestInput("selection manifest is not published")
    if manifest.get("test_data_accessed") is not False:
        raise InvalidBacktestInput("selection manifest does not prove the test embargo")
    identity = manifest.get("identity")
    selection_key_value = manifest.get("selection_key")
    if (
        not isinstance(identity, dict)
        or not isinstance(selection_key_value, str)
        or hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        != selection_key_value
    ):
        raise InvalidBacktestInput("selection manifest identity is invalid")
    return manifest, digest


def _artifact_body(
    store: ObjectStorage, metadata: Any, field: str
) -> bytes:
    if not isinstance(metadata, dict) or not isinstance(metadata.get("uri"), str):
        raise InvalidBacktestInput(f"{field} artifact metadata is invalid")
    body = store.read_bytes(metadata["uri"])
    if (
        metadata.get("bytes") != len(body)
        or metadata.get("sha256") != hashlib.sha256(body).hexdigest()
    ):
        raise InvalidBacktestInput(f"{field} artifact digest or size changed")
    return body


def evaluate_experiment(
    arguments: argparse.Namespace,
    settings: ExperimentSettings,
    *,
    store: ObjectStorage | None = None,
    run_id: str | None = None,
    started_at: datetime | None = None,
    candle_loader: CandleLoader = load_candles,
    range_loader: RangeLoader = load_candle_range,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    output = arguments.output or settings.output_prefix
    _safe_output(output, settings, arguments.local_development)
    if is_local_uri(arguments.selection_manifest) and not arguments.local_development:
        raise InvalidBacktestInput(
            "local selection manifests require explicit local-development mode"
        )
    if not is_local_uri(arguments.selection_manifest) and not normalize_uri(
        arguments.selection_manifest
    ).startswith(normalize_uri(output) + "/selections/"):
        raise InvalidBacktestInput("selection manifest is outside the experiment output")
    selection, selection_digest = load_selection_manifest(
        storage,
        arguments.selection_manifest,
        arguments.selection_manifest_sha256,
    )
    if selection.get("selection_status") != "selected":
        raise InvalidBacktestInput("selection has no eligible candidate to evaluate")
    spec_uri = selection.get("experiment_spec_uri")
    raw_spec_digest = selection.get("experiment_spec_raw_sha256")
    if not isinstance(spec_uri, str) or not isinstance(raw_spec_digest, str):
        raise InvalidBacktestInput("selection does not pin its experiment specification")
    spec = load_experiment_spec(
        storage.read_bytes(spec_uri),
        spec_uri=spec_uri,
        expected_sha256=raw_spec_digest,
        allowed_spec_prefix=settings.spec_prefix,
        local_development=arguments.local_development,
        maximum_candidates=settings.maximum_candidates,
        maximum_candidate_candle_evaluations=(
            settings.maximum_candidate_candle_evaluations
        ),
    )
    source = _source(
        spec, settings, storage, local_development=arguments.local_development
    )
    validate_distinct_prefixes(source.output_uri, output)
    expected_selection_key = selection_key(spec, source)
    if (
        selection.get("selection_key") != expected_selection_key
        or selection.get("experiment_spec_canonical_sha256") != spec.canonical_sha256
    ):
        raise InvalidBacktestInput("selection no longer matches its pinned inputs")
    selected = selection.get("selected_candidate")
    if not isinstance(selected, dict):
        raise InvalidBacktestInput("selected candidate is missing")
    candidate = next(
        (
            item
            for item in spec.candidates
            if item.candidate_id == selected.get("candidate_id")
            and item.fast_period == selected.get("fast_period")
            and item.slow_period == selected.get("slow_period")
        ),
        None,
    )
    if candidate is None:
        raise InvalidBacktestInput("selected candidate was changed after preparation")

    key = evaluation_key(expected_selection_key, selection_digest)
    manifest_uri = child_uri(output, "evaluations", key, "manifest.json")
    existing = _existing(storage, manifest_uri, "evaluation_key", key)
    if existing is not None:
        return {
            **existing,
            "published_status": "published",
            "status": "resolved_existing_evaluation",
        }

    candidate_output = child_uri(output, "backtests")
    candidate_settings = replace(settings.backtest, output_prefix=candidate_output)
    strategy_report = run_application(
        _backtest_arguments(
            spec,
            candidate,
            spec.test,
            output=candidate_output,
            local_development=arguments.local_development,
        ),
        candidate_settings,
        store=storage,
        candle_loader=candle_loader,
    )
    strategy_manifest_body = storage.read_bytes(strategy_report["manifest_uri"])
    baseline_row, baseline_result = _baseline_row(
        spec,
        source,
        "test",
        spec.test,
        settings=settings,
        range_loader=range_loader,
    )
    strategy_summary = strategy_report.get("summary")
    if not isinstance(strategy_summary, dict):
        raise InvalidBacktestInput("test backtest summary is missing")
    strategy_percentage = _decimal(
        strategy_summary.get("percentage_return"), "strategy percentage_return"
    )
    strategy_absolute = _decimal(
        strategy_summary.get("absolute_return"), "strategy absolute_return"
    )
    comparison = {
        "baseline": baseline_row,
        "evaluation_range": "out_of_sample",
        "execution_mode": "simulation",
        "selected_candidate": candidate.as_dict(),
        "selection_basis": "train_and_validation_only",
        "strategy": strategy_summary,
        "strategy_backtest_key": strategy_report["backtest_key"],
        "strategy_backtest_manifest_sha256": hashlib.sha256(
            strategy_manifest_body
        ).hexdigest(),
        "strategy_backtest_manifest_uri": strategy_report["manifest_uri"],
        "strategy_excess_absolute_return": (
            strategy_absolute - baseline_row["absolute_return"]
        ),
        "strategy_excess_percentage_return": (
            strategy_percentage - baseline_row["percentage_return"]
        ),
        "test_range": spec.test.as_dict(),
    }

    actual_run_id = run_id or str(uuid4())
    run_uri = child_uri(output, "runs", actual_run_id, "evaluation")
    baseline_tables = result_tables(baseline_result)
    artifacts: dict[str, dict[str, Any]] = {}
    for source_name, target_name in (
        ("fills.parquet", "baseline_fills.parquet"),
        ("equity_curve.parquet", "baseline_equity_curve.parquet"),
    ):
        table = baseline_tables[source_name]
        artifacts[target_name] = _publish(
            storage,
            child_uri(run_uri, target_name),
            _parquet_bytes(table),
            rows=table.num_rows,
            content_type="application/vnd.apache.parquet",
        )
    comparison_body = canonical_json_bytes(comparison)
    artifacts["comparison.json"] = _publish(
        storage,
        child_uri(run_uri, "comparison.json"),
        comparison_body,
        rows=1,
        content_type="application/json",
    )

    started = started_at or datetime.now(UTC)
    manifest: dict[str, Any] = {
        "artifacts": artifacts,
        "candle_manifest_sha256": source.manifest_sha256,
        "candle_manifest_uri": source.manifest_uri,
        "candle_snapshot_key": source.snapshot_key,
        "completed_at": datetime.now(UTC),
        "evaluation_key": key,
        "execution_mode": "simulation",
        "experiment_engine_version": EXPERIMENT_ENGINE_VERSION,
        "experiment_result_schema_version": EXPERIMENT_RESULT_SCHEMA_VERSION,
        "manifest_uri": manifest_uri,
        "run_id": actual_run_id,
        "run_output_uri": run_uri,
        "selected_candidate": candidate.as_dict(),
        "selection_key": expected_selection_key,
        "selection_manifest_sha256": selection_digest,
        "selection_manifest_uri": arguments.selection_manifest,
        "started_at": started,
        "status": "published",
        "strategy_backtest": {
            "key": strategy_report["backtest_key"],
            "manifest_sha256": hashlib.sha256(strategy_manifest_body).hexdigest(),
            "manifest_uri": strategy_report["manifest_uri"],
        },
        "summary": comparison,
        "test_range": spec.test.as_dict(),
    }
    if not storage.try_write_bytes_append_only(
        manifest_uri, canonical_json_bytes(manifest), content_type="application/json"
    ):
        concurrent = _existing(storage, manifest_uri, "evaluation_key", key)
        if concurrent is None:
            raise InvalidBacktestInput("evaluation publication race was unresolved")
        return {
            **concurrent,
            "published_status": "published",
            "status": "resolved_existing_evaluation",
        }
    return json.loads(canonical_json_bytes(manifest))
