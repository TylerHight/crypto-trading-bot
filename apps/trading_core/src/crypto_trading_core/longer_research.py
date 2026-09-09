"""Coverage-gated research using the existing sealed experiment engine.

Coverage inspects timestamps and lineage, never OHLC prices. Opaque Parquet
bytes are hashed and frozen locally; selection queries only train/validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from crypto_trading_core.backtest import load_candle_range
from crypto_trading_core.contracts import (
    HISTORICAL_CANDLE_SCHEMA_VERSION,
    InvalidBacktestInput,
    canonical_json_bytes,
    is_local_uri,
    normalize_uri,
    parse_utc_minute,
    validate_distinct_prefixes,
)
from crypto_trading_core.experiment_contracts import ExperimentSpec, load_experiment_spec
from crypto_trading_core.experiments import (
    ExperimentSettings,
    _safe_output,
    _source,
    evaluate_experiment,
    prepare_experiment,
)
from crypto_trading_core.storage import ObjectStorage, child_uri, parse_location

VERSION = "longer-research-v1"
MINIMUM_MINUTES = 90 * 24 * 60
MINUTE = timedelta(minutes=1)
METADATA_COLUMNS = [
    "exchange",
    "symbol",
    "window_start",
    "window_end",
    "source_curated_snapshot_key",
    "candle_schema_version",
]


@dataclass(frozen=True)
class GapPolicy:
    """Pinned rules for a source that genuinely omitted one-minute candles."""

    name: str
    raw_sha256: str
    source_manifest_sha256: str
    approval_status: str
    declared_missing_ranges: tuple[tuple[datetime, datetime], ...]


def _range_pairs(value: Any, field: str) -> tuple[tuple[datetime, datetime], ...]:
    if not isinstance(value, list) or not value:
        raise InvalidBacktestInput(f"{field} must contain at least one UTC range")
    ranges: list[tuple[datetime, datetime]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {"start", "end"}:
            raise InvalidBacktestInput(f"{field}[{index}] must contain only start and end")
        start, end = item.get("start"), item.get("end")
        if not isinstance(start, str) or not isinstance(end, str):
            raise InvalidBacktestInput(f"{field}[{index}] boundaries must be strings")
        parsed = (
            parse_utc_minute(start, f"{field}[{index}].start"),
            parse_utc_minute(end, f"{field}[{index}].end"),
        )
        if parsed[0] >= parsed[1]:
            raise InvalidBacktestInput(f"{field}[{index}] must increase")
        ranges.append(parsed)
    if ranges != sorted(ranges) or any(
        end > next_start for (_, end), (next_start, _) in pairwise(ranges)
    ):
        raise InvalidBacktestInput(f"{field} must be ordered and non-overlapping")
    return tuple(ranges)


def load_gap_policy(
    spec: ExperimentSpec,
    storage: ObjectStorage,
    settings: ExperimentSettings,
    *,
    local_development: bool,
) -> GapPolicy | None:
    """Load a spec-pinned policy without giving it authority to enable research."""
    if spec.gap_policy_uri is None or spec.gap_policy_sha256 is None:
        return None
    uri = spec.gap_policy_uri
    if is_local_uri(uri):
        if not local_development:
            raise InvalidBacktestInput("local gap policies require explicit local-development mode")
    elif not normalize_uri(uri).startswith(normalize_uri(settings.spec_prefix) + "/"):
        raise InvalidBacktestInput("gap policy is outside the allowed specification prefix")
    body = storage.read_bytes(uri)
    digest = _sha(body)
    if digest != spec.gap_policy_sha256:
        raise InvalidBacktestInput("gap policy SHA-256 digest does not match")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("gap policy is invalid UTF-8 JSON") from error
    expected = {
        "gap_policy_version",
        "name",
        "approval_status",
        "source_manifest_sha256",
        "missing_candle_action",
        "indicator_action",
        "pending_order_action",
        "open_position_action",
        "declared_missing_ranges",
    }
    if not isinstance(document, dict) or set(document) != expected:
        raise InvalidBacktestInput("gap policy fields do not match contiguous-source-minutes-v1")
    if document.get("gap_policy_version") != "contiguous-source-minutes-v1":
        raise InvalidBacktestInput("unsupported gap policy version")
    name = document.get("name")
    source_manifest_sha256 = document.get("source_manifest_sha256")
    approval_status = document.get("approval_status")
    if not isinstance(name, str) or not name:
        raise InvalidBacktestInput("gap policy name is invalid")
    if not isinstance(source_manifest_sha256, str) or len(source_manifest_sha256) != 64:
        raise InvalidBacktestInput("gap policy source manifest digest is invalid")
    if approval_status != "pending_human_review":
        raise InvalidBacktestInput("gap policy cannot enable research without a separate reviewed implementation")
    required_actions = {
        "missing_candle_action": "exclude_and_reset",
        "indicator_action": "reset_after_gap",
        "pending_order_action": "cancel_at_gap",
        "open_position_action": "exclude_cross_gap_returns",
    }
    if any(document.get(field) != value for field, value in required_actions.items()):
        raise InvalidBacktestInput("gap policy contains an unsupported handling action")
    return GapPolicy(
        name=name,
        raw_sha256=digest,
        source_manifest_sha256=source_manifest_sha256,
        approval_status=approval_status,
        declared_missing_ranges=_range_pairs(document["declared_missing_ranges"], "declared_missing_ranges"),
    )


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _publish(store: ObjectStorage, uri: str, document: dict[str, Any]) -> dict[str, Any]:
    body = canonical_json_bytes(document)
    store.try_write_bytes_append_only(uri, body, content_type="application/json")
    if store.read_bytes(uri) != body:
        raise InvalidBacktestInput("immutable research publication conflicts or failed read-back")
    return {"uri": uri, "sha256": _sha(body)}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def missing_ranges(start: datetime, end: datetime, present: set[datetime]) -> list[dict[str, Any]]:
    """Losslessly encode every missing minute as half-open UTC ranges."""
    result = []
    cursor = start
    for stamp in sorted(value for value in present if start <= value < end):
        if stamp > cursor:
            result.append(
                {"start": cursor, "end": stamp, "minutes": int((stamp - cursor) / MINUTE)}
            )
        cursor = stamp + MINUTE
    if cursor < end:
        result.append({"start": cursor, "end": end, "minutes": int((end - cursor) / MINUTE)})
    return result


def coverage_report(spec: ExperimentSpec, rows: list[dict[str, Any]]) -> dict[str, Any]:
    start, end = spec.train.start, spec.test.end
    warmup_start = start - (max(item.slow_period for item in spec.candidates) - 1) * MINUTE
    present: set[datetime] = set()
    duplicates = 0
    for row in rows:
        stamp, finish = _utc(row["window_start"]), _utc(row["window_end"])
        if stamp.second or stamp.microsecond or finish != stamp + MINUTE:
            raise InvalidBacktestInput("coverage encountered a non-minute candle")
        duplicates += int(stamp in present)
        present.add(stamp)
    gaps = missing_ranges(start, end, present)
    warmup_gaps = missing_ranges(warmup_start, start, present)
    expected = int((end - start) / MINUTE)
    available = sum(start <= stamp < end for stamp in present)
    reasons = []
    if expected < MINIMUM_MINUTES:
        reasons.append("experiment_window_shorter_than_90_days")
    if available < MINIMUM_MINUTES:
        reasons.append("fewer_than_90_days_of_candles")
    if gaps:
        reasons.append("missing_candle_minutes")
    if warmup_gaps:
        reasons.append("missing_warmup_minutes")
    if duplicates:
        reasons.append("duplicate_candle_minutes")
    return {
        "status": "sufficient" if not reasons else "inconclusive",
        "reasons": reasons,
        "required_calendar_days": 90,
        "expected_minutes": expected,
        "available_minutes": available,
        "missing_minutes": sum(item["minutes"] for item in gaps),
        "missing_ranges": gaps,
        "warmup_missing_ranges": warmup_gaps,
        "duplicate_minutes": duplicates,
        "requested_start": start,
        "requested_end": end,
        "source_first_minute": min(present) if present else None,
        "source_last_minute": max(present) if present else None,
        "price_columns_accessed": False,
    }


def _merged_ranges(ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def apply_gap_policy(
    spec: ExperimentSpec,
    rows: list[dict[str, Any]],
    coverage: dict[str, Any],
    policy: GapPolicy,
) -> dict[str, Any]:
    """Record exactly which genuine source minutes make an SMA window unusable.

    This is deliberately metadata-only. It cannot observe prices, select a
    candidate, simulate a fill, or unlock the test period.
    """
    if policy.source_manifest_sha256 != spec.candle_manifest_sha256:
        raise InvalidBacktestInput("gap policy does not pin the candle manifest")
    observed_gaps = tuple((item["start"], item["end"]) for item in coverage["missing_ranges"])
    if observed_gaps != policy.declared_missing_ranges:
        raise InvalidBacktestInput("gap policy does not match the source-gap inventory")
    max_slow_period = max(item.slow_period for item in spec.candidates)
    invalidated = _merged_ranges(
        [
            (
                start,
                min(spec.test.end, end + (max_slow_period - 1) * MINUTE),
            )
            for start, end in policy.declared_missing_ranges
            if start < spec.test.end
        ]
    )
    present = {_utc(row["window_start"]) for row in rows}

    def invalid(stamp: datetime) -> bool:
        return any(start <= stamp < end for start, end in invalidated)

    ranges = {
        "train": spec.train,
        "validation": spec.validation,
        "test": spec.test,
    }
    valid_by_range = {
        name: sum(interval.start <= stamp < interval.end and not invalid(stamp) for stamp in present)
        for name, interval in ranges.items()
    }
    invalidated_present_minutes = sum(
        spec.train.start <= stamp < spec.test.end and invalid(stamp) for stamp in present
    )
    return {
        **coverage,
        "source_coverage_status": coverage["status"],
        "status": "policy_review_required",
        "reasons": [*coverage["reasons"], "gap_policy_review_required"],
        "gap_policy": {
            "name": policy.name,
            "version": "contiguous-source-minutes-v1",
            "sha256": policy.raw_sha256,
            "approval_status": policy.approval_status,
            "missing_candle_action": "exclude_and_reset",
            "indicator_action": "reset_after_gap",
            "pending_order_action": "cancel_at_gap",
            "open_position_action": "exclude_cross_gap_returns",
        },
        "policy_invalidated_ranges": [
            {
                "start": start,
                "end": end,
                "minutes": int((end - start) / MINUTE),
            }
            for start, end in invalidated
        ],
        "policy_invalidated_present_minutes": invalidated_present_minutes,
        "policy_valid_minutes": coverage["available_minutes"] - invalidated_present_minutes,
        "policy_valid_minutes_by_range": {
            **valid_by_range,
            "selection": valid_by_range["train"] + valid_by_range["validation"],
        },
    }


def _files(store: ObjectStorage, prefix: str) -> list[str]:
    location = parse_location(prefix)
    if location.scheme == "file":
        return sorted(str(path) for path in Path(location.key).rglob("*.parquet"))
    paginator = store._s3().get_paginator("list_objects_v2")
    return sorted(
        f"s3a://{location.bucket}/{item['Key']}"
        for page in paginator.paginate(
            Bucket=location.bucket, Prefix=location.key.rstrip("/") + "/"
        )
        for item in page.get("Contents", [])
        if item["Key"].endswith(".parquet")
    )


def research_summary(coverage: dict[str, Any], evaluation: dict[str, Any] | None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "inconclusive",
        "selected_candidate": None,
        "strategy_return": None,
        "buy_and_hold_return": None,
        "excess_return": None,
        "paper_trial_supported": False,
        "recommendation": "Do not start a paper trial.",
        "message": f"Not enough complete history: {coverage['available_minutes']:,} of {coverage['expected_minutes']:,} minutes available. Prepare 90 complete days of candles.",
    }
    if coverage["status"] == "policy_review_required":
        summary.update(
            status="policy_review_required",
            message=(
                "The gap-safe policy records the missing source minutes and dependent "
                "SMA windows. Human review is required before strategy selection."
            ),
            recommendation="Review the gap-safe policy. Do not select a strategy or start a paper trial.",
        )
        return summary
    if coverage["status"] != "sufficient":
        return summary
    if evaluation is None:
        summary.update(
            status="no_candidate",
            message="No strategy passed selection. Do not start a paper trial.",
        )
        return summary
    comparison = evaluation["summary"]
    excess = Decimal(comparison["strategy_excess_percentage_return"])
    if not excess.is_finite():
        raise InvalidBacktestInput("OOS excess return must be finite")
    supported = excess > 0
    summary.update(
        status="evaluated",
        selected_candidate=evaluation["selected_candidate"],
        strategy_return=comparison["strategy"]["percentage_return"],
        buy_and_hold_return=comparison["baseline"]["percentage_return"],
        excess_return=comparison["strategy_excess_percentage_return"],
        paper_trial_supported=supported,
        recommendation=(
            "Evidence supports considering a paper-only trial."
            if supported
            else "Do not start a paper trial."
        ),
        message=(
            "The selected strategy beat buy-and-hold after costs."
            if supported
            else "The selected strategy did not beat buy-and-hold after costs."
        ),
    )
    return summary


def _freeze(
    store: ObjectStorage,
    source: Any,
    spec: ExperimentSpec,
    root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    inventory, metadata = [], []
    count = 0
    historical = source.manifest.get("candle_schema_version") == HISTORICAL_CANDLE_SCHEMA_VERSION
    lineage_column = "source_archive_key" if historical else "source_curated_snapshot_key"
    columns = [name for name in METADATA_COLUMNS if name != "source_curated_snapshot_key"] + [
        lineage_column
    ]
    expected_files = {item["uri"]: item for item in source.manifest.get("files", [])}
    for index, uri in enumerate(_files(store, source.output_uri)):
        body = store.read_bytes(uri)
        if historical and (
            uri not in expected_files
            or _sha(body) != expected_files[uri]["sha256"]
            or len(body) != expected_files[uri]["bytes"]
        ):
            raise InvalidBacktestInput("historical candle file digest or inventory changed")
        inventory.append({"uri": uri, "bytes": len(body), "sha256": _sha(body)})
        # Column projection prevents research coverage from exposing test prices.
        table = pq.ParquetFile(BytesIO(body)).read(columns=columns)
        count += table.num_rows
        for row in table.to_pylist():
            if (
                row[lineage_column] != source.manifest.get(lineage_column)
                or row["candle_schema_version"] != source.manifest["candle_schema_version"]
            ):
                raise InvalidBacktestInput("candle metadata lineage changed")
            if row["exchange"] == spec.exchange and row["symbol"] == spec.symbol:
                metadata.append(row)
        # Preserve the engine's date-partition pruning on the frozen input.
        relative = uri.replace("\\", "/").split("interval=1m/", 1)
        if len(relative) != 2 or ".." in relative[1].split("/"):
            raise InvalidBacktestInput("candle file lacks valid one-minute partitions")
        target = root / "interval=1m" / str(Path(relative[1]).parent) / f"{index}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    if count != source.candle_count:
        raise InvalidBacktestInput("candle inventory count does not match its manifest")
    if historical and len(inventory) != len(expected_files):
        raise InvalidBacktestInput("historical candle inventory is incomplete")
    return inventory, metadata


def run_research(
    arguments: argparse.Namespace,
    settings: ExperimentSettings,
    *,
    store: ObjectStorage | None = None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    output = arguments.output or child_uri(settings.output_prefix, "longer_research")
    _safe_output(output, settings, arguments.local_development)
    body = storage.read_bytes(arguments.spec)
    spec = load_experiment_spec(
        body,
        spec_uri=arguments.spec,
        expected_sha256=arguments.spec_sha256,
        allowed_spec_prefix=settings.spec_prefix,
        local_development=arguments.local_development,
        maximum_candidates=5,
        maximum_candidate_candle_evaluations=settings.maximum_candidate_candle_evaluations,
    )
    if (
        spec.symbol != "BTC-USD"
        or spec.train.end != spec.validation.start
        or spec.validation.end != spec.test.start
    ):
        raise InvalidBacktestInput(
            "longer research requires BTC-USD and adjacent train/validation/test ranges"
        )
    if spec.test.end - spec.train.start > timedelta(days=366):
        raise InvalidBacktestInput("longer research is bounded to 366 calendar days")
    source = _source(spec, settings, storage, local_development=arguments.local_development)
    validate_distinct_prefixes(source.output_uri, output)
    historical = source.manifest.get("candle_schema_version") == HISTORICAL_CANDLE_SCHEMA_VERSION
    gap_policy = load_gap_policy(
        spec, storage, settings, local_development=arguments.local_development
    )
    if historical and gap_policy is None:
        raise InvalidBacktestInput("historical candle research requires a pinned gap policy")
    key = _sha(
        canonical_json_bytes(
            {
                "version": VERSION,
                "spec": spec.canonical_sha256,
                "gap_policy_sha256": gap_policy.raw_sha256 if gap_policy else None,
            }
        )
    )
    _publish(
        storage,
        child_uri(output, "registrations", spec.name + ".json"),
        {
            "version": VERSION,
            "research_key": key,
            "spec_sha256": _sha(body),
            "candle_manifest_sha256": source.manifest_sha256,
            "gap_policy_sha256": gap_policy.raw_sha256 if gap_policy else None,
        },
    )
    base = child_uri(output, "runs", key)
    spec_uri = child_uri(base, "spec.json")
    # Commit the exact configuration before even inspecting candle timestamps.
    storage.try_write_bytes_append_only(spec_uri, body, content_type="application/json")
    if storage.read_bytes(spec_uri) != body:
        raise InvalidBacktestInput("registered experiment configuration changed")
    lineage = [{"uri": source.manifest_uri, "sha256": source.manifest_sha256}]
    lineage_prefix = "source_archive" if historical else "source_curated"
    curated_uri = source.manifest.get(lineage_prefix + "_manifest_uri")
    curated_sha = source.manifest.get(lineage_prefix + "_manifest_sha256")
    if not isinstance(curated_uri, str) or not isinstance(curated_sha, str):
        raise InvalidBacktestInput("research requires a pinned source manifest")
    curated_body = storage.read_bytes(curated_uri)
    curated = json.loads(curated_body)
    if (
        _sha(curated_body) != curated_sha
        or curated.get("status") != "published"
        or curated.get("archive_key" if historical else "snapshot_key")
        != source.manifest.get(
            "source_archive_key" if historical else "source_curated_snapshot_key"
        )
    ):
        raise InvalidBacktestInput("curated source manifest digest or identity changed")
    if historical:
        for page in curated.get("pages", []):
            page_body = storage.read_bytes(page["uri"])
            if _sha(page_body) != page["sha256"]:
                raise InvalidBacktestInput("historical source response archive changed")
    lineage.append({"uri": curated_uri, "sha256": curated_sha})

    with TemporaryDirectory(prefix="longer-research-") as temporary:
        frozen = Path(temporary)
        inventory, rows = _freeze(storage, source, spec, frozen)
        coverage = coverage_report(spec, rows)
        if gap_policy is not None:
            coverage = apply_gap_policy(spec, rows, coverage, gap_policy)
        coverage.update(
            {
                "version": VERSION,
                "source_manifests": lineage,
                "candle_files": inventory,
                "spec_sha256": _sha(body),
            }
        )
        coverage_ref = _publish(storage, child_uri(base, "coverage.json"), coverage)
        if getattr(arguments, "coverage_only", False):
            readiness = {
                "version": "research-readiness-v1",
                "status": "published",
                "research_key": key,
                "coverage": coverage_ref,
                "candle_manifest_uri": source.manifest_uri,
                "candle_manifest_sha256": source.manifest_sha256,
                "ready": coverage["status"] == "sufficient",
                "strategy_selected": False,
                "test_prices_accessed": False,
                "gap_policy": coverage.get("gap_policy"),
                "policy_valid_minutes_by_range": coverage.get("policy_valid_minutes_by_range"),
                "coverage_summary": {
                    name: coverage[name]
                    for name in (
                        "status",
                        "available_minutes",
                        "expected_minutes",
                        "missing_minutes",
                        "required_calendar_days",
                    )
                },
            }
            ref = _publish(storage, child_uri(output, "readiness", key, "manifest.json"), readiness)
            return {**readiness, "manifest_uri": ref["uri"], "manifest_sha256": ref["sha256"]}
        report_uri = child_uri(output, "reports", key, "manifest.json")
        existing = storage.try_read_bytes(report_uri)
        if existing is not None:
            report = json.loads(existing)
            if report.get("coverage") != coverage_ref or report.get("research_key") != key:
                raise InvalidBacktestInput("existing research report conflicts with coverage")
            saved_evaluation = None
            for field in ("selection", "evaluation"):
                ref = report.get(field)
                if ref:
                    evidence_body = storage.read_bytes(ref["uri"])
                    if _sha(evidence_body) != ref["sha256"]:
                        raise InvalidBacktestInput("sealed research evidence changed")
                    if field == "evaluation":
                        saved_evaluation = json.loads(evidence_body)
            if report.get("summary") != research_summary(coverage, saved_evaluation):
                raise InvalidBacktestInput("sealed research summary conflicts with its evidence")
            return {**report, "manifest_uri": report_uri, "manifest_sha256": _sha(existing)}

        evaluation = None
        selection_ref = evaluation_ref = None
        if coverage["status"] == "sufficient":
            # The engine already keys publications by specification and input;
            # avoid repeating a 64-character key in every nested artifact path.
            engine_output = child_uri(output, "engine")
            engine_settings = replace(
                settings,
                spec_prefix=base,
                output_prefix=engine_output,
                backtest=replace(
                    settings.backtest,
                    maximum_input_candles=max(
                        settings.backtest.maximum_input_candles, 366 * 1440 + 10000
                    ),
                ),
            )
            test_unlocked = False

            def bounded_range(source: Any, **kwargs: Any) -> Any:
                if not test_unlocked and kwargs["end"] > spec.test.start:
                    raise InvalidBacktestInput(
                        "test-period price access is blocked during selection"
                    )
                return load_candle_range(replace(source, output_uri=str(frozen)), **kwargs)

            def bounded_candles(source: Any, spec: Any, **kwargs: Any) -> Any:
                return bounded_range(
                    source,
                    exchange=spec.exchange,
                    symbol=spec.symbol,
                    start=spec.start,
                    end=spec.end,
                    warmup_candles=spec.slow_period - 1,
                    **kwargs,
                )

            selection = prepare_experiment(
                argparse.Namespace(
                    spec=spec_uri,
                    spec_sha256=_sha(body),
                    output=engine_output,
                    local_development=arguments.local_development,
                ),
                engine_settings,
                store=storage,
                candle_loader=bounded_candles,
                range_loader=bounded_range,
            )
            selection_ref = {
                "uri": selection["manifest_uri"],
                "sha256": _sha(storage.read_bytes(selection["manifest_uri"])),
            }
            if selection["selection_status"] == "selected":
                test_unlocked = True
                evaluation = evaluate_experiment(
                    argparse.Namespace(
                        selection_manifest=selection_ref["uri"],
                        selection_manifest_sha256=selection_ref["sha256"],
                        output=engine_output,
                        local_development=arguments.local_development,
                    ),
                    engine_settings,
                    store=storage,
                    candle_loader=bounded_candles,
                    range_loader=bounded_range,
                )
                evaluation_ref = {
                    "uri": evaluation["manifest_uri"],
                    "sha256": _sha(storage.read_bytes(evaluation["manifest_uri"])),
                }
        report = {
            "version": VERSION,
            "status": "published",
            "research_key": key,
            "spec": {"uri": spec_uri, "sha256": _sha(body)},
            "coverage": coverage_ref,
            "coverage_summary": {
                name: coverage[name]
                for name in (
                    "status",
                    "required_calendar_days",
                    "available_minutes",
                    "expected_minutes",
                    "missing_minutes",
                    "reasons",
                )
            },
            "gap_policy": coverage.get("gap_policy"),
            "policy_valid_minutes_by_range": coverage.get("policy_valid_minutes_by_range"),
            "selection": selection_ref,
            "evaluation": evaluation_ref,
            "summary": research_summary(coverage, evaluation),
            "test_prices_accessed": evaluation_ref is not None,
        }
        ref = _publish(storage, report_uri, report)
        return {
            **json.loads(canonical_json_bytes(report)),
            "manifest_uri": ref["uri"],
            "manifest_sha256": ref["sha256"],
        }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed, coverage-gated 90-day research experiment."
    )
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--output")
    parser.add_argument("--local-development", action="store_true")
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Publish research readiness without selecting or evaluating strategies.",
    )
    try:
        report = run_research(parser.parse_args(argv), ExperimentSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Research rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    if report.get("version") == "research-readiness-v1":
        print("History ready for research." if report["ready"] else "History is incomplete.")
        print("RESEARCH_READINESS_JSON=" + canonical_json_bytes(report).decode("ascii"))
        if not report["ready"]:
            raise SystemExit(2)
        return
    print(report["summary"]["message"])
    print(report["summary"]["recommendation"])
    print("RESEARCH_REPORT_JSON=" + canonical_json_bytes(report).decode("ascii"))
    if report["summary"]["status"] != "evaluated":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
