"""Sealed historical research that resets all strategy state at real source gaps.

This runner is deliberately separate from :mod:`longer_research`.  It consumes
an explicit, immutable human approval and reports independent segment results;
it never represents their aggregate as a continuously tradable return.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypeVar

from crypto_trading_domain.backtest import Candle, InvalidBacktest, run_backtest, run_buy_and_hold

from crypto_trading_core.backtest import load_candle_range
from crypto_trading_core.contracts import (
    HISTORICAL_CANDLE_SCHEMA_VERSION,
    InvalidBacktestInput,
    PublishedCandleSnapshot,
    canonical_json_bytes,
    is_local_uri,
    validate_distinct_prefixes,
)
from crypto_trading_core.experiment_contracts import (
    Candidate,
    ExperimentRange,
    ExperimentSpec,
    load_experiment_spec,
)
from crypto_trading_core.experiments import (
    ExperimentSettings,
    _safe_output,
    _source,
    select_candidate,
)
from crypto_trading_core.longer_research import (
    MINUTE,
    _freeze,
    _publish,
    _sha,
    coverage_report,
    load_gap_policy,
)
from crypto_trading_core.storage import ObjectStorage, child_uri

VERSION = "gap-aware-sealed-research-v2"
AGGREGATION_VERSION = "unweighted-valid-segment-mean-v1"
MAX_EQUITY_POINTS = 240
MAX_TRADE_MARKERS = 400
Item = TypeVar("Item")


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def contiguous_segments(
    timestamps: list[datetime], interval: ExperimentRange
) -> tuple[ExperimentRange, ...]:
    """Return the maximal published one-minute runs inside one experiment range."""
    present = sorted({_utc(value) for value in timestamps if interval.start <= _utc(value) < interval.end})
    if not present:
        return ()
    result: list[ExperimentRange] = []
    start = previous = present[0]
    for stamp in present[1:]:
        if stamp != previous + MINUTE:
            result.append(ExperimentRange(start, previous + MINUTE))
            start = stamp
        previous = stamp
    result.append(ExperimentRange(start, previous + MINUTE))
    return tuple(result)


def eligible_segments(
    segments: tuple[ExperimentRange, ...], slow_period: int
) -> tuple[tuple[ExperimentRange, ...], list[dict[str, Any]]]:
    """Reserve a full SMA warm-up and two independent evaluation candles."""
    required = slow_period + 1
    valid: list[ExperimentRange] = []
    exclusions: list[dict[str, Any]] = []
    for segment in segments:
        if segment.candles < required:
            exclusions.append(
                {
                    "start": segment.start,
                    "end": segment.end,
                    "candles": segment.candles,
                    "required_candles": required,
                    "reason": "insufficient_sma_warmup_and_two_evaluation_candles",
                }
            )
        else:
            valid.append(segment)
    return tuple(valid), exclusions


def _range_dict(interval: ExperimentRange) -> dict[str, datetime]:
    return {"start": interval.start, "end": interval.end}


def _approved_review(
    store: ObjectStorage,
    *,
    review_uri: str,
    review_sha256: str,
    spec: ExperimentSpec,
) -> dict[str, Any]:
    body = store.read_bytes(review_uri)
    if _sha(body) != review_sha256:
        raise InvalidBacktestInput("approved gap-policy review SHA-256 digest does not match")
    try:
        review = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("approved gap-policy review is invalid JSON") from error
    evidence = review.get("evidence") if isinstance(review, dict) else None
    required = {
        "status": "published",
        "version": "gap-policy-review-v1",
        "decision": "approved",
    }
    if any(review.get(field) != value for field, value in required.items()) or not isinstance(evidence, dict):
        raise InvalidBacktestInput("gap-policy review is not an immutable approval")
    reviewed_policy_uri = evidence.get("gap_policy_uri")
    same_local_policy = (
        isinstance(reviewed_policy_uri, str)
        and spec.gap_policy_uri is not None
        and is_local_uri(reviewed_policy_uri)
        and is_local_uri(spec.gap_policy_uri)
        and Path(reviewed_policy_uri).resolve() == Path(spec.gap_policy_uri).resolve()
    )
    if (
        (reviewed_policy_uri != spec.gap_policy_uri and not same_local_policy)
        or evidence.get("gap_policy_sha256") != spec.gap_policy_sha256
        or evidence.get("source_manifest_sha256") != spec.candle_manifest_sha256
    ):
        raise InvalidBacktestInput("approved review does not match the pinned policy or candle source")
    for name in ("report", "coverage"):
        uri, digest = evidence.get(f"{name}_uri"), evidence.get(f"{name}_sha256")
        if not isinstance(uri, str) or not isinstance(digest, str) or _sha(store.read_bytes(uri)) != digest:
            raise InvalidBacktestInput(f"approved review {name} evidence changed")
    return review


def _load_segment(
    source: PublishedCandleSnapshot,
    interval: ExperimentRange,
    *,
    spec: ExperimentSpec,
    settings: ExperimentSettings,
    test_unlocked: bool,
) -> tuple[Candle, ...]:
    if not test_unlocked and interval.end > spec.test.start:
        raise InvalidBacktestInput("test-period price access is blocked during selection")
    return load_candle_range(
        source,
        exchange=spec.exchange,
        symbol=spec.symbol,
        start=interval.start,
        end=interval.end,
        warmup_candles=0,
        storage_settings=settings.backtest.storage,
        maximum_input_candles=settings.backtest.maximum_input_candles,
    )


def _metrics(results: list[Any]) -> dict[str, Any]:
    if not results:
        raise InvalidBacktestInput("candidate has no valid source segments")
    summaries = [item.summary for item in results]
    count = Decimal(len(summaries))
    return {
        "aggregation": AGGREGATION_VERSION,
        "segment_count": len(summaries),
        "absolute_return": sum((item.absolute_return for item in summaries), Decimal(0)) / count,
        "percentage_return": sum((item.percentage_return for item in summaries), Decimal(0)) / count,
        "maximum_drawdown": max(item.maximum_drawdown for item in summaries),
        "fill_count": sum(item.buys + item.sells for item in summaries),
        "total_fees": sum((item.total_fees for item in summaries), Decimal(0)),
        "gross_traded_notional": sum((item.gross_traded_notional for item in summaries), Decimal(0)),
        "evaluation_candles": sum(item.evaluation_candles for item in summaries),
    }


def _sample(items: tuple[Item, ...], maximum: int) -> tuple[Item, ...]:
    """Keep first/last points and evenly spaced evidence between them."""
    if len(items) <= maximum:
        return items
    stride = max(1, (len(items) - 2) // (maximum - 2))
    sampled = (*items[::stride],)
    if sampled[-1] != items[-1]:
        sampled = (*sampled, items[-1])
    return sampled[: maximum - 1] + (items[-1],)


def _chart_evidence(result: Any) -> dict[str, Any]:
    return {
        "equity_points": [
            {
                "time": item.window_start,
                "equity": item.equity,
                "drawdown": item.drawdown,
                "position": item.position.value,
            }
            for item in _sample(result.equity_curve, MAX_EQUITY_POINTS)
        ],
        "trade_marker_total": len(result.fills),
        "trade_markers": [
            {
                "time": item.fill_time,
                "side": item.side.value,
                "execution_price": item.execution_price,
                "fee": item.fee,
            }
            for item in _sample(result.fills, MAX_TRADE_MARKERS)
        ],
    }


def evaluate_candidate_segments(
    candidate: Candidate,
    segments: tuple[ExperimentRange, ...],
    *,
    source: PublishedCandleSnapshot,
    spec: ExperimentSpec,
    settings: ExperimentSettings,
    test_unlocked: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Run isolated simulations. Each segment has a new cash, SMA and order state."""
    valid, exclusions = eligible_segments(segments, candidate.slow_period)
    results, evidence = [], []
    for segment in valid:
        candles = _load_segment(
            source, segment, spec=spec, settings=settings, test_unlocked=test_unlocked
        )
        evaluation = ExperimentRange(segment.start + (candidate.slow_period - 1) * MINUTE, segment.end)
        try:
            strategy = run_backtest(
                candles,
                start=evaluation.start,
                end=evaluation.end,
                starting_cash=spec.starting_cash,
                fast_period=candidate.fast_period,
                slow_period=candidate.slow_period,
                fee_bps=spec.fee_bps,
                slippage_bps=spec.slippage_bps,
            )
            baseline = run_buy_and_hold(
                candles[candidate.slow_period - 1 :],
                start=evaluation.start,
                end=evaluation.end,
                starting_cash=spec.starting_cash,
                fee_bps=spec.fee_bps,
                slippage_bps=spec.slippage_bps,
            )
        except InvalidBacktest as error:
            raise InvalidBacktestInput(str(error)) from error
        results.append(strategy)
        evidence.append(
            {
                "source_segment": _range_dict(segment),
                "evaluation_range": _range_dict(evaluation),
                "strategy": asdict(strategy.summary),
                "baseline": asdict(baseline.summary),
                "chart": _chart_evidence(strategy),
                "pending_decision_cancelled_at_segment_end": bool(
                    strategy.summary.unfilled_terminal_decisions
                ),
            }
        )
    return _metrics(results), evidence, exclusions


def _segment_report(
    spec: ExperimentSpec, rows: list[dict[str, Any]]
) -> dict[str, tuple[ExperimentRange, ...]]:
    timestamps = [_utc(row["window_start"]) for row in rows]
    return {
        "train": contiguous_segments(timestamps, spec.train),
        "validation": contiguous_segments(timestamps, spec.validation),
        "test": contiguous_segments(timestamps, spec.test),
    }


def _candidate_selection(
    spec: ExperimentSpec,
    segments: dict[str, tuple[ExperimentRange, ...]],
    *,
    source: PublishedCandleSnapshot,
    settings: ExperimentSettings,
) -> tuple[Candidate | None, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    evidence: dict[str, Any] = {"aggregation": AGGREGATION_VERSION, "candidates": {}}
    for candidate in spec.candidates:
        candidate_evidence: dict[str, Any] = {}
        for name in ("train", "validation"):
            metrics, per_segment, exclusions = evaluate_candidate_segments(
                candidate,
                segments[name],
                source=source,
                spec=spec,
                settings=settings,
                test_unlocked=False,
            )
            rows.append({"candidate_id": candidate.candidate_id, "range_name": name, **metrics})
            candidate_evidence[name] = {
                "segments": per_segment,
                "excluded_segments": exclusions,
                "aggregate": metrics,
            }
        evidence["candidates"][candidate.candidate_id] = candidate_evidence
    selected_id, ranking = select_candidate(rows, spec.selection_policy)
    selected = next((item for item in spec.candidates if item.candidate_id == selected_id), None)
    evidence["ranking"] = ranking
    return selected, evidence


def _summary(selected: Candidate | None, evaluation: dict[str, Any] | None) -> dict[str, Any]:
    if selected is None:
        return {
            "status": "no_candidate",
            "selected_candidate": None,
            "paper_trial_supported": False,
            "recommendation": "Do not start a paper trial.",
            "message": "No candidate passed the fixed segmented selection rule.",
        }
    assert evaluation is not None
    strategy, baseline = evaluation["aggregate"], evaluation["baseline_aggregate"]
    excess = strategy["percentage_return"] - baseline["percentage_return"]
    return {
        "status": "segmented_evaluated",
        "selected_candidate": selected.as_dict(),
        "strategy_return": strategy["percentage_return"],
        "buy_and_hold_return": baseline["percentage_return"],
        "excess_return": excess,
        "paper_trial_supported": False,
        "recommendation": "Review segmented research; it does not support a paper trial by itself.",
        "message": "Independent source segments were evaluated with resets at every gap.",
    }


def run_research(
    arguments: argparse.Namespace, settings: ExperimentSettings, *, store: ObjectStorage | None = None
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.backtest.storage)
    output = arguments.output or child_uri(settings.output_prefix, "gap_aware_research")
    _safe_output(output, settings, arguments.local_development)
    body = storage.read_bytes(arguments.spec)
    spec = load_experiment_spec(
        body, spec_uri=arguments.spec, expected_sha256=arguments.spec_sha256,
        allowed_spec_prefix=settings.spec_prefix, local_development=arguments.local_development,
        maximum_candidates=5, maximum_candidate_candle_evaluations=settings.maximum_candidate_candle_evaluations,
    )
    if (
        spec.symbol != "BTC-USD"
        or spec.train.end != spec.validation.start
        or spec.validation.end != spec.test.start
    ):
        raise InvalidBacktestInput("gap-aware research requires adjacent BTC-USD train, validation and test ranges")
    if spec.gap_policy_uri is None or spec.gap_policy_sha256 is None:
        raise InvalidBacktestInput("gap-aware research requires a pinned gap policy")
    source = _source(spec, settings, storage, local_development=arguments.local_development)
    if source.manifest.get("candle_schema_version") != HISTORICAL_CANDLE_SCHEMA_VERSION:
        raise InvalidBacktestInput("gap-aware research requires the approved historical candle source")
    validate_distinct_prefixes(source.output_uri, output)
    policy = load_gap_policy(spec, storage, settings, local_development=arguments.local_development)
    if policy is None:
        raise InvalidBacktestInput("gap-aware research requires a valid pending policy document")
    review = _approved_review(storage, review_uri=arguments.review, review_sha256=arguments.review_sha256, spec=spec)
    key = _sha(canonical_json_bytes({"version": VERSION, "spec": spec.canonical_sha256, "review": arguments.review_sha256}))
    registration = {"version": VERSION, "research_key": key, "spec_sha256": _sha(body), "review_sha256": arguments.review_sha256}
    _publish(storage, child_uri(output, "registrations", VERSION, spec.name + ".json"), registration)
    report_uri = child_uri(output, "reports", key, "manifest.json")
    existing = storage.try_read_bytes(report_uri)
    if existing is not None:
        report = json.loads(existing)
        if report.get("research_key") != key:
            raise InvalidBacktestInput("existing gap-aware report conflicts with registration")
        return {**report, "manifest_uri": report_uri, "manifest_sha256": _sha(existing)}
    with TemporaryDirectory(prefix="gap-aware-research-") as temporary:
        frozen = Path(temporary)
        inventory, rows = _freeze(storage, source, spec, frozen)
        coverage = coverage_report(spec, rows)
        observed = tuple((item["start"], item["end"]) for item in coverage["missing_ranges"])
        if observed != policy.declared_missing_ranges:
            raise InvalidBacktestInput("source-gap inventory differs from the approved policy")
        if review["evidence"].get("source_gaps") != [
            {"start": start.isoformat().replace("+00:00", "Z"), "end": end.isoformat().replace("+00:00", "Z")}
            for start, end in policy.declared_missing_ranges
        ]:
            raise InvalidBacktestInput("approved review source-gap inventory differs from policy")
        frozen_source = replace(source, output_uri=str(frozen))
        # Source and policy bytes are pinned before any close/open column is loaded.
        segments = _segment_report(spec, rows)
        selected, selection = _candidate_selection(
            spec, segments, source=frozen_source, settings=settings
        )
        selection_document = {
            "version": VERSION,
            "status": "published",
            "research_key": key,
            "selection_basis": "train_and_validation_contiguous_segments_only",
            "aggregation": AGGREGATION_VERSION,
            "selected_candidate": selected.as_dict() if selected else None,
            "test_prices_accessed": False,
            "segments": {name: [_range_dict(value) for value in values] for name, values in segments.items() if name != "test"},
            "evidence": selection,
        }
        selection_ref = _publish(storage, child_uri(output, "selections", key, "manifest.json"), selection_document)
        evaluation = evaluation_ref = None
        if selected is not None:
            metrics, per_segment, exclusions = evaluate_candidate_segments(
                selected, segments["test"], source=frozen_source, spec=spec, settings=settings, test_unlocked=True
            )
            baseline_results = [item["baseline"] for item in per_segment]
            baseline_metrics = {
                "aggregation": AGGREGATION_VERSION,
                "segment_count": len(baseline_results),
                "percentage_return": sum((item["percentage_return"] for item in baseline_results), Decimal(0)) / Decimal(len(baseline_results)),
                "total_fees": sum((item["total_fees"] for item in baseline_results), Decimal(0)),
            }
            evaluation = {"aggregate": metrics, "baseline_aggregate": baseline_metrics, "segments": per_segment, "excluded_segments": exclusions}
            evaluation_document = {
                "version": VERSION, "status": "published", "research_key": key,
                "selection_manifest": selection_ref, "selected_candidate": selected.as_dict(),
                "evaluation_basis": "valid_test_contiguous_segments_only", "evaluation": evaluation,
                "research_only": True, "paper_trial_supported": False,
            }
            evaluation_ref = _publish(storage, child_uri(output, "evaluations", key, "manifest.json"), evaluation_document)
        report = {
            "version": VERSION, "status": "published", "research_key": key,
            "spec": {"uri": arguments.spec, "sha256": _sha(body)},
            "review": {"uri": arguments.review, "sha256": arguments.review_sha256, "reviewer": review.get("reviewer")},
            "source": {"manifest_uri": source.manifest_uri, "manifest_sha256": source.manifest_sha256, "candle_files": inventory},
            "gap_policy": {"uri": spec.gap_policy_uri, "sha256": spec.gap_policy_sha256, "actions": {"missing": "exclude_and_reset", "indicator": "reset_after_gap", "pending": "cancel_at_gap", "position": "exclude_cross_gap_returns"}},
            "source_gaps": coverage["missing_ranges"],
            "segments": {name: [_range_dict(value) for value in values] for name, values in segments.items()},
            "selection": selection_ref, "evaluation": evaluation_ref,
            "summary": _summary(selected, evaluation), "research_only": True,
            "test_prices_accessed_before_selection": False,
        }
        ref = _publish(storage, report_uri, report)
    return {**json.loads(canonical_json_bytes(report)), "manifest_uri": ref["uri"], "manifest_sha256": ref["sha256"]}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run sealed gap-aware BTC historical research.")
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--review", required=True)
    parser.add_argument("--review-sha256", required=True)
    parser.add_argument("--output")
    parser.add_argument("--local-development", action="store_true")
    try:
        report = run_research(parser.parse_args(argv), ExperimentSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Gap-aware research rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("GAP_AWARE_RESEARCH_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
