"""Fixed exploratory screen for a daily BTC selloff-reversal hypothesis.

The screen deliberately reads an already sealed historical candle publication.
It separates train, validation, and reporting intervals in its calculation, but
does not claim that the reporting interval is newly observed or trade-ready.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.daily_breakout import _decode, _write_bytes
from crypto_trading_core.daily_momentum import DAY, DailyCandle, buy_and_hold

VERSION = "daily-mean-reversion-historical-screen-v1"


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.time() != datetime.min.time()
    ):
        raise ValueError("daily boundaries must be UTC midnight")
    return parsed.astimezone(UTC)


def _decimal(value: object) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("numeric assumptions must be finite")
    return result


def load_spec(path: Path, digest: str) -> dict[str, Any]:
    body = path.read_bytes()
    if _hash(body) != digest.lower():
        raise ValueError("mean-reversion screen specification SHA-256 does not match")
    spec = json.loads(body)
    if not isinstance(spec, dict) or spec.get("version") != VERSION:
        raise ValueError("unsupported mean-reversion screen specification")
    if spec.get("name") != "btc-usd-daily-selloff-reversal-screen-v1":
        raise ValueError("mean-reversion screen name is invalid")
    if (spec.get("source"), spec.get("symbol"), spec.get("granularity_seconds")) != (
        "coinbase-exchange",
        "BTC-USD",
        86400,
    ):
        raise ValueError("mean-reversion screen requires Coinbase BTC-USD daily candles")
    ranges = spec.get("ranges")
    if not isinstance(ranges, dict):
        raise TypeError("screen ranges are invalid")
    train, validation, reporting = (
        (_date(ranges[name]["start"]), _date(ranges[name]["end"]))
        for name in ("train", "validation", "reporting")
    )
    if not train[0] < train[1] == validation[0] < validation[1] == reporting[0] < reporting[1]:
        raise ValueError("screen ranges must be contiguous and nonempty")
    warmup = _date(spec["source_warmup_start"])
    source_end = _date(spec["source_end"])
    if not warmup < train[0] or source_end != reporting[1] or source_end > datetime.now(UTC):
        raise ValueError("screen source boundaries are invalid")
    candidates = spec.get("candidates")
    if not isinstance(candidates, list) or not 2 <= len(candidates) <= 10:
        raise ValueError("screen candidate grid is invalid")
    identifiers = set()
    for candidate in candidates:
        if not isinstance(candidate, dict) or not isinstance(candidate.get("id"), str):
            raise TypeError("screen candidate identifier is invalid")
        identifiers.add(candidate["id"])
        if not 0 < _decimal(candidate.get("selloff_threshold_pct")) <= 30:
            raise ValueError("selloff threshold must be between zero and thirty percent")
        if (
            type(candidate.get("holding_days")) is not int
            or not 1 <= candidate["holding_days"] <= 30
        ):
            raise ValueError("holding days must be an integer between one and thirty")
    if len(identifiers) != len(candidates):
        raise ValueError("screen candidate identifiers must be unique")
    if (
        _decimal(spec["starting_cash"]) <= 0
        or _decimal(spec["fee_bps"]) < 0
        or _decimal(spec["slippage_bps"]) < 0
        or spec.get("cost_multipliers") != [1, 2, 3]
        or type(spec.get("minimum_train_entries")) is not int
        or type(spec.get("minimum_validation_entries")) is not int
        or spec["minimum_train_entries"] < 1
        or spec["minimum_validation_entries"] < 1
        or not 0 < _decimal(spec["maximum_drawdown"]) <= 1
    ):
        raise ValueError("screen costs or gates are invalid")
    report = spec.get("source_report")
    if not isinstance(report, dict) or not isinstance(report.get("path"), str):
        raise TypeError("screen source report is invalid")
    if not isinstance(report.get("sha256"), str) or len(report["sha256"]) != 64:
        raise ValueError("screen source report SHA-256 is invalid")
    return spec


def _load_source(spec: dict[str, Any], base: Path) -> tuple[DailyCandle, ...]:
    report_path = (base / spec["source_report"]["path"]).resolve()
    report_body = report_path.read_bytes()
    if _hash(report_body) != spec["source_report"]["sha256"]:
        raise ValueError("sealed source report SHA-256 does not match")
    report = json.loads(report_body)
    chunks = report.get("source_chunks")
    if not isinstance(chunks, list) or not chunks:
        raise ValueError("sealed source report has no source chunks")
    candles: tuple[DailyCandle, ...] = ()
    expected_start = _date(spec["source_warmup_start"])
    for chunk in chunks:
        if not isinstance(chunk, dict) or not isinstance(chunk.get("uri"), str):
            raise TypeError("sealed source report has an invalid source chunk")
        start, end = _date(str(chunk["start"])), _date(str(chunk["end"]))
        if start != expected_start or not start < end:
            raise ValueError("sealed source chunks are not continuous")
        source_path = (base / chunk["uri"]).resolve()
        body = source_path.read_bytes()
        if _hash(body) != chunk.get("sha256"):
            raise ValueError("sealed source chunk SHA-256 does not match")
        candles += _decode(body, start, end)
        expected_start = end
    if expected_start != _date(spec["source_end"]):
        raise ValueError("sealed source chunks do not cover the screen interval")
    return candles


def simulate(
    candles: tuple[DailyCandle, ...],
    *,
    start: datetime,
    end: datetime,
    selloff_threshold_pct: Decimal,
    holding_days: int,
    cash: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> dict[str, Decimal | int | bool | str | None]:
    """Buy next open after a prior-close selloff and exit after fixed daily holds."""

    evaluation = [index for index, candle in enumerate(candles) if start <= candle.start < end]
    if not evaluation or len(evaluation) != (end - start).days or evaluation[0] < 1:
        raise ValueError("mean-reversion evaluation has missing daily candles or warmup")
    if any(candles[index].start != start + day * DAY for day, index in enumerate(evaluation)):
        raise ValueError("mean-reversion evaluation has non-contiguous daily candles")
    with localcontext() as context:
        context.prec = 50
        initial = peak = cash
        base = fees = drawdown = Decimal(0)
        entries = exits = 0
        exit_at: int | None = None
        pending_entry = False
        fee_rate, slip_rate = fee_bps / 10000, slippage_bps / 10000
        for index in evaluation:
            candle = candles[index]
            if base > 0 and exit_at is not None and index >= exit_at:
                execution = candle.open * (1 - slip_rate)
                fee = base * execution * fee_rate
                cash = base * execution - fee
                fees += fee
                base = Decimal(0)
                exits += 1
                exit_at = None
            if base == 0 and pending_entry:
                execution = candle.open * (1 + slip_rate)
                base = cash / (execution * (1 + fee_rate))
                fees += base * execution * fee_rate
                cash = Decimal(0)
                entries += 1
                exit_at = index + holding_days
                pending_entry = False
            previous_close = candles[index - 1].close
            daily_return_pct = (candle.close / previous_close - 1) * 100
            if base == 0 and not pending_entry and daily_return_pct <= -selloff_threshold_pct:
                pending_entry = True
            equity = cash + base * candle.close
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        return {
            "percentage_return": (equity / initial - 1) * 100,
            "maximum_drawdown": drawdown,
            "fill_count": entries + exits,
            "entries": entries,
            "completed_round_trips": exits,
            "total_fees": fees,
            "ending_equity": equity,
            "open_position": base > 0,
            "unfilled_final_signal": "buy" if pending_entry else None,
        }


def _with_baseline(
    candles: tuple[DailyCandle, ...],
    *,
    start: datetime,
    end: datetime,
    candidate: dict[str, Any],
    spec: dict[str, Any],
    multiplier: int,
) -> dict[str, Any]:
    cash = _decimal(spec["starting_cash"])
    fee = _decimal(spec["fee_bps"]) * multiplier
    slippage = _decimal(spec["slippage_bps"]) * multiplier
    strategy = simulate(
        candles,
        start=start,
        end=end,
        selloff_threshold_pct=_decimal(candidate["selloff_threshold_pct"]),
        holding_days=candidate["holding_days"],
        cash=cash,
        fee_bps=fee,
        slippage_bps=slippage,
    )
    baseline = buy_and_hold(
        candles, start=start, end=end, cash=cash, fee_bps=fee, slippage_bps=slippage
    )
    return {
        "strategy": strategy,
        "buy_and_hold": baseline,
        "excess_return_pct": strategy["percentage_return"] - baseline["percentage_return"],
    }


def _selection_reasons(result: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for interval, minimum_entries in (
        ("train", spec["minimum_train_entries"]),
        ("validation", spec["minimum_validation_entries"]),
    ):
        strategy = result[interval]["strategy"]
        if strategy["entries"] < minimum_entries:
            reasons.append(f"{interval}_insufficient_entries")
        if strategy["percentage_return"] <= 0:
            reasons.append(f"{interval}_return_not_positive")
        if result[interval]["excess_return_pct"] <= 0:
            reasons.append(f"{interval}_excess_return_not_positive")
        if strategy["maximum_drawdown"] > _decimal(spec["maximum_drawdown"]):
            reasons.append(f"{interval}_drawdown_exceeds_limit")
    return reasons


def run(spec_path: Path, spec_digest: str, output: Path, *, base: Path) -> dict[str, Any]:
    spec = load_spec(spec_path, spec_digest)
    root = output / spec["name"] / spec_digest.lower()
    report_path = root / "report.json"
    if report_path.exists():
        existing = report_path.read_bytes()
        if _hash(existing) != _hash(canonical_json_bytes(json.loads(existing))):
            raise ValueError("existing screen report is not canonical JSON")
        return json.loads(existing)
    candles = _load_source(spec, base)
    ranges = {
        name: (_date(spec["ranges"][name]["start"]), _date(spec["ranges"][name]["end"]))
        for name in ("train", "validation", "reporting")
    }
    candidate_results: list[dict[str, Any]] = []
    for candidate in spec["candidates"]:
        result = {"candidate": candidate}
        for name in ("train", "validation"):
            result[name] = _with_baseline(
                candles,
                start=ranges[name][0],
                end=ranges[name][1],
                candidate=candidate,
                spec=spec,
                multiplier=1,
            )
        result["selection_failure_reasons"] = _selection_reasons(result, spec)
        candidate_results.append(result)
    eligible = sorted(
        (item for item in candidate_results if not item["selection_failure_reasons"]),
        key=lambda item: (-item["validation"]["excess_return_pct"], item["candidate"]["id"]),
    )
    selected = eligible[0]["candidate"] if eligible else None
    scenarios: list[dict[str, Any]] = []
    reporting_reasons: list[str] = []
    if selected is not None:
        for multiplier in spec["cost_multipliers"]:
            outcome = _with_baseline(
                candles,
                start=ranges["reporting"][0],
                end=ranges["reporting"][1],
                candidate=selected,
                spec=spec,
                multiplier=multiplier,
            )
            scenarios.append({"cost_multiplier": multiplier, **outcome})
            if multiplier <= 2:
                strategy = outcome["strategy"]
                if strategy["completed_round_trips"] < 1:
                    reporting_reasons.append(f"{multiplier}x_reporting_has_no_completed_round_trip")
                if strategy["percentage_return"] <= 0:
                    reporting_reasons.append(f"{multiplier}x_reporting_return_not_positive")
                if outcome["excess_return_pct"] <= 0:
                    reporting_reasons.append(f"{multiplier}x_reporting_excess_return_not_positive")
                if strategy["maximum_drawdown"] > _decimal(spec["maximum_drawdown"]):
                    reporting_reasons.append(f"{multiplier}x_reporting_drawdown_exceeds_limit")
    report = {
        "version": VERSION,
        "status": "exploratory_historical_screen",
        "spec_sha256": spec_digest.lower(),
        "source_report": spec["source_report"],
        "ranges": spec["ranges"],
        "candidate_results": candidate_results,
        "selected_candidate": selected,
        "selection_status": "selected" if selected is not None else "no_candidate_selected",
        "reporting_scenarios": scenarios,
        "research_gate_passed": selected is not None and not reporting_reasons,
        "research_gate_failure_reasons": reporting_reasons,
        "paper_trial_eligible": False,
        "paper_trial_blockers": [
            "exploratory_history_previously_accessed",
            "manual_review_required",
            *(["no_candidate_selected"] if selected is None else []),
            *reporting_reasons,
        ],
    }
    _write_bytes(report_path, canonical_json_bytes(report))
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Screen a fixed BTC daily selloff-reversal hypothesis"
    )
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    try:
        report = run(args.spec, args.spec_sha256, args.output, base=args.base)
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"Historical mean-reversion screen rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print(
        "HISTORICAL_MEAN_REVERSION_SCREEN_JSON="
        + canonical_json_bytes(
            {
                "report_uri": str(
                    args.output / args.spec.stem / report["spec_sha256"] / "report.json"
                ),
                "selection_status": report["selection_status"],
                "selected_candidate": report["selected_candidate"],
                "research_gate_passed": report["research_gate_passed"],
            }
        ).decode("ascii")
    )


if __name__ == "__main__":
    main()
