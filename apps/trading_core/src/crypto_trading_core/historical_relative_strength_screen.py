"""Immediate, fixed historical screen for cross-asset daily relative strength."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from crypto_exchange_adapters.coinbase_rest import CoinbaseRestError

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.daily_breakout import _decode, _write_bytes
from crypto_trading_core.daily_momentum import DailyCandle, buy_and_hold, fetch_candles

VERSION = "daily-relative-strength-historical-screen-v1"


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
        raise ValueError("relative-strength screen specification SHA-256 does not match")
    spec = json.loads(body)
    if not isinstance(spec, dict) or spec.get("version") != VERSION:
        raise ValueError("unsupported relative-strength screen specification")
    if spec.get("name") != "usd-spot-daily-relative-strength-screen-v1":
        raise ValueError("relative-strength screen name is invalid")
    products = spec.get("products")
    if products != ["BTC-USD", "ETH-USD", "LTC-USD", "BCH-USD"]:
        raise ValueError("relative-strength screen has an unsupported product universe")
    if spec.get("source") != "coinbase-exchange" or spec.get("granularity_seconds") != 86400:
        raise ValueError("relative-strength screen requires Coinbase daily candles")
    warmup, source_end = (_date(spec[key]) for key in ("warmup_start", "source_end"))
    ranges = spec.get("ranges")
    if not isinstance(ranges, dict):
        raise TypeError("relative-strength screen ranges are invalid")
    train, validation, reporting = (
        (_date(ranges[name]["start"]), _date(ranges[name]["end"]))
        for name in ("train", "validation", "reporting")
    )
    if not (
        warmup
        < train[0]
        < train[1]
        == validation[0]
        < validation[1]
        == reporting[0]
        < reporting[1]
        == source_end
        <= datetime.now(UTC)
    ):
        raise ValueError("relative-strength source or partition dates are invalid")
    candidates = spec.get("candidates")
    if not isinstance(candidates, list) or not 2 <= len(candidates) <= 6:
        raise ValueError("relative-strength candidate grid is invalid")
    identifiers = set()
    for candidate in candidates:
        if not isinstance(candidate, dict) or not isinstance(candidate.get("id"), str):
            raise TypeError("relative-strength candidate identifier is invalid")
        identifiers.add(candidate["id"])
        if (
            type(candidate.get("lookback_days")) is not int
            or not 7 <= candidate["lookback_days"] <= 180
        ):
            raise ValueError("relative-strength lookback must be 7..180 days")
        if candidate.get("rebalance_days") != 7:
            raise ValueError("relative-strength screen requires weekly rebalancing")
    if len(identifiers) != len(candidates):
        raise ValueError("relative-strength candidate identifiers must be unique")
    if (train[0] - warmup).days < max(item["lookback_days"] for item in candidates) + 1:
        raise ValueError("relative-strength warmup is incomplete")
    if (
        type(spec.get("request_days")) is not int
        or not 1 <= spec["request_days"] <= 300
        or _decimal(spec["starting_cash"]) <= 0
        or _decimal(spec["fee_bps"]) < 0
        or _decimal(spec["slippage_bps"]) < 0
        or spec.get("cost_multipliers") != [1, 2, 3]
        or type(spec.get("minimum_train_round_trips")) is not int
        or type(spec.get("minimum_validation_round_trips")) is not int
        or spec["minimum_train_round_trips"] < 1
        or spec["minimum_validation_round_trips"] < 1
        or not 0 < _decimal(spec["maximum_drawdown"]) <= 1
    ):
        raise ValueError("relative-strength costs or selection gates are invalid")
    return spec


def _source(
    spec: dict[str, Any], root: Path
) -> tuple[dict[str, tuple[DailyCandle, ...]], list[dict[str, Any]]]:
    start, end = _date(spec["warmup_start"]), _date(spec["source_end"])
    source: dict[str, tuple[DailyCandle, ...]] = {}
    source_chunks: list[dict[str, Any]] = []
    for product in spec["products"]:
        candles: tuple[DailyCandle, ...] = ()
        cursor = start
        sequence = 1
        while cursor < end:
            next_end = min(cursor + timedelta(days=spec["request_days"]), end)
            path = root / "sources" / product / f"chunk-{sequence:03d}.json"
            if path.exists():
                body = path.read_bytes()
                batch = _decode(body, cursor, next_end)
            else:
                batch, body = fetch_candles(cursor, next_end, product_id=product)
                _write_bytes(path, body)
            source_chunks.append(
                {
                    "product": product,
                    "start": cursor,
                    "end": next_end,
                    "uri": str(path),
                    "sha256": _hash(body),
                }
            )
            candles += batch
            cursor, sequence = next_end, sequence + 1
        if len(candles) != (end - start).days:
            raise ValueError(f"{product} source does not have complete daily coverage")
        source[product] = candles
    return source, source_chunks


def simulate(
    candles_by_product: dict[str, tuple[DailyCandle, ...]],
    *,
    start: datetime,
    end: datetime,
    lookback_days: int,
    rebalance_days: int,
    cash: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> dict[str, Decimal | int | str | None]:
    """Hold the top trailing-return asset, selecting before the next daily open."""

    products = tuple(sorted(candles_by_product))
    if not products:
        raise ValueError("relative-strength universe is empty")
    reference = candles_by_product[products[0]]
    evaluation = [index for index, candle in enumerate(reference) if start <= candle.start < end]
    if not evaluation or len(evaluation) != (end - start).days or evaluation[0] < lookback_days + 1:
        raise ValueError("relative-strength evaluation has missing candles or warmup")
    for product in products:
        candles = candles_by_product[product]
        if len(candles) != len(reference) or any(
            candle.start != reference[index].start for index, candle in enumerate(candles)
        ):
            raise ValueError("relative-strength asset calendars do not match")
    with localcontext() as context:
        context.prec = 50
        initial = peak = cash
        held_product: str | None = None
        quantity = fees = drawdown = Decimal(0)
        fills = completed_round_trips = rebalances = 0
        fee_rate, slippage_rate = fee_bps / 10000, slippage_bps / 10000
        for offset, index in enumerate(evaluation):
            if offset % rebalance_days == 0:
                returns = {
                    product: (
                        candles_by_product[product][index - 1].close
                        / candles_by_product[product][index - 1 - lookback_days].close
                        - 1
                    )
                    for product in products
                }
                target = max(products, key=lambda product: (returns[product], product))
                rebalances += 1
                if target != held_product:
                    if held_product is not None:
                        execution = candles_by_product[held_product][index].open * (
                            1 - slippage_rate
                        )
                        fee = quantity * execution * fee_rate
                        cash = quantity * execution - fee
                        fees += fee
                        quantity = Decimal(0)
                        fills += 1
                        completed_round_trips += 1
                    execution = candles_by_product[target][index].open * (1 + slippage_rate)
                    quantity = cash / (execution * (1 + fee_rate))
                    fee = quantity * execution * fee_rate
                    fees += fee
                    cash = Decimal(0)
                    fills += 1
                    held_product = target
            equity = cash + quantity * candles_by_product[held_product][index].close
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        return {
            "percentage_return": (equity / initial - 1) * 100,
            "maximum_drawdown": drawdown,
            "fill_count": fills,
            "completed_round_trips": completed_round_trips,
            "rebalance_count": rebalances,
            "total_fees": fees,
            "ending_equity": equity,
            "final_product": held_product,
        }


def _outcome(
    candles_by_product: dict[str, tuple[DailyCandle, ...]],
    *,
    start: datetime,
    end: datetime,
    candidate: dict[str, Any],
    spec: dict[str, Any],
    multiplier: int,
) -> dict[str, Any]:
    cash = _decimal(spec["starting_cash"])
    fee, slippage = (
        _decimal(spec["fee_bps"]) * multiplier,
        _decimal(spec["slippage_bps"]) * multiplier,
    )
    strategy = simulate(
        candles_by_product,
        start=start,
        end=end,
        lookback_days=candidate["lookback_days"],
        rebalance_days=candidate["rebalance_days"],
        cash=cash,
        fee_bps=fee,
        slippage_bps=slippage,
    )
    baseline = buy_and_hold(
        candles_by_product["BTC-USD"],
        start=start,
        end=end,
        cash=cash,
        fee_bps=fee,
        slippage_bps=slippage,
    )
    return {
        "strategy": strategy,
        "buy_and_hold_btc": baseline,
        "excess_return_pct": strategy["percentage_return"] - baseline["percentage_return"],
    }


def _selection_reasons(result: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for name, minimum_round_trips in (
        ("train", spec["minimum_train_round_trips"]),
        ("validation", spec["minimum_validation_round_trips"]),
    ):
        strategy = result[name]["strategy"]
        if strategy["completed_round_trips"] < minimum_round_trips:
            reasons.append(f"{name}_insufficient_round_trips")
        if strategy["percentage_return"] <= 0:
            reasons.append(f"{name}_return_not_positive")
        if result[name]["excess_return_pct"] <= 0:
            reasons.append(f"{name}_excess_return_not_positive")
        if strategy["maximum_drawdown"] > _decimal(spec["maximum_drawdown"]):
            reasons.append(f"{name}_drawdown_exceeds_limit")
    return reasons


def _validate_cached_source(report: dict[str, Any]) -> None:
    chunks = report.get("source_chunks")
    if not isinstance(chunks, list) or not chunks:
        raise ValueError("existing relative-strength report has no source chunks")
    for chunk in chunks:
        if not isinstance(chunk, dict) or not isinstance(chunk.get("uri"), str):
            raise TypeError("existing relative-strength source chunk is invalid")
        body = Path(chunk["uri"]).read_bytes()
        if _hash(body) != chunk.get("sha256"):
            raise ValueError("sealed relative-strength source SHA-256 does not match")


def run(spec_path: Path, digest: str, output: Path) -> dict[str, Any]:
    spec = load_spec(spec_path, digest)
    root = output / spec["name"] / digest.lower()
    report_path = root / "report.json"
    if report_path.exists():
        body = report_path.read_bytes()
        if body != canonical_json_bytes(json.loads(body)):
            raise ValueError("existing relative-strength report is not canonical JSON")
        existing = json.loads(body)
        _validate_cached_source(existing)
        return existing
    candles_by_product, source_chunks = _source(spec, root)
    ranges = {
        name: (_date(spec["ranges"][name]["start"]), _date(spec["ranges"][name]["end"]))
        for name in ("train", "validation", "reporting")
    }
    candidate_results: list[dict[str, Any]] = []
    for candidate in spec["candidates"]:
        result: dict[str, Any] = {"candidate": candidate}
        for name in ("train", "validation"):
            result[name] = _outcome(
                candles_by_product,
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
    selected_candidate = eligible[0]["candidate"] if eligible else None
    reporting_scenarios: list[dict[str, Any]] = []
    reporting_reasons: list[str] = []
    if selected_candidate is not None:
        for multiplier in spec["cost_multipliers"]:
            outcome = _outcome(
                candles_by_product,
                start=ranges["reporting"][0],
                end=ranges["reporting"][1],
                candidate=selected_candidate,
                spec=spec,
                multiplier=multiplier,
            )
            reporting_scenarios.append({"cost_multiplier": multiplier, **outcome})
            if multiplier <= 2:
                strategy = outcome["strategy"]
                if strategy["completed_round_trips"] < 1:
                    reporting_reasons.append(f"{multiplier}x_reporting_insufficient_round_trips")
                if strategy["percentage_return"] <= 0:
                    reporting_reasons.append(f"{multiplier}x_reporting_return_not_positive")
                if outcome["excess_return_pct"] <= 0:
                    reporting_reasons.append(f"{multiplier}x_reporting_excess_return_not_positive")
                if strategy["maximum_drawdown"] > _decimal(spec["maximum_drawdown"]):
                    reporting_reasons.append(f"{multiplier}x_reporting_drawdown_exceeds_limit")
    report = {
        "version": VERSION,
        "status": "exploratory_historical_screen",
        "spec_sha256": digest.lower(),
        "source_chunks": source_chunks,
        "ranges": spec["ranges"],
        "candidate_results": candidate_results,
        "selected_candidate": selected_candidate,
        "selection_status": "selected"
        if selected_candidate is not None
        else "no_candidate_selected",
        "reporting_scenarios": reporting_scenarios,
        "research_gate_passed": selected_candidate is not None and not reporting_reasons,
        "research_gate_failure_reasons": reporting_reasons,
        "paper_trial_eligible": False,
        "paper_trial_blockers": [
            "exploratory_historical_result",
            "manual_review_required",
            *(["no_candidate_selected"] if selected_candidate is None else []),
            *reporting_reasons,
        ],
    }
    _write_bytes(report_path, canonical_json_bytes(report))
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Screen fixed Coinbase daily relative-strength candidates"
    )
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = run(args.spec, args.spec_sha256, args.output)
    except (OSError, ValueError, TypeError, KeyError, CoinbaseRestError) as error:
        print(f"Historical relative-strength screen rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print(
        "HISTORICAL_RELATIVE_STRENGTH_SCREEN_JSON="
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
