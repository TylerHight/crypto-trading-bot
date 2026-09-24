"""Sealed, research-only BTC daily momentum study on Coinbase daily candles."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from urllib.parse import urlencode

from crypto_trading_core.contracts import canonical_json_bytes

DAY = timedelta(days=1)
ENDPOINT = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
SPEC_VERSION = "daily-momentum-study-v1"
# Calculation and promotion semantics are versioned separately from frozen specs.
VERSION = "daily-momentum-study-v2"
POLICY_VERSION = "daily-momentum-promotion-v2"
FORWARD_VERSION = "daily-momentum-forward-v2"
PAPER_UNSUPPORTED = "daily_paper_execution_not_supported"


@dataclass(frozen=True)
class DailyCandle:
    start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def _date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or parsed.time() != datetime.min.time():
        raise ValueError("daily boundaries must be UTC midnight")
    return parsed.astimezone(UTC)


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def load_spec(path: Path, digest: str) -> dict:
    body = path.read_bytes()
    if _hash(body) != digest.lower():
        raise ValueError("study specification SHA-256 does not match")
    spec = json.loads(body)
    if spec.get("version") != SPEC_VERSION or spec.get("source") != "coinbase-exchange":
        raise ValueError("unsupported study specification")
    if spec.get("symbol") != "BTC-USD" or spec.get("granularity_seconds") != 86400:
        raise ValueError("study requires Coinbase BTC-USD daily candles")
    train, validation, test = (spec["ranges"][name] for name in ("train", "validation", "test"))
    if not (
        _date(spec["warmup_start"]) < _date(train["start"]) < _date(train["end"])
        == _date(validation["start"]) < _date(validation["end"])
        == _date(test["start"]) < _date(test["end"])
        <= datetime.now(UTC)
    ):
        raise ValueError("daily study ranges are invalid")
    candidates = spec["candidates"]
    if not 2 <= len(candidates) <= 10 or len({item["id"] for item in candidates}) != len(candidates):
        raise ValueError("candidate grid is invalid")
    for item in candidates:
        if not (0 < item["fast_days"] < item["slow_days"] <= 90):
            raise ValueError("candidate periods are invalid")
    if (_date(train["start"]) - _date(spec["warmup_start"])).days < max(
        item["slow_days"] - 1 for item in candidates
    ):
        raise ValueError("daily warmup is incomplete")
    if Decimal(spec["starting_cash"]) <= 0 or Decimal(spec["fee_bps"]) < 0 or Decimal(spec["slippage_bps"]) < 0:
        raise ValueError("portfolio assumptions are invalid")
    if spec["cost_multipliers"] != [1, 2, 3]:
        raise ValueError("cost sensitivity grid must be 1x, 2x, 3x")
    return spec


def decode_candles(body: bytes, start: datetime, end: datetime) -> tuple[DailyCandle, ...]:
    rows = json.loads(body, parse_float=Decimal)
    if not isinstance(rows, list) or len(rows) > 300:
        raise ValueError("daily candle response is invalid")
    by_day: dict[datetime, DailyCandle] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) != 6:
            raise ValueError("daily candle row is invalid")
        stamp = datetime.fromtimestamp(row[0], UTC)
        if stamp.time() != datetime.min.time() or not start <= stamp < end:
            continue
        low, high, opening, close, volume = (Decimal(value) for value in row[1:])
        if not (0 < low <= opening <= high and low <= close <= high and volume > 0):
            raise ValueError("daily candle OHLCV bounds are invalid")
        candle = DailyCandle(stamp, opening, high, low, close, volume)
        if stamp in by_day and by_day[stamp] != candle:
            raise ValueError("conflicting daily candle")
        by_day[stamp] = candle
    expected = (end - start).days
    ordered = tuple(by_day[stamp] for stamp in sorted(by_day))
    if len(ordered) != expected or any(
        candle.start != start + index * DAY for index, candle in enumerate(ordered)
    ):
        raise ValueError("daily source has missing candle days")
    return ordered


def fetch_candles(start: datetime, end: datetime) -> tuple[tuple[DailyCandle, ...], bytes]:
    if not 1 <= (end - start).days <= 300:
        raise ValueError("daily source request must cover 1..300 days")
    query = urlencode(
        {
            "granularity": 86400,
            "start": start.isoformat(),
            "end": (end - DAY).isoformat(),
        }
    )
    from crypto_exchange_adapters.coinbase_rest import UrllibHttpTransport

    transport = UrllibHttpTransport()
    for attempt in range(4):
        response = transport.request(
            ENDPOINT + "?" + query,
            {"Accept": "application/json", "User-Agent": "crypto-daily-research/1.0"},
            15.0,
        )
        if response.status == 200:
            return decode_candles(response.body, start, end), response.body
        if response.status not in {429, 500, 502, 503, 504} or attempt == 3:
            raise ValueError(f"Coinbase daily candles returned HTTP {response.status}")
        time.sleep(min(2**attempt, 5))
    raise AssertionError("bounded source retry exited unexpectedly")


def simulate(
    candles: tuple[DailyCandle, ...], *, start: datetime, end: datetime,
    fast: int, slow: int, cash: Decimal, fee_bps: Decimal,
    slippage_bps: Decimal,
) -> dict:
    """Evaluate independent range state with decisions at close and next-open fills."""
    with localcontext() as context:
        context.prec = 50
        evaluation = [index for index, candle in enumerate(candles) if start <= candle.start < end]
        if len(evaluation) != (end - start).days:
            raise ValueError("evaluation has missing days")
        if evaluation[0] < slow - 1:
            raise ValueError("strategy warmup is incomplete")
        initial = cash
        base = Decimal(0)
        target = False
        pending: bool | None = None
        peak = cash
        drawdown = Decimal(0)
        fills = 0
        round_trips = 0
        fees = Decimal(0)
        for index in evaluation:
            candle = candles[index]
            if pending is not None:
                fee_rate = fee_bps / 10000
                slip_rate = slippage_bps / 10000
                if pending:
                    execution = candle.open * (1 + slip_rate)
                    quantity = cash / (execution * (1 + fee_rate))
                    fee = quantity * execution * fee_rate
                    cash -= quantity * execution + fee
                    base = quantity
                else:
                    execution = candle.open * (1 - slip_rate)
                    fee = base * execution * fee_rate
                    cash += base * execution - fee
                    base = Decimal(0)
                    round_trips += 1
                fees += fee
                fills += 1
                pending = None
            closes = [item.close for item in candles[index - slow + 1 : index + 1]]
            wanted = sum(closes[-fast:]) / fast > sum(closes) / slow
            if wanted != target:
                target = wanted
                pending = wanted
            equity = cash + base * candle.close
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        return {
            "percentage_return": (equity / initial - 1) * 100,
            "maximum_drawdown": drawdown,
            "fill_count": fills,
            "completed_round_trips": round_trips,
            "total_fees": fees,
            "ending_equity": equity,
        }


def buy_and_hold(
    candles: tuple[DailyCandle, ...], *, start: datetime, end: datetime,
    cash: Decimal, fee_bps: Decimal, slippage_bps: Decimal,
) -> dict:
    evaluation = tuple(candle for candle in candles if start <= candle.start < end)
    if len(evaluation) != (end - start).days:
        raise ValueError("baseline has missing days")
    with localcontext() as context:
        context.prec = 50
        execution = evaluation[0].open * (1 + slippage_bps / 10000)
        quantity = cash / (execution * (1 + fee_bps / 10000))
        fee = quantity * execution * fee_bps / 10000
        peak = cash
        drawdown = Decimal(0)
        for candle in evaluation:
            equity = quantity * candle.close
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        return {
            "percentage_return": (equity / cash - 1) * 100,
            "maximum_drawdown": drawdown,
            "fill_count": 1,
            "completed_round_trips": 0,
            "total_fees": fee,
            "ending_equity": equity,
        }


def _write(path: Path, document: dict) -> str:
    body = canonical_json_bytes(document)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(body)
    if path.read_bytes() != body:
        raise ValueError("research artifact read-back failed")
    return _hash(body)


def _candidate_reasons(row: dict, spec: dict) -> list[str]:
    reasons = []
    train, validation = row["train"], row["validation"]
    if int(train["fill_count"]) < max(1, spec["minimum_train_fills"]):
        reasons.append("training_has_insufficient_fills")
    if Decimal(train["percentage_return"]) <= 0:
        reasons.append("training_return_not_positive")
    if int(validation["fill_count"]) < 1:
        reasons.append("validation_has_no_fills")
    if Decimal(validation["percentage_return"]) <= 0:
        reasons.append("validation_return_not_positive")
    if Decimal(validation["maximum_drawdown"]) > Decimal(spec["maximum_validation_drawdown"]):
        reasons.append("validation_drawdown_exceeds_limit")
    return reasons


def _eligible_candidates(results: list[dict], spec: dict) -> list[dict]:
    eligible = [item for item in results if not _candidate_reasons(item, spec)]
    return sorted(
        eligible,
        key=lambda item: (-Decimal(item["validation"]["percentage_return"]), item["candidate"]["id"]),
    )


def _validate_selection(selection: dict, spec: dict, *, require_selected: bool = True) -> None:
    """Reject legacy evidence before any cache hit or new test-price access."""
    if selection.get("version") != VERSION or selection.get("policy_version") != POLICY_VERSION:
        raise ValueError("selection uses a legacy policy; historical artifacts are audit-only")
    if selection.get("test_prices_accessed") is not False:
        raise ValueError("selection cannot be evaluated")
    if [item["candidate"] for item in selection["candidate_results"]] != spec["candidates"]:
        raise ValueError("selection candidates do not match the sealed specification")
    eligible = _eligible_candidates(selection["candidate_results"], spec)
    expected = eligible[0]["candidate"] if eligible else None
    expected_status = "selected" if eligible else "no_candidate_selected"
    if selection.get("selected_candidate") != expected or selection.get("status") != expected_status:
        raise ValueError("selection fails the current participation and performance policy")
    if require_selected and not eligible:
        raise ValueError("selection has no eligible candidate")


def _promotion_decision(scenarios: list[dict], spec: dict) -> dict:
    if [item["cost_multiplier"] for item in scenarios] != spec["cost_multipliers"]:
        raise ValueError("evaluation cost scenarios do not match the sealed specification")
    reasons = []
    for scenario in scenarios[:2]:
        strategy = scenario["strategy"]
        prefix = f"{scenario['cost_multiplier']}x_"
        if int(strategy.get("completed_round_trips", 0)) < 1 or int(strategy["fill_count"]) < 2:
            reasons.append(prefix + "test_has_no_completed_round_trip")
        if Decimal(strategy["percentage_return"]) <= 0:
            reasons.append(prefix + "test_return_not_positive")
        excess = Decimal(strategy["percentage_return"]) - Decimal(
            scenario["buy_and_hold"]["percentage_return"]
        )
        if excess != Decimal(scenario["excess_return_pct"]):
            # Values are serialized from a 28-digit subtraction, so use the same
            # default decimal context used when constructing the scenarios.
            raise ValueError("evaluation excess return does not match its component returns")
        if excess <= Decimal(spec["minimum_test_excess_return_pct"]):
            reasons.append(prefix + "test_excess_return_below_threshold")
        if Decimal(strategy["maximum_drawdown"]) > Decimal(spec["maximum_validation_drawdown"]):
            reasons.append(prefix + "test_drawdown_exceeds_limit")
    return {
        "research_gate_passed": not reasons,
        "research_gate_reasons": reasons,
        "paper_trial_eligible": False,
        "paper_trial_reasons": [*reasons, PAPER_UNSUPPORTED],
    }


def _validate_evaluation(evaluation: dict, selection: dict, digest: str, spec: dict) -> None:
    if evaluation.get("version") != VERSION or evaluation.get("policy_version") != POLICY_VERSION:
        raise ValueError("evaluation uses legacy calculations or policy; historical artifacts are audit-only")
    if (
        evaluation.get("selection_sha256") != digest.lower()
        or evaluation.get("selected_candidate") != selection["selected_candidate"]
        or evaluation.get("test_prices_accessed") is not True
    ):
        raise ValueError("evaluation is not linked to the current sealed selection")
    decision = _promotion_decision(evaluation["scenarios"], spec)
    if any(evaluation.get(name) != value for name, value in decision.items()):
        raise ValueError("cached evaluation has inconsistent promotion results")


def prepare(spec_path: Path, digest: str, output: Path) -> dict:
    spec = load_spec(spec_path, digest)
    key = _hash((VERSION + digest).encode())
    selection_path = output / "selections" / key / "selection.json"
    if selection_path.exists():
        cached = json.loads(selection_path.read_bytes())
        if cached.get("spec_sha256") != digest.lower():
            raise ValueError("cached selection is linked to a different specification")
        _validate_selection(cached, spec, require_selected=False)
        return cached
    start = _date(spec["warmup_start"])
    end = _date(spec["ranges"]["validation"]["end"])
    candles, response = fetch_candles(start, end)
    source_uri = output / "sources" / key / "train-validation.json"
    source_uri.parent.mkdir(parents=True, exist_ok=True)
    with source_uri.open("xb") as source:
        source.write(response)
    if source_uri.read_bytes() != response:
        raise ValueError("source response read-back failed")
    cash = Decimal(spec["starting_cash"])
    fee = Decimal(spec["fee_bps"])
    slip = Decimal(spec["slippage_bps"])
    results = []
    for candidate in spec["candidates"]:
        result = {"candidate": candidate}
        for name in ("train", "validation"):
            interval = spec["ranges"][name]
            result[name] = simulate(
                candles, start=_date(interval["start"]), end=_date(interval["end"]),
                fast=candidate["fast_days"], slow=candidate["slow_days"],
                cash=cash, fee_bps=fee, slippage_bps=slip,
            )
        results.append(result)
    eligible = _eligible_candidates(results, spec)
    selection = {
        "version": VERSION,
        "policy_version": POLICY_VERSION,
        "spec_uri": str(spec_path),
        "spec_sha256": digest.lower(),
        "source_uri": str(source_uri),
        "source_sha256": _hash(response),
        "status": "selected" if eligible else "no_candidate_selected",
        "selected_candidate": eligible[0]["candidate"] if eligible else None,
        "candidate_results": results,
        "test_prices_accessed": False,
    }
    _write(selection_path, selection)
    return selection


def evaluate(selection_path: Path, selection_digest: str, output: Path) -> dict:
    selection_body = selection_path.read_bytes()
    if _hash(selection_body) != selection_digest.lower():
        raise ValueError("selection SHA-256 does not match")
    selection = json.loads(selection_body)
    spec = load_spec(Path(selection["spec_uri"]), selection["spec_sha256"])
    _validate_selection(selection, spec)
    key = _hash((selection_digest.lower() + VERSION).encode())
    report_path = output / "evaluations" / key / "report.json"
    if report_path.exists():
        cached = json.loads(report_path.read_bytes())
        _validate_evaluation(cached, selection, selection_digest, spec)
        return cached
    test_start = _date(spec["ranges"]["test"]["start"])
    test_end = _date(spec["ranges"]["test"]["end"])
    slow = selection["selected_candidate"]["slow_days"]
    fetch_start = test_start - (slow - 1) * DAY
    prior_body = Path(selection["source_uri"]).read_bytes()
    if _hash(prior_body) != selection["source_sha256"]:
        raise ValueError("sealed source response SHA-256 changed")
    prior = decode_candles(
        prior_body, _date(spec["warmup_start"]),
        _date(spec["ranges"]["validation"]["end"]),
    )
    candles, response = fetch_candles(fetch_start, test_end)
    overlap = tuple(candle for candle in prior if candle.start >= fetch_start)
    if overlap != candles[: len(overlap)]:
        raise ValueError("Coinbase changed candles in the sealed warmup overlap")
    source_uri = output / "sources" / key / "test.json"
    source_uri.parent.mkdir(parents=True, exist_ok=True)
    with source_uri.open("xb") as source:
        source.write(response)
    if source_uri.read_bytes() != response:
        raise ValueError("test source response read-back failed")
    cash = Decimal(spec["starting_cash"])
    scenarios = []
    for multiplier in spec["cost_multipliers"]:
        fee = Decimal(spec["fee_bps"]) * multiplier
        slip = Decimal(spec["slippage_bps"]) * multiplier
        candidate = selection["selected_candidate"]
        strategy = simulate(
            candles, start=test_start, end=test_end,
            fast=candidate["fast_days"], slow=candidate["slow_days"],
            cash=cash, fee_bps=fee, slippage_bps=slip,
        )
        baseline = buy_and_hold(
            candles, start=test_start, end=test_end,
            cash=cash, fee_bps=fee, slippage_bps=slip,
        )
        scenarios.append(
            {
                "cost_multiplier": multiplier,
                "strategy": strategy,
                "buy_and_hold": baseline,
                "excess_return_pct": strategy["percentage_return"] - baseline["percentage_return"],
            }
        )
    report = {
        "version": VERSION,
        "policy_version": POLICY_VERSION,
        "selection_uri": str(selection_path),
        "selection_sha256": selection_digest.lower(),
        "test_source_uri": str(source_uri),
        "test_source_sha256": _hash(response),
        "selected_candidate": selection["selected_candidate"],
        "scenarios": scenarios,
        **_promotion_decision(scenarios, spec),
        "test_prices_accessed": True,
    }
    _write(report_path, report)
    return report


def review_paper_trial(selection: dict, evaluation: dict) -> dict:
    """Review current or historical artifacts without authorizing daily execution."""
    chosen = selection["selected_candidate"]
    row = next(
        item for item in selection["candidate_results"]
        if item["candidate"]["id"] == chosen["id"]
    )
    reasons = []
    if int(row["validation"]["fill_count"]) == 0:
        reasons.append("validation_has_no_fills")
    if Decimal(row["validation"]["percentage_return"]) <= 0:
        reasons.append("validation_return_not_positive")
    if Decimal(row["train"]["percentage_return"]) <= 0:
        reasons.append("training_return_not_positive")
    if int(evaluation["scenarios"][0]["strategy"].get("completed_round_trips", 0)) < 1:
        reasons.append("test_has_no_completed_round_trip")
    if (
        selection.get("version") != VERSION
        or selection.get("policy_version") != POLICY_VERSION
        or evaluation.get("version") != VERSION
        or evaluation.get("policy_version") != POLICY_VERSION
    ):
        reasons.append("legacy_evidence_requires_audit_only")
    if not evaluation.get("research_gate_passed", False):
        reasons.append("current_research_gate_not_verified")
    reasons.append(PAPER_UNSUPPORTED)
    return {
        "review_version": "daily-momentum-operational-review-v2",
        "recommendation": "do_not_start_paper_pilot",
        "reasons": reasons,
    }


def forward(
    forward_spec_path: Path, forward_spec_sha256: str,
    selection_path: Path, evaluation_path: Path, output: Path,
) -> dict:
    """Run the frozen candidate on a later interval without new selection."""
    spec_body = forward_spec_path.read_bytes()
    if _hash(spec_body) != forward_spec_sha256.lower():
        raise ValueError("forward specification SHA-256 does not match")
    forward_spec = json.loads(spec_body)
    if forward_spec.get("version") != "daily-momentum-forward-v1":
        raise ValueError("unsupported forward specification")
    selection_body = selection_path.read_bytes()
    evaluation_body = evaluation_path.read_bytes()
    if (
        _hash(selection_body) != forward_spec["selection_sha256"]
        or _hash(evaluation_body) != forward_spec["evaluation_sha256"]
    ):
        raise ValueError("forward study is not linked to the pinned selection and evaluation")
    selection = json.loads(selection_body)
    evaluation = json.loads(evaluation_body)
    spec = load_spec(Path(selection["spec_uri"]), selection["spec_sha256"])
    _validate_selection(selection, spec)
    _validate_evaluation(evaluation, selection, _hash(selection_body), spec)
    candidate = selection["selected_candidate"]
    if candidate["id"] != forward_spec["candidate_id"]:
        raise ValueError("forward candidate differs from the sealed selection")
    start, end = _date(forward_spec["start"]), _date(forward_spec["end"])
    if (
        start != _date(spec["ranges"]["test"]["end"])
        or end <= start
        or end > datetime.now(UTC)
        or forward_spec["cost_multipliers"] != spec["cost_multipliers"]
    ):
        raise ValueError("forward interval or cost grid is invalid")
    key = _hash((forward_spec_sha256.lower() + VERSION).encode())
    report_path = output / "forward" / key / "report.json"
    if report_path.exists():
        cached = json.loads(report_path.read_bytes())
        if (
            cached.get("version") != FORWARD_VERSION
            or cached.get("policy_version") != POLICY_VERSION
            or cached.get("forward_spec_sha256") != forward_spec_sha256.lower()
            or cached.get("selection_sha256") != _hash(selection_body)
            or cached.get("evaluation_sha256") != _hash(evaluation_body)
            or cached.get("candidate") != candidate
        ):
            raise ValueError("cached forward report uses legacy policy or inconsistent lineage")
        decision = _promotion_decision(cached["scenarios"], spec)
        if any(cached.get(name) != value for name, value in decision.items()):
            raise ValueError("cached forward report has inconsistent promotion results")
        return cached
    fetch_start = start - (candidate["slow_days"] - 1) * DAY
    prior_response = Path(evaluation["test_source_uri"]).read_bytes()
    if _hash(prior_response) != evaluation["test_source_sha256"]:
        raise ValueError("first test source SHA-256 changed")
    prior_start = _date(spec["ranges"]["test"]["start"]) - (candidate["slow_days"] - 1) * DAY
    prior = decode_candles(prior_response, prior_start, start)
    candles, response = fetch_candles(fetch_start, end)
    overlap = tuple(candle for candle in prior if candle.start >= fetch_start)
    if overlap != candles[: len(overlap)]:
        raise ValueError("Coinbase changed candles in the forward warmup overlap")
    source_uri = output / "sources" / key / "forward.json"
    source_uri.parent.mkdir(parents=True, exist_ok=True)
    with source_uri.open("xb") as source:
        source.write(response)
    if source_uri.read_bytes() != response:
        raise ValueError("forward source response read-back failed")
    cash = Decimal(spec["starting_cash"])
    scenarios = []
    for multiplier in forward_spec["cost_multipliers"]:
        fee = Decimal(spec["fee_bps"]) * multiplier
        slip = Decimal(spec["slippage_bps"]) * multiplier
        strategy = simulate(
            candles, start=start, end=end,
            fast=candidate["fast_days"], slow=candidate["slow_days"],
            cash=cash, fee_bps=fee, slippage_bps=slip,
        )
        baseline = buy_and_hold(
            candles, start=start, end=end, cash=cash,
            fee_bps=fee, slippage_bps=slip,
        )
        scenarios.append(
            {
                "cost_multiplier": multiplier,
                "strategy": strategy,
                "buy_and_hold": baseline,
                "excess_return_pct": strategy["percentage_return"] - baseline["percentage_return"],
            }
        )
    report = {
        "version": FORWARD_VERSION,
        "policy_version": POLICY_VERSION,
        "forward_spec_sha256": forward_spec_sha256.lower(),
        "selection_sha256": _hash(selection_body),
        "evaluation_sha256": _hash(evaluation_body),
        "source_uri": str(source_uri),
        "source_sha256": _hash(response),
        "candidate": candidate,
        "start": start,
        "end": end,
        "scenarios": scenarios,
        **_promotion_decision(scenarios, spec),
    }
    _write(report_path, report)
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Sealed Coinbase daily momentum research")
    commands = parser.add_subparsers(dest="command", required=True)
    first = commands.add_parser("prepare")
    first.add_argument("--spec", required=True, type=Path)
    first.add_argument("--spec-sha256", required=True)
    second = commands.add_parser("evaluate")
    second.add_argument("--selection", required=True, type=Path)
    second.add_argument("--selection-sha256", required=True)
    third = commands.add_parser("forward")
    third.add_argument("--forward-spec", required=True, type=Path)
    third.add_argument("--forward-spec-sha256", required=True)
    third.add_argument("--selection", required=True, type=Path)
    third.add_argument("--evaluation", required=True, type=Path)
    for command in (first, second, third):
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(args.spec, args.spec_sha256, args.output)
        elif args.command == "evaluate":
            result = evaluate(args.selection, args.selection_sha256, args.output)
        else:
            result = forward(
                args.forward_spec, args.forward_spec_sha256,
                args.selection, args.evaluation, args.output,
            )
    except (OSError, ValueError) as error:
        print(f"Daily momentum research rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("DAILY_MOMENTUM_JSON=" + canonical_json_bytes(result).decode("ascii"))
    if args.command == "evaluate":
        selection = json.loads(args.selection.read_bytes())
        review = review_paper_trial(selection, result)
        print("DAILY_MOMENTUM_REVIEW_JSON=" + canonical_json_bytes(review).decode("ascii"))
    if args.command == "prepare" and result["status"] != "selected":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
