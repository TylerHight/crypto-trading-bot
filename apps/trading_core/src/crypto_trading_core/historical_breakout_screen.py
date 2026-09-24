"""Fixed, exploratory historical screen of the prospective daily breakout rule."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_exchange_adapters.coinbase_rest import CoinbaseRestError

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.daily_breakout import (
    _date,
    _decimal,
    _decode,
    _hash,
    _write_bytes,
    load_spec,
    simulate,
)
from crypto_trading_core.daily_momentum import DailyCandle, fetch_candles

SCREEN_VERSION = "daily-breakout-historical-screen-v1"


def load_screen_spec(path: Path, digest: str, study_digest: str) -> dict:
    body = path.read_bytes()
    if _hash(body) != digest.lower():
        raise ValueError("historical screen specification SHA-256 does not match")
    spec = json.loads(body)
    if not isinstance(spec, dict) or spec.get("version") != SCREEN_VERSION:
        raise ValueError("unsupported historical screen specification")
    if spec.get("name") != "btc-usd-breakout-historical-screen-v1":
        raise ValueError("historical screen name is invalid")
    if spec.get("study_spec_sha256") != study_digest.lower():
        raise ValueError("historical screen is linked to a different frozen study")
    if (spec.get("source"), spec.get("symbol"), spec.get("granularity_seconds")) != (
        "coinbase-exchange", "BTC-USD", 86400,
    ):
        raise ValueError("historical screen requires Coinbase BTC-USD daily candles")
    warmup, start, end = (_date(spec[key]) for key in ("warmup_start", "start", "end"))
    years = spec["years"]
    if (
        not isinstance(years, list)
        or not years
        or any(type(year) is not int for year in years)
        or years != list(range(start.year, end.year))
        or start != datetime(start.year, 1, 1, tzinfo=UTC)
        or end != datetime(end.year, 1, 1, tzinfo=UTC)
        or not warmup < start < end <= datetime.now(UTC)
        or type(spec["request_days"]) is not int
        or not 1 <= spec["request_days"] <= 300
    ):
        raise ValueError("historical screen dates or request size are invalid")
    return spec


def _outcomes(candles: tuple[DailyCandle, ...], start: datetime, end: datetime,
              study: dict, multiplier: int) -> dict:
    kwargs = {
        "start": start, "end": end,
        "entry_days": study["entry_days"], "exit_days": study["exit_days"],
        "cash": _decimal(study["starting_cash"]),
        "fee_bps": _decimal(study["fee_bps"]) * multiplier,
        "slippage_bps": _decimal(study["slippage_bps"]) * multiplier,
    }
    strategy = simulate(candles, **kwargs)
    baseline = simulate(candles, **kwargs, hold=True)
    return {
        "strategy": strategy,
        "buy_and_hold": baseline,
        "excess_return_pct": strategy["percentage_return"] - baseline["percentage_return"],
    }


def run(
    study_path: Path, study_digest: str, screen_path: Path, screen_digest: str,
    output: Path,
) -> dict:
    study, _ = load_spec(study_path, study_digest)
    screen = load_screen_spec(screen_path, screen_digest, study_digest)
    warmup = _date(screen["warmup_start"])
    start = _date(screen["start"])
    if (start - warmup).days < study["entry_days"]:
        raise ValueError("historical screen has insufficient channel warmup")
    end = _date(screen["end"])
    root = output / screen["name"] / screen_digest.lower()
    source_dir = root / "sources"
    candles: tuple[DailyCandle, ...] = ()
    source_hashes: list[dict] = []
    cursor = warmup
    while cursor < end:
        next_end = min(cursor + timedelta(days=screen["request_days"]), end)
        source_path = source_dir / f"chunk-{len(source_hashes) + 1:03d}.json"
        if source_path.exists():
            response = source_path.read_bytes()
        else:
            _, response = fetch_candles(cursor, next_end)
            _decode(response, cursor, next_end)
            _write_bytes(source_path, response)
        candles += _decode(response, cursor, next_end)
        source_hashes.append({
            "start": cursor, "end": next_end,
            "uri": str(source_path), "sha256": _hash(response),
        })
        cursor = next_end
    if len(candles) != (end - warmup).days:
        raise ValueError("historical screen has missing days")
    scenarios = []
    for multiplier in study["cost_multipliers"]:
        overall = _outcomes(candles, start, end, study, multiplier)
        by_year = []
        for year in screen["years"]:
            year_start = datetime(year, 1, 1, tzinfo=UTC)
            year_end = datetime(year + 1, 1, 1, tzinfo=UTC)
            by_year.append({
                "year": year,
                **_outcomes(candles, year_start, year_end, study, multiplier),
            })
        scenarios.append({
            "cost_multiplier": multiplier,
            "overall": overall,
            "by_year": by_year,
        })
    report = {
        "version": SCREEN_VERSION,
        "status": "exploratory_historical_screen",
        "study_spec_sha256": study_digest.lower(),
        "screen_spec_sha256": screen_digest.lower(),
        "source_chunks": source_hashes,
        "warmup_start": warmup,
        "start": start,
        "end": end,
        "evaluated_days": (end - start).days,
        "scenarios": scenarios,
        "paper_trial_eligible": False,
        "prospective_result": False,
    }
    _write_bytes(root / "report.json", canonical_json_bytes(report))
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Screen the frozen breakout rule on past years")
    parser.add_argument("--study-spec", type=Path, required=True)
    parser.add_argument("--study-spec-sha256", required=True)
    parser.add_argument("--screen-spec", type=Path, required=True)
    parser.add_argument("--screen-spec-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = run(
            args.study_spec, args.study_spec_sha256,
            args.screen_spec, args.screen_spec_sha256,
            args.output,
        )
    except (OSError, ValueError, TypeError, KeyError, CoinbaseRestError) as error:
        print(f"Historical breakout screen rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    # Keep routine stdout compact; the immutable report holds all scenarios.
    first = report["scenarios"][0]
    print("HISTORICAL_BREAKOUT_SCREEN_JSON=" + canonical_json_bytes({
        "report_uri": str(args.output / args.screen_spec.stem / args.screen_spec_sha256.lower() / "report.json"),
        "evaluated_days": report["evaluated_days"],
        "strategy_return_pct": first["overall"]["strategy"]["percentage_return"],
        "buy_and_hold_return_pct": first["overall"]["buy_and_hold"]["percentage_return"],
        "excess_return_pct": first["overall"]["excess_return_pct"],
        "completed_round_trips": first["overall"]["strategy"]["completed_round_trips"],
    }).decode("ascii"))


if __name__ == "__main__":
    main()
