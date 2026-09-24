"""Prospective daily breakout research with frozen rules and sealed public data."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from functools import wraps
from pathlib import Path

from crypto_exchange_adapters.coinbase_rest import CoinbaseRestError

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.daily_momentum import DailyCandle, decode_candles, fetch_candles

DAY = timedelta(days=1)
VERSION = "daily-breakout-study-v1"


def _now() -> datetime:
    return datetime.now(UTC)


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0) or parsed.time() != datetime.min.time():
        raise ValueError("daily boundaries must be UTC midnight")
    return parsed.astimezone(UTC)


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("artifact timestamps must be UTC")
    return parsed.astimezone(UTC)


def _decimal(value: object) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("numeric assumptions must be finite")
    return result


def load_spec(path: Path, digest: str) -> tuple[dict, bytes]:
    body = path.read_bytes()
    if _hash(body) != digest.lower():
        raise ValueError("study specification SHA-256 does not match")
    spec = json.loads(body)
    if not isinstance(spec, dict) or spec.get("version") != VERSION:
        raise ValueError("unsupported study specification")
    if not isinstance(spec.get("name"), str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,99}", spec["name"]):
        raise ValueError("invalid study name")
    if (spec.get("source"), spec.get("symbol"), spec.get("granularity_seconds")) != (
        "coinbase-exchange", "BTC-USD", 86400,
    ):
        raise ValueError("study requires Coinbase BTC-USD daily candles")
    warmup, start, end = (_date(spec[key]) for key in ("warmup_start", "start", "end"))
    entry, exit_days = spec["entry_days"], spec["exit_days"]
    if not (
        type(entry) is int and type(exit_days) is int and 0 < exit_days < entry <= 90
        and warmup < start < end and (start - warmup).days >= entry
        and (end - warmup).days <= 300
    ):
        raise ValueError("invalid channel periods, warmup, or study dates")
    if (
        _decimal(spec["starting_cash"]) <= 0
        or not 0 <= _decimal(spec["fee_bps"]) < Decimal(10000) / 3
        or not 0 <= _decimal(spec["slippage_bps"]) < Decimal(10000) / 3
        or spec["cost_multipliers"] != [1, 2, 3]
        or type(spec["minimum_round_trips"]) is not int or spec["minimum_round_trips"] < 1
        or not 0 < _decimal(spec["maximum_drawdown"]) <= 1
        or _decimal(spec["minimum_excess_return_pct"]) < 0
        or type(spec["settlement_delay_seconds"]) is not int
        or spec["settlement_delay_seconds"] < 3600
    ):
        raise ValueError("invalid costs or research thresholds")
    return spec, body


@contextmanager
def _study_lock(root: Path) -> Iterator[None]:
    """Serialize lifecycle operations; the OS releases the lock on process exit."""
    root.mkdir(parents=True, exist_ok=True)
    # Keep this inode after unlocking: deleting it could let a later caller lock
    # a different file while an existing caller still holds the original lock.
    with (root / ".study.lock").open("a+b") as handle:
        try:
            if sys.platform == "win32":
                import msvcrt

                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise ValueError("another study operation holds the lock; retry after it finishes") from error
        try:
            yield
        finally:
            if sys.platform == "win32":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _serialized(
    operation: Callable[[Path, str, Path], dict],
) -> Callable[[Path, str, Path], dict]:
    @wraps(operation)
    def run(spec_path: Path, digest: str, output: Path) -> dict:
        spec, _ = load_spec(spec_path, digest)
        with _study_lock(output / spec["name"]):
            return operation(spec_path, digest, output)

    return run


def _write_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != body:
            raise ValueError("immutable artifact already exists with different bytes")
        return
    with path.open("xb") as target:
        target.write(body)
    if path.read_bytes() != body:
        raise ValueError("immutable artifact failed read-back")


def _write_document(directory: Path, document: dict, prefix: str = "") -> str:
    body = canonical_json_bytes(document)
    digest = _hash(body)
    _write_bytes(directory / f"{prefix}{digest}.json", body)
    return digest


def _read_document(path: Path) -> tuple[dict, str]:
    body = path.read_bytes()
    digest = _hash(body)
    if path.stem.split("-")[-1] != digest:
        raise ValueError("artifact SHA-256 does not match its filename")
    document = json.loads(body)
    if not isinstance(document, dict):
        raise TypeError("artifact document must be an object")
    return document, digest


def _registration(spec: dict, body: bytes, root: Path) -> tuple[dict, str]:
    paths = list((root / "registration").glob("*.json"))
    if len(paths) != 1:
        raise ValueError("study must have exactly one sealed registration")
    registration, digest = _read_document(paths[0])
    if (
        registration.get("version") != VERSION
        or registration.get("spec_sha256") != _hash(body)
        or registration.get("name") != spec["name"]
        or _time(registration["registered_at"]) >= _date(spec["start"])
        or _time(registration["registered_at"]) > _now()
    ):
        raise ValueError("registration differs from specification or was made too late")
    if (root / f"spec-{_hash(body)}.json").read_bytes() != body:
        raise ValueError("sealed specification SHA-256 changed")
    return registration, digest


def _decode(body: bytes, start: datetime, end: datetime) -> tuple[DailyCandle, ...]:
    """Reject malformed and non-finite rows even outside the requested interval."""
    try:
        rows = json.loads(body, parse_float=Decimal)
        if not isinstance(rows, list) or len(rows) > 300:
            raise ValueError("daily candle response must contain at most 300 rows")
        for row in rows:
            if not isinstance(row, list) or len(row) != 6:
                raise ValueError("daily candle row must have six values")
            if any(isinstance(value, bool) or not isinstance(value, (int, str, Decimal)) for value in row):
                raise ValueError("daily candle values must be numeric")
            timestamp, low, high, opening, close, volume = (_decimal(value) for value in row)
            if timestamp % 86400 != 0:
                raise ValueError("daily candle timestamp must be UTC midnight")
            if not (0 < low <= opening <= high and low <= close <= high and volume > 0):
                raise ValueError("daily candle OHLCV bounds are invalid")
        return decode_candles(body, start, end)
    except (InvalidOperation, TypeError, OverflowError, OSError) as error:
        raise ValueError("invalid daily candle values") from error


def _closed_end(now: datetime, spec: dict) -> datetime:
    settled = now - timedelta(seconds=spec["settlement_delay_seconds"])
    return min(settled.replace(hour=0, minute=0, second=0, microsecond=0), _date(spec["end"]))


def _evidence(
    spec: dict, root: Path, registration: dict, registration_digest: str,
) -> tuple[tuple[DailyCandle, ...], datetime, list[str]]:
    candles: tuple[DailyCandle, ...] = ()
    cursor = _date(spec["warmup_start"])
    hashes: list[str] = []
    previous_time = _time(registration["registered_at"])
    for sequence, path in enumerate(sorted((root / "captures").glob("*.json")), 1):
        capture, digest = _read_document(path)
        start, end = _date(capture["start"]), _date(capture["end"])
        captured_at = _time(capture["captured_at"])
        if (
            path.name != f"{sequence:06d}-{digest}.json"
            or capture.get("version") != VERSION
            or capture.get("registration_sha256") != registration_digest
            or capture.get("spec_sha256") != registration["spec_sha256"]
            or capture.get("previous_capture_sha256") != (hashes[-1] if hashes else None)
            or start != cursor or not start < end <= _date(spec["end"])
            or not previous_time <= captured_at <= _now()
            or end > _closed_end(captured_at, spec)
        ):
            raise ValueError("capture chain has invalid provenance, chronology, or continuity")
        source_hash = capture["source_sha256"]
        if not isinstance(source_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", source_hash):
            raise ValueError("invalid source SHA-256")
        source = (root / "sources" / f"{source_hash}.json").read_bytes()
        if _hash(source) != source_hash:
            raise ValueError("sealed source SHA-256 changed")
        candles += _decode(source, start, end)
        hashes.append(digest)
        cursor, previous_time = end, captured_at
    return candles, cursor, hashes


@_serialized
def register(spec_path: Path, digest: str, output: Path) -> dict:
    spec, body = load_spec(spec_path, digest)
    root = output / spec["name"]
    if list((root / "registration").glob("*.json")):
        registration, registration_digest = _registration(spec, body, root)
        _evidence(spec, root, registration, registration_digest)
        return registration
    registered_at = _now()
    if registered_at >= _date(spec["start"]):
        raise ValueError("prospective registration must precede study start")
    if list(root.glob("spec-*.json")) and not (root / f"spec-{_hash(body)}.json").exists():
        raise ValueError("study name already has a different sealed specification")
    registration = {
        "version": VERSION, "name": spec["name"], "status": "registered",
        "spec_sha256": _hash(body), "registered_at": registered_at.isoformat(),
        "paper_trial_eligible": False,
    }
    _write_bytes(root / f"spec-{_hash(body)}.json", body)
    _write_document(root / "registration", registration)
    return registration


@_serialized
def collect(spec_path: Path, digest: str, output: Path) -> dict:
    spec, body = load_spec(spec_path, digest)
    root = output / spec["name"]
    registration, registration_digest = _registration(spec, body, root)
    candles, cursor, hashes = _evidence(spec, root, registration, registration_digest)
    end = _closed_end(_now(), spec)
    collected_days = 0
    if cursor < end:
        _, source = fetch_candles(cursor, end)
        batch = _decode(source, cursor, end)
        capture = {
            "version": VERSION, "spec_sha256": _hash(body),
            "registration_sha256": registration_digest,
            "previous_capture_sha256": hashes[-1] if hashes else None,
            "start": cursor.isoformat(), "end": end.isoformat(),
            "captured_at": _now().isoformat(), "source_sha256": _hash(source),
        }
        _write_bytes(root / "sources" / f"{_hash(source)}.json", source)
        _write_document(root / "captures", capture, f"{len(hashes) + 1:06d}-")
        collected_days = len(batch)
        candles, cursor, hashes = _evidence(spec, root, registration, registration_digest)
    return {
        "version": VERSION, "status": "collected", "spec_sha256": _hash(body),
        "collected_days": collected_days, "sealed_days": len(candles),
        "sealed_end": cursor.isoformat(), "capture_count": len(hashes),
        "complete": cursor == _date(spec["end"]), "paper_trial_eligible": False,
    }


def simulate(
    candles: tuple[DailyCandle, ...], *, start: datetime, end: datetime,
    entry_days: int, exit_days: int, cash: Decimal, fee_bps: Decimal,
    slippage_bps: Decimal, hold: bool = False,
) -> dict:
    """Trade previous high/low channels at the next open, marking any final holding."""
    evaluation = [index for index, candle in enumerate(candles) if start <= candle.start < end]
    if not evaluation or len(evaluation) != (end - start).days or any(
        candles[index].start != start + day * DAY for day, index in enumerate(evaluation)
    ):
        raise ValueError("evaluation has missing candle days")
    if not hold and (evaluation[0] < entry_days or any(
        candles[index].start != start - (evaluation[0] - index) * DAY
        for index in range(evaluation[0] - entry_days, evaluation[0])
    )):
        raise ValueError("channel warmup is incomplete")
    with localcontext() as context:
        context.prec = 50
        initial = peak = cash
        base = fees = drawdown = Decimal(0)
        pending: bool | None = True if hold else None
        fills = round_trips = 0
        fee_rate, slip_rate = fee_bps / 10000, slippage_bps / 10000
        for index in evaluation:
            candle = candles[index]
            if pending is not None:
                if pending:
                    execution = candle.open * (1 + slip_rate)
                    base = cash / (execution * (1 + fee_rate))
                    fee = base * execution * fee_rate
                    cash = Decimal(0)
                else:
                    execution = candle.open * (1 - slip_rate)
                    fee = base * execution * fee_rate
                    cash = base * execution - fee
                    base = Decimal(0)
                    round_trips += 1
                fees += fee
                fills += 1
                pending = None
            if not hold:
                if base == 0 and candle.close > max(
                    item.high for item in candles[index - entry_days:index]
                ):
                    pending = True
                elif base > 0 and candle.close < min(
                    item.low for item in candles[index - exit_days:index]
                ):
                    pending = False
            equity = cash + base * candle.close
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        return {
            "percentage_return": (equity / initial - 1) * 100,
            "maximum_drawdown": drawdown, "fill_count": fills,
            "completed_round_trips": round_trips, "total_fees": fees,
            "ending_equity": equity, "open_position": base > 0,
            "unfilled_final_signal": "buy" if pending else ("sell" if pending is False else None),
        }


@_serialized
def evaluate(spec_path: Path, digest: str, output: Path) -> dict:
    spec, body = load_spec(spec_path, digest)
    root = output / spec["name"]
    registration, registration_digest = _registration(spec, body, root)
    candles, cursor, hashes = _evidence(spec, root, registration, registration_digest)
    end = _date(spec["end"])
    ready_at = end + timedelta(seconds=spec["settlement_delay_seconds"])
    if _now() < ready_at or cursor != end:
        return {
            "version": VERSION, "status": "waiting", "spec_sha256": _hash(body),
            "reason": "study_not_finished" if _now() < ready_at else "missing_sealed_candles",
            "earliest_evaluation_at": ready_at.isoformat(), "sealed_end": cursor.isoformat(),
            "research_gate_passed": False, "paper_trial_eligible": False,
        }
    scenarios = []
    reasons: list[str] = []
    for multiplier in spec["cost_multipliers"]:
        kwargs = {
            "start": _date(spec["start"]), "end": end,
            "entry_days": spec["entry_days"], "exit_days": spec["exit_days"],
            "cash": _decimal(spec["starting_cash"]),
            "fee_bps": _decimal(spec["fee_bps"]) * multiplier,
            "slippage_bps": _decimal(spec["slippage_bps"]) * multiplier,
        }
        strategy = simulate(candles, **kwargs)
        baseline = simulate(candles, **kwargs, hold=True)
        excess = strategy["percentage_return"] - baseline["percentage_return"]
        scenarios.append({
            "cost_multiplier": multiplier, "strategy": strategy, "buy_and_hold": baseline,
            "cash": {"percentage_return": Decimal(0), "ending_equity": kwargs["cash"]},
            "excess_return_pct": excess,
        })
        if multiplier <= 2:
            tests = {
                "return_not_positive": strategy["percentage_return"] <= 0,
                "insufficient_excess_return": excess <= _decimal(spec["minimum_excess_return_pct"]),
                "drawdown_limit_exceeded": strategy["maximum_drawdown"] > _decimal(spec["maximum_drawdown"]),
                "insufficient_completed_round_trips": strategy["completed_round_trips"] < spec["minimum_round_trips"],
            }
            reasons.extend(f"{multiplier}x_{reason}" for reason, failed in tests.items() if failed)
    report = {
        "version": VERSION, "report_version": "daily-breakout-evaluation-v1", "status": "evaluated",
        "spec_sha256": _hash(body), "registration_sha256": registration_digest,
        "capture_sha256": hashes, "start": spec["start"], "end": spec["end"],
        "scenarios": scenarios, "research_gate_passed": not reasons,
        "research_gate_failure_reasons": reasons, "paper_trial_eligible": False,
        "paper_trial_blockers": ["daily_execution_not_supported", "manual_review_required"],
    }
    existing = list((root / "reports").glob("*.json"))
    if existing:
        if len(existing) != 1:
            raise ValueError("study must have at most one evaluation report")
        prior, _ = _read_document(existing[0])
        if canonical_json_bytes(prior) != canonical_json_bytes(report):
            raise ValueError("sealed evaluation differs from reproducible results")
    _write_document(root / "reports", report)
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Prospective Coinbase daily breakout research")
    commands = parser.add_subparsers(dest="command", required=True)
    for action in ("register", "collect", "evaluate"):
        command = commands.add_parser(action)
        command.add_argument("--spec", required=True, type=Path)
        command.add_argument("--spec-sha256", required=True)
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = {"register": register, "collect": collect, "evaluate": evaluate}[args.command](
            args.spec, args.spec_sha256, args.output,
        )
    except (OSError, ValueError, TypeError, KeyError, InvalidOperation, OverflowError, CoinbaseRestError) as error:
        print(f"Daily breakout research rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("DAILY_BREAKOUT_JSON=" + canonical_json_bytes(result).decode("ascii"))
    if args.command == "evaluate" and not result["research_gate_passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
