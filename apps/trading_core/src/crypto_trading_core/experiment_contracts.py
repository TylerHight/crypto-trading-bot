from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from crypto_trading_core.contracts import (
    BACKTEST_ENGINE_VERSION,
    DECIMAL_QUANTUM,
    EXCHANGE_PATTERN,
    SHA256_PATTERN,
    STRATEGY_VERSION,
    SYMBOL_PATTERN,
    InvalidBacktestInput,
    PublishedCandleSnapshot,
    canonical_json_bytes,
    is_local_uri,
    normalize_uri,
    parse_utc_minute,
)

EXPERIMENT_SPEC_VERSION = "v1"
EXPERIMENT_ENGINE_VERSION = "strategy-experiment-engine-v1"
SELECTION_POLICY_VERSION = "validation-return-selection-v1"
BASELINE_VERSION = "buy-and-hold-long-only-v1"
EXPERIMENT_RESULT_SCHEMA_VERSION = "v1"
NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
CANDIDATE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class ExperimentRange:
    start: datetime
    end: datetime

    @property
    def candles(self) -> int:
        return int((self.end - self.start) / timedelta(minutes=1))

    def as_dict(self) -> dict[str, datetime]:
        return {"end": self.end, "start": self.start}


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    fast_period: int
    slow_period: int

    def as_dict(self) -> dict[str, str | int]:
        return {
            "candidate_id": self.candidate_id,
            "fast_period": self.fast_period,
            "slow_period": self.slow_period,
        }


@dataclass(frozen=True)
class SelectionPolicy:
    minimum_train_fills: int
    maximum_train_drawdown: Decimal
    maximum_validation_drawdown: Decimal

    def as_dict(self) -> dict[str, str | int]:
        return {
            "maximum_train_drawdown": _decimal_text(self.maximum_train_drawdown),
            "maximum_validation_drawdown": _decimal_text(
                self.maximum_validation_drawdown
            ),
            "minimum_train_fills": self.minimum_train_fills,
        }


@dataclass(frozen=True)
class ExperimentSpec:
    name: str
    spec_uri: str
    raw_sha256: str
    candle_manifest_uri: str
    candle_manifest_sha256: str
    exchange: str
    symbol: str
    starting_cash: Decimal
    fee_bps: Decimal
    slippage_bps: Decimal
    train: ExperimentRange
    validation: ExperimentRange
    test: ExperimentRange
    candidates: tuple[Candidate, ...]
    selection_policy: SelectionPolicy

    def canonical_document(self) -> dict[str, Any]:
        return {
            "candle_manifest_sha256": self.candle_manifest_sha256,
            "candle_manifest_uri": self.candle_manifest_uri,
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "exchange": self.exchange,
            "experiment_spec_version": EXPERIMENT_SPEC_VERSION,
            "fee_bps": _decimal_text(self.fee_bps),
            "name": self.name,
            "ranges": {
                "test": self.test.as_dict(),
                "train": self.train.as_dict(),
                "validation": self.validation.as_dict(),
            },
            "selection_policy": self.selection_policy.as_dict(),
            "slippage_bps": _decimal_text(self.slippage_bps),
            "starting_cash": _decimal_text(self.starting_cash),
            "symbol": self.symbol,
        }

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.canonical_document())).hexdigest()


def _exact_fields(value: Any, expected: set[str], field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise InvalidBacktestInput(f"{field} fields do not match experiment spec v1")
    return value


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise InvalidBacktestInput(f"{field} must be an integer >= {minimum}")
    return value


def _decimal(value: Any, field: str) -> Decimal:
    if not isinstance(value, str):
        raise InvalidBacktestInput(f"{field} must be an exact decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InvalidBacktestInput(f"{field} must be an exact decimal string") from error
    if not parsed.is_finite():
        raise InvalidBacktestInput(f"{field} must be finite")
    try:
        with localcontext() as context:
            context.prec = 114
            quantized = parsed.quantize(DECIMAL_QUANTUM)
    except InvalidOperation as error:
        raise InvalidBacktestInput(f"{field} exceeds decimal(38,18)") from error
    if quantized != parsed or (quantized and quantized.adjusted() >= 20):
        raise InvalidBacktestInput(f"{field} exceeds decimal(38,18)")
    return parsed


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _range(value: Any, field: str) -> ExperimentRange:
    item = _exact_fields(value, {"start", "end"}, field)
    start = item.get("start")
    end = item.get("end")
    if not isinstance(start, str) or not isinstance(end, str):
        raise InvalidBacktestInput(f"{field} boundaries must be strings")
    parsed = ExperimentRange(
        parse_utc_minute(start, f"{field}.start"),
        parse_utc_minute(end, f"{field}.end"),
    )
    if parsed.candles < 2:
        raise InvalidBacktestInput(f"{field} must contain at least two candles")
    return parsed


def load_experiment_spec(
    body: bytes,
    *,
    spec_uri: str,
    expected_sha256: str,
    allowed_spec_prefix: str,
    local_development: bool,
    maximum_candidates: int = 50,
    maximum_candidate_candle_evaluations: int = 5_000_000,
) -> ExperimentSpec:
    if is_local_uri(spec_uri):
        if not local_development:
            raise InvalidBacktestInput(
                "local experiment specifications require explicit local-development mode"
            )
    elif not normalize_uri(spec_uri).startswith(normalize_uri(allowed_spec_prefix) + "/"):
        raise InvalidBacktestInput("experiment specification is outside the allowed prefix")
    digest = hashlib.sha256(body).hexdigest()
    if not SHA256_PATTERN.fullmatch(expected_sha256.lower()) or digest != expected_sha256.lower():
        raise InvalidBacktestInput("experiment specification SHA-256 digest does not match")
    try:
        raw = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput("experiment specification is invalid UTF-8 JSON") from error
    document = _exact_fields(
        raw,
        {
            "experiment_spec_version",
            "name",
            "candle_manifest_uri",
            "candle_manifest_sha256",
            "exchange",
            "symbol",
            "starting_cash",
            "fee_bps",
            "slippage_bps",
            "ranges",
            "candidates",
            "selection_policy",
        },
        "experiment specification",
    )
    if document.get("experiment_spec_version") != EXPERIMENT_SPEC_VERSION:
        raise InvalidBacktestInput("unsupported experiment specification version")
    name = document.get("name")
    candle_manifest_uri = document.get("candle_manifest_uri")
    candle_digest = document.get("candle_manifest_sha256")
    exchange = document.get("exchange")
    symbol = document.get("symbol")
    if not isinstance(name, str) or not NAME_PATTERN.fullmatch(name):
        raise InvalidBacktestInput("experiment name has an invalid format")
    if not isinstance(candle_manifest_uri, str) or not candle_manifest_uri.strip():
        raise InvalidBacktestInput("candle manifest URI is invalid")
    if not isinstance(candle_digest, str) or not SHA256_PATTERN.fullmatch(candle_digest):
        raise InvalidBacktestInput("candle manifest digest is invalid")
    if not isinstance(exchange, str) or not EXCHANGE_PATTERN.fullmatch(exchange):
        raise InvalidBacktestInput("experiment exchange has an invalid format")
    if not isinstance(symbol, str) or not SYMBOL_PATTERN.fullmatch(symbol):
        raise InvalidBacktestInput("experiment symbol has an invalid format")

    ranges = _exact_fields(document.get("ranges"), {"train", "validation", "test"}, "ranges")
    train = _range(ranges["train"], "ranges.train")
    validation = _range(ranges["validation"], "ranges.validation")
    test = _range(ranges["test"], "ranges.test")
    if not (train.end <= validation.start < validation.end <= test.start < test.end):
        raise InvalidBacktestInput("train, validation, and test ranges overlap or are unordered")

    raw_candidates = document.get("candidates")
    if not isinstance(raw_candidates, list) or not 2 <= len(raw_candidates) <= maximum_candidates:
        raise InvalidBacktestInput("candidate count must be between 2 and the configured cap")
    candidates: list[Candidate] = []
    for index, raw_candidate in enumerate(raw_candidates):
        item = _exact_fields(
            raw_candidate,
            {"candidate_id", "fast_period", "slow_period"},
            f"candidates[{index}]",
        )
        candidate_id = item.get("candidate_id")
        fast = _integer(item.get("fast_period"), f"candidates[{index}].fast_period", minimum=1)
        slow = _integer(item.get("slow_period"), f"candidates[{index}].slow_period", minimum=2)
        if not isinstance(candidate_id, str) or not CANDIDATE_PATTERN.fullmatch(candidate_id):
            raise InvalidBacktestInput(f"candidates[{index}].candidate_id is invalid")
        if fast >= slow:
            raise InvalidBacktestInput("every candidate requires fast_period < slow_period")
        candidates.append(Candidate(candidate_id, fast, slow))
    if len({item.candidate_id for item in candidates}) != len(candidates):
        raise InvalidBacktestInput("candidate IDs must be unique")
    if len({(item.fast_period, item.slow_period) for item in candidates}) != len(candidates):
        raise InvalidBacktestInput("candidate period pairs must be unique")
    candidates.sort(key=lambda item: item.candidate_id)

    raw_policy = _exact_fields(
        document.get("selection_policy"),
        {
            "minimum_train_fills",
            "maximum_train_drawdown",
            "maximum_validation_drawdown",
        },
        "selection_policy",
    )
    policy = SelectionPolicy(
        minimum_train_fills=_integer(
            raw_policy.get("minimum_train_fills"), "minimum_train_fills"
        ),
        maximum_train_drawdown=_decimal(
            raw_policy.get("maximum_train_drawdown"), "maximum_train_drawdown"
        ),
        maximum_validation_drawdown=_decimal(
            raw_policy.get("maximum_validation_drawdown"),
            "maximum_validation_drawdown",
        ),
    )
    for value in (
        policy.maximum_train_drawdown,
        policy.maximum_validation_drawdown,
    ):
        if value < 0 or value > 1:
            raise InvalidBacktestInput("maximum drawdown thresholds must be in [0, 1]")
    starting_cash = _decimal(document.get("starting_cash"), "starting_cash")
    fee_bps = _decimal(document.get("fee_bps"), "fee_bps")
    slippage_bps = _decimal(document.get("slippage_bps"), "slippage_bps")
    if starting_cash <= 0 or fee_bps < 0 or slippage_bps < 0:
        raise InvalidBacktestInput("cash must be positive and costs nonnegative")
    if fee_bps >= 10_000 or slippage_bps >= 10_000:
        raise InvalidBacktestInput("fee and slippage basis points must be below 10000")

    evaluations = sum(
        train.candles + validation.candles + 2 * (candidate.slow_period - 1)
        for candidate in candidates
    )
    evaluations += (
        test.candles
        + max(candidate.slow_period for candidate in candidates)
        - 1
        + train.candles
        + validation.candles
        + test.candles
    )
    if evaluations > maximum_candidate_candle_evaluations:
        raise InvalidBacktestInput("experiment exceeds the candidate-candle resource cap")

    return ExperimentSpec(
        name=name,
        spec_uri=spec_uri,
        raw_sha256=digest,
        candle_manifest_uri=candle_manifest_uri,
        candle_manifest_sha256=candle_digest,
        exchange=exchange,
        symbol=symbol,
        starting_cash=starting_cash,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        train=train,
        validation=validation,
        test=test,
        candidates=tuple(candidates),
        selection_policy=policy,
    )


def selection_identity(
    spec: ExperimentSpec, source: PublishedCandleSnapshot
) -> dict[str, Any]:
    return {
        "backtest_engine_version": BACKTEST_ENGINE_VERSION,
        "baseline_version": BASELINE_VERSION,
        "candle_manifest_sha256": source.manifest_sha256,
        "candle_snapshot_key": source.snapshot_key,
        "experiment_engine_version": EXPERIMENT_ENGINE_VERSION,
        "experiment_result_schema_version": EXPERIMENT_RESULT_SCHEMA_VERSION,
        "experiment_spec_canonical_sha256": spec.canonical_sha256,
        "selection_policy_version": SELECTION_POLICY_VERSION,
        "strategy_version": STRATEGY_VERSION,
    }


def selection_key(spec: ExperimentSpec, source: PublishedCandleSnapshot) -> str:
    return hashlib.sha256(canonical_json_bytes(selection_identity(spec, source))).hexdigest()


def evaluation_key(selection_key_value: str, selection_sha256: str) -> str:
    identity = {
        "backtest_engine_version": BACKTEST_ENGINE_VERSION,
        "baseline_version": BASELINE_VERSION,
        "experiment_engine_version": EXPERIMENT_ENGINE_VERSION,
        "experiment_result_schema_version": EXPERIMENT_RESULT_SCHEMA_VERSION,
        "selection_key": selection_key_value,
        "selection_manifest_sha256": selection_sha256,
        "strategy_version": STRATEGY_VERSION,
    }
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
