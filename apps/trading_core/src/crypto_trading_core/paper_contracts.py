from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from typing import Any

from crypto_trading_domain.backtest import (
    IncrementalBacktestState,
    PortfolioState,
    StrategyDecision,
)

from crypto_trading_core.contracts import (
    DECIMAL_QUANTUM,
    EXCHANGE_PATTERN,
    SHA256_PATTERN,
    SYMBOL_PATTERN,
    canonical_json_bytes,
)

PAPER_ENGINE_VERSION = "paper-trading-engine-v1"
PAPER_SCHEMA_VERSION = "v1"
SESSION_ID_PATTERN = re.compile(r"^[a-f0-9]{64}$")
ACTOR_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}$")
COMMAND_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class InvalidPaperTrading(ValueError):
    """A paper-trading input, transition, or durable invariant is invalid."""


class PaperSessionState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"


def decimal38(
    value: Decimal,
    field: str,
    *,
    positive: bool = False,
    allow_negative: bool = False,
) -> Decimal:
    if (
        not value.is_finite()
        or (not allow_negative and value < 0)
        or (positive and value <= 0)
    ):
        qualifier = "positive" if positive else "nonnegative"
        if allow_negative:
            raise InvalidPaperTrading(f"{field} must be finite")
        raise InvalidPaperTrading(f"{field} must be finite and {qualifier}")
    try:
        with localcontext() as context:
            context.prec = 114
            result = value.quantize(DECIMAL_QUANTUM)
    except InvalidOperation as error:
        raise InvalidPaperTrading(f"{field} exceeds decimal(38,18)") from error
    if result and result.adjusted() >= 20:
        raise InvalidPaperTrading(f"{field} exceeds decimal(38,18)")
    return result


def utc_minute(value: datetime, field: str) -> datetime:
    if (
        value.tzinfo is None
        or value.utcoffset() != timedelta(0)
        or value.second
        or value.microsecond
    ):
        raise InvalidPaperTrading(f"{field} must be an aligned UTC minute")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class PaperSessionSpec:
    evaluation_manifest_uri: str
    evaluation_manifest_sha256: str
    selection_manifest_uri: str
    selection_manifest_sha256: str
    candle_manifest_uri: str
    candle_manifest_sha256: str
    evaluation_key: str
    selection_key: str
    exchange: str
    symbol: str
    candidate_id: str
    fast_period: int
    slow_period: int
    starting_cash: Decimal
    fee_bps: Decimal
    slippage_bps: Decimal
    test_end: datetime
    approved_by: str
    approval_note: str
    maximum_drawdown: Decimal
    strategy_version: str
    backtest_engine_version: str
    experiment_engine_version: str
    paper_engine_version: str = PAPER_ENGINE_VERSION
    paper_schema_version: str = PAPER_SCHEMA_VERSION
    pilot_id: str | None = None
    forward_start: datetime | None = None

    def __post_init__(self) -> None:
        for digest, field in (
            (self.evaluation_manifest_sha256, "evaluation manifest digest"),
            (self.selection_manifest_sha256, "selection manifest digest"),
            (self.candle_manifest_sha256, "candle manifest digest"),
            (self.evaluation_key, "evaluation key"),
            (self.selection_key, "selection key"),
        ):
            if not SHA256_PATTERN.fullmatch(digest):
                raise InvalidPaperTrading(f"{field} is invalid")
        if not EXCHANGE_PATTERN.fullmatch(self.exchange):
            raise InvalidPaperTrading("exchange is invalid")
        if not SYMBOL_PATTERN.fullmatch(self.symbol):
            raise InvalidPaperTrading("symbol is invalid")
        if not self.candidate_id or len(self.candidate_id) > 128:
            raise InvalidPaperTrading("candidate ID is invalid")
        if self.fast_period <= 0 or self.slow_period <= self.fast_period:
            raise InvalidPaperTrading("strategy periods are invalid")
        decimal38(self.starting_cash, "starting_cash", positive=True)
        decimal38(self.fee_bps, "fee_bps")
        decimal38(self.slippage_bps, "slippage_bps")
        drawdown = decimal38(self.maximum_drawdown, "maximum_drawdown")
        if drawdown <= 0 or drawdown > 1:
            raise InvalidPaperTrading("maximum_drawdown must be in (0, 1]")
        utc_minute(self.test_end, "test_end")
        if self.forward_start is not None:
            utc_minute(self.forward_start, "forward_start")
            if self.forward_start < self.test_end:
                raise InvalidPaperTrading("forward_start cannot precede test_end")
        if not ACTOR_PATTERN.fullmatch(self.approved_by):
            raise InvalidPaperTrading("approved_by has an invalid format")
        if not self.approval_note.strip() or len(self.approval_note) > 1000:
            raise InvalidPaperTrading("approval_note must contain 1 to 1000 characters")
        for uri, field in (
            (self.evaluation_manifest_uri, "evaluation manifest URI"),
            (self.selection_manifest_uri, "selection manifest URI"),
            (self.candle_manifest_uri, "candle manifest URI"),
        ):
            if not uri.strip():
                raise InvalidPaperTrading(f"{field} is empty")
        if self.pilot_id is not None and not SESSION_ID_PATTERN.fullmatch(self.pilot_id):
            raise InvalidPaperTrading("pilot ID is invalid")

    def identity(self) -> dict[str, Any]:
        identity: dict[str, Any] = {
            "approval_note": self.approval_note.strip(),
            "approved_by": self.approved_by,
            "backtest_engine_version": self.backtest_engine_version,
            "evaluation_manifest_sha256": self.evaluation_manifest_sha256,
            "experiment_engine_version": self.experiment_engine_version,
            "maximum_drawdown": format(self.maximum_drawdown.normalize(), "f"),
            "paper_engine_version": self.paper_engine_version,
            "paper_schema_version": self.paper_schema_version,
            "starting_cash": format(self.starting_cash.normalize(), "f"),
            "strategy_version": self.strategy_version,
        }
        if self.pilot_id is not None:
            identity["pilot_id"] = self.pilot_id
        if self.forward_start is not None:
            identity["forward_start"] = self.forward_start
        return identity

    @property
    def processing_start(self) -> datetime:
        """First candle that contributes to the forward paper result."""

        return self.forward_start or self.test_end

    @property
    def session_id(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.identity())).hexdigest()


@dataclass(frozen=True)
class PaperSession:
    spec: PaperSessionSpec
    state: PaperSessionState
    strategy_state: IncrementalBacktestState
    created_at: datetime
    updated_at: datetime
    first_candle_time: datetime | None = None
    last_candle_time: datetime | None = None
    processed_candles: int = 0
    decisions: int = 0
    buys: int = 0
    sells: int = 0
    total_fees: Decimal = Decimal(0)
    current_equity: Decimal | None = None
    maximum_drawdown: Decimal = Decimal(0)
    baseline_portfolio: PortfolioState | None = None
    baseline_peak_equity: Decimal | None = None
    baseline_equity: Decimal | None = None
    baseline_maximum_drawdown: Decimal = Decimal(0)
    baseline_fee: Decimal = Decimal(0)
    last_state_reason: str = "session_created"
    last_state_changed_at: datetime | None = None

    @property
    def session_id(self) -> str:
        return self.spec.session_id

    def with_state(
        self,
        state: PaperSessionState,
        *,
        reason: str,
        changed_at: datetime,
    ) -> PaperSession:
        return replace(
            self,
            state=state,
            updated_at=changed_at,
            last_state_reason=reason,
            last_state_changed_at=changed_at,
        )


@dataclass(frozen=True)
class PaperCandleInput:
    manifest_uri: str
    manifest_sha256: str
    snapshot_key: str
    command_id: str

    def __post_init__(self) -> None:
        if not self.manifest_uri.strip():
            raise InvalidPaperTrading("candle manifest URI is empty")
        if not SHA256_PATTERN.fullmatch(self.manifest_sha256):
            raise InvalidPaperTrading("candle manifest digest is invalid")
        if not SHA256_PATTERN.fullmatch(self.snapshot_key):
            raise InvalidPaperTrading("candle snapshot key is invalid")
        if not COMMAND_ID_PATTERN.fullmatch(self.command_id):
            raise InvalidPaperTrading("command ID has an invalid format")


@dataclass(frozen=True)
class StoredPaperCandle:
    window_start: datetime
    manifest_uri: str
    manifest_sha256: str
    snapshot_key: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(frozen=True)
class PaperMutation:
    session: PaperSession
    candles: tuple[StoredPaperCandle, ...]
    decisions: tuple[StrategyDecision, ...]
    fills: tuple[Any, ...]
    equity: tuple[Any, ...]
    report: dict[str, Any]


def validate_actor(actor: str) -> str:
    value = actor.strip()
    if not ACTOR_PATTERN.fullmatch(value):
        raise InvalidPaperTrading("actor has an invalid format")
    return value


def validate_command_id(command_id: str) -> str:
    value = command_id.strip()
    if not COMMAND_ID_PATTERN.fullmatch(value):
        raise InvalidPaperTrading("command ID has an invalid format")
    return value


def validate_session_id(session_id: str) -> str:
    value = session_id.strip()
    if not SESSION_ID_PATTERN.fullmatch(value):
        raise InvalidPaperTrading("paper session ID is invalid")
    return value


def validate_reason(reason: str) -> str:
    value = reason.strip()
    if not value or len(value) > 1000:
        raise InvalidPaperTrading("reason must contain 1 to 1000 characters")
    return value
