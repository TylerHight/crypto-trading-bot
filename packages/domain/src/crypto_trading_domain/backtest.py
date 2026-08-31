from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import (
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    Decimal,
    InvalidOperation,
    localcontext,
)
from enum import StrEnum
from typing import Protocol

DECIMAL_QUANTUM = Decimal("0.000000000000000001")
ONE_MINUTE = timedelta(minutes=1)
STRATEGY_VERSION = "sma-crossover-long-only-v1"


class InvalidBacktest(ValueError):
    """Backtest input or a deterministic portfolio invariant is invalid."""


class TargetPosition(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"


class FillSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise InvalidBacktest(f"{field} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _q18(value: Decimal, *, rounding: str = ROUND_HALF_EVEN) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = 114
            result = value.quantize(DECIMAL_QUANTUM, rounding=rounding)
    except (InvalidOperation, ValueError) as error:
        raise InvalidBacktest("decimal value exceeds scale-18 publication bounds") from error
    if not result.is_finite() or result.adjusted() >= 20:
        # decimal(38,18) permits at most 20 integer digits.
        raise InvalidBacktest("decimal value exceeds decimal(38,18)")
    return result


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        raise InvalidBacktest("decimal ratio denominator must be positive")
    with localcontext() as context:
        context.prec = 114
        return numerator / denominator


@dataclass(frozen=True)
class Candle:
    exchange: str
    symbol: str
    window_start: datetime
    window_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        start = _utc(self.window_start, "window_start")
        end = _utc(self.window_end, "window_end")
        if start.second or start.microsecond or end != start + ONE_MINUTE:
            raise InvalidBacktest("candle must be one aligned half-open UTC minute")
        if not self.exchange.strip() or not self.symbol.strip():
            raise InvalidBacktest("candle exchange and symbol must be non-empty")
        values = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise InvalidBacktest("candle prices must be finite and positive")
        if self.low > self.open or self.open > self.high:
            raise InvalidBacktest("candle open is outside its low/high range")
        if self.low > self.close or self.close > self.high:
            raise InvalidBacktest("candle close is outside its low/high range")


@dataclass(frozen=True)
class StrategyDecision:
    decision_time: datetime
    observed_candle_window_start: datetime
    fast_sma: Decimal
    slow_sma: Decimal
    previous_target: TargetPosition
    new_target: TargetPosition
    strategy_version: str = STRATEGY_VERSION


@dataclass(frozen=True)
class SimulatedFill:
    fill_time: datetime
    decision_time: datetime
    side: FillSide
    base_quantity: Decimal
    reference_open_price: Decimal
    execution_price: Decimal
    gross_notional: Decimal
    fee: Decimal
    cash_after: Decimal
    base_after: Decimal


@dataclass(frozen=True)
class PortfolioState:
    cash: Decimal
    base_quantity: Decimal

    @property
    def position(self) -> TargetPosition:
        return TargetPosition.LONG if self.base_quantity > 0 else TargetPosition.FLAT


@dataclass(frozen=True)
class EquityObservation:
    window_start: datetime
    close: Decimal
    cash: Decimal
    base_quantity: Decimal
    position: TargetPosition
    equity: Decimal
    drawdown: Decimal


@dataclass(frozen=True)
class BacktestSummary:
    starting_equity: Decimal
    ending_equity: Decimal
    absolute_return: Decimal
    percentage_return: Decimal
    maximum_drawdown: Decimal
    decisions: int
    buys: int
    sells: int
    unfilled_terminal_decisions: int
    total_fees: Decimal
    gross_traded_notional: Decimal
    percentage_candles_long: Decimal
    warmup_candles: int
    evaluation_candles: int
    ending_cash: Decimal
    ending_base_quantity: Decimal


@dataclass(frozen=True)
class BacktestResult:
    decisions: tuple[StrategyDecision, ...]
    fills: tuple[SimulatedFill, ...]
    equity_curve: tuple[EquityObservation, ...]
    summary: BacktestSummary


class Strategy(Protocol):
    version: str

    def prime(self, candle: Candle) -> None: ...

    def observe(self, candle: Candle) -> StrategyDecision | None: ...


class SmaCrossoverStrategy:
    version = STRATEGY_VERSION

    def __init__(self, fast_period: int, slow_period: int) -> None:
        if isinstance(fast_period, bool) or fast_period <= 0:
            raise InvalidBacktest("fast_period must be a positive integer")
        if isinstance(slow_period, bool) or slow_period <= fast_period:
            raise InvalidBacktest("slow_period must be greater than fast_period")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self._closes: deque[Decimal] = deque(maxlen=slow_period)
        self._target = TargetPosition.FLAT

    def prime(self, candle: Candle) -> None:
        self._closes.append(candle.close)

    def observe(self, candle: Candle) -> StrategyDecision | None:
        self._closes.append(candle.close)
        if len(self._closes) < self.slow_period:
            raise InvalidBacktest("strategy was not supplied enough warm-up candles")
        closes = tuple(self._closes)
        with localcontext() as context:
            context.prec = 114
            fast_sma = _q18(sum(closes[-self.fast_period :], Decimal(0)) / self.fast_period)
            slow_sma = _q18(sum(closes, Decimal(0)) / self.slow_period)
        target = (
            TargetPosition.LONG
            if fast_sma > slow_sma
            else TargetPosition.FLAT
        )
        if target == self._target:
            return None
        decision = StrategyDecision(
            decision_time=candle.window_end,
            observed_candle_window_start=candle.window_start,
            fast_sma=fast_sma,
            slow_sma=slow_sma,
            previous_target=self._target,
            new_target=target,
        )
        self._target = target
        return decision


def _validate_costs(fee_bps: Decimal, slippage_bps: Decimal) -> None:
    for value, field in ((fee_bps, "fee_bps"), (slippage_bps, "slippage_bps")):
        if not value.is_finite() or value < 0 or value >= 10_000:
            raise InvalidBacktest(f"{field} must be finite and in [0, 10000)")


def _fill(
    state: PortfolioState,
    decision: StrategyDecision,
    candle: Candle,
    *,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> tuple[PortfolioState, SimulatedFill]:
    fee_rate = _ratio(fee_bps, Decimal(10_000))
    slippage_rate = _ratio(slippage_bps, Decimal(10_000))
    with localcontext() as context:
        context.prec = 114
        if decision.new_target is TargetPosition.LONG:
            if state.position is not TargetPosition.FLAT:
                raise InvalidBacktest("LONG decision cannot fill an existing long position")
            execution_price = _q18(candle.open * (Decimal(1) + slippage_rate))
            quantity = _q18(
                _ratio(state.cash, execution_price * (Decimal(1) + fee_rate)),
                rounding=ROUND_DOWN,
            )
            if quantity <= 0:
                raise InvalidBacktest("starting cash cannot buy a scale-18 base quantity")
            gross_notional = _q18(quantity * execution_price)
            fee = _q18(gross_notional * fee_rate)
            cash = _q18(state.cash - gross_notional - fee)
            if cash < 0:
                raise InvalidBacktest("simulated buy overdraws cash")
            next_state = PortfolioState(cash=cash, base_quantity=quantity)
            side = FillSide.BUY
        else:
            if state.position is not TargetPosition.LONG:
                raise InvalidBacktest("FLAT decision cannot fill an empty position")
            execution_price = _q18(candle.open * (Decimal(1) - slippage_rate))
            if execution_price <= 0:
                raise InvalidBacktest("sell execution price must be positive")
            quantity = state.base_quantity
            gross_notional = _q18(quantity * execution_price)
            fee = _q18(gross_notional * fee_rate)
            cash = _q18(state.cash + gross_notional - fee)
            next_state = PortfolioState(cash=cash, base_quantity=_q18(Decimal(0)))
            side = FillSide.SELL

    return next_state, SimulatedFill(
        fill_time=candle.window_start,
        decision_time=decision.decision_time,
        side=side,
        base_quantity=quantity,
        reference_open_price=_q18(candle.open),
        execution_price=execution_price,
        gross_notional=gross_notional,
        fee=fee,
        cash_after=next_state.cash,
        base_after=next_state.base_quantity,
    )


def _validate_candle_sequence(
    candles: tuple[Candle, ...],
    *,
    start: datetime,
    end: datetime,
    slow_period: int,
) -> tuple[tuple[Candle, ...], tuple[Candle, ...]]:
    start = _utc(start, "start")
    end = _utc(end, "end")
    if start.second or start.microsecond or end.second or end.microsecond or start >= end:
        raise InvalidBacktest("start and end must be increasing UTC minute boundaries")
    if not candles:
        raise InvalidBacktest("backtest candle sequence is empty")
    expected_first = start - ONE_MINUTE * (slow_period - 1)
    expected_count = slow_period - 1 + int((end - start) / ONE_MINUTE)
    if len(candles) != expected_count:
        raise InvalidBacktest("candle sequence does not cover warm-up and evaluation range")

    exchange = candles[0].exchange
    symbol = candles[0].symbol
    previous: datetime | None = None
    for index, candle in enumerate(candles):
        if candle.exchange != exchange or candle.symbol != symbol:
            raise InvalidBacktest("backtest candles must have one exchange and symbol")
        expected = expected_first + ONE_MINUTE * index
        if candle.window_start != expected or (previous is not None and candle.window_start <= previous):
            raise InvalidBacktest("candle sequence has a duplicate, gap, or ordering error")
        previous = candle.window_start

    warmup = tuple(candle for candle in candles if candle.window_start < start)
    evaluation = tuple(candle for candle in candles if start <= candle.window_start < end)
    if len(warmup) != slow_period - 1 or len(evaluation) < 2:
        raise InvalidBacktest("backtest requires complete warm-up and at least two candles")
    return warmup, evaluation


def run_backtest(
    candles: tuple[Candle, ...],
    *,
    start: datetime,
    end: datetime,
    starting_cash: Decimal,
    fast_period: int,
    slow_period: int,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> BacktestResult:
    """Run the deterministic next-open, long-only SMA backtest."""

    _validate_costs(fee_bps, slippage_bps)
    cash = _q18(starting_cash)
    if cash <= 0:
        raise InvalidBacktest("starting_cash must be positive")
    strategy = SmaCrossoverStrategy(fast_period, slow_period)
    warmup, evaluation = _validate_candle_sequence(
        candles, start=start, end=end, slow_period=slow_period
    )
    for candle in warmup:
        strategy.prime(candle)

    state = PortfolioState(cash=cash, base_quantity=_q18(Decimal(0)))
    decisions: list[StrategyDecision] = []
    fills: list[SimulatedFill] = []
    observations: list[EquityObservation] = []
    pending: StrategyDecision | None = None
    peak = cash

    for candle in evaluation:
        if pending is not None:
            state, fill = _fill(
                state,
                pending,
                candle,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
            )
            fills.append(fill)
            pending = None

        decision = strategy.observe(candle)
        if decision is not None:
            decisions.append(decision)
            pending = decision

        equity = _q18(state.cash + state.base_quantity * candle.close)
        peak = max(peak, equity)
        drawdown = _q18(_ratio(peak - equity, peak))
        observations.append(
            EquityObservation(
                window_start=candle.window_start,
                close=_q18(candle.close),
                cash=state.cash,
                base_quantity=state.base_quantity,
                position=state.position,
                equity=equity,
                drawdown=drawdown,
            )
        )

    ending_equity = observations[-1].equity
    absolute_return = _q18(ending_equity - cash)
    percentage_return = _q18(_ratio(absolute_return, cash) * Decimal(100))
    total_fees = _q18(sum((fill.fee for fill in fills), Decimal(0)))
    gross_notional = _q18(
        sum((fill.gross_notional for fill in fills), Decimal(0))
    )
    long_candles = sum(
        observation.position is TargetPosition.LONG for observation in observations
    )
    percentage_long = _q18(
        _ratio(Decimal(long_candles), Decimal(len(observations))) * Decimal(100)
    )
    summary = BacktestSummary(
        starting_equity=cash,
        ending_equity=ending_equity,
        absolute_return=absolute_return,
        percentage_return=percentage_return,
        maximum_drawdown=max(observation.drawdown for observation in observations),
        decisions=len(decisions),
        buys=sum(fill.side is FillSide.BUY for fill in fills),
        sells=sum(fill.side is FillSide.SELL for fill in fills),
        unfilled_terminal_decisions=int(pending is not None),
        total_fees=total_fees,
        gross_traded_notional=gross_notional,
        percentage_candles_long=percentage_long,
        warmup_candles=len(warmup),
        evaluation_candles=len(evaluation),
        ending_cash=state.cash,
        ending_base_quantity=state.base_quantity,
    )
    return BacktestResult(tuple(decisions), tuple(fills), tuple(observations), summary)
