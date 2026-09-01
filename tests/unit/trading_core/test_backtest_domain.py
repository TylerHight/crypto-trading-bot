from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from crypto_trading_domain.backtest import (
    Candle,
    FillSide,
    InvalidBacktest,
    TargetPosition,
    advance_incremental_backtest,
    initialize_incremental_backtest,
    run_backtest,
)

START = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def candle(minute: int, *, open_price: str, close_price: str) -> Candle:
    window_start = START + timedelta(minutes=minute)
    opening = Decimal(open_price)
    closing = Decimal(close_price)
    return Candle(
        exchange="coinbase",
        symbol="BTC-USD",
        window_start=window_start,
        window_end=window_start + timedelta(minutes=1),
        open=opening,
        high=max(opening, closing),
        low=min(opening, closing),
        close=closing,
    )


def fixture_candles() -> tuple[Candle, ...]:
    # Minutes -2 and -1 are warm-up. The final close creates a terminal LONG
    # decision, but there is no following open at which it could fill.
    return (
        candle(-2, open_price="10", close_price="10"),
        candle(-1, open_price="10", close_price="10"),
        candle(0, open_price="11", close_price="11"),
        candle(1, open_price="12", close_price="12"),
        candle(2, open_price="9", close_price="9"),
        candle(3, open_price="8", close_price="8"),
        candle(4, open_price="1000", close_price="1000"),
    )


def run_fixture():
    return run_backtest(
        fixture_candles(),
        start=START,
        end=START + timedelta(minutes=5),
        starting_cash=Decimal(1000),
        fast_period=2,
        slow_period=3,
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
    )


def test_signals_fill_only_at_the_next_open_and_terminal_signal_is_unfilled() -> None:
    result = run_fixture()

    assert [decision.new_target for decision in result.decisions] == [
        TargetPosition.LONG,
        TargetPosition.FLAT,
        TargetPosition.LONG,
    ]
    assert [fill.side for fill in result.fills] == [FillSide.BUY, FillSide.SELL]
    assert result.fills[0].decision_time == START + timedelta(minutes=1)
    assert result.fills[0].fill_time == START + timedelta(minutes=1)
    assert result.fills[0].reference_open_price == Decimal("12.000000000000000000")
    assert result.fills[1].decision_time == START + timedelta(minutes=3)
    assert result.fills[1].fill_time == START + timedelta(minutes=3)
    assert result.fills[1].reference_open_price == Decimal("8.000000000000000000")
    assert result.summary.unfilled_terminal_decisions == 1
    assert result.summary.buys == result.summary.sells == 1
    assert result.summary.ending_base_quantity == 0


def test_cost_math_is_exact_and_cash_never_goes_negative() -> None:
    result = run_fixture()
    buy, sell = result.fills

    assert buy.execution_price == Decimal("12.006000000000000000")
    assert buy.base_quantity == Decimal("82.959848097199740103")
    assert buy.gross_notional == Decimal("996.015936254980079677")
    assert buy.fee == Decimal("3.984063745019920319")
    assert buy.cash_after == Decimal("0.000000000000000004")
    assert sell.execution_price == Decimal("7.996000000000000000")
    assert sell.cash_after >= 0
    assert result.summary.ending_equity == Decimal("660.693557603668285381")
    assert result.summary.maximum_drawdown == Decimal("0.339306442396331715")
    assert result.summary.percentage_candles_long == Decimal("40.000000000000000000")
    assert all(observation.cash >= 0 for observation in result.equity_curve)
    assert all(observation.base_quantity >= 0 for observation in result.equity_curve)


def test_equal_averages_remain_flat() -> None:
    flat = tuple(candle(index, open_price="10", close_price="10") for index in range(-2, 3))
    result = run_backtest(
        flat,
        start=START,
        end=START + timedelta(minutes=3),
        starting_cash=Decimal(100),
        fast_period=2,
        slow_period=3,
        fee_bps=Decimal(0),
        slippage_bps=Decimal(0),
    )

    assert result.decisions == ()
    assert result.fills == ()
    assert result.summary.ending_equity == Decimal("100.000000000000000000")


@pytest.mark.parametrize(
    ("fast", "slow"),
    [(0, 3), (3, 3), (4, 3)],
)
def test_periods_are_validated(fast: int, slow: int) -> None:
    with pytest.raises(InvalidBacktest, match="period"):
        run_backtest(
            fixture_candles(),
            start=START,
            end=START + timedelta(minutes=5),
            starting_cash=Decimal(100),
            fast_period=fast,
            slow_period=slow,
            fee_bps=Decimal(0),
            slippage_bps=Decimal(0),
        )


@pytest.mark.parametrize("cost", [Decimal(-1), Decimal(10000), Decimal("NaN")])
def test_invalid_costs_are_rejected(cost: Decimal) -> None:
    with pytest.raises(InvalidBacktest, match="bps"):
        run_backtest(
            fixture_candles(),
            start=START,
            end=START + timedelta(minutes=5),
            starting_cash=Decimal(100),
            fast_period=2,
            slow_period=3,
            fee_bps=cost,
            slippage_bps=Decimal(0),
        )


def test_gap_reordering_and_decimal_overflow_fail_closed() -> None:
    with pytest.raises(InvalidBacktest, match="gap|ordering"):
        run_backtest(
            tuple(reversed(fixture_candles())),
            start=START,
            end=START + timedelta(minutes=5),
            starting_cash=Decimal(100),
            fast_period=2,
            slow_period=3,
            fee_bps=Decimal(0),
            slippage_bps=Decimal(0),
        )
    with pytest.raises(InvalidBacktest, match="decimal"):
        run_backtest(
            fixture_candles(),
            start=START,
            end=START + timedelta(minutes=5),
            starting_cash=Decimal("1e20"),
            fast_period=2,
            slow_period=3,
            fee_bps=Decimal(0),
            slippage_bps=Decimal(0),
        )


def test_incremental_engine_matches_backtest_across_process_boundaries() -> None:
    candles = fixture_candles()
    expected = run_fixture()
    state = initialize_incremental_backtest(
        candles[:2], starting_cash=Decimal(1000), slow_period=3
    )
    decisions = []
    fills = []
    equity = []
    for item in candles[2:]:
        step = advance_incremental_backtest(
            state,
            item,
            fast_period=2,
            slow_period=3,
            fee_bps=Decimal(40),
            slippage_bps=Decimal(5),
        )
        state = step.state
        if step.decision is not None:
            decisions.append(step.decision)
        if step.fill is not None:
            fills.append(step.fill)
        equity.append(step.equity)

    assert tuple(decisions) == expected.decisions
    assert tuple(fills) == expected.fills
    assert tuple(equity) == expected.equity_curve
    assert state.pending_decision == expected.decisions[-1]
