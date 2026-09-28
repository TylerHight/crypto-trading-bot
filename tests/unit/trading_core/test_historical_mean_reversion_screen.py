from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_trading_core.daily_momentum import DailyCandle
from crypto_trading_core.historical_mean_reversion_screen import (
    _selection_reasons,
    simulate,
)

DAY = timedelta(days=1)
START = datetime(2025, 1, 1, tzinfo=UTC)


def _candle(day: int, opening: str, close: str) -> DailyCandle:
    return DailyCandle(
        START + day * DAY,
        Decimal(opening),
        max(Decimal(opening), Decimal(close)),
        min(Decimal(opening), Decimal(close)),
        Decimal(close),
        Decimal(1),
    )


def test_selloff_signal_uses_close_then_next_open_and_fixed_holding_period() -> None:
    candles = (
        _candle(-1, "100", "100"),
        _candle(0, "100", "94"),  # Signal only: 6% close-to-close selloff.
        _candle(1, "90", "95"),  # Buy at this open.
        _candle(2, "100", "100"),  # Sell at this open after one complete daily session.
    )

    result = simulate(
        candles,
        start=START,
        end=START + 3 * DAY,
        selloff_threshold_pct=Decimal(5),
        holding_days=1,
        cash=Decimal(1000),
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
    )

    assert result["entries"] == result["completed_round_trips"] == 1
    assert result["fill_count"] == 2
    assert result["ending_equity"] > Decimal(1000)
    assert result["open_position"] is False


def test_selection_requires_activity_return_excess_and_drawdown_in_both_ranges() -> (
    None
):
    passing = {
        "train": {
            "strategy": {
                "entries": 5,
                "percentage_return": Decimal(1),
                "maximum_drawdown": Decimal("0.24"),
            },
            "excess_return_pct": Decimal("0.1"),
        },
        "validation": {
            "strategy": {
                "entries": 3,
                "percentage_return": Decimal(1),
                "maximum_drawdown": Decimal("0.25"),
            },
            "excess_return_pct": Decimal("0.1"),
        },
    }
    spec = {
        "minimum_train_entries": 5,
        "minimum_validation_entries": 3,
        "maximum_drawdown": "0.25",
    }

    assert _selection_reasons(passing, spec) == []
    passing["validation"]["strategy"]["entries"] = 0
    passing["validation"]["excess_return_pct"] = Decimal(0)

    assert _selection_reasons(passing, spec) == [
        "validation_insufficient_entries",
        "validation_excess_return_not_positive",
    ]
