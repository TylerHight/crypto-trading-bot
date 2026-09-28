from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_trading_core.daily_momentum import DailyCandle
from crypto_trading_core.historical_relative_strength_screen import simulate

DAY = timedelta(days=1)
START = datetime(2025, 1, 1, tzinfo=UTC)


def _candles(closes: list[str]) -> tuple[DailyCandle, ...]:
    return tuple(
        DailyCandle(
            START + index * DAY,
            Decimal(close),
            Decimal(close),
            Decimal(close),
            Decimal(close),
            Decimal(1),
        )
        for index, close in enumerate(closes)
    )


def test_relative_strength_uses_prior_closed_returns_and_next_open_switch() -> None:
    candles = {
        "BTC-USD": _candles(["100", "100", "100", "100", "100", "100"]),
        "ETH-USD": _candles(["100", "100", "120", "120", "120", "120"]),
    }

    result = simulate(
        candles,
        start=START + 3 * DAY,
        end=START + 6 * DAY,
        lookback_days=2,
        rebalance_days=7,
        cash=Decimal(1000),
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
    )

    assert result["final_product"] == "ETH-USD"
    assert result["fill_count"] == 1
    assert result["completed_round_trips"] == 0
    assert result["rebalance_count"] == 1


def test_relative_strength_rejects_asset_calendars_that_do_not_align() -> None:
    candles = {
        "BTC-USD": _candles(["100", "100", "100", "100"]),
        "ETH-USD": _candles(["100", "100", "100"]),
    }

    try:
        simulate(
            candles,
            start=START + 3 * DAY,
            end=START + 4 * DAY,
            lookback_days=2,
            rebalance_days=7,
            cash=Decimal(1000),
            fee_bps=Decimal(40),
            slippage_bps=Decimal(5),
        )
    except ValueError as error:
        assert "calendars" in str(error)
    else:
        raise AssertionError("mismatched product calendars should be rejected")
