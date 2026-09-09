from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from crypto_trading_domain.backtest import BacktestResult

DECIMAL = pa.decimal128(38, 18)
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

DECISION_SCHEMA = pa.schema(
    [
        ("decision_time", UTC_TIMESTAMP),
        ("observed_candle_window_start", UTC_TIMESTAMP),
        ("fast_sma", DECIMAL),
        ("slow_sma", DECIMAL),
        ("previous_target", pa.string()),
        ("new_target", pa.string()),
        ("strategy_version", pa.string()),
    ]
)

FILL_SCHEMA = pa.schema(
    [
        ("fill_time", UTC_TIMESTAMP),
        ("decision_time", UTC_TIMESTAMP),
        ("side", pa.string()),
        ("base_quantity", DECIMAL),
        ("reference_open_price", DECIMAL),
        ("execution_price", DECIMAL),
        ("gross_notional", DECIMAL),
        ("fee", DECIMAL),
        ("cash_after", DECIMAL),
        ("base_after", DECIMAL),
    ]
)

EQUITY_SCHEMA = pa.schema(
    [
        ("window_start", UTC_TIMESTAMP),
        ("close", DECIMAL),
        ("cash", DECIMAL),
        ("base_quantity", DECIMAL),
        ("position", pa.string()),
        ("equity", DECIMAL),
        ("drawdown", DECIMAL),
    ]
)


def _enum_values(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value.value if hasattr(value, "value") else value for key, value in row.items()}


def result_tables(result: BacktestResult) -> dict[str, pa.Table]:
    decisions = [_enum_values(asdict(item)) for item in result.decisions]
    fills = [_enum_values(asdict(item)) for item in result.fills]
    equity = [_enum_values(asdict(item)) for item in result.equity_curve]
    return {
        "decisions.parquet": pa.Table.from_pylist(decisions, schema=DECISION_SCHEMA),
        "fills.parquet": pa.Table.from_pylist(fills, schema=FILL_SCHEMA),
        "equity_curve.parquet": pa.Table.from_pylist(equity, schema=EQUITY_SCHEMA),
    }
