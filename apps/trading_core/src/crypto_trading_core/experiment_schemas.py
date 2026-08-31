from __future__ import annotations

from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

DECIMAL = pa.decimal128(38, 18)

CANDIDATE_RESULT_SCHEMA = pa.schema(
    [
        ("candidate_id", pa.string()),
        ("fast_period", pa.int32()),
        ("slow_period", pa.int32()),
        ("range_name", pa.string()),
        ("backtest_key", pa.string()),
        ("backtest_manifest_uri", pa.string()),
        ("backtest_manifest_sha256", pa.string()),
        ("starting_equity", DECIMAL),
        ("ending_equity", DECIMAL),
        ("absolute_return", DECIMAL),
        ("percentage_return", DECIMAL),
        ("maximum_drawdown", DECIMAL),
        ("fill_count", pa.int64()),
        ("total_fees", DECIMAL),
        ("gross_traded_notional", DECIMAL),
        ("percentage_candles_long", DECIMAL),
        ("baseline_percentage_return", DECIMAL),
        ("excess_percentage_return", DECIMAL),
        ("evaluation_candles", pa.int64()),
    ]
)

BASELINE_RESULT_SCHEMA = pa.schema(
    [
        ("range_name", pa.string()),
        ("baseline_key", pa.string()),
        ("baseline_version", pa.string()),
        ("starting_equity", DECIMAL),
        ("ending_equity", DECIMAL),
        ("absolute_return", DECIMAL),
        ("percentage_return", DECIMAL),
        ("maximum_drawdown", DECIMAL),
        ("fill_count", pa.int64()),
        ("total_fees", DECIMAL),
        ("gross_traded_notional", DECIMAL),
        ("percentage_candles_long", DECIMAL),
        ("evaluation_candles", pa.int64()),
    ]
)


def candidate_result_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=CANDIDATE_RESULT_SCHEMA)


def baseline_result_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BASELINE_RESULT_SCHEMA)
