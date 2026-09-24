from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from crypto_trading_core.contracts import InvalidBacktestInput
from crypto_trading_core.research_workspace import (
    ResearchWorkspaceSettings,
    create_workspace,
)
from crypto_trading_core.storage import StorageSettings


def _parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _settings(root: Path) -> ResearchWorkspaceSettings:
    candles = root / "candles"
    manifest = root / "manifests" / "published" / "manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    document = {"candle_output_uri": str(candles)}
    body = json.dumps(document).encode()
    manifest.write_bytes(body)
    _parquet(
        candles / "interval=1m" / "event_date=2026-06-10" / "part.parquet",
        [{"window_start": "2026-06-10 00:00:00", "close": 100.0}],
    )
    backtest_manifest = (
        root / "backtests" / "manifests" / "candidate-key" / "manifest.json"
    )
    backtest_manifest.parent.mkdir(parents=True, exist_ok=True)
    backtest_manifest.write_text(
        json.dumps({"backtest_run_id": "run-a", "backtest_key": "candidate-key"})
    )
    _parquet(
        root / "backtests" / "run-a" / "fills.parquet",
        [{"side": "BUY", "decision_time": "2026-06-10 00:00:00"}],
    )
    _parquet(
        root / "backtests" / "run-a" / "equity_curve.parquet",
        [{"equity": 10000.0, "window_start": "2026-06-10 00:00:00"}],
    )
    _parquet(
        root / "backtests" / "run-a" / "decisions.parquet",
        [{"decision": "BUY", "decision_time": "2026-06-10 00:00:00"}],
    )
    _parquet(
        root / "experiments" / "run-a" / "selection" / "candidate_results.parquet",
        [
            {
                "candidate_id": "sma-5-20",
                "fast_period": 5,
                "slow_period": 20,
                "range_name": "train",
                "backtest_key": "candidate-key",
                "percentage_return": -1.0,
            }
        ],
    )
    _parquet(
        root / "experiments" / "run-a" / "selection" / "baseline_results.parquet",
        [{"range_name": "train", "percentage_return": 1.0}],
    )
    _parquet(
        root / "experiments" / "run-a" / "evaluation" / "baseline_fills.parquet",
        [{"side": "BUY"}],
    )
    _parquet(
        root / "experiments" / "run-a" / "evaluation" / "baseline_equity_curve.parquet",
        [{"equity": 10000.0}],
    )
    report = root / "gap" / "reports" / "one" / "manifest.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        json.dumps(
            {
                "status": "published",
                "summary": {
                    "status": "no_candidate",
                    "message": "No strategy passed.",
                    "recommendation": "Do not start a paper trial.",
                },
            }
        )
    )
    selection = root / "gap" / "selections" / "one" / "manifest.json"
    selection.parent.mkdir(parents=True, exist_ok=True)
    selection.write_text(
        json.dumps(
            {
                "evidence": {
                    "candidates": {
                        "sma-5-20": {
                            "train": {
                                "segments": [
                                    {
                                        "chart": {
                                            "equity_points": [
                                                {
                                                    "time": "2026-06-10T00:00:00Z",
                                                    "equity": "10000",
                                                    "drawdown": "0",
                                                    "position": "FLAT",
                                                }
                                            ],
                                            "trade_markers": [
                                                {
                                                    "time": "2026-06-10T00:01:00Z",
                                                    "side": "BUY",
                                                    "execution_price": "70000",
                                                    "fee": "2.50",
                                                }
                                            ],
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        )
    )
    return ResearchWorkspaceSettings(
        workspace_path=root / "research.duckdb",
        candle_manifest_prefix=str(root / "manifests"),
        backtest_prefix=str(root / "backtests"),
        experiment_prefix=str(root / "experiments"),
        gap_aware_prefix=str(root / "gap"),
        storage=StorageSettings(),
    )


def test_workspace_queries_source_parquet_and_published_samples(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    candle = (
        tmp_path / "candles" / "interval=1m" / "event_date=2026-06-10" / "part.parquet"
    )
    before = hashlib.sha256(candle.read_bytes()).hexdigest()

    created = create_workspace(settings)

    assert Path(created["workspace_path"]).exists()
    assert created["candle_manifest_sha256"]
    assert created["dbeaver_init_sql"] == ""
    assert (
        "Daily BTC close-price line" in Path(created["starter_queries_sql"]).read_text()
    )
    connection = duckdb.connect(created["workspace_path"], read_only=True)
    try:
        assert connection.execute(
            "SELECT count(*) FROM research_data.candles"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT side FROM research_data.backtest_fills"
        ).fetchone() == ("BUY",)
        assert connection.execute(
            "SELECT backtest_run_id FROM research_data.backtest_runs"
        ).fetchone() == ("run-a",)
        assert connection.execute(
            "SELECT experiment_run_id FROM research_data.experiment_runs"
        ).fetchone() == ("run-a",)
        assert connection.execute(
            "SELECT count(*) FROM information_schema.table_constraints "
            "WHERE table_schema = 'research_model' AND constraint_type = 'FOREIGN KEY'"
        ).fetchone() == (15,)
        assert connection.execute(
            "SELECT backtest_run_id FROM research_model.backtest_fills"
        ).fetchone() == ("run-a",)
        assert connection.execute(
            "SELECT r.candidate_id, b.backtest_run_id, d.decision_time, f.side "
            "FROM research_model.candidate_results r "
            "JOIN research_model.backtest_runs b USING (backtest_key) "
            "JOIN research_model.backtest_decisions d USING (backtest_run_id) "
            "JOIN research_model.backtest_fills f "
            "ON f.backtest_run_id = d.backtest_run_id "
            "AND f.decision_time = d.decision_time"
        ).fetchone() == ("sma-5-20", "run-a", "2026-06-10 00:00:00", "BUY")
        assert connection.execute(
            "SELECT candidate_id, side, execution_price, fee FROM research_data.gap_aware_trade_samples"
        ).fetchone() == ("sma-5-20", "BUY", 70000, 2.5)
        assert connection.execute(
            "SELECT research_status, recommendation FROM research_data.gap_aware_reports"
        ).fetchone() == ("no_candidate", "Do not start a paper trial.")
        assert connection.execute(
            "SELECT position, equity FROM research_data.gap_aware_equity_samples"
        ).fetchone() == ("FLAT", 10000)
    finally:
        connection.close()
    assert hashlib.sha256(candle.read_bytes()).hexdigest() == before


def test_workspace_does_not_replace_an_existing_file_without_explicit_request(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    create_workspace(settings)

    with pytest.raises(InvalidBacktestInput, match="already exists"):
        create_workspace(settings)

    create_workspace(settings, replace=True)


def test_failed_refresh_keeps_the_existing_workspace(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    created = create_workspace(settings)
    workspace = Path(created["workspace_path"])
    before = hashlib.sha256(workspace.read_bytes()).hexdigest()
    missing = ResearchWorkspaceSettings(
        **{**settings.__dict__, "candle_manifest_prefix": str(tmp_path / "missing")}
    )

    with pytest.raises(InvalidBacktestInput, match="No historical candle manifest"):
        create_workspace(missing, replace=True)

    assert hashlib.sha256(workspace.read_bytes()).hexdigest() == before


def test_workspace_keeps_a_local_database_read_only_to_its_source_files(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    created = create_workspace(settings)

    connection = duckdb.connect(created["workspace_path"], read_only=True)
    try:
        with pytest.raises(duckdb.InvalidInputException):
            connection.execute(
                "CREATE TABLE research_data.should_not_write (id INTEGER)"
            )
    finally:
        connection.close()
