import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from crypto_trading_core.backtest import (
    BacktestSettings,
    build_parser,
    run_application,
)
from crypto_trading_core.contracts import (
    BacktestSpec,
    InvalidBacktestInput,
    load_candle_snapshot,
)
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from crypto_trading_core.validate_backtest import validate_manifest

START = datetime(2026, 1, 1, tzinfo=UTC)
DECIMAL = pa.decimal128(38, 18)


def _write_candle_snapshot(tmp_path: Path) -> tuple[Path, str]:
    run = tmp_path / "candles" / "runs" / "source-run"
    run.mkdir(parents=True)
    closes = ["10", "10", "11", "12", "9", "8", "1000"]
    rows_by_partition: dict[tuple[date, str], list[dict[str, object]]] = {}
    for index, value in enumerate(closes, start=-2):
        timestamp = START + timedelta(minutes=index)
        price = Decimal(value).quantize(Decimal("0.000000000000000001"))
        rows_by_partition.setdefault(
            (timestamp.date(), f"{timestamp.hour:02d}"), []
        ).append(
            {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "window_start": timestamp.replace(tzinfo=None),
                "window_end": (timestamp + timedelta(minutes=1)).replace(tzinfo=None),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "base_volume": Decimal("1.000000000000000000"),
                "quote_volume": price,
                "vwap": price,
                "trade_count": 1,
                "source_curated_snapshot_key": "b" * 64,
                "candle_schema_version": "v1",
            }
        )
    schema = pa.schema(
        [
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("us")),
            ("window_end", pa.timestamp("us")),
            ("open", DECIMAL),
            ("high", DECIMAL),
            ("low", DECIMAL),
            ("close", DECIMAL),
            ("base_volume", DECIMAL),
            ("quote_volume", DECIMAL),
            ("vwap", DECIMAL),
            ("trade_count", pa.int64()),
            ("source_curated_snapshot_key", pa.string()),
            ("candle_schema_version", pa.string()),
        ]
    )
    for (event_date, event_hour), rows in rows_by_partition.items():
        partition = (
            run
            / "interval=1m"
            / f"event_date={event_date.isoformat()}"
            / f"event_hour={event_hour}"
        )
        partition.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(list(reversed(rows)), schema=schema),
            partition / "part.parquet",
        )
    manifest = {
        "status": "published",
        "mode": "apply",
        "snapshot_key": "a" * 64,
        "candle_schema_version": "v1",
        "interval": "1m",
        "candle_output_uri": str(run),
        "candle_count": sum(len(rows) for rows in rows_by_partition.values()),
        "source_curated_snapshot_key": "b" * 64,
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    path = tmp_path / "candles" / "manifests" / "source" / "manifest.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()


def _arguments(manifest: Path, digest: str, output: Path):
    return build_parser().parse_args(
        [
            "--candle-manifest",
            str(manifest),
            "--candle-manifest-sha256",
            digest,
            "--exchange",
            "coinbase",
            "--symbol",
            "BTC-USD",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:05:00Z",
            "--starting-cash",
            "1000",
            "--fast-period",
            "2",
            "--slow-period",
            "3",
            "--fee-bps",
            "40",
            "--slippage-bps",
            "5",
            "--output",
            str(output),
            "--local-development",
        ]
    )


def _settings(output: Path) -> BacktestSettings:
    return BacktestSettings(
        storage=StorageSettings(),
        source_manifest_prefix="unused-for-local-input",
        source_output_prefix="unused-for-local-input",
        output_prefix=str(output),
        maximum_input_candles=100,
    )


def test_local_application_publishes_valid_idempotent_results(tmp_path: Path) -> None:
    source_manifest, digest = _write_candle_snapshot(tmp_path)
    output = tmp_path / "backtests"
    arguments = _arguments(source_manifest, digest, output)
    settings = _settings(output)

    first = run_application(
        arguments,
        settings,
        run_id="fixed-run",
        started_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    assert first["status"] == "published"
    assert first["execution_mode"] == "simulation"
    assert first["summary"]["decisions"] == 3
    assert first["summary"]["buys"] == 1
    assert first["summary"]["sells"] == 1
    assert first["summary"]["unfilled_terminal_decisions"] == 1

    validation = validate_manifest(
        first["manifest_uri"],
        settings=settings,
        local_development=True,
    )
    assert validation["status"] == "valid"
    assert validation["fills"] == 2

    second = run_application(arguments, settings, run_id="should-not-be-written")
    assert second["status"] == "resolved_existing_backtest"
    assert list((output / "manifests").rglob("manifest.json")) == [
        Path(first["manifest_uri"])
    ]
    assert not (output / "runs" / "should-not-be-written").exists()


def test_validator_rejects_a_tampered_artifact(tmp_path: Path) -> None:
    source_manifest, digest = _write_candle_snapshot(tmp_path)
    output = tmp_path / "backtests"
    settings = _settings(output)
    report = run_application(
        _arguments(source_manifest, digest, output), settings, run_id="tamper-run"
    )
    fills = Path(report["artifacts"]["fills.parquet"]["uri"])
    fills.write_bytes(fills.read_bytes() + b"tampered")

    with pytest.raises(InvalidBacktestInput, match="byte count|digest"):
        validate_manifest(
            report["manifest_uri"], settings=settings, local_development=True
        )


def test_manifest_pinning_and_key_versioning() -> None:
    manifest = {
        "status": "published",
        "mode": "apply",
        "snapshot_key": "a" * 64,
        "candle_schema_version": "v1",
        "interval": "1m",
        "candle_output_uri": "s3a://crypto-data/candles/runs/run-1",
        "candle_count": 50,
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    digest = hashlib.sha256(body).hexdigest()
    source = load_candle_snapshot(
        body,
        manifest_uri="manifest.json",
        expected_sha256=digest,
        allowed_manifest_prefix="s3a://crypto-data/candles/manifests",
        local_development=True,
    )
    base = BacktestSpec(
        candle_snapshot_key=source.snapshot_key,
        candle_manifest_sha256=source.manifest_sha256,
        exchange="coinbase",
        symbol="BTC-USD",
        start=START,
        end=START + timedelta(minutes=20),
        starting_cash=Decimal(1000),
        fast_period=5,
        slow_period=10,
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
    )
    changed = BacktestSpec(**{**vars(base), "fee_bps": Decimal(41)})

    assert base.key == base.key
    assert changed.key != base.key
    with pytest.raises(InvalidBacktestInput, match="digest"):
        load_candle_snapshot(
            body,
            manifest_uri="manifest.json",
            expected_sha256="0" * 64,
            allowed_manifest_prefix="unused",
            local_development=True,
        )


def test_local_paths_require_explicit_development_mode(tmp_path: Path) -> None:
    source_manifest, digest = _write_candle_snapshot(tmp_path)
    arguments = _arguments(source_manifest, digest, tmp_path / "output")
    arguments.local_development = False
    with pytest.raises(InvalidBacktestInput, match="local"):
        run_application(
            arguments,
            _settings(tmp_path / "output"),
            store=ObjectStorage(StorageSettings()),
        )
