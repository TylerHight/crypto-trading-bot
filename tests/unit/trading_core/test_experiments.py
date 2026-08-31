import argparse
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from crypto_trading_core.backtest import (
    BacktestSettings,
    load_candle_range,
    load_candles,
)
from crypto_trading_core.contracts import InvalidBacktestInput
from crypto_trading_core.evaluate_experiment import build_parser as evaluation_parser
from crypto_trading_core.experiment_contracts import (
    SelectionPolicy,
    load_experiment_spec,
)
from crypto_trading_core.experiments import (
    ExperimentSettings,
    evaluate_experiment,
    prepare_experiment,
    select_candidate,
)
from crypto_trading_core.storage import StorageSettings
from crypto_trading_core.validate_experiment import (
    validate_evaluation,
    validate_selection,
)
from crypto_trading_domain.backtest import Candle, run_buy_and_hold

START = datetime(2026, 1, 1, tzinfo=UTC)
DECIMAL = pa.decimal128(38, 18)


def _candle_snapshot(tmp_path: Path) -> tuple[Path, str]:
    run = tmp_path / "candles" / "runs" / "source"
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
    prices = [10, 11, 9, 12, 8, 13, 7, 14, 6, 15, 5, 16, 4, 17, 3, 18, 2]
    partitions: dict[tuple[str, str], list[dict[str, object]]] = {}
    for minute, value in enumerate(prices):
        timestamp = START + timedelta(minutes=minute)
        price = Decimal(value).quantize(Decimal("0.000000000000000001"))
        partitions.setdefault(
            (timestamp.date().isoformat(), f"{timestamp.hour:02d}"), []
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
    for (event_date, event_hour), rows in partitions.items():
        partition = (
            run
            / "interval=1m"
            / f"event_date={event_date}"
            / f"event_hour={event_hour}"
        )
        partition.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), partition / "part.parquet")
    manifest = {
        "candle_count": len(prices),
        "candle_output_uri": str(run),
        "candle_schema_version": "v1",
        "interval": "1m",
        "mode": "apply",
        "snapshot_key": "a" * 64,
        "source_curated_snapshot_key": "b" * 64,
        "status": "published",
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    path = tmp_path / "candles" / "manifests" / "source" / "manifest.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()


def _spec_document(manifest: Path, digest: str, *, reverse: bool = False) -> dict:
    candidates = [
        {"candidate_id": "sma-1-2", "fast_period": 1, "slow_period": 2},
        {"candidate_id": "sma-2-3", "fast_period": 2, "slow_period": 3},
    ]
    if reverse:
        candidates.reverse()
    return {
        "experiment_spec_version": "v1",
        "name": "btc-usd-sma-v1",
        "candle_manifest_uri": str(manifest),
        "candle_manifest_sha256": digest,
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "starting_cash": "1000.000000000000000000",
        "fee_bps": "40",
        "slippage_bps": "5",
        "ranges": {
            "train": {"start": "2026-01-01T00:03:00Z", "end": "2026-01-01T00:07:00Z"},
            "validation": {
                "start": "2026-01-01T00:08:00Z",
                "end": "2026-01-01T00:12:00Z",
            },
            "test": {"start": "2026-01-01T00:13:00Z", "end": "2026-01-01T00:17:00Z"},
        },
        "candidates": candidates,
        "selection_policy": {
            "minimum_train_fills": 0,
            "maximum_train_drawdown": "1.000000000000000000",
            "maximum_validation_drawdown": "1.000000000000000000",
        },
    }


def _write_spec(tmp_path: Path, document: dict) -> tuple[Path, str]:
    body = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
    path = tmp_path / "specs" / "experiment.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()


def _settings(tmp_path: Path) -> ExperimentSettings:
    return ExperimentSettings(
        backtest=BacktestSettings(
            storage=StorageSettings(),
            source_manifest_prefix="unused",
            source_output_prefix="unused",
            output_prefix=str(tmp_path / "backtests"),
            maximum_input_candles=1000,
        ),
        spec_prefix="unused",
        output_prefix=str(tmp_path / "experiments"),
        maximum_candidates=50,
        maximum_candidate_candle_evaluations=10000,
    )


def test_spec_is_strict_and_candidate_order_has_one_canonical_identity(tmp_path: Path) -> None:
    manifest, digest = _candle_snapshot(tmp_path)
    first_body = json.dumps(_spec_document(manifest, digest), separators=(",", ":")).encode()
    second_body = json.dumps(
        _spec_document(manifest, digest, reverse=True), separators=(",", ":")
    ).encode()
    first = load_experiment_spec(
        first_body,
        spec_uri=str(tmp_path / "first.json"),
        expected_sha256=hashlib.sha256(first_body).hexdigest(),
        allowed_spec_prefix="unused",
        local_development=True,
    )
    second = load_experiment_spec(
        second_body,
        spec_uri=str(tmp_path / "second.json"),
        expected_sha256=hashlib.sha256(second_body).hexdigest(),
        allowed_spec_prefix="unused",
        local_development=True,
    )
    assert first.canonical_sha256 == second.canonical_sha256
    assert first.raw_sha256 != second.raw_sha256

    invalid = _spec_document(manifest, digest)
    invalid["unknown"] = True
    body = json.dumps(invalid).encode()
    with pytest.raises(InvalidBacktestInput, match="fields"):
        load_experiment_spec(
            body,
            spec_uri=str(tmp_path / "invalid.json"),
            expected_sha256=hashlib.sha256(body).hexdigest(),
            allowed_spec_prefix="unused",
            local_development=True,
        )


def test_duplicate_candidates_ranges_and_resource_caps_are_rejected(tmp_path: Path) -> None:
    manifest, digest = _candle_snapshot(tmp_path)
    base = _spec_document(manifest, digest)
    invalid_documents = []

    duplicate_id = deepcopy(base)
    duplicate_id["candidates"][1]["candidate_id"] = "sma-1-2"
    invalid_documents.append(duplicate_id)
    duplicate_periods = deepcopy(base)
    duplicate_periods["candidates"][1].update({"fast_period": 1, "slow_period": 2})
    invalid_documents.append(duplicate_periods)
    overlap = deepcopy(base)
    overlap["ranges"]["validation"]["start"] = "2026-01-01T00:06:00Z"
    invalid_documents.append(overlap)

    for index, document in enumerate(invalid_documents):
        body = json.dumps(document).encode()
        with pytest.raises(InvalidBacktestInput):
            load_experiment_spec(
                body,
                spec_uri=str(tmp_path / f"invalid-{index}.json"),
                expected_sha256=hashlib.sha256(body).hexdigest(),
                allowed_spec_prefix="unused",
                local_development=True,
            )

    body = json.dumps(base).encode()
    with pytest.raises(InvalidBacktestInput, match="resource cap"):
        load_experiment_spec(
            body,
            spec_uri=str(tmp_path / "too-large.json"),
            expected_sha256=hashlib.sha256(body).hexdigest(),
            allowed_spec_prefix="unused",
            local_development=True,
            maximum_candidate_candle_evaluations=10,
        )


def test_selection_policy_applies_all_tie_breaks_and_no_candidate_outcome() -> None:
    def rows(return_a: str, return_b: str, fee_a: str = "1", fee_b: str = "1"):
        output = []
        for candidate, value, fee in (
            ("a", return_a, fee_a),
            ("b", return_b, fee_b),
        ):
            for range_name in ("train", "validation"):
                output.append(
                    {
                        "candidate_id": candidate,
                        "range_name": range_name,
                        "fill_count": 2,
                        "maximum_drawdown": Decimal("0.1"),
                        "percentage_return": Decimal(value),
                        "total_fees": Decimal(fee),
                    }
                )
        return output

    permissive = SelectionPolicy(2, Decimal("0.5"), Decimal("0.5"))
    assert select_candidate(rows("2", "1"), permissive)[0] == "a"
    assert select_candidate(rows("2", "2", "2", "1"), permissive)[0] == "b"
    assert select_candidate(rows("2", "2"), permissive)[0] == "a"
    strict = SelectionPolicy(3, Decimal("0.5"), Decimal("0.5"))
    selected, evidence = select_candidate(rows("2", "1"), strict)
    assert selected is None
    assert all(not item["eligible"] for item in evidence)


def test_buy_and_hold_buys_first_open_with_shared_exact_cost_math() -> None:
    candles = tuple(
        Candle(
            "coinbase",
            "BTC-USD",
            START + timedelta(minutes=index),
            START + timedelta(minutes=index + 1),
            Decimal(price),
            Decimal(price),
            Decimal(price),
            Decimal(price),
        )
        for index, price in enumerate(("10", "12", "8"))
    )
    result = run_buy_and_hold(
        candles,
        start=START,
        end=START + timedelta(minutes=3),
        starting_cash=Decimal(1000),
        fee_bps=Decimal(40),
        slippage_bps=Decimal(5),
    )
    assert result.fills[0].fill_time == START
    assert result.fills[0].reference_open_price == Decimal("10.000000000000000000")
    assert result.fills[0].execution_price == Decimal("10.005000000000000000")
    assert result.summary.buys == 1
    assert result.summary.sells == 0
    assert result.summary.percentage_candles_long == Decimal("100.000000000000000000")


def test_prepare_never_requests_test_candles_and_evaluation_has_no_overrides(
    tmp_path: Path,
) -> None:
    manifest, digest = _candle_snapshot(tmp_path)
    spec_path, spec_digest = _write_spec(tmp_path, _spec_document(manifest, digest))
    settings = _settings(tmp_path)
    test_start = START + timedelta(minutes=13)
    requested_ends: list[datetime] = []

    def candidate_spy(source, spec, **kwargs):
        requested_ends.append(spec.end)
        assert spec.end <= test_start
        return load_candles(source, spec, **kwargs)

    def range_spy(source, **kwargs):
        requested_ends.append(kwargs["end"])
        assert kwargs["end"] <= test_start
        return load_candle_range(source, **kwargs)

    prepared = prepare_experiment(
        argparse.Namespace(
            spec=str(spec_path),
            spec_sha256=spec_digest,
            output=settings.output_prefix,
            local_development=True,
        ),
        settings,
        run_id="selection-run",
        candle_loader=candidate_spy,
        range_loader=range_spy,
    )
    assert prepared["status"] == "published"
    assert prepared["test_data_accessed"] is False
    assert prepared["selection_status"] == "selected"
    assert requested_ends and all(value <= test_start for value in requested_ends)
    assert evaluation_parser().parse_args(
        [
            "--selection-manifest",
            "manifest.json",
            "--selection-manifest-sha256",
            "a" * 64,
        ]
    )
    with pytest.raises(SystemExit):
        evaluation_parser().parse_args(
            [
                "--selection-manifest",
                "manifest.json",
                "--selection-manifest-sha256",
                "a" * 64,
                "--fast-period",
                "99",
            ]
        )

    selection_body = Path(prepared["manifest_uri"]).read_bytes()
    selection_validation = validate_selection(
        prepared["manifest_uri"],
        hashlib.sha256(selection_body).hexdigest(),
        settings=settings,
        local_development=True,
    )
    assert selection_validation["status"] == "valid"
    evaluated = evaluate_experiment(
        argparse.Namespace(
            selection_manifest=prepared["manifest_uri"],
            selection_manifest_sha256=hashlib.sha256(selection_body).hexdigest(),
            output=settings.output_prefix,
            local_development=True,
        ),
        settings,
        run_id="evaluation-run",
    )
    assert evaluated["status"] == "published"
    assert evaluated["summary"]["selection_basis"] == "train_and_validation_only"
    assert evaluated["summary"]["evaluation_range"] == "out_of_sample"
    evaluation_body = Path(evaluated["manifest_uri"]).read_bytes()
    evaluation_validation = validate_evaluation(
        evaluated["manifest_uri"],
        hashlib.sha256(evaluation_body).hexdigest(),
        settings=settings,
        local_development=True,
    )
    assert evaluation_validation["status"] == "valid"

    repeated = evaluate_experiment(
        argparse.Namespace(
            selection_manifest=prepared["manifest_uri"],
            selection_manifest_sha256=hashlib.sha256(selection_body).hexdigest(),
            output=settings.output_prefix,
            local_development=True,
        ),
        settings,
    )
    assert repeated["status"] == "resolved_existing_evaluation"


def test_no_eligible_candidate_is_sealed_and_cannot_be_evaluated(tmp_path: Path) -> None:
    manifest, digest = _candle_snapshot(tmp_path)
    document = _spec_document(manifest, digest)
    document["selection_policy"]["minimum_train_fills"] = 100
    spec_path, spec_digest = _write_spec(tmp_path, document)
    settings = _settings(tmp_path)
    prepared = prepare_experiment(
        argparse.Namespace(
            spec=str(spec_path),
            spec_sha256=spec_digest,
            output=settings.output_prefix,
            local_development=True,
        ),
        settings,
    )
    assert prepared["selection_status"] == "no_candidate_selected"
    assert prepared["selected_candidate"] is None
    selection_body = Path(prepared["manifest_uri"]).read_bytes()
    with pytest.raises(InvalidBacktestInput, match="no eligible"):
        evaluate_experiment(
            argparse.Namespace(
                selection_manifest=prepared["manifest_uri"],
                selection_manifest_sha256=hashlib.sha256(selection_body).hexdigest(),
                output=settings.output_prefix,
                local_development=True,
            ),
            settings,
        )
