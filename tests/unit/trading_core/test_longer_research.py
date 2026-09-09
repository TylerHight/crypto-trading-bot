import argparse
import hashlib
import json
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from crypto_trading_core import longer_research
from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes
from crypto_trading_core.experiment_contracts import load_experiment_spec
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from test_experiments import (
    START,
    _candle_snapshot,
    _settings,
    _spec_document,
    _write_spec,
)


def fixture(tmp_path, *, complete=False, no_candidate=False):
    manifest, _ = _candle_snapshot(tmp_path)
    document = json.loads(manifest.read_bytes())
    curated = tmp_path / "curated.json"
    curated.write_bytes(
        canonical_json_bytes({"status": "published", "snapshot_key": "b" * 64})
    )
    document.update(
        source_curated_manifest_uri=str(curated),
        source_curated_manifest_sha256=hashlib.sha256(curated.read_bytes()).hexdigest(),
    )
    if complete:
        path = next(Path(document["candle_output_uri"]).rglob("*.parquet"))
        original = pq.ParquetFile(path).read()
        row = original.to_pylist()[0]
        count = 90 * 1440 + 3
        # One file per date preserves real date pruning in the engine.
        for day in range(91):
            rows = []
            for minute in range(day * 1440, min((day + 1) * 1440, count)):
                stamp = START.replace(tzinfo=None) + timedelta(minutes=minute)
                rows.append(
                    {
                        **row,
                        "window_start": stamp,
                        "window_end": stamp + timedelta(minutes=1),
                    }
                )
            target = (
                Path(document["candle_output_uri"])
                / "interval=1m"
                / f"event_date={(START + timedelta(days=day)).date()}"
                / "event_hour=00"
                / "part.parquet"
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), target)
        document["candle_count"] = count
    manifest.write_bytes(canonical_json_bytes(document))
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    spec_doc = _spec_document(manifest, digest)
    start = START + timedelta(minutes=3)
    spec_doc["ranges"] = {
        "train": {"start": start, "end": start + timedelta(days=60)},
        "validation": {
            "start": start + timedelta(days=60),
            "end": start + timedelta(days=75),
        },
        "test": {
            "start": start + timedelta(days=75),
            "end": start + timedelta(days=90),
        },
    }
    if no_candidate:
        spec_doc["selection_policy"]["minimum_train_fills"] = 1000000
    path, spec_digest = _write_spec(
        tmp_path, json.loads(canonical_json_bytes(spec_doc))
    )
    settings = _settings(tmp_path)
    from dataclasses import replace

    settings = replace(settings, maximum_candidate_candle_evaluations=5000000)
    args = argparse.Namespace(
        spec=str(path),
        spec_sha256=spec_digest,
        output=str(tmp_path / "r"),
        local_development=True,
    )
    return args, settings


def test_insufficient_history_publishes_all_gaps_and_never_selects(
    tmp_path, monkeypatch
):
    args, settings = fixture(tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail("selection must not execute with insufficient history")

    monkeypatch.setattr(longer_research, "prepare_experiment", forbidden)
    result = longer_research.run_research(args, settings)
    assert result["summary"]["status"] == "inconclusive"
    assert result["summary"]["paper_trial_supported"] is False
    assert result["selection"] is None
    assert result["test_prices_accessed"] is False
    coverage_body = Path(result["coverage"]["uri"]).read_bytes()
    assert hashlib.sha256(coverage_body).hexdigest() == result["coverage"]["sha256"]
    coverage = json.loads(coverage_body)
    assert coverage["available_minutes"] == 14
    assert coverage["missing_minutes"] == 129600 - 14
    assert (
        sum(gap["minutes"] for gap in coverage["missing_ranges"])
        == coverage["missing_minutes"]
    )
    assert coverage["price_columns_accessed"] is False
    assert len(coverage["source_manifests"]) == 2
    assert longer_research.run_research(args, settings) == result


def test_coverage_detects_duplicates_warmup_and_leading_internal_trailing_gaps(
    tmp_path,
):
    args, _ = fixture(tmp_path)
    spec = load_experiment_spec(
        Path(args.spec).read_bytes(),
        spec_uri=args.spec,
        expected_sha256=args.spec_sha256,
        allowed_spec_prefix="unused",
        local_development=True,
    )
    start = spec.train.start
    stamps = [start + timedelta(minutes=n) for n in (1, 1, 3)]
    report = longer_research.coverage_report(
        spec,
        [
            {"window_start": stamp, "window_end": stamp + timedelta(minutes=1)}
            for stamp in stamps
        ],
    )
    assert report["duplicate_minutes"] == 1
    assert [gap["minutes"] for gap in report["missing_ranges"]] == [1, 1, 129596]
    assert report["warmup_missing_ranges"]
    assert report["status"] == "inconclusive"


def test_changed_inventory_or_config_is_rejected(tmp_path):
    args, settings = fixture(tmp_path)
    result = longer_research.run_research(args, settings)
    coverage = json.loads(Path(result["coverage"]["uri"]).read_bytes())
    parquet = Path(coverage["candle_files"][0]["uri"])
    table = pq.ParquetFile(parquet).read()
    pq.write_table(table, parquet, compression="gzip")
    with pytest.raises(InvalidBacktestInput, match="conflicts"):
        longer_research.run_research(args, settings)
    Path(args.spec).write_text("{}")
    with pytest.raises(InvalidBacktestInput, match="SHA-256"):
        longer_research.run_research(args, settings)


@pytest.mark.parametrize("no_candidate", [False, True])
def test_90_day_local_end_to_end_is_sealed_and_read_back(tmp_path, no_candidate):
    args, settings = fixture(tmp_path, complete=True, no_candidate=no_candidate)
    result = longer_research.run_research(args, settings)
    assert result["coverage_summary"]["status"] == "sufficient"
    selection = json.loads(Path(result["selection"]["uri"]).read_bytes())
    assert selection["test_data_accessed"] is False
    if no_candidate:
        assert result["summary"]["status"] == "no_candidate"
        assert result["evaluation"] is None
        assert result["summary"]["paper_trial_supported"] is False
    else:
        from decimal import Decimal

        assert result["summary"]["status"] == "evaluated"
        evaluation_body = Path(result["evaluation"]["uri"]).read_bytes()
        assert (
            hashlib.sha256(evaluation_body).hexdigest()
            == result["evaluation"]["sha256"]
        )
        evaluation = json.loads(evaluation_body)
        assert evaluation["selected_candidate"] == selection["selected_candidate"]
        assert evaluation["selection_manifest_sha256"] == result["selection"]["sha256"]
        summary = result["summary"]
        assert Decimal(summary["excess_return"]) == Decimal(
            summary["strategy_return"]
        ) - Decimal(summary["buy_and_hold_return"])
        assert summary["paper_trial_supported"] == (
            Decimal(summary["excess_return"]) > 0
        )
        assert Decimal(evaluation["summary"]["baseline"]["total_fees"]) > 0
    assert longer_research.run_research(args, settings) == result
    # A freshly hashed candidate change cannot reselect after the sealed result.
    modified = json.loads(Path(args.spec).read_bytes())
    modified["candidates"][0]["slow_period"] = 4
    modified_body = canonical_json_bytes(modified)
    Path(args.spec).write_bytes(modified_body)
    args.spec_sha256 = hashlib.sha256(modified_body).hexdigest()
    with pytest.raises(InvalidBacktestInput, match="conflicts"):
        longer_research.run_research(args, settings)


def test_registered_name_rejects_retuning_even_with_a_new_digest(tmp_path):
    args, settings = fixture(tmp_path)
    longer_research.run_research(args, settings)
    spec = json.loads(Path(args.spec).read_bytes())
    spec["fee_bps"] = "41"
    body = canonical_json_bytes(spec)
    Path(args.spec).write_bytes(body)
    args.spec_sha256 = hashlib.sha256(body).hexdigest()
    with pytest.raises(InvalidBacktestInput, match="conflicts"):
        longer_research.run_research(args, settings)


def test_test_price_access_is_blocked_during_selection(tmp_path, monkeypatch):
    args, settings = fixture(tmp_path, complete=True)

    def try_test_access(arguments, settings, **kwargs):
        kwargs["range_loader"](None, end=START + timedelta(days=90))
        pytest.fail("test access was allowed")

    monkeypatch.setattr(longer_research, "prepare_experiment", try_test_access)
    with pytest.raises(
        InvalidBacktestInput, match="test-period price access is blocked"
    ):
        longer_research.run_research(args, settings)


def test_publication_rejects_conflicting_bytes(tmp_path):
    store = ObjectStorage(StorageSettings())
    uri = str(tmp_path / "manifest.json")
    longer_research._publish(store, uri, {"a": 1})
    with pytest.raises(InvalidBacktestInput, match="conflicts"):
        longer_research._publish(store, uri, {"a": 2})


@pytest.mark.parametrize(
    "excess,supported", [("-0.01", False), ("0", False), ("0.01", True)]
)
def test_recommendation_requires_strictly_positive_net_excess(excess, supported):
    from decimal import Decimal

    evaluation = {
        "selected_candidate": {"candidate_id": "sma-1-2"},
        "summary": {
            "strategy": {"percentage_return": str(Decimal("0.02") + Decimal(excess))},
            "baseline": {"percentage_return": "0.02"},
            "strategy_excess_percentage_return": excess,
        },
    }
    coverage = {
        "status": "sufficient",
        "available_minutes": 129600,
        "expected_minutes": 129600,
    }
    summary = longer_research.research_summary(coverage, evaluation)
    assert summary["paper_trial_supported"] is supported
    if not supported:
        assert summary["recommendation"] == "Do not start a paper trial."
    coverage["status"] = "inconclusive"
    assert (
        longer_research.research_summary(coverage, evaluation)["paper_trial_supported"]
        is False
    )


def test_existing_summary_tampering_is_rejected(tmp_path):
    args, settings = fixture(tmp_path)
    result = longer_research.run_research(args, settings)
    path = Path(result["manifest_uri"])
    report = json.loads(path.read_bytes())
    report["summary"]["paper_trial_supported"] = True
    path.write_bytes(canonical_json_bytes(report))
    with pytest.raises(InvalidBacktestInput, match="summary conflicts"):
        longer_research.run_research(args, settings)
