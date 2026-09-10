from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from crypto_trading_core import gap_aware_research
from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes
from crypto_trading_core.experiment_contracts import Candidate, ExperimentRange
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from crypto_trading_domain.backtest import Candle

START = datetime(2026, 6, 10, tzinfo=UTC)


def _candles(start: datetime) -> tuple[Candle, ...]:
    values = (Decimal(3), Decimal(1), Decimal(3))
    return tuple(
        Candle(
            exchange="coinbase",
            symbol="BTC-USD",
            window_start=start + timedelta(minutes=index),
            window_end=start + timedelta(minutes=index + 1),
            open=value,
            high=value,
            low=value,
            close=value,
        )
        for index, value in enumerate(values)
    )


def test_segments_preserve_real_gap_boundaries_and_short_ranges_are_excluded():
    interval = ExperimentRange(START, START + timedelta(minutes=10))
    segments = gap_aware_research.contiguous_segments(
        [
            START,
            START + timedelta(minutes=1),
            START + timedelta(minutes=2),
            START + timedelta(minutes=5),
            START + timedelta(minutes=6),
            START + timedelta(minutes=9),
        ],
        interval,
    )

    assert [(item.start, item.end) for item in segments] == [
        (START, START + timedelta(minutes=3)),
        (START + timedelta(minutes=5), START + timedelta(minutes=7)),
        (START + timedelta(minutes=9), START + timedelta(minutes=10)),
    ]
    valid, excluded = gap_aware_research.eligible_segments(segments, slow_period=2)
    assert valid == (segments[0],)
    assert [item["reason"] for item in excluded] == [
        "insufficient_sma_warmup_and_two_evaluation_candles",
        "insufficient_sma_warmup_and_two_evaluation_candles",
    ]


def test_segment_runs_reset_state_cancel_terminal_orders_and_never_cross_gap(monkeypatch):
    first = ExperimentRange(START, START + timedelta(minutes=3))
    second = ExperimentRange(START + timedelta(minutes=5), START + timedelta(minutes=8))
    requested: list[ExperimentRange] = []

    def load(_source, interval, **_kwargs):
        requested.append(interval)
        return _candles(interval.start)

    monkeypatch.setattr(gap_aware_research, "_load_segment", load)
    spec = SimpleNamespace(
        starting_cash=Decimal(100), fee_bps=Decimal(0), slippage_bps=Decimal(0)
    )
    metrics, evidence, exclusions = gap_aware_research.evaluate_candidate_segments(
        Candidate("sma-1-2", 1, 2),
        (first, second),
        source=object(),
        spec=spec,
        settings=object(),
        test_unlocked=False,
    )

    assert requested == [first, second]
    assert metrics["segment_count"] == 2
    assert not exclusions
    assert all(item["pending_decision_cancelled_at_segment_end"] for item in evidence)
    assert all(item["chart"]["equity_points"] for item in evidence)
    assert all(item["chart"]["trade_marker_total"] == 0 for item in evidence)
    assert all(item["source_segment"]["end"] <= second.start or item["source_segment"]["start"] >= second.start for item in evidence)


def test_test_access_is_rejected_before_any_price_loader_runs(monkeypatch):
    called = False

    def unexpected(*_args, **_kwargs):
        nonlocal called
        called = True
        return ()

    monkeypatch.setattr(gap_aware_research, "load_candle_range", unexpected)
    spec = SimpleNamespace(test=ExperimentRange(START, START + timedelta(minutes=10)), exchange="coinbase", symbol="BTC-USD")
    with pytest.raises(InvalidBacktestInput, match="test-period price access"):
        gap_aware_research._load_segment(
            object(), spec.test, spec=spec, settings=object(), test_unlocked=False
        )
    assert called is False


def test_approved_review_requires_all_pinned_evidence_bytes(tmp_path):
    store = ObjectStorage(StorageSettings())
    report, coverage = tmp_path / "report.json", tmp_path / "coverage.json"
    report.write_bytes(b"report")
    coverage.write_bytes(b"coverage")
    policy_sha, source_sha = "a" * 64, "b" * 64
    spec = SimpleNamespace(
        gap_policy_uri="policy.json", gap_policy_sha256=policy_sha, candle_manifest_sha256=source_sha
    )
    review = {
        "status": "published", "version": "gap-policy-review-v1", "decision": "approved",
        "evidence": {
            "gap_policy_uri": "policy.json", "gap_policy_sha256": policy_sha,
            "source_manifest_sha256": source_sha, "report_uri": str(report),
            "report_sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
            "coverage_uri": str(coverage), "coverage_sha256": hashlib.sha256(coverage.read_bytes()).hexdigest(),
        },
    }
    path = tmp_path / "review.json"
    body = canonical_json_bytes(review)
    path.write_bytes(body)
    assert gap_aware_research._approved_review(
        store, review_uri=str(path), review_sha256=hashlib.sha256(body).hexdigest(), spec=spec
    )["decision"] == "approved"
    coverage.write_bytes(b"changed")
    with pytest.raises(InvalidBacktestInput, match="coverage evidence changed"):
        gap_aware_research._approved_review(
            store, review_uri=str(path), review_sha256=hashlib.sha256(body).hexdigest(), spec=spec
        )
