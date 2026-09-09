import argparse
import hashlib
from pathlib import Path

import pytest
from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes
from crypto_trading_core.gap_policy_review import record_review
from crypto_trading_core.storage import ObjectStorage, StorageSettings
from test_experiments import _settings


def _write(path: Path, document: dict) -> str:
    body = canonical_json_bytes(document)
    path.write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def _arguments(tmp_path: Path, decision="approved") -> argparse.Namespace:
    policy = tmp_path / "policy.json"
    policy_sha = _write(
        policy,
        {
            "gap_policy_version": "contiguous-source-minutes-v1",
            "name": "policy",
            "approval_status": "pending_human_review",
            "source_manifest_sha256": "a" * 64,
            "missing_candle_action": "exclude_and_reset",
            "indicator_action": "reset_after_gap",
            "pending_order_action": "cancel_at_gap",
            "open_position_action": "exclude_cross_gap_returns",
            "declared_missing_ranges": [
                {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"}
            ],
        },
    )
    coverage = tmp_path / "coverage.json"
    coverage_sha = _write(
        coverage,
        {
            "source_manifests": [{"sha256": "a" * 64}],
            "missing_ranges": [
                {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"}
            ],
        },
    )
    report = tmp_path / "report.json"
    report_sha = _write(
        report,
        {
            "version": "longer-research-v1",
            "status": "published",
            "summary": {"status": "policy_review_required"},
            "gap_policy": {"sha256": policy_sha},
            "coverage": {"uri": str(coverage), "sha256": coverage_sha},
        },
    )
    return argparse.Namespace(
        policy=str(policy),
        policy_sha256=policy_sha,
        report=str(report),
        report_sha256=report_sha,
        decision=decision,
        reviewer="strategy-owner",
        note="Explicitly approved the pinned gap policy.",
        decided_at="2026-09-09T17:00:00Z",
        output=str(tmp_path / "reviews"),
        local_development=True,
    )


def test_review_is_pinned_append_only_and_rejects_a_conflicting_decision(tmp_path):
    arguments = _arguments(tmp_path)
    settings = _settings(tmp_path)
    store = ObjectStorage(StorageSettings())

    result = record_review(arguments, settings, store=store)

    assert result["decision"] == "approved"
    assert Path(result["manifest_uri"]).exists()
    assert record_review(arguments, settings, store=store) == result
    arguments.decision = "rejected"
    with pytest.raises(InvalidBacktestInput, match="conflicting review decision"):
        record_review(arguments, settings, store=store)


def test_rejection_is_recorded_and_tampered_evidence_is_rejected(tmp_path):
    arguments = _arguments(tmp_path, decision="rejected")
    settings = _settings(tmp_path)
    store = ObjectStorage(StorageSettings())

    assert record_review(arguments, settings, store=store)["decision"] == "rejected"
    Path(arguments.report).write_bytes(b"{}")
    with pytest.raises(InvalidBacktestInput, match="report SHA-256"):
        record_review(arguments, settings, store=store)
