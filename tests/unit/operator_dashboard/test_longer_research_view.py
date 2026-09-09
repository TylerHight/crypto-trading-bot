import hashlib

import pytest
from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.sources import LiveDashboardSource
from crypto_operator_dashboard.web import _operator_summary, _research_view
from test_sources import NOW, FakeS3, _body, _documents


def _report(status="inconclusive", excess="-0.01"):
    evaluated = status == "evaluated"
    from decimal import Decimal

    return {
        "version": "longer-research-v1",
        "status": "published",
        "coverage_summary": {
            "status": "sufficient" if evaluated else "inconclusive",
            "available_minutes": 129600 if evaluated else 900,
            "expected_minutes": 129600,
            "missing_minutes": 0 if evaluated else 128700,
        },
        "summary": {
            "status": status,
            "selected_candidate": {"candidate_id": "sma-15-60"} if evaluated else None,
            "strategy_return": str(Decimal("0.02") + Decimal(excess))
            if evaluated
            else None,
            "buy_and_hold_return": "0.02" if evaluated else None,
            "excess_return": excess if evaluated else None,
            "paper_trial_supported": evaluated and Decimal(excess) > 0,
            "recommendation": "Do not start a paper trial."
            if not evaluated or Decimal(excess) <= 0
            else "Evidence supports considering a paper-only trial.",
            "message": "Not enough complete history."
            if not evaluated
            else "Saved test result.",
        },
    }


def _source(report):
    documents = _documents()
    documents["research/reports/manifest.json"] = _body(report)
    return LiveDashboardSource(
        DashboardSettings(research_report_prefix="research/reports/"),
        s3_client=FakeS3(documents),
    )


def test_inconclusive_research_supersedes_old_results_and_is_visible_at_top():
    report = _report()
    research = _source(report)._research_status(NOW)
    assert research["status"] == "inconclusive"
    assert research["oos"] is None
    assert research["paper_trial_supported"] is False
    assert (
        research["publication"]["sha256"] == hashlib.sha256(_body(report)).hexdigest()
    )
    page = _operator_summary(
        {"research": research, "pilot": {"status": "not_registered"}}
    )
    assert "Not enough data" in page
    assert "Do not start a paper trial." in page
    assert "Prepare 90 days of BTC history" in page
    assert "sma-5-20" not in page
    details = _research_view(research)
    assert "History coverage" in details
    assert "128700" in details


@pytest.mark.parametrize("excess", ["-0.01", "0", "0.01"])
def test_dashboard_matches_published_comparison_and_recommendation(excess):
    report = _report("evaluated", excess)
    research = _source(report)._research_status(NOW)
    assert research["status"] == "evaluated"
    assert research["oos"]["candidate"] == report["summary"]["selected_candidate"]
    for field in ("strategy_return", "buy_and_hold_return", "excess_return"):
        assert research["oos"][field] == report["summary"][field]
    assert (
        research["paper_trial_supported"] == report["summary"]["paper_trial_supported"]
    )
    assert research["recommendation"] == report["summary"]["recommendation"]


def test_invalid_recommendation_does_not_fall_back_to_old_positive_result():
    report = _report("evaluated")
    report["summary"]["paper_trial_supported"] = True
    research = _source(report)._research_status(NOW)
    assert research["status"] == "invalid"
    assert "oos" not in research


def test_gap_policy_review_is_visible_and_cannot_support_a_paper_trial():
    report = _report("policy_review_required")
    report["gap_policy"] = {
        "approval_status": "pending_human_review",
        "missing_candle_action": "exclude_and_reset",
        "indicator_action": "reset_after_gap",
    }
    report["policy_valid_minutes_by_range"] = {"selection": 107517, "test": 21600}

    research = _source(report)._research_status(NOW)

    assert research["status"] == "policy_review_required"
    assert research["paper_trial_supported"] is False
    page = _operator_summary(
        {"research": research, "pilot": {"status": "not_registered"}}
    )
    assert "Policy review needed" in page
    assert "Review the gap-safe policy before strategy selection" in page
    details = _research_view(research)
    assert "Gap-safe policy" in details
    assert "107517" in details


def test_gap_policy_review_is_the_dashboard_next_action():
    action = LiveDashboardSource._next_action(
        {"status": "healthy"},
        {"status": "healthy"},
        {"status": "healthy"},
        {"status": "healthy"},
        {"status": "not_registered"},
        {"status": "policy_review_required"},
        {"status": "gaps_found"},
    )

    assert action["action"] == "Review the gap-safe policy before strategy selection."


def test_dashboard_renders_gap_aware_result_as_research_only():
    report = {
        "version": "gap-aware-sealed-research-v1",
        "status": "published",
        "research_only": True,
        "test_prices_accessed_before_selection": False,
        "segments": {"train": [{}, {}], "validation": [{}], "test": [{}, {}]},
        "gap_policy": {"sha256": "a" * 64},
        "summary": {
            "status": "segmented_evaluated",
            "selected_candidate": {"candidate_id": "sma-15-60"},
            "strategy_return": "0.03",
            "buy_and_hold_return": "0.02",
            "excess_return": "0.01",
            "paper_trial_supported": False,
            "recommendation": "Review segmented research; it does not support a paper trial by itself.",
            "message": "Independent source segments were evaluated with resets at every gap.",
        },
    }
    documents = _documents()
    documents["gap-aware/reports/manifest.json"] = _body(report)
    source = LiveDashboardSource(
        DashboardSettings(gap_aware_research_prefix="gap-aware/reports/"),
        s3_client=FakeS3(documents),
    )

    research = source._research_status(NOW)

    assert research["status"] == "segmented_evaluated"
    assert research["paper_trial_supported"] is False
    assert research["segment_counts"] == {"train": 2, "validation": 1, "test": 2}
    page = _research_view(research)
    assert "Source segments" in page
    assert "Each segment resets SMA" in page


def test_corrupt_report_does_not_fall_back_to_old_result():
    source = _source(_report())
    source._s3_client.documents["research/reports/manifest.json"] = b"not json"
    assert source._research_status(NOW)["status"] == "invalid"
