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


def test_corrupt_report_does_not_fall_back_to_old_result():
    source = _source(_report())
    source._s3_client.documents["research/reports/manifest.json"] = b"not json"
    assert source._research_status(NOW)["status"] == "invalid"
