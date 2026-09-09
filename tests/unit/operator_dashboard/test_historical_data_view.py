from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.sources import LiveDashboardSource
from crypto_operator_dashboard.web import _operator_summary
from test_sources import NOW, FakeS3, _body, _documents


def test_source_complete_gaps_are_clear_at_top_and_drive_next_action():
    documents = _documents()
    documents["history/manifest.json"] = _body(
        {
            "candle_schema_version": "exchange-ohlcv-v1",
            "source_kind": "exchange_ohlcv",
            "coverage": {
                "status": "incomplete",
                "available_minutes": 129834,
                "expected_minutes": 129839,
                "missing_minutes": 5,
                "conflicting_minutes": [],
            },
        }
    )
    source = LiveDashboardSource(
        DashboardSettings(
            historical_manifest_prefix="history/",
            raw_prefix="raw/",
            raw_audit_prefix="audit/",
            curated_manifest_prefix="curated/",
            candle_manifest_prefix="candles/",
            selection_manifest_prefix="selection/",
            evaluation_manifest_prefix="evaluation/",
            research_report_prefix="absent/",
        ),
        now=lambda: NOW,
        s3_client=FakeS3(documents),
        quality_reader=lambda: None,
    )
    history = source._historical_status(NOW)
    assert history["status"] == "gaps_found"
    assert history["coverage"]["missing_minutes"] == 5
    page = _operator_summary(
        {
            "history": history,
            "research": {
                "status": "inconclusive",
                "explanation": "Old result",
                "recommendation": "Do not start a paper trial.",
            },
            "pilot": {"status": "not_registered"},
        }
    )
    assert "5 source gaps" in page
    assert (
        "Define and test how strategy research should handle the five Coinbase outage minutes"
        in page
    )


def test_invalid_historical_manifest_is_rejected():
    source = LiveDashboardSource(
        DashboardSettings(historical_manifest_prefix="history/"),
        now=lambda: NOW,
        s3_client=FakeS3({"history/x.json": _body({})}),
    )
    assert source._historical_status(NOW)["status"] == "invalid"
