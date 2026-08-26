from datetime import UTC, datetime, timedelta

from crypto_historical_backfill.reconcile_coinbase_trades import (
    _qualify_raw_integrity,
    build_parser,
)
from crypto_historical_backfill.reconciliation import RawIntegrityEvidence

END = datetime(2026, 8, 25, 14, 1, tzinfo=UTC)


def test_cli_requires_explicit_bounds_and_integrity_report() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--symbol",
            "BTC-USD",
            "--start-at",
            "2026-08-25T14:00:00Z",
            "--end-at",
            "2026-08-25T14:01:00Z",
            "--raw-integrity-report",
            "audit.json",
        ]
    )

    assert args.start_at.tzinfo is UTC
    assert args.end_at.tzinfo is UTC
    assert args.maximum_window_seconds == 900


def test_integrity_report_must_cover_the_requested_interval() -> None:
    evidence = RawIntegrityEvidence(
        "passed",
        "market.trades.raw.v1",
        "2026-08-25T14:00:30Z",
    )

    qualified = _qualify_raw_integrity(evidence, interval_end=END)

    assert qualified.status == "stale_for_requested_interval"


def test_recent_matching_integrity_report_is_accepted() -> None:
    evidence = RawIntegrityEvidence(
        "passed",
        "market.trades.raw.v1",
        (END + timedelta(seconds=1)).isoformat(),
    )

    assert _qualify_raw_integrity(evidence, interval_end=END) == evidence
