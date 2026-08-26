import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from crypto_exchange_adapters.coinbase_rest import CoinbaseTradeCoverage, RestWindow
from crypto_exchange_adapters.models import ExchangeTrade
from crypto_historical_backfill.archive import ArchivedTrade, ArchiveScan
from crypto_historical_backfill.reconciliation import (
    RawIntegrityEvidence,
    persist_outcome,
    reconcile_trades,
    reconciliation_key,
)
from crypto_historical_backfill.storage import ObjectStorage, StorageSettings

START = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)
END = START + timedelta(minutes=1)
RUN_ID = UUID("e32bf4ea-4e84-4d6a-91dc-7d651a49252c")
INTEGRITY = RawIntegrityEvidence(
    status="passed",
    topic="market.trades.raw.v1",
    completed_at="2026-08-25T14:01:00Z",
)


def rest_trade(
    trade_id: str, second: int = 0, symbol: str = "BTC-USD"
) -> ExchangeTrade:
    return ExchangeTrade(
        exchange="coinbase",
        symbol=symbol,
        source_event_id=trade_id,
        source_sequence=None,
        event_time=START + timedelta(seconds=second),
        raw_payload={},
    )


def rest(*trades: ExchangeTrade, complete: bool = True) -> CoinbaseTradeCoverage:
    return CoinbaseTradeCoverage(
        trades=tuple(trades),
        requests=(RestWindow(START, END, len(trades), "complete"),),
        raw_trade_count=len(trades),
        duplicate_identities=0,
        coverage_complete=complete,
        unresolved_reason=None if complete else "source_history_unavailable",
    )


def archived(*ids: str, malformed: int = 0) -> ArchiveScan:
    trades = tuple(
        ArchivedTrade("BTC-USD", trade_id, START + timedelta(seconds=index))
        for index, trade_id in enumerate(ids)
    )
    return ArchiveScan(
        trades=trades,
        archived_trade_count=len(trades),
        duplicate_identities=0,
        malformed_values=malformed,
        malformed_samples=(),
    )


def outcome(source: CoinbaseTradeCoverage, archive: ArchiveScan, **kwargs: object):
    return reconcile_trades(
        symbol="BTC-USD",
        start_at=START,
        end_at=END,
        raw_integrity=kwargs.pop("raw_integrity", INTEGRITY),
        rest=source,
        archive=archive,
        sample_limit=int(kwargs.pop("sample_limit", 20)),
        run_id=RUN_ID,
        started_at=START,
        completed_at=END,
        **kwargs,
    )


def test_identical_rest_and_archive_identities_pass() -> None:
    result = outcome(rest(rest_trade("one")), archived("one"))

    assert result.status == "passed"
    assert result.report["missing_from_archive"] == 0
    assert result.report["archive_only"] == 0


def test_rest_only_identity_is_a_confirmed_gap() -> None:
    result = outcome(rest(rest_trade("one"), rest_trade("two")), archived("one"))

    assert result.status == "gaps_found"
    assert result.report["missing_from_archive"] == 1
    assert result.confirmed_missing[0]["source_event_id"] == "two"


def test_duplicate_rest_identity_creates_one_missing_finding() -> None:
    source = CoinbaseTradeCoverage(
        trades=(rest_trade("same"),),
        requests=(RestWindow(START, END, 2, "complete"),),
        raw_trade_count=2,
        duplicate_identities=1,
        coverage_complete=True,
    )

    result = outcome(source, archived())

    assert result.report["missing_from_archive"] == 1
    assert result.report["duplicate_rest_identities"] == 1
    assert len(result.confirmed_missing) == 1


def test_archive_only_identity_is_reported_separately() -> None:
    result = outcome(rest(rest_trade("one")), archived("one", "archive-only"))

    assert result.status == "passed"
    assert result.report["archive_only"] == 1


def test_symbols_are_reconciled_independently() -> None:
    btc = outcome(rest(rest_trade("btc")), archived("btc"))
    eth = reconcile_trades(
        symbol="ETH-USD",
        start_at=START,
        end_at=END,
        raw_integrity=INTEGRITY,
        rest=rest(rest_trade("eth", symbol="ETH-USD")),
        archive=ArchiveScan(
            trades=(ArchivedTrade("ETH-USD", "eth", START),),
            archived_trade_count=1,
            duplicate_identities=0,
            malformed_values=0,
            malformed_samples=(),
        ),
        sample_limit=20,
        run_id=RUN_ID,
        started_at=START,
        completed_at=END,
    )

    assert btc.status == eth.status == "passed"
    assert btc.report["symbol"] == "BTC-USD"
    assert eth.report["symbol"] == "ETH-USD"


def test_equal_counts_with_different_identities_does_not_pass() -> None:
    result = outcome(rest(rest_trade("rest-only")), archived("archive-only"))

    assert result.status == "gaps_found"
    assert result.report["rest_trades"] == result.report["archived_trades"] == 1


def test_incomplete_rest_coverage_is_unresolved_and_does_not_confirm_gaps() -> None:
    result = outcome(rest(rest_trade("one"), complete=False), archived())

    assert result.status == "unresolved_source_history_unavailable"
    assert result.confirmed_missing == ()


def test_failed_raw_integrity_prevents_gap_classification() -> None:
    result = outcome(
        rest(rest_trade("one")),
        archived(),
        raw_integrity=RawIntegrityEvidence("failed", "market.trades.raw.v1", None),
    )

    assert result.status == "unresolved_raw_integrity_failure"
    assert result.confirmed_missing == ()


def test_malformed_archive_values_make_result_unresolved() -> None:
    result = outcome(rest(rest_trade("one")), archived("one", malformed=1))

    assert result.status.startswith("unresolved_")
    assert result.report["failure_reason"] == "malformed_archive_values"


def test_samples_are_capped_and_sorted() -> None:
    result = outcome(
        rest(rest_trade("three"), rest_trade("one"), rest_trade("two")),
        archived(),
        sample_limit=2,
    )

    samples = result.report["samples"]["missing_from_archive"]
    assert [sample["source_event_id"] for sample in samples] == ["one", "three"]
    assert len(result.confirmed_missing) == 3


def test_key_is_deterministic_and_normalizes_symbol() -> None:
    assert reconciliation_key(" btc-usd ", START, END) == reconciliation_key(
        "BTC-USD", START, END
    )


def test_raw_integrity_evidence_accepts_console_output() -> None:
    evidence = RawIntegrityEvidence.from_bytes(
        b'noise\nAUDIT_REPORT_JSON={"status":"passed","topic":"topic"}\n'
    )

    assert evidence == RawIntegrityEvidence("passed", "topic", None)


def test_persistence_writes_full_findings_and_is_append_only(tmp_path) -> None:
    result = outcome(
        rest(rest_trade("one"), rest_trade("two"), rest_trade("three")),
        archived(),
        sample_limit=1,
    )
    storage = ObjectStorage(StorageSettings())

    persisted = persist_outcome(storage, str(tmp_path), result)

    report_path = tmp_path / "reports" / "event_date=2026-08-25" / f"{RUN_ID}.json"
    findings_path = tmp_path / "findings" / "event_date=2026-08-25" / f"{RUN_ID}.json"
    report = json.loads(report_path.read_text())
    findings = json.loads(findings_path.read_text())
    assert report["report_uri"] == str(report_path)
    assert report["findings_sha256"] == sha256(findings_path.read_bytes()).hexdigest()
    assert len(findings["missing_from_archive"]) == 3
    assert len(persisted.report["samples"]["missing_from_archive"]) == 1

    with pytest.raises(FileExistsError):
        persist_outcome(storage, str(tmp_path), result)
