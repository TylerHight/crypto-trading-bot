import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from crypto_exchange_adapters.coinbase_rest import CoinbaseTradeCoverage, RestWindow
from crypto_exchange_adapters.models import ExchangeTrade
from crypto_historical_backfill.archive import ArchivedTrade, ArchiveScan
from crypto_historical_backfill.backfill import (
    BackfillService,
    BackfillStateStore,
    InvalidBackfillInput,
    load_backfill_input,
)
from crypto_historical_backfill.kafka import (
    AmbiguousPublicationError,
    KafkaReceipt,
    RetryablePublicationError,
)
from crypto_historical_backfill.reconciliation import canonical_json_bytes
from crypto_historical_backfill.storage import ObjectStorage, StorageSettings
from crypto_trading_domain import event_id_for_trade

START = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)
END = START + timedelta(minutes=1)
RECONCILIATION_RUN_ID = UUID("e32bf4ea-4e84-4d6a-91dc-7d651a49252c")
BACKFILL_RUN_ID = UUID("8b3a4086-f5cf-4c10-90ae-3dbeb56398cc")
KEY = "coinbase:BTC-USD:2026-08-25T14:00:00Z:2026-08-25T14:01:00Z:v3"


def finding(trade_id: str, *, symbol: str = "BTC-USD", second: int = 1) -> dict[str, str]:
    return {
        "symbol": symbol,
        "source_event_id": trade_id,
        "event_time": (START + timedelta(seconds=second)).isoformat().replace("+00:00", "Z"),
    }


def write_input(
    tmp_path: Path,
    *,
    findings: list[dict[str, str]] | None = None,
    report_changes: dict[str, Any] | None = None,
    findings_changes: dict[str, Any] | None = None,
    legacy: bool = False,
) -> tuple[ObjectStorage, str, str]:
    storage = ObjectStorage(StorageSettings())
    findings_prefix = str(tmp_path / "reconciliation" / "findings")
    findings_uri = str(Path(findings_prefix) / f"{RECONCILIATION_RUN_ID}.json")
    document: dict[str, Any] = {
        "run_id": str(RECONCILIATION_RUN_ID),
        "reconciliation_key": KEY,
        "missing_from_archive": findings or [finding("one")],
    }
    document.update(findings_changes or {})
    findings_bytes = canonical_json_bytes(document)
    storage.write_bytes_append_only(
        findings_uri, findings_bytes, content_type="application/json"
    )
    report: dict[str, Any] = {
        "run_id": str(RECONCILIATION_RUN_ID),
        "reconciliation_key": KEY,
        "incident_id": "203a3e67-6c17-4d14-b9ba-a98d725a1ab2",
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "start_at": "2026-08-25T14:00:00Z",
        "end_at_exclusive": "2026-08-25T14:01:00Z",
        "status": "gaps_found",
        "raw_integrity_status": "passed",
        "raw_integrity_topic": "market.trades.raw.v1",
        "raw_integrity_completed_at": "2026-08-25T14:01:00Z",
        "missing_from_archive": len(document["missing_from_archive"]),
        "findings_uri": findings_uri,
    }
    if not legacy:
        report["findings_sha256"] = sha256(findings_bytes).hexdigest()
    report.update(report_changes or {})
    report_uri = str(tmp_path / "reconciliation" / "reports" / "report.json")
    storage.write_bytes_append_only(
        report_uri, canonical_json_bytes(report), content_type="application/json"
    )
    return storage, report_uri, findings_prefix


def load(tmp_path: Path, **kwargs: Any):
    storage, report_uri, prefix = write_input(tmp_path, **kwargs)
    value = load_backfill_input(
        storage=storage,
        report_uri=report_uri,
        findings_prefix=prefix,
        maximum_window=timedelta(minutes=15),
    )
    return storage, value


@pytest.mark.parametrize(
    ("report_changes", "findings_changes", "message"),
    [
        ({"status": "passed"}, None, "status"),
        ({"run_id": "7777f9cf-adad-44c2-b9b5-ea13b1d304a0"}, None, "run IDs"),
        (None, {"reconciliation_key": "different"}, "keys"),
        ({"missing_from_archive": 2}, None, "counts"),
        ({"findings_sha256": "0" * 64}, None, "digest"),
    ],
)
def test_invalid_linked_inputs_are_rejected(
    tmp_path: Path,
    report_changes: dict[str, Any] | None,
    findings_changes: dict[str, Any] | None,
    message: str,
) -> None:
    storage, report_uri, prefix = write_input(
        tmp_path,
        report_changes=report_changes,
        findings_changes=findings_changes,
    )

    with pytest.raises(InvalidBackfillInput, match=message):
        load_backfill_input(
            storage=storage,
            report_uri=report_uri,
            findings_prefix=prefix,
            maximum_window=timedelta(minutes=15),
        )


def test_findings_outside_configured_prefix_are_rejected(tmp_path: Path) -> None:
    storage, report_uri, _ = write_input(tmp_path)

    with pytest.raises(InvalidBackfillInput, match="outside"):
        load_backfill_input(
            storage=storage,
            report_uri=report_uri,
            findings_prefix=str(tmp_path / "different"),
            maximum_window=timedelta(minutes=15),
        )


def test_duplicate_finding_identity_is_rejected(tmp_path: Path) -> None:
    storage, report_uri, prefix = write_input(
        tmp_path, findings=[finding("same"), finding("same")]
    )

    with pytest.raises(InvalidBackfillInput, match="duplicate"):
        load_backfill_input(
            storage=storage,
            report_uri=report_uri,
            findings_prefix=prefix,
            maximum_window=timedelta(minutes=15),
        )


def test_legacy_findings_are_explicit_and_dry_run_only(tmp_path: Path) -> None:
    storage, report_uri, prefix = write_input(tmp_path, legacy=True)

    with pytest.raises(InvalidBackfillInput, match="allow-legacy"):
        load_backfill_input(
            storage=storage,
            report_uri=report_uri,
            findings_prefix=prefix,
            maximum_window=timedelta(minutes=15),
        )
    value = load_backfill_input(
        storage=storage,
        report_uri=report_uri,
        findings_prefix=prefix,
        maximum_window=timedelta(minutes=15),
        allow_legacy_findings=True,
    )
    assert value.legacy_findings
    with pytest.raises(InvalidBackfillInput, match="dry-run-only"):
        load_backfill_input(
            storage=storage,
            report_uri=report_uri,
            findings_prefix=prefix,
            maximum_window=timedelta(minutes=15),
            allow_legacy_findings=True,
            apply=True,
        )


def trade(trade_id: str, *, symbol: str = "BTC-USD", second: int = 1) -> ExchangeTrade:
    return ExchangeTrade(
        exchange="coinbase",
        symbol=symbol,
        source_event_id=trade_id,
        source_sequence=None,
        event_time=START + timedelta(seconds=second),
        raw_payload={
            "trade_id": trade_id,
            "product_id": symbol,
            "time": (START + timedelta(seconds=second)).isoformat(),
            "price": "redacted-from-report",
        },
    )


def coverage(*trades: ExchangeTrade, complete: bool = True) -> CoinbaseTradeCoverage:
    return CoinbaseTradeCoverage(
        trades=trades,
        requests=(RestWindow(START, END, len(trades), "complete"),),
        raw_trade_count=len(trades),
        duplicate_identities=0,
        coverage_complete=complete,
        unresolved_reason=None if complete else "source_history_unavailable",
    )


class FakeArchive:
    def __init__(
        self,
        archived: tuple[ArchivedTrade, ...] = (),
        positions: dict[tuple[str, int, int], int] | None = None,
    ) -> None:
        self.archived = archived
        self.positions = positions or {}
        self.position_queries: list[tuple[str, int, int]] = []
        self.position_windows: list[tuple[datetime, datetime]] = []

    def scan(self, **_: Any) -> ArchiveScan:
        return ArchiveScan(self.archived, len(self.archived), 0, 0, ())

    def count_position(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        start_at: datetime,
        end_at: datetime,
    ) -> int:
        position = (topic, partition, offset)
        self.position_queries.append(position)
        self.position_windows.append((start_at, end_at))
        return self.positions.get(position, 0)


class FakePublisher:
    def __init__(self, outcomes: list[KafkaReceipt | Exception]) -> None:
        self.outcomes = outcomes
        self.events: list[Any] = []
        self.closed = False

    def publish(self, event: Any) -> KafkaReceipt:
        self.events.append(event)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self, timeout_seconds: float = 10.0) -> None:
        self.closed = True


def service(
    tmp_path: Path,
    archive: FakeArchive,
    publisher: FakePublisher,
    *,
    sample_limit: int = 20,
) -> BackfillService:
    storage = ObjectStorage(StorageSettings())
    return BackfillService(
        archive=archive,
        state=BackfillStateStore(storage, str(tmp_path / "state")),
        publisher_factory=lambda: publisher,
        sample_limit=sample_limit,
        verification_timeout_seconds=0,
        verification_poll_seconds=0.01,
    )


def test_dry_run_is_ready_without_creating_a_producer(tmp_path: Path) -> None:
    storage, value = load(tmp_path / "input")
    created = False

    def forbidden_factory() -> FakePublisher:
        nonlocal created
        created = True
        raise AssertionError("producer must not be created")

    result = BackfillService(
        archive=FakeArchive(),
        state=BackfillStateStore(storage, str(tmp_path / "state")),
        publisher_factory=forbidden_factory,
        verification_timeout_seconds=0,
    ).run(value, coverage(trade("one")), apply=False, backfill_run_id=BACKFILL_RUN_ID)

    assert result["status"] == "dry_run_ready"
    assert created is False


def test_source_change_and_already_archived_do_not_publish(tmp_path: Path) -> None:
    _, value = load(tmp_path / "input")
    publisher = FakePublisher([])
    changed = service(tmp_path / "changed", FakeArchive(), publisher).run(
        value, coverage(), apply=True
    )
    archived = ArchivedTrade("BTC-USD", "one", START + timedelta(seconds=1))
    skipped = service(tmp_path / "skipped", FakeArchive((archived,)), publisher).run(
        value, coverage(trade("one")), apply=True
    )

    assert changed["status"] == "unresolved"
    assert changed["samples"]["unresolved_source_changed"][0]["source_event_id"] == "one"
    assert skipped["status"] == "resolved_no_action_needed"
    assert publisher.events == []


def test_apply_publishes_canonical_event_and_verifies_exact_position(tmp_path: Path) -> None:
    _, value = load(tmp_path / "input")
    acknowledged_at = datetime(2026, 8, 26, 16, 5, tzinfo=UTC)
    receipt = KafkaReceipt("market.trades.raw.v1", 2, 99, acknowledged_at)
    archive = FakeArchive(positions={(receipt.topic, receipt.partition, receipt.offset): 1})
    publisher = FakePublisher([receipt])

    result = service(tmp_path, archive, publisher).run(
        value,
        coverage(trade("one")),
        apply=True,
        backfill_run_id=BACKFILL_RUN_ID,
    )

    event = publisher.events[0]
    assert result["status"] == "resolved_backfilled"
    assert event.event_id == event_id_for_trade("coinbase", "BTC-USD", "one")
    assert event.producer == "apps.historical_backfill"
    assert event.source_sequence is None
    assert event.causation_id == RECONCILIATION_RUN_ID
    assert archive.position_queries == [(receipt.topic, receipt.partition, receipt.offset)]
    assert archive.position_windows == [
        (
            acknowledged_at - timedelta(minutes=5),
            acknowledged_at + timedelta(minutes=5),
        )
    ]
    receipt_files = list((tmp_path / "state" / "receipts").rglob("*.json"))
    stored = json.loads(receipt_files[0].read_text())
    assert stored["kafka_partition"] == 2 and stored["kafka_offset"] == 99
    assert stored["kafka_acknowledged_at"] == "2026-08-26T16:05:00Z"
    assert "payload" not in stored and "price" not in json.dumps(result)


def test_verification_timeout_preserves_pending_state_and_claim_blocks_republish(
    tmp_path: Path,
) -> None:
    _, value = load(tmp_path / "input")
    receipt = KafkaReceipt("market.trades.raw.v1", 0, 7)
    publisher = FakePublisher([receipt])
    first = service(tmp_path, FakeArchive(), publisher).run(
        value, coverage(trade("one")), apply=True
    )
    second = service(tmp_path, FakeArchive(), publisher).run(
        value, coverage(trade("one")), apply=True
    )

    assert first["samples"]["published_pending_archive"][0]["kafka_offset"] == 7
    assert second["samples"]["published_pending_archive"][0]["kafka_offset"] == 7
    assert second["publish_attempted"] == 0
    assert second["kafka_acknowledged"] == 0
    assert len(publisher.events) == 1


def test_rerun_after_archival_publishes_nothing(tmp_path: Path) -> None:
    _, value = load(tmp_path / "input")
    receipt = KafkaReceipt("market.trades.raw.v1", 0, 7)
    publisher = FakePublisher([receipt])
    service(
        tmp_path,
        FakeArchive(positions={(receipt.topic, receipt.partition, receipt.offset): 1}),
        publisher,
    ).run(value, coverage(trade("one")), apply=True)
    archived = ArchivedTrade("BTC-USD", "one", START + timedelta(seconds=1))
    result = service(tmp_path, FakeArchive((archived,)), publisher).run(
        value, coverage(trade("one")), apply=True
    )

    assert result["status"] == "resolved_no_action_needed"
    assert len(publisher.events) == 1


def test_two_attempts_cannot_both_claim_same_finding(tmp_path: Path) -> None:
    storage, value = load(tmp_path / "input")
    state = BackfillStateStore(storage, str(tmp_path / "state"))

    assert state.claim(value, value.findings[0], BACKFILL_RUN_ID)
    assert not state.claim(value, value.findings[0], UUID(int=2))


def test_missing_acknowledgement_is_ambiguous(tmp_path: Path) -> None:
    _, value = load(tmp_path / "input")
    publisher = FakePublisher([AmbiguousPublicationError("crash window")])

    result = service(tmp_path, FakeArchive(), publisher).run(
        value, coverage(trade("one")), apply=True
    )

    assert result["status"] == "unresolved"
    assert result["ambiguous"] == 1


def test_success_and_failure_is_partial_and_samples_are_safe_and_capped(tmp_path: Path) -> None:
    _, value = load(
        tmp_path / "input", findings=[finding("one"), finding("two", second=2)]
    )
    receipt = KafkaReceipt("market.trades.raw.v1", 0, 8)
    publisher = FakePublisher([receipt, RetryablePublicationError("known failure")])
    archive = FakeArchive(positions={(receipt.topic, receipt.partition, receipt.offset): 1})

    result = service(tmp_path, archive, publisher, sample_limit=1).run(
        value, coverage(trade("one"), trade("two", second=2)), apply=True
    )

    assert result["status"] == "partially_resolved"
    assert all(len(samples) <= 1 for samples in result["samples"].values())
    assert "payload" not in json.dumps(result)


def test_btc_and_eth_claims_are_independent(tmp_path: Path) -> None:
    storage, btc = load(tmp_path / "btc")
    eth_key = "coinbase:ETH-USD:2026-08-25T14:00:00Z:2026-08-25T14:01:00Z:v3"
    _, eth = load(
        tmp_path / "eth",
        findings=[finding("one", symbol="ETH-USD")],
        report_changes={"symbol": "ETH-USD", "reconciliation_key": eth_key},
        findings_changes={"reconciliation_key": eth_key},
    )
    state = BackfillStateStore(storage, str(tmp_path / "state"))

    assert state.identity_token(btc, btc.findings[0]) != state.identity_token(eth, eth.findings[0])
