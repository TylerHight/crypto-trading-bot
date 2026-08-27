from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from crypto_exchange_adapters.coinbase_rest import CoinbaseTradeCoverage
from crypto_exchange_adapters.models import ExchangeTrade
from crypto_trading_domain import MarketTradeRawEvent, market_trade_event

from .archive import ArchiveScan
from .kafka import (
    AmbiguousPublicationError,
    KafkaReceipt,
    RetryablePublicationError,
)
from .reconciliation import canonical_json_bytes, reconciliation_key, utc_text
from .storage import ObjectStorage, child_uri, parse_location

EXPECTED_FINDING_FIELDS = frozenset({"symbol", "source_event_id", "event_time"})
RAW_TOPIC = "market.trades.raw.v1"


class InvalidBackfillInput(ValueError):
    """Persisted reconciliation input is not safe to use for publication."""


class ArchiveLike(Protocol):
    def scan(
        self,
        *,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
        sample_limit: int,
    ) -> ArchiveScan: ...

    def count_position(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        start_at: datetime,
        end_at: datetime,
    ) -> int: ...


class PublisherLike(Protocol):
    def publish(self, event: MarketTradeRawEvent) -> KafkaReceipt: ...

    def close(self, timeout_seconds: float = 10.0) -> None: ...


@dataclass(frozen=True)
class ConfirmedFinding:
    symbol: str
    source_event_id: str
    event_time: datetime

    @property
    def identity(self) -> tuple[str, str]:
        return self.symbol, self.source_event_id

    def safe_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "source_event_id": self.source_event_id,
            "event_time": utc_text(self.event_time),
        }


@dataclass(frozen=True)
class BackfillInput:
    reconciliation_run_id: UUID
    reconciliation_key: str
    incident_id: UUID | None
    symbol: str
    start_at: datetime
    end_at: datetime
    findings: tuple[ConfirmedFinding, ...]
    legacy_findings: bool


def _json_object(content: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBackfillInput(f"{name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise InvalidBackfillInput(f"{name} must be a JSON object")
    return value


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise InvalidBackfillInput(f"{name} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise InvalidBackfillInput(f"{name} is not ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidBackfillInput(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _uuid(value: object, name: str, *, optional: bool = False) -> UUID | None:
    if optional and value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as error:
        raise InvalidBackfillInput(f"{name} must be a UUID") from error


def _is_beneath(uri: str, prefix: str) -> bool:
    location = parse_location(uri)
    root = parse_location(prefix)
    if location.scheme != root.scheme or location.bucket != root.bucket:
        return False
    if location.scheme == "file":
        try:
            Path(location.key).resolve().relative_to(Path(root.key).resolve())
            return True
        except ValueError:
            return False
    clean_root = root.key.strip("/")
    clean_key = location.key.strip("/")
    return clean_key == clean_root or clean_key.startswith(f"{clean_root}/")


def load_backfill_input(
    *,
    storage: ObjectStorage,
    report_uri: str,
    findings_prefix: str,
    maximum_window: timedelta,
    allow_legacy_findings: bool = False,
    apply: bool = False,
) -> BackfillInput:
    """Validate the complete persisted chain before Kafka can be contacted."""

    report = _json_object(storage.read_bytes(report_uri), "reconciliation report")
    if report.get("status") != "gaps_found":
        raise InvalidBackfillInput("reconciliation report status must be gaps_found")
    if report.get("exchange") != "coinbase":
        raise InvalidBackfillInput("reconciliation report exchange must be coinbase")
    symbol = report.get("symbol")
    key = report.get("reconciliation_key")
    findings_uri = report.get("findings_uri")
    if not isinstance(symbol, str) or not symbol.strip():
        raise InvalidBackfillInput("reconciliation report symbol is missing")
    symbol = symbol.strip().upper()
    if not isinstance(key, str) or not key:
        raise InvalidBackfillInput("reconciliation key is missing")
    if not isinstance(findings_uri, str) or not _is_beneath(findings_uri, findings_prefix):
        raise InvalidBackfillInput("findings URI is outside the configured prefix")
    start_at = _aware_utc(report.get("start_at"), "start_at")
    end_at = _aware_utc(report.get("end_at_exclusive"), "end_at_exclusive")
    if start_at >= end_at or end_at - start_at > maximum_window:
        raise InvalidBackfillInput("reconciliation interval is invalid or exceeds the bound")
    if key != reconciliation_key(symbol, start_at, end_at):
        raise InvalidBackfillInput("reconciliation key does not match its symbol and interval")
    if report.get("raw_integrity_status") != "passed":
        raise InvalidBackfillInput("raw-integrity evidence is not passing")
    if report.get("raw_integrity_topic") != RAW_TOPIC:
        raise InvalidBackfillInput("raw-integrity evidence is for the wrong Kafka topic")
    integrity_completed = _aware_utc(
        report.get("raw_integrity_completed_at"), "raw_integrity_completed_at"
    )
    if integrity_completed < end_at:
        raise InvalidBackfillInput("raw-integrity evidence does not cover the interval")

    run_id = _uuid(report.get("run_id"), "report run_id")
    assert run_id is not None
    incident_id = _uuid(report.get("incident_id"), "incident_id", optional=True)
    findings_bytes = storage.read_bytes(findings_uri)
    findings_document = _json_object(findings_bytes, "findings document")
    digest = report.get("findings_sha256")
    legacy = digest is None
    if legacy:
        if not allow_legacy_findings:
            raise InvalidBackfillInput("legacy findings require --allow-legacy-findings")
        if apply:
            raise InvalidBackfillInput("legacy findings are dry-run-only")
    elif not isinstance(digest, str) or sha256(findings_bytes).hexdigest() != digest:
        raise InvalidBackfillInput("findings SHA-256 digest mismatch")
    if findings_document.get("run_id") != str(run_id):
        raise InvalidBackfillInput("report and findings run IDs differ")
    if findings_document.get("reconciliation_key") != key:
        raise InvalidBackfillInput("report and findings reconciliation keys differ")
    values = findings_document.get("missing_from_archive")
    if not isinstance(values, list) or not values:
        raise InvalidBackfillInput("findings document has no confirmed findings")
    if report.get("missing_from_archive") != len(values):
        raise InvalidBackfillInput("report and findings counts differ")

    findings: list[ConfirmedFinding] = []
    identities: set[tuple[str, str]] = set()
    for index, value in enumerate(values):
        if not isinstance(value, dict) or set(value) != EXPECTED_FINDING_FIELDS:
            raise InvalidBackfillInput(f"finding {index} has unexpected fields")
        finding_symbol = value.get("symbol")
        source_event_id = value.get("source_event_id")
        if (
            finding_symbol != symbol
            or not isinstance(source_event_id, str)
            or not source_event_id
            or source_event_id != source_event_id.strip()
        ):
            raise InvalidBackfillInput(f"finding {index} has an invalid identity")
        event_time = _aware_utc(value.get("event_time"), f"finding {index} event_time")
        if not start_at <= event_time < end_at:
            raise InvalidBackfillInput(f"finding {index} is outside the interval")
        finding = ConfirmedFinding(symbol, source_event_id, event_time)
        if finding.identity in identities:
            raise InvalidBackfillInput("duplicate finding identity")
        identities.add(finding.identity)
        findings.append(finding)
    return BackfillInput(
        reconciliation_run_id=run_id,
        reconciliation_key=key,
        incident_id=incident_id,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        findings=tuple(findings),
        legacy_findings=legacy,
    )


class BackfillStateStore:
    """Append-only safe metadata for claims, receipts, transitions, and runs."""

    def __init__(self, storage: ObjectStorage, output_base: str) -> None:
        self.storage = storage
        self.output_base = output_base

    @staticmethod
    def identity_token(value: BackfillInput, finding: ConfirmedFinding) -> str:
        identity = f"{value.reconciliation_run_id}:{finding.symbol}:{finding.source_event_id}"
        return sha256(identity.encode()).hexdigest()

    def write_attempt(self, value: BackfillInput, backfill_run_id: UUID, *, apply: bool) -> None:
        document = {
            "backfill_run_id": str(backfill_run_id),
            "reconciliation_run_id": str(value.reconciliation_run_id),
            "reconciliation_key": value.reconciliation_key,
            "symbol": value.symbol,
            "mode": "apply" if apply else "dry_run",
            "state": "started",
            "started_at": utc_text(datetime.now(UTC)),
            "confirmed_findings": len(value.findings),
        }
        self.storage.write_bytes_append_only(
            child_uri(self.output_base, "attempts", f"{backfill_run_id}.json"),
            canonical_json_bytes(document),
            content_type="application/json",
        )

    def claim(
        self, value: BackfillInput, finding: ConfirmedFinding, backfill_run_id: UUID
    ) -> bool:
        uri = child_uri(
            self.output_base,
            "claims",
            str(value.reconciliation_run_id),
            f"{self.identity_token(value, finding)}.json",
        )
        document = {
            "backfill_run_id": str(backfill_run_id),
            "reconciliation_run_id": str(value.reconciliation_run_id),
            "state": "publish_claimed",
            **finding.safe_dict(),
        }
        return self.storage.try_write_bytes_append_only(
            uri, canonical_json_bytes(document), content_type="application/json"
        )

    def write_receipt(
        self,
        value: BackfillInput,
        finding: ConfirmedFinding,
        backfill_run_id: UUID,
        receipt: KafkaReceipt,
    ) -> None:
        document = {
            "backfill_run_id": str(backfill_run_id),
            "reconciliation_run_id": str(value.reconciliation_run_id),
            "state": "published_pending_archive",
            **finding.safe_dict(),
            **receipt.as_dict(),
        }
        self.storage.write_bytes_append_only(
            self._receipt_uri(value, finding),
            canonical_json_bytes(document),
            content_type="application/json",
        )

    def read_receipt(
        self, value: BackfillInput, finding: ConfirmedFinding
    ) -> KafkaReceipt | None:
        content = self.storage.try_read_bytes(self._receipt_uri(value, finding))
        if content is None:
            return None
        payload = _json_object(content, "Kafka receipt")
        try:
            topic_value = payload["kafka_topic"]
            partition_value = payload["kafka_partition"]
            offset_value = payload["kafka_offset"]
            if (
                not isinstance(topic_value, str)
                or not topic_value
                or isinstance(partition_value, bool)
                or isinstance(offset_value, bool)
            ):
                raise TypeError
            partition = int(partition_value)
            offset = int(offset_value)
            if partition < 0 or offset < 0:
                raise ValueError
            acknowledged_value = payload.get("kafka_acknowledged_at")
            acknowledged_at = (
                _aware_utc(acknowledged_value, "kafka_acknowledged_at")
                if acknowledged_value is not None
                else None
            )
            return KafkaReceipt(
                topic=topic_value,
                partition=partition,
                offset=offset,
                acknowledged_at=acknowledged_at,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise InvalidBackfillInput("stored Kafka receipt is malformed") from error

    def record_state(
        self,
        value: BackfillInput,
        finding: ConfirmedFinding,
        backfill_run_id: UUID,
        state: str,
        receipt: KafkaReceipt | None = None,
    ) -> None:
        document: dict[str, object] = {
            "backfill_run_id": str(backfill_run_id),
            "reconciliation_run_id": str(value.reconciliation_run_id),
            "state": state,
            "recorded_at": utc_text(datetime.now(UTC)),
            **finding.safe_dict(),
        }
        if receipt:
            document.update(receipt.as_dict())
        self.storage.write_bytes_append_only(
            child_uri(
                self.output_base,
                "finding-states",
                self.identity_token(value, finding)[:40],
                f"{uuid4()}.json",
            ),
            canonical_json_bytes(document),
            content_type="application/json",
        )

    def write_run(self, report: Mapping[str, object]) -> str:
        uri = child_uri(
            self.output_base,
            "runs",
            f"event_date={str(report['started_at'])[:10]}",
            f"{report['backfill_run_id']}.json",
        )
        body = dict(report)
        body["report_uri"] = uri
        self.storage.write_bytes_append_only(
            uri, canonical_json_bytes(body), content_type="application/json"
        )
        return uri

    def _receipt_uri(self, value: BackfillInput, finding: ConfirmedFinding) -> str:
        return child_uri(
            self.output_base,
            "receipts",
            str(value.reconciliation_run_id),
            f"{self.identity_token(value, finding)}.json",
        )


@dataclass(frozen=True)
class FindingResult:
    finding: ConfirmedFinding
    state: str
    receipt: KafkaReceipt | None = None
    publication_attempted: bool = False
    acknowledged_this_run: bool = False

    def safe_dict(self) -> dict[str, object]:
        value: dict[str, object] = {**self.finding.safe_dict(), "state": self.state}
        if self.receipt:
            value.update(self.receipt.as_dict())
        return value


class BackfillService:
    def __init__(
        self,
        *,
        archive: ArchiveLike,
        state: BackfillStateStore,
        publisher_factory: Callable[[], PublisherLike],
        sample_limit: int = 20,
        verification_timeout_seconds: float = 30.0,
        verification_poll_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if sample_limit <= 0 or verification_timeout_seconds < 0 or verification_poll_seconds <= 0:
            raise ValueError("Backfill bounds are invalid")
        self.archive = archive
        self.state = state
        self.publisher_factory = publisher_factory
        self.sample_limit = sample_limit
        self.verification_timeout = verification_timeout_seconds
        self.verification_poll = verification_poll_seconds
        self.monotonic = monotonic
        self.sleep = sleep

    def run(
        self,
        value: BackfillInput,
        coverage: CoinbaseTradeCoverage,
        *,
        apply: bool,
        backfill_run_id: UUID | None = None,
        attempt_recorded: bool = False,
    ) -> dict[str, object]:
        run_id = backfill_run_id or uuid4()
        started_at = datetime.now(UTC)
        if not attempt_recorded:
            self.state.write_attempt(value, run_id, apply=apply)
        source = {(trade.symbol, trade.source_event_id): trade for trade in coverage.trades}
        results: list[FindingResult] = []
        publisher: PublisherLike | None = None
        try:
            for finding in value.findings:
                self.state.record_state(value, finding, run_id, "confirmed_missing")
                trade = source.get(finding.identity)
                if (
                    not coverage.coverage_complete
                    or trade is None
                    or trade.event_time.astimezone(UTC) != finding.event_time
                ):
                    results.append(self._record(value, finding, run_id, "unresolved_source_changed"))
                    continue
                scan = self.archive.scan(
                    symbol=value.symbol,
                    start_at=value.start_at,
                    end_at=value.end_at,
                    sample_limit=self.sample_limit,
                )
                if scan.malformed_values:
                    results.append(self._record(value, finding, run_id, "failed_permanent"))
                    continue
                if finding.identity in {item.identity for item in scan.trades}:
                    results.append(self._record(value, finding, run_id, "skipped_already_archived"))
                    continue
                if not apply:
                    results.append(self._record(value, finding, run_id, "dry_run_ready"))
                    continue
                if not self.state.claim(value, finding, run_id):
                    receipt = self.state.read_receipt(value, finding)
                    if receipt is None:
                        results.append(
                            self._record(
                                value, finding, run_id, "unresolved_ambiguous_publication"
                            )
                        )
                    else:
                        results.append(self._verify(value, finding, run_id, receipt))
                    continue
                self.state.record_state(value, finding, run_id, "publish_claimed")
                if publisher is None:
                    publisher = self.publisher_factory()
                event = self._event(value, trade)
                try:
                    receipt = publisher.publish(event)
                except AmbiguousPublicationError:
                    results.append(
                        self._record(
                            value,
                            finding,
                            run_id,
                            "unresolved_ambiguous_publication",
                            publication_attempted=True,
                        )
                    )
                    continue
                except (RetryablePublicationError, BufferError):
                    results.append(
                        self._record(
                            value,
                            finding,
                            run_id,
                            "failed_retryable",
                            publication_attempted=True,
                        )
                    )
                    continue
                self.state.write_receipt(value, finding, run_id, receipt)
                self.state.record_state(
                    value, finding, run_id, "published_pending_archive", receipt
                )
                results.append(
                    self._verify(
                        value,
                        finding,
                        run_id,
                        receipt,
                        publication_attempted=True,
                        acknowledged_this_run=True,
                    )
                )
        finally:
            if publisher is not None:
                publisher.close()
        report = self._report(value, run_id, started_at, results, apply=apply)
        report["report_uri"] = self.state.write_run(report)
        return report

    def _verify(
        self,
        value: BackfillInput,
        finding: ConfirmedFinding,
        run_id: UUID,
        receipt: KafkaReceipt,
        *,
        publication_attempted: bool = False,
        acknowledged_this_run: bool = False,
    ) -> FindingResult:
        deadline = self.monotonic() + self.verification_timeout
        if receipt.acknowledged_at is None:
            partition_start = value.start_at
            partition_end = value.end_at
        else:
            partition_start = receipt.acknowledged_at - timedelta(minutes=5)
            partition_end = receipt.acknowledged_at + timedelta(minutes=5)
        while True:
            count = self.archive.count_position(
                topic=receipt.topic,
                partition=receipt.partition,
                offset=receipt.offset,
                start_at=partition_start,
                end_at=partition_end,
            )
            if count == 1:
                return self._record(
                    value,
                    finding,
                    run_id,
                    "resolved_backfilled",
                    receipt,
                    publication_attempted=publication_attempted,
                    acknowledged_this_run=acknowledged_this_run,
                )
            if count > 1:
                return self._record(
                    value,
                    finding,
                    run_id,
                    "unresolved_ambiguous_publication",
                    receipt,
                    publication_attempted=publication_attempted,
                    acknowledged_this_run=acknowledged_this_run,
                )
            if self.monotonic() >= deadline:
                return self._record(
                    value,
                    finding,
                    run_id,
                    "published_pending_archive",
                    receipt,
                    publication_attempted=publication_attempted,
                    acknowledged_this_run=acknowledged_this_run,
                )
            self.sleep(min(self.verification_poll, max(0.0, deadline - self.monotonic())))

    def _record(
        self,
        value: BackfillInput,
        finding: ConfirmedFinding,
        run_id: UUID,
        state: str,
        receipt: KafkaReceipt | None = None,
        *,
        publication_attempted: bool = False,
        acknowledged_this_run: bool = False,
    ) -> FindingResult:
        self.state.record_state(value, finding, run_id, state, receipt)
        return FindingResult(
            finding,
            state,
            receipt,
            publication_attempted,
            acknowledged_this_run,
        )

    @staticmethod
    def _event(value: BackfillInput, trade: ExchangeTrade) -> MarketTradeRawEvent:
        return market_trade_event(
            exchange=trade.exchange,
            symbol=trade.symbol,
            source_event_id=trade.source_event_id,
            event_time=trade.event_time,
            source_sequence=None,
            producer="apps.historical_backfill",
            correlation_id=value.incident_id,
            causation_id=value.reconciliation_run_id,
            payload=trade.raw_payload,
        )

    def _report(
        self,
        value: BackfillInput,
        run_id: UUID,
        started_at: datetime,
        results: list[FindingResult],
        *,
        apply: bool,
    ) -> dict[str, object]:
        states = [result.state for result in results]
        resolved = {"skipped_already_archived", "resolved_backfilled"}
        publish_attempted = sum(result.publication_attempted for result in results)
        if not apply and "dry_run_ready" in states and all(
            state in {"dry_run_ready", "skipped_already_archived"} for state in states
        ):
            status = "dry_run_ready"
        elif all(state in resolved for state in states) and publish_attempted == 0:
            status = "resolved_no_action_needed"
        elif all(state in resolved for state in states):
            status = "resolved_backfilled"
        elif any(state in resolved for state in states):
            status = "partially_resolved"
        elif "failed_permanent" in states:
            status = "failed_permanent"
        elif "failed_retryable" in states:
            status = "failed_retryable"
        else:
            status = "unresolved"
        sample_states = sorted(set(states))
        return {
            "backfill_run_id": str(run_id),
            "reconciliation_run_id": str(value.reconciliation_run_id),
            "reconciliation_key": value.reconciliation_key,
            "mode": "apply" if apply else "dry_run",
            "symbol": value.symbol,
            "started_at": utc_text(started_at),
            "completed_at": utc_text(datetime.now(UTC)),
            "status": status,
            "confirmed_findings": len(results),
            "already_archived": states.count("skipped_already_archived"),
            "publish_attempted": publish_attempted,
            "kafka_acknowledged": sum(result.acknowledged_this_run for result in results),
            "archive_verified": states.count("resolved_backfilled"),
            "ambiguous": states.count("unresolved_ambiguous_publication")
            + states.count("published_pending_archive"),
            "failed": states.count("failed_retryable") + states.count("failed_permanent"),
            "samples": {
                state: [
                    result.safe_dict() for result in results if result.state == state
                ][: self.sample_limit]
                for state in sample_states
            },
        }


def exit_code(report: Mapping[str, object]) -> int:
    status = report.get("status")
    if status in {"resolved_no_action_needed", "resolved_backfilled"}:
        return 0
    if status == "dry_run_ready":
        return 2
    if status == "failed_permanent":
        return 4
    return 3
