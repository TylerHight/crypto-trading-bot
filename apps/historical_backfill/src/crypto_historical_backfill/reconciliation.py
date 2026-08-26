import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from crypto_exchange_adapters.coinbase_rest import CoinbaseTradeCoverage

from .archive import ArchivedTrade, ArchiveScan
from .storage import ObjectStorage, child_uri


def canonical_json_bytes(payload: object) -> bytes:
    """Serialize durable linked inputs deterministically for hashing."""

    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RawIntegrityEvidence:
    status: str
    topic: str | None
    completed_at: str | None

    @classmethod
    def from_bytes(cls, content: bytes) -> "RawIntegrityEvidence":
        text = content.decode("utf-8", errors="strict").strip()
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            report_lines = [
                line.partition("=")[2]
                for line in text.splitlines()
                if line.startswith("AUDIT_REPORT_JSON=")
            ]
            if not report_lines:
                raise ValueError("Raw integrity evidence does not contain JSON") from None
            payload = json.loads(report_lines[-1])
        if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
            raise TypeError("Raw integrity evidence lacks a status")
        return cls(
            status=payload["status"],
            topic=payload.get("topic") if isinstance(payload.get("topic"), str) else None,
            completed_at=(
                payload.get("completed_at")
                if isinstance(payload.get("completed_at"), str)
                else None
            ),
        )


@dataclass(frozen=True)
class ReconciliationOutcome:
    report: dict[str, Any]
    confirmed_missing: tuple[dict[str, str], ...]

    @property
    def status(self) -> str:
        return str(self.report["status"])


def reconciliation_key(
    symbol: str,
    start_at: datetime,
    end_at: datetime,
    source_api_version: str = "v3",
) -> str:
    return ":".join(
        [
            "coinbase",
            symbol.strip().upper(),
            utc_text(start_at),
            utc_text(end_at),
            source_api_version,
        ]
    )


def _safe_rest_trade(trade: Any) -> dict[str, str]:
    return {
        "symbol": str(trade.symbol),
        "source_event_id": str(trade.source_event_id),
        "event_time": utc_text(trade.event_time),
    }


def reconcile_trades(
    *,
    symbol: str,
    start_at: datetime,
    end_at: datetime,
    raw_integrity: RawIntegrityEvidence,
    rest: CoinbaseTradeCoverage,
    archive: ArchiveScan,
    sample_limit: int,
    incident_id: UUID | None = None,
    run_id: UUID | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> ReconciliationOutcome:
    """Compare normalized source identities and apply conservative status policy."""

    actual_run_id = run_id or uuid4()
    actual_started_at = started_at or datetime.now(UTC)
    actual_completed_at = completed_at or datetime.now(UTC)
    rest_by_identity = {(trade.symbol, trade.source_event_id): trade for trade in rest.trades}
    archive_by_identity: dict[tuple[str, str], ArchivedTrade] = {
        trade.identity: trade for trade in archive.trades
    }
    missing_identities = sorted(rest_by_identity.keys() - archive_by_identity.keys())
    archive_only_identities = sorted(archive_by_identity.keys() - rest_by_identity.keys())
    confirmed_missing = tuple(
        _safe_rest_trade(rest_by_identity[identity]) for identity in missing_identities
    )
    archive_only = tuple(
        archive_by_identity[identity].safe_dict() for identity in archive_only_identities
    )

    failure_reason: str | None = None
    if raw_integrity.status != "passed":
        status = "unresolved_raw_integrity_failure"
        failure_reason = f"raw_integrity_status={raw_integrity.status}"
    elif not rest.coverage_complete:
        if rest.unresolved_reason in {
            "source_result_truncated",
            "source_request_limit_exceeded",
        }:
            status = "unresolved_source_result_truncated"
        else:
            status = "unresolved_source_history_unavailable"
        failure_reason = rest.unresolved_reason
    elif archive.malformed_values:
        status = "unresolved_source_history_unavailable"
        failure_reason = "malformed_archive_values"
    elif confirmed_missing:
        status = "gaps_found"
    else:
        status = "passed"

    report: dict[str, Any] = {
        "run_id": str(actual_run_id),
        "reconciliation_key": reconciliation_key(symbol, start_at, end_at),
        "incident_id": str(incident_id) if incident_id else None,
        "exchange": "coinbase",
        "symbol": symbol.strip().upper(),
        "start_at": utc_text(start_at),
        "end_at_exclusive": utc_text(end_at),
        "started_at": utc_text(actual_started_at),
        "completed_at": utc_text(actual_completed_at),
        "status": status,
        "failure_reason": failure_reason,
        "raw_integrity_status": raw_integrity.status,
        "raw_integrity_topic": raw_integrity.topic,
        "raw_integrity_completed_at": raw_integrity.completed_at,
        "rest_requests": len(rest.requests),
        "rest_windows": [window.as_dict() for window in rest.requests],
        "rest_trades": rest.raw_trade_count,
        "unique_rest_trades": len(rest.trades),
        "archived_trades": archive.archived_trade_count,
        "unique_archived_trades": len(archive.trades),
        "missing_from_archive": len(confirmed_missing),
        "archive_only": len(archive_only),
        "duplicate_rest_identities": rest.duplicate_identities,
        "duplicate_archived_identities": archive.duplicate_identities,
        "malformed_archive_values": archive.malformed_values,
        "samples": {
            "missing_from_archive": list(confirmed_missing[:sample_limit]),
            "archive_only": list(archive_only[:sample_limit]),
            "malformed_archive_values": list(archive.malformed_samples[:sample_limit]),
        },
    }
    return ReconciliationOutcome(
        report=report,
        confirmed_missing=(confirmed_missing if status == "gaps_found" else ()),
    )


def persist_outcome(
    storage: ObjectStorage,
    output_base: str,
    outcome: ReconciliationOutcome,
) -> ReconciliationOutcome:
    """Append a report and, for confirmed gaps, its complete safe identity list."""

    report = dict(outcome.report)
    run_id = str(report["run_id"])
    event_date = str(report["started_at"])[:10]
    findings_uri: str | None = None
    if outcome.confirmed_missing:
        findings_uri = child_uri(
            output_base,
            "findings",
            f"event_date={event_date}",
            f"{run_id}.json",
        )
        findings_document = {
            "run_id": run_id,
            "reconciliation_key": report["reconciliation_key"],
            "missing_from_archive": list(outcome.confirmed_missing),
        }
        findings_bytes = canonical_json_bytes(findings_document)
        storage.write_bytes_append_only(
            findings_uri,
            findings_bytes,
            content_type="application/json",
        )
        report["findings_sha256"] = sha256(findings_bytes).hexdigest()

    report_uri = child_uri(
        output_base,
        "reports",
        f"event_date={event_date}",
        f"{run_id}.json",
    )
    report["findings_uri"] = findings_uri
    report.setdefault("findings_sha256", None)
    report["report_uri"] = report_uri
    storage.write_bytes_append_only(
        report_uri,
        json.dumps(report, indent=2, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    return ReconciliationOutcome(report=report, confirmed_missing=outcome.confirmed_missing)
