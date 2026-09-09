from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

from .config import DashboardSettings

LOGGER = logging.getLogger(__name__)


class DashboardSource(Protocol):
    """A source of safe, already-normalized dashboard data."""

    def read(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class Artifact:
    name: str
    status: str
    source: str
    observed_at: datetime | None
    uri: str | None = None
    sha256: str | None = None
    details: Mapping[str, str] | None = None

    def document(self) -> dict[str, Any]:
        return {
            "details": dict(self.details or {}),
            "name": self.name,
            "observed_at": _timestamp(self.observed_at),
            "sha256": self.sha256,
            "source": self.source,
            "status": self.status,
            "uri": self.uri,
        }


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _safe_text(value: object) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _timestamp(value) or ""
    if value is None:
        return "—"
    return str(value)


class LiveDashboardSource:
    """Read-only adapters for Kafka, MinIO/S3, PostgreSQL, and a draft plan."""

    def __init__(
        self,
        settings: DashboardSettings,
        *,
        now: Callable[[], datetime] | None = None,
        s3_client: Any | None = None,
        quality_reader: Callable[[], dict[str, Any] | None] | None = None,
        database_connector: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        self._now = now or (lambda: datetime.now(UTC))
        self._s3_client = s3_client
        self._quality_reader = quality_reader or self._read_latest_quality_event
        self._database_connector = database_connector or self._connect_database

    def read(self) -> dict[str, Any]:
        now = self._now().astimezone(UTC)
        storage = self._storage_status(now)
        collector = self._collector_status(now)
        raw_archive = self._raw_archive_status(now)
        pilot, database = self._pilot_status(now)
        trusted_data = {
            "raw_integrity": self._raw_integrity_artifact(now).document(),
            "curated_trades": self._curated_artifact(now).document(),
            "one_minute_candles": self._candle_artifact(now).document(),
        }
        research = self._research_status(now)
        history = self._historical_status(now)
        draft_plan = self._draft_plan(now)
        return {
            "generated_at": _timestamp(now),
            "pipeline": [collector, raw_archive, storage, database],
            "pilot": pilot,
            "research": research,
            "history": history,
            "trusted_data": trusted_data,
            "draft_plan": draft_plan,
            "next_action": self._next_action(
                collector, raw_archive, storage, database, pilot, research, history
            ),
        }

    def _historical_status(self, now: datetime) -> dict[str, Any]:
        artifact, document = self._read_latest_json(
            "Historical BTC candles", self.settings.historical_manifest_prefix, now
        )
        if document is None:
            return {"status": artifact.status, "publication": artifact.document()}
        coverage = document.get("coverage")
        if (
            document.get("candle_schema_version") != "exchange-ohlcv-v1"
            or document.get("source_kind") != "exchange_ohlcv"
            or not isinstance(coverage, dict)
            or not isinstance(coverage.get("missing_minutes"), int)
        ):
            return {"status": "invalid", "message": "Historical dataset metadata is invalid."}
        expected = coverage.get("expected_minutes")
        available = coverage.get("available_minutes")
        ready = coverage.get("status") == "ready"
        missing = coverage["missing_minutes"]
        source_complete = (
            isinstance(expected, int)
            and isinstance(available, int)
            and available + missing == expected
            and not coverage.get("conflicting_minutes")
        )
        if not source_complete:
            return {"status": "incomplete", "message": "Historical download is incomplete."}
        return {
            "status": "ready" if ready else "gaps_found",
            "message": (
                "Historical data is ready for research."
                if ready
                else f"Historical download is complete; Coinbase published no candle for {missing} minutes."
            ),
            "next_action": (
                "Create a new fixed strategy experiment."
                if ready
                else "Define and test a gap-handling policy before strategy research."
            ),
            "coverage": {
                "available_minutes": coverage.get("available_minutes"),
                "expected_minutes": coverage.get("expected_minutes"),
                "missing_minutes": missing,
            },
            "publication": artifact.document(),
        }

    def _source_status(
        self,
        *,
        name: str,
        status: str,
        source: str,
        observed_at: datetime | None,
        detail: str,
    ) -> dict[str, str | None]:
        return {
            "detail": detail,
            "name": name,
            "observed_at": _timestamp(observed_at),
            "source": source,
            "status": status,
        }

    def _freshness(self, observed_at: datetime | None, now: datetime) -> str:
        if observed_at is None:
            return "unknown"
        if now - observed_at > timedelta(seconds=self.settings.stale_after_seconds):
            return "stale"
        return "healthy"

    def _artifact_freshness(self, observed_at: datetime | None, now: datetime) -> str:
        """Artifacts remain current for a workday; live feed checks stay fast."""

        if observed_at is None:
            return "unknown"
        if now - observed_at > timedelta(seconds=self.settings.artifact_stale_after_seconds):
            return "stale"
        return "healthy"

    def _s3(self) -> Any:
        if self._s3_client is None:
            import boto3  # type: ignore[import-untyped]

            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.settings.s3_endpoint_url,
                aws_access_key_id=self.settings.s3_access_key,
                aws_secret_access_key=self.settings.s3_secret_key,
                region_name=self.settings.s3_region,
            )
        return self._s3_client

    def _storage_status(self, now: datetime) -> dict[str, str | None]:
        try:
            self._s3().list_objects_v2(Bucket=self.settings.s3_bucket, MaxKeys=1)
        except Exception:  # noqa: BLE001 - an unavailable remote source is dashboard data.
            return self._source_status(
                name="MinIO object storage",
                status="unavailable",
                source="MinIO read-only probe",
                observed_at=None,
                detail="Object storage could not be read.",
            )
        return self._source_status(
            name="MinIO object storage",
            status="healthy",
            source="MinIO read-only probe",
            observed_at=now,
            detail="Object storage is readable.",
        )

    def _latest_object(self, prefix: str, *, suffix: str) -> dict[str, Any] | None:
        response = self._s3().list_objects_v2(
            Bucket=self.settings.s3_bucket,
            Prefix=prefix,
            MaxKeys=1000,
        )
        candidates = [
            item
            for item in response.get("Contents", [])
            if isinstance(item, dict) and str(item.get("Key", "")).endswith(suffix)
        ]
        if not candidates:
            return None
        return max(
            candidates, key=lambda item: item.get("LastModified", datetime.min.replace(tzinfo=UTC))
        )

    def _read_latest_json(
        self, name: str, prefix: str, now: datetime
    ) -> tuple[Artifact, dict[str, Any] | None]:
        try:
            item = self._latest_object(prefix, suffix=".json")
        except Exception:  # noqa: BLE001 - an unavailable remote source is dashboard data.
            return (
                Artifact(name, "unavailable", "MinIO object storage", None),
                None,
            )
        if item is None:
            return Artifact(name, "missing", "MinIO object storage", None), None
        key = str(item["Key"])
        modified = _parse_timestamp(item.get("LastModified"))
        try:
            response = self._s3().get_object(Bucket=self.settings.s3_bucket, Key=key)
            size = int(response.get("ContentLength", 0))
            if size > 1_000_000:
                raise ValueError("artifact exceeds dashboard inspection limit")
            body = response["Body"].read()
            document = json.loads(body)
            if not isinstance(document, dict):
                raise TypeError("artifact is not a JSON object")
        except Exception:  # noqa: BLE001 - invalid artifact content is safe to display as invalid.
            return (
                Artifact(
                    name,
                    "invalid",
                    "MinIO object storage",
                    modified,
                    uri=f"s3a://{self.settings.s3_bucket}/{key}",
                ),
                None,
            )
        return (
            Artifact(
                name,
                self._artifact_freshness(modified, now),
                "MinIO immutable publication",
                modified,
                uri=f"s3a://{self.settings.s3_bucket}/{key}",
                sha256=hashlib.sha256(body).hexdigest(),
            ),
            document,
        )

    def _raw_archive_status(self, now: datetime) -> dict[str, str | None]:
        try:
            item = self._latest_raw_object(now)
        except Exception:  # noqa: BLE001 - an unavailable remote source is dashboard data.
            return self._source_status(
                name="Raw archive",
                status="unavailable",
                source="MinIO raw Parquet metadata",
                observed_at=None,
                detail="Raw archive metadata could not be read.",
            )
        if item is None:
            return self._source_status(
                name="Raw archive",
                status="missing",
                source="MinIO raw Parquet metadata",
                observed_at=None,
                detail="No raw Parquet object was found in the inspected prefix.",
            )
        modified = _parse_timestamp(item.get("LastModified"))
        return self._source_status(
            name="Raw archive",
            status=self._freshness(modified, now),
            source="MinIO raw Parquet metadata",
            observed_at=modified,
            detail="A bounded raw Parquet metadata inspection found an archive object.",
        )

    def _latest_raw_object(self, now: datetime) -> dict[str, Any] | None:
        """Inspect the current and two preceding UTC-hour partitions before a bounded fallback."""
        candidates: list[dict[str, Any]] = []
        for hour_offset in range(3):
            partition_time = now - timedelta(hours=hour_offset)
            prefix = (
                f"{self.settings.raw_prefix.rstrip('/')}/"
                f"event_date={partition_time.date():%Y-%m-%d}/"
                f"event_hour={partition_time:%H}/"
            )
            item = self._latest_object(prefix, suffix=".parquet")
            if item is not None:
                candidates.append(item)
        if candidates:
            return max(
                candidates,
                key=lambda item: item.get("LastModified", datetime.min.replace(tzinfo=UTC)),
            )
        return self._latest_object(self.settings.raw_prefix, suffix=".parquet")

    def _read_latest_quality_event(self) -> dict[str, Any] | None:
        from confluent_kafka import (  # type: ignore[import-untyped]
            Consumer,
            KafkaError,
            TopicPartition,
        )

        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "broker.address.family": "v4",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "group.id": f"operator-dashboard-readonly-{uuid4()}",
            }
        )
        try:
            metadata = consumer.list_topics(timeout=self.settings.request_timeout_seconds)
            topic = metadata.topics.get(self.settings.kafka_quality_topic)
            if topic is None or topic.error is not None:
                return None
            events: list[dict[str, Any]] = []
            for partition in topic.partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(self.settings.kafka_quality_topic, partition),
                    timeout=self.settings.request_timeout_seconds,
                )
                if high <= low:
                    continue
                consumer.assign(
                    [
                        TopicPartition(
                            self.settings.kafka_quality_topic,
                            partition,
                            max(low, high - self.settings.kafka_scan_records),
                        )
                    ]
                )
                deadline = time.monotonic() + self.settings.request_timeout_seconds
                while time.monotonic() < deadline:
                    message = consumer.poll(0.1)
                    if message is None:
                        continue
                    error = message.error()
                    if error is not None:
                        if error.code() == KafkaError._PARTITION_EOF:
                            break
                        raise RuntimeError("Kafka quality topic could not be read")
                    payload = message.value()
                    if payload is None:
                        continue
                    try:
                        value = json.loads(payload)
                    except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if (
                        isinstance(value, dict)
                        and value.get("observation_type") == "health_summary"
                    ):
                        events.append(value)
            if not events:
                return None
            return max(events, key=lambda event: str(event.get("detected_at", "")))
        finally:
            consumer.close()

    def _collector_status(self, now: datetime) -> dict[str, str | None]:
        try:
            event = self._quality_reader()
        except Exception as error:  # noqa: BLE001 - an unavailable remote source is dashboard data.
            LOGGER.warning("Dashboard collector read failed: %s", type(error).__name__)
            return self._source_status(
                name="Collector feed",
                status="unavailable",
                source="Kafka data-quality topic",
                observed_at=None,
                detail="The latest collector health summary could not be read.",
            )
        if event is None:
            return self._source_status(
                name="Collector feed",
                status="missing",
                source="Kafka data-quality topic",
                observed_at=None,
                detail="No collector health summary is available yet.",
            )
        observed = _parse_timestamp(event.get("detected_at"))
        details = [
            f"trades={_safe_text(event.get('trades_observed'))}",
            f"sequence gaps={_safe_text(event.get('sequence_gap_count'))}",
            f"malformed={_safe_text(event.get('malformed_message_count'))}",
        ]
        return self._source_status(
            name="Collector feed",
            status=self._freshness(observed, now),
            source="Kafka data-quality topic",
            observed_at=observed,
            detail=", ".join(details),
        )

    def _raw_integrity_artifact(self, now: datetime) -> Artifact:
        artifact, document = self._read_latest_json(
            "Raw integrity audit", self.settings.raw_audit_prefix, now
        )
        if document is None:
            return artifact
        partitions = document.get("partitions")
        if not isinstance(partitions, list):
            return Artifact(
                artifact.name,
                "invalid",
                artifact.source,
                artifact.observed_at,
                artifact.uri,
                artifact.sha256,
            )
        kafka_records = sum(
            int(item.get("kafka_records", 0)) for item in partitions if isinstance(item, dict)
        )
        missing = sum(
            int(item.get("missing_from_parquet", 0))
            for item in partitions
            if isinstance(item, dict)
        )
        duplicates = sum(
            int(item.get("duplicate_parquet_positions", 0))
            for item in partitions
            if isinstance(item, dict)
        )
        return Artifact(
            artifact.name,
            artifact.status,
            artifact.source,
            artifact.observed_at,
            artifact.uri,
            artifact.sha256,
            {
                "Kafka records": str(kafka_records),
                "Missing archive positions": str(missing),
                "Duplicate archive positions": str(duplicates),
                "Result": _safe_text(document.get("status")),
            },
        )

    def _curated_artifact(self, now: datetime) -> Artifact:
        artifact, document = self._read_latest_json(
            "Curated trades", self.settings.curated_manifest_prefix, now
        )
        if document is None:
            return artifact
        return Artifact(
            artifact.name,
            artifact.status,
            artifact.source,
            artifact.observed_at,
            artifact.uri,
            artifact.sha256,
            {
                "Logical trades": _safe_text(document.get("curated_logical_trades")),
                "Quarantined rows": _safe_text(document.get("quarantined_input_rows")),
                "Result": _safe_text(document.get("status")),
                "Snapshot key": _safe_text(document.get("snapshot_key")),
            },
        )

    def _candle_artifact(self, now: datetime) -> Artifact:
        artifact, document = self._read_latest_json(
            "One-minute candles", self.settings.candle_manifest_prefix, now
        )
        if document is None:
            return artifact
        return Artifact(
            artifact.name,
            artifact.status,
            artifact.source,
            artifact.observed_at,
            artifact.uri,
            artifact.sha256,
            {
                "Candles": _safe_text(document.get("candle_count")),
                "Source trades": _safe_text(document.get("source_curated_trades")),
                "Result": _safe_text(document.get("status")),
                "Snapshot key": _safe_text(document.get("snapshot_key")),
            },
        )

    def _research_status(self, now: datetime) -> dict[str, Any]:
        publication, report = self._read_latest_json(
            "Longer strategy research", self.settings.research_report_prefix, now
        )
        if report is not None:
            summary = report.get("summary")
            if (
                report.get("version") != "longer-research-v1"
                or report.get("status") != "published"
                or not isinstance(summary, dict)
                or summary.get("status") not in {"inconclusive", "no_candidate", "evaluated"}
            ):
                return {
                    "status": "invalid",
                    "explanation": "The research report could not be verified.",
                }
            if summary["status"] == "evaluated":
                try:
                    strategy_return = Decimal(str(summary["strategy_return"]))
                    baseline_return = Decimal(str(summary["buy_and_hold_return"]))
                    excess = Decimal(str(summary["excess_return"]))
                    coverage = report["coverage_summary"]
                    valid = (
                        all(
                            value.is_finite()
                            for value in (strategy_return, baseline_return, excess)
                        )
                        and strategy_return - baseline_return == excess
                        and coverage["status"] == "sufficient"
                        and coverage["available_minutes"] >= 129600
                        and coverage["missing_minutes"] == 0
                        and bool(summary.get("selected_candidate"))
                        and summary.get("paper_trial_supported") is (excess > 0)
                    )
                except (KeyError, TypeError, ValueError, ArithmeticError):
                    valid = False
                if not valid:
                    return {
                        "status": "invalid",
                        "explanation": "The research comparison is incomplete or inconsistent.",
                    }
            elif summary.get("paper_trial_supported") is not False:
                return {
                    "status": "invalid",
                    "explanation": "Incomplete research cannot support a paper trial.",
                }
            return {
                "status": summary["status"],
                "explanation": summary.get("message"),
                "recommendation": summary.get("recommendation"),
                "paper_trial_supported": summary.get("paper_trial_supported") is True,
                "coverage": report.get("coverage_summary"),
                "publication": publication.document(),
                "oos": {
                    "candidate": summary.get("selected_candidate"),
                    "strategy_return": summary.get("strategy_return"),
                    "buy_and_hold_return": summary.get("buy_and_hold_return"),
                    "excess_return": summary.get("excess_return"),
                }
                if summary["status"] == "evaluated"
                else None,
            }
        if publication.status not in {"missing"}:
            return {
                "status": publication.status,
                "explanation": "Longer research could not be read. Check object storage.",
            }
        selection, selection_document = self._read_latest_json(
            "Sealed candidate selection", self.settings.selection_manifest_prefix, now
        )
        evaluation, evaluation_document = self._read_latest_json(
            "OOS evaluation", self.settings.evaluation_manifest_prefix, now
        )
        result: dict[str, Any] = {
            "evaluation": evaluation.document(),
            "explanation": "Out-of-sample results are evidence, not a profitability claim.",
            "selection": selection.document(),
            "status": "unavailable",
        }
        if selection_document is not None:
            result["selection_summary"] = {
                "candidate": selection_document.get("selected_candidate"),
                "test_data_accessed_before_selection": selection_document.get("test_data_accessed"),
                "test_range": selection_document.get("ranges", {}).get("test"),
                "validation_range": selection_document.get("ranges", {}).get("validation"),
                "train_range": selection_document.get("ranges", {}).get("train"),
            }
        if evaluation_document is not None:
            summary = evaluation_document.get("summary")
            if isinstance(summary, dict):
                strategy = summary.get("strategy")
                baseline = summary.get("baseline")
                result["oos"] = {
                    "buy_and_hold_return": (
                        baseline.get("percentage_return") if isinstance(baseline, dict) else None
                    ),
                    "candidate": evaluation_document.get("selected_candidate"),
                    "excess_return": summary.get("strategy_excess_percentage_return"),
                    "strategy_return": (
                        strategy.get("percentage_return") if isinstance(strategy, dict) else None
                    ),
                    "test_range": evaluation_document.get("test_range"),
                }
                result["status"] = evaluation.status
        elif selection_document is not None:
            result["status"] = selection.status
        return result

    def _connect_database(self) -> Any:
        import psycopg  # type: ignore[import-untyped]
        from psycopg.rows import dict_row  # type: ignore[import-untyped]

        return psycopg.connect(self.settings.database_url, row_factory=dict_row)

    def _pilot_status(self, now: datetime) -> tuple[dict[str, Any], dict[str, str | None]]:
        try:
            with self._database_connector() as connection:
                connection.execute("BEGIN READ ONLY")
                row = connection.execute(
                    """
                    SELECT p.pilot_id, p.state AS pilot_state, p.plan, p.raw_plan_sha256,
                           p.canonical_plan_sha256, p.created_at, p.updated_at, p.finalized_at,
                           p.assessment, s.state AS session_state, s.cash, s.base_quantity,
                           s.current_equity, s.total_fees, s.updated_at AS session_updated_at,
                           (SELECT count(*) FROM paper_candle_inputs c
                            WHERE c.session_id = p.session_id) AS processed_candles,
                           (SELECT count(*) FROM paper_fills f
                            WHERE f.session_id = p.session_id) AS fills
                    FROM paper_pilots p
                    JOIN paper_sessions s ON s.session_id = p.session_id
                    ORDER BY p.created_at DESC
                    LIMIT 1
                    """
                ).fetchone()
        except Exception:  # noqa: BLE001 - an unavailable remote source is dashboard data.
            database = self._source_status(
                name="PostgreSQL paper state",
                status="unavailable",
                source="PostgreSQL read-only query",
                observed_at=None,
                detail="Paper-pilot state could not be read.",
            )
            return (
                {
                    "source": "PostgreSQL read-only query",
                    "status": "unavailable",
                },
                database,
            )
        database = self._source_status(
            name="PostgreSQL paper state",
            status="healthy",
            source="PostgreSQL read-only query",
            observed_at=now,
            detail="Paper-pilot state is readable.",
        )
        if row is None:
            return (
                {
                    "message": "No real paper pilot has been registered.",
                    "source": "PostgreSQL read-only query",
                    "status": "not_registered",
                },
                database,
            )
        plan = row.get("plan") if isinstance(row, Mapping) else None
        assessment = row.get("assessment") if isinstance(row, Mapping) else None
        plan_fields = {
            key: _safe_text(plan.get(key))
            for key in (
                "start_not_before",
                "minimum_calendar_days",
                "minimum_processed_candles",
                "minimum_fills",
                "maximum_drawdown",
                "minimum_excess_return_over_buy_and_hold",
                "maximum_data_gap_events",
                "maximum_conflict_events",
                "maximum_unplanned_pauses",
            )
            if isinstance(plan, Mapping) and key in plan
        }
        return (
            {
                "assessment_verdict": (
                    assessment.get("verdict") if isinstance(assessment, Mapping) else None
                ),
                "created_at": _timestamp(_parse_timestamp(row.get("created_at"))),
                "metrics": {
                    "cash": _safe_text(row.get("cash")),
                    "fills": _safe_text(row.get("fills")),
                    "marked_equity": _safe_text(row.get("current_equity")),
                    "position_quantity": _safe_text(row.get("base_quantity")),
                    "processed_candles": _safe_text(row.get("processed_candles")),
                    "total_fees": _safe_text(row.get("total_fees")),
                },
                "pilot_id": _safe_text(row.get("pilot_id")),
                "plan": plan_fields,
                "plan_raw_sha256": _safe_text(row.get("raw_plan_sha256")),
                "session_state": _safe_text(row.get("session_state")),
                "status": _safe_text(row.get("pilot_state")),
                "updated_at": _timestamp(_parse_timestamp(row.get("session_updated_at"))),
            },
            database,
        )

    def _draft_plan(self, now: datetime) -> dict[str, Any]:
        path = self.settings.pilot_plan_path
        try:
            body = path.read_bytes()
            plan = json.loads(body)
            if not isinstance(plan, dict):
                raise TypeError("pilot plan is not a JSON object")
        except FileNotFoundError:
            return {"status": "missing"}
        except Exception:  # noqa: BLE001 - a draft plan must never make the dashboard fail.
            return {"status": "invalid"}
        display = {
            key: _safe_text(plan.get(key))
            for key in (
                "name",
                "start_not_before",
                "minimum_calendar_days",
                "minimum_processed_candles",
                "minimum_fills",
                "maximum_drawdown",
                "minimum_excess_return_over_buy_and_hold",
                "maximum_data_gap_events",
                "maximum_conflict_events",
                "maximum_unplanned_pauses",
            )
            if key in plan
        }
        return {
            "details": display,
            "observed_at": _timestamp(now),
            "raw_sha256": hashlib.sha256(body).hexdigest(),
            "status": "draft",
        }

    @staticmethod
    def _next_action(
        collector: Mapping[str, object],
        raw_archive: Mapping[str, object],
        storage: Mapping[str, object],
        database: Mapping[str, object],
        pilot: Mapping[str, object],
        research: Mapping[str, object],
        history: Mapping[str, object],
    ) -> dict[str, str]:
        for item in (collector, raw_archive, storage, database):
            if item.get("status") in {"unavailable", "missing"}:
                return {
                    "action": "Investigate the unavailable data source before relying on newer evidence.",
                    "runbook": "docs/runbooks/local-market-data-pipeline.md",
                }
        if history.get("status") == "ready":
            return {
                "action": str(history.get("next_action")),
                "runbook": "apps/historical_backfill/README.md#historical-research-candles",
            }
        if history.get("status") == "gaps_found":
            return {
                "action": str(history.get("next_action")),
                "runbook": "apps/historical_backfill/README.md#historical-research-candles",
            }
        if research.get("recommendation"):
            return {
                "action": str(research["recommendation"])
                + (
                    " Prepare 90 complete days of BTC candles."
                    if research.get("status") == "inconclusive"
                    else ""
                ),
                "runbook": "apps/trading_core/README.md#longer-strategy-research",
            }
        if pilot.get("status") == "not_registered":
            return {
                "action": "Review the sealed OOS result and explicitly approve or decline the paper-only pilot.",
                "runbook": "apps/trading_core/README.md#pre-registered-paper-pilot",
            }
        if research.get("status") in {"missing", "unavailable"}:
            return {
                "action": "Create and validate a sealed strategy evaluation before considering a pilot.",
                "runbook": "apps/trading_core/README.md#sealed-trainvalidationtest-experiment",
            }
        return {
            "action": "Review the current pilot evidence and follow its precommitted workflow.",
            "runbook": "apps/trading_core/README.md#pre-registered-paper-pilot",
        }
