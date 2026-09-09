from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.sources import LiveDashboardSource

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


class _Body:
    def __init__(self, value: bytes) -> None:
        self.value = value

    def read(self) -> bytes:
        return self.value


class FakeS3:
    def __init__(self, documents: dict[str, bytes]) -> None:
        self.documents = documents
        self.calls: list[str] = []

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str = "", MaxKeys: int = 1000
    ) -> dict[str, Any]:
        self.calls.append("list")
        objects = [
            {"Key": key, "LastModified": NOW}
            for key in self.documents
            if key.startswith(Prefix)
        ]
        return {"Contents": objects[:MaxKeys]}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.calls.append("get")
        body = self.documents[Key]
        return {"Body": _Body(body), "ContentLength": len(body)}


class FakeConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: str):
        self.queries.append(query)
        return self

    @staticmethod
    def fetchone() -> None:
        return None


class PartitionedRawS3(FakeS3):
    def list_objects_v2(
        self, *, Bucket: str, Prefix: str = "", MaxKeys: int = 1000
    ) -> dict[str, Any]:
        if Prefix == "raw/":
            self.calls.append("list")
            return {
                "Contents": [
                    {
                        "Key": "raw/event_date=2026-08-14/event_hour=16/old.parquet",
                        "LastModified": datetime(2026, 8, 14, 16, tzinfo=UTC),
                    }
                ]
            }
        return super().list_objects_v2(Bucket=Bucket, Prefix=Prefix, MaxKeys=MaxKeys)


def _body(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _documents() -> dict[str, bytes]:
    candidate = {"candidate_id": "sma-5-20", "fast_period": 5, "slow_period": 20}
    return {
        "raw/data.parquet": b"parquet",
        "audit/audit.json": _body(
            {
                "partitions": [
                    {
                        "duplicate_parquet_positions": 0,
                        "kafka_records": 12,
                        "missing_from_parquet": 0,
                    }
                ],
                "status": "passed",
            }
        ),
        "curated/manifest.json": _body(
            {
                "curated_logical_trades": 10,
                "quarantined_input_rows": 2,
                "snapshot_key": "a" * 64,
                "status": "published",
            }
        ),
        "candles/manifest.json": _body(
            {
                "candle_count": 4,
                "snapshot_key": "b" * 64,
                "source_curated_trades": 10,
                "status": "published",
            }
        ),
        "selection/manifest.json": _body(
            {
                "ranges": {
                    "test": {"end": "2026-09-04T12:00:00Z"},
                    "train": {"end": "2026-09-04T10:00:00Z"},
                    "validation": {"end": "2026-09-04T11:00:00Z"},
                },
                "selected_candidate": candidate,
                "status": "published",
                "test_data_accessed": False,
            }
        ),
        "evaluation/manifest.json": _body(
            {
                "selected_candidate": candidate,
                "status": "published",
                "summary": {
                    "baseline": {"percentage_return": "0.010000000000000000"},
                    "strategy": {"percentage_return": "0.020000000000000000"},
                    "strategy_excess_percentage_return": "0.010000000000000000",
                },
                "test_range": {"end": "2026-09-04T12:00:00Z"},
            }
        ),
    }


def test_live_source_reads_only_safe_metadata_and_reports_no_registered_pilot(
    tmp_path: Path,
) -> None:
    plan = tmp_path / "pilot.json"
    plan.write_text(
        json.dumps(
            {
                "maximum_drawdown": "0.200000000000000000",
                "minimum_calendar_days": 30,
                "name": "dashboard-test",
                "start_not_before": "2026-09-07T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    storage = FakeS3(_documents())
    connection = FakeConnection()
    source = LiveDashboardSource(
        DashboardSettings(
            pilot_plan_path=plan,
            raw_prefix="raw/",
            raw_audit_prefix="audit/",
            curated_manifest_prefix="curated/",
            candle_manifest_prefix="candles/",
            selection_manifest_prefix="selection/",
            evaluation_manifest_prefix="evaluation/",
        ),
        now=lambda: NOW,
        s3_client=storage,
        quality_reader=lambda: {
            "detected_at": "2026-09-04T12:00:00Z",
            "malformed_message_count": 0,
            "sequence_gap_count": 0,
            "trades_observed": 44,
        },
        database_connector=lambda: connection,
    )

    snapshot = source.read()

    assert snapshot["pipeline"][0]["status"] == "healthy"
    assert snapshot["trusted_data"]["raw_integrity"]["details"]["Kafka records"] == "12"
    assert (
        snapshot["trusted_data"]["curated_trades"]["details"]["Logical trades"] == "10"
    )
    assert snapshot["research"]["oos"]["excess_return"] == "0.010000000000000000"
    assert snapshot["pilot"]["status"] == "not_registered"
    assert snapshot["pilot"].get("metrics") is None
    assert snapshot["draft_plan"]["status"] == "draft"
    assert snapshot["next_action"]["action"].startswith("Review the sealed OOS")
    assert all(call in {"get", "list"} for call in storage.calls)
    assert connection.queries[0] == "BEGIN READ ONLY"
    assert all(
        "INSERT" not in query and "UPDATE" not in query for query in connection.queries
    )


def test_live_source_marks_old_evidence_stale(tmp_path: Path) -> None:
    storage = FakeS3(_documents())
    source = LiveDashboardSource(
        DashboardSettings(
            pilot_plan_path=tmp_path / "missing.json",
            raw_prefix="raw/",
            raw_audit_prefix="audit/",
            curated_manifest_prefix="curated/",
            candle_manifest_prefix="candles/",
            selection_manifest_prefix="selection/",
            evaluation_manifest_prefix="evaluation/",
            stale_after_seconds=60,
            artifact_stale_after_seconds=60,
        ),
        now=lambda: datetime(2026, 9, 4, 12, 2, tzinfo=UTC),
        s3_client=storage,
        quality_reader=lambda: None,
        database_connector=FakeConnection,
    )

    snapshot = source.read()

    assert snapshot["trusted_data"]["raw_integrity"]["status"] == "stale"
    assert snapshot["pipeline"][0]["status"] == "missing"
    assert snapshot["draft_plan"]["status"] == "missing"


def test_live_source_checks_recent_raw_hour_partitions_before_old_global_page(
    tmp_path: Path,
) -> None:
    documents = _documents()
    documents["raw/event_date=2026-09-04/event_hour=12/current.parquet"] = (
        b"current-parquet"
    )
    storage = PartitionedRawS3(documents)
    source = LiveDashboardSource(
        DashboardSettings(
            pilot_plan_path=tmp_path / "missing.json",
            raw_prefix="raw/",
            raw_audit_prefix="audit/",
            curated_manifest_prefix="curated/",
            candle_manifest_prefix="candles/",
            selection_manifest_prefix="selection/",
            evaluation_manifest_prefix="evaluation/",
        ),
        now=lambda: NOW,
        s3_client=storage,
        quality_reader=lambda: None,
        database_connector=FakeConnection,
    )

    snapshot = source.read()

    assert snapshot["pipeline"][1]["name"] == "Raw archive"
    assert snapshot["pipeline"][1]["status"] == "healthy"
