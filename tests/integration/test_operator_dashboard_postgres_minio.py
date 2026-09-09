from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.sources import LiveDashboardSource

pytestmark = pytest.mark.integration


def _enabled() -> bool:
    return os.getenv("RUN_DASHBOARD_INTEGRATION_TESTS") == "1"


def _body(document: dict[str, object]) -> bytes:
    return json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _objects(client, prefix: str) -> list[dict[str, object]]:
    return client.list_objects_v2(Bucket="crypto-data", Prefix=prefix).get(
        "Contents", []
    )


@pytest.mark.skipif(
    not _enabled(),
    reason="set RUN_DASHBOARD_INTEGRATION_TESTS=1 with local PostgreSQL and MinIO available",
)
def test_dashboard_reads_postgres_and_minio_without_mutating_state(
    tmp_path: Path,
) -> None:
    import boto3
    import psycopg

    database_url = os.getenv(
        "OPERATOR_DASHBOARD_DATABASE_URL",
        "postgresql://paper_app:paper_app@127.0.0.1:5432/crypto_trading",
    )
    endpoint = os.getenv("OPERATOR_DASHBOARD_S3_ENDPOINT", "http://127.0.0.1:9000")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("OPERATOR_DASHBOARD_S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv(
            "OPERATOR_DASHBOARD_S3_SECRET_KEY", "minioadmin"
        ),
        region_name="us-east-1",
    )
    root = f"integration/operator-dashboard/{uuid4()}"
    candidate = {"candidate_id": "sma-5-20", "fast_period": 5, "slow_period": 20}
    documents = {
        f"{root}/raw/data.parquet": b"fixture-parquet",
        f"{root}/audit/audit.json": _body(
            {
                "partitions": [
                    {
                        "duplicate_parquet_positions": 0,
                        "kafka_records": 3,
                        "missing_from_parquet": 0,
                    }
                ],
                "status": "passed",
            }
        ),
        f"{root}/curated/manifest.json": _body(
            {
                "curated_logical_trades": 3,
                "quarantined_input_rows": 0,
                "snapshot_key": "a" * 64,
                "status": "published",
            }
        ),
        f"{root}/candles/manifest.json": _body(
            {
                "candle_count": 2,
                "snapshot_key": "b" * 64,
                "source_curated_trades": 3,
                "status": "published",
            }
        ),
        f"{root}/selection/manifest.json": _body(
            {
                "ranges": {"test": {"end": "2026-09-04T12:00:00Z"}},
                "selected_candidate": candidate,
                "status": "published",
                "test_data_accessed": False,
            }
        ),
        f"{root}/evaluation/manifest.json": _body(
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
    plan = tmp_path / "dashboard-plan.json"
    plan_body = _body(
        {"name": "integration-dashboard", "start_not_before": "2026-09-07T00:00:00Z"}
    )
    plan.write_bytes(plan_body)
    try:
        for key, body in documents.items():
            client.put_object(Bucket="crypto-data", Key=key, Body=body)
        before_objects = _objects(client, root)
        with psycopg.connect(database_url) as connection:
            connection.execute("BEGIN READ ONLY")
            before_pilots = connection.execute(
                "SELECT count(*) FROM paper_pilots"
            ).fetchone()[0]
            before_sessions = connection.execute(
                "SELECT count(*) FROM paper_sessions"
            ).fetchone()[0]

        source = LiveDashboardSource(
            DashboardSettings(
                database_url=database_url,
                pilot_plan_path=plan,
                raw_prefix=f"{root}/raw/",
                raw_audit_prefix=f"{root}/audit/",
                curated_manifest_prefix=f"{root}/curated/",
                candle_manifest_prefix=f"{root}/candles/",
                selection_manifest_prefix=f"{root}/selection/",
                evaluation_manifest_prefix=f"{root}/evaluation/",
                research_report_prefix=f"{root}/research/",
                historical_manifest_prefix=f"{root}/history/",
            ),
            now=lambda: datetime.now(UTC),
            s3_client=client,
            quality_reader=lambda: None,
        )
        snapshot = source.read()

        assert (
            snapshot["trusted_data"]["raw_integrity"]["details"]["Kafka records"] == "3"
        )
        assert (
            snapshot["trusted_data"]["curated_trades"]["details"]["Logical trades"]
            == "3"
        )
        assert (
            snapshot["trusted_data"]["one_minute_candles"]["details"]["Candles"] == "2"
        )
        assert snapshot["research"]["oos"]["candidate"] == candidate
        assert (
            snapshot["draft_plan"]["raw_sha256"]
            == hashlib.sha256(plan_body).hexdigest()
        )
        if before_pilots == 0:
            assert snapshot["pilot"]["status"] == "not_registered"

        assert len(_objects(client, root)) == len(before_objects)
        with psycopg.connect(database_url) as connection:
            connection.execute("BEGIN READ ONLY")
            assert (
                connection.execute("SELECT count(*) FROM paper_pilots").fetchone()[0]
                == before_pilots
            )
            assert (
                connection.execute("SELECT count(*) FROM paper_sessions").fetchone()[0]
                == before_sessions
            )
    finally:
        objects = _objects(client, root)
        if objects:
            client.delete_objects(
                Bucket="crypto-data",
                Delete={"Objects": [{"Key": item["Key"]} for item in objects]},
            )
