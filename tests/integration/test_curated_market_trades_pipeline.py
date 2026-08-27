import json
import os
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

pytestmark = pytest.mark.integration


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS") == "1"


def _event_id(symbol: str, trade_id: str) -> str:
    identity = json.dumps(
        ["market.trade.raw", "v1", "coinbase", symbol, trade_id],
        separators=(",", ":"),
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _event(trade_id: str, producer: str, price: str = "100.25") -> dict[str, object]:
    trace_id = str(uuid4())
    return {
        "event_id": _event_id("BTC-USD", trade_id),
        "event_type": "market.trade.raw",
        "schema_version": "v1",
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "event_time": "2026-08-25T14:00:01Z",
        "ingested_at": "2026-08-26T17:00:00Z",
        "source_event_id": trade_id,
        "source_sequence": 9 if producer == "apps.collector" else None,
        "producer": producer,
        "trace_id": trace_id,
        "correlation_id": None,
        "causation_id": str(uuid4()) if producer == "apps.historical_backfill" else None,
        "payload": {
            "trade_id": trade_id,
            "product_id": "BTC-USD",
            "price": price,
            "size": "0.500000000000000000",
            "side": "BUY",
            "time": "2026-08-25T14:00:01Z",
        },
    }


def _raw_row(document: dict[str, object], offset: int) -> dict[str, object]:
    value = json.dumps(document, separators=(",", ":")).encode()
    return {
        "kafka_topic": "market.trades.raw.v1",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": datetime(2026, 8, 26, 17, offset, tzinfo=UTC),
        "kafka_key": b"coinbase:BTC-USD",
        "kafka_value": value,
        "kafka_headers": [
            {"key": "event_type", "value": b"market.trade.raw"},
            {"key": "schema_version", "value": b"v1"},
            {"key": "trace_id", "value": str(document["trace_id"]).encode()},
        ],
    }


def _run_job(report_uri: str, raw_uri: str, output_uri: str, quarantine_uri: str, apply: bool):
    command = [
        "podman",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "-e",
        "CURATION_EVIDENCE_PREFIX=s3a://crypto-data/evidence/curation-integration",
        "raw-sink",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--conf",
        "spark.jars.ivy=/opt/spark/.ivy2",
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "/opt/spark/work-dir/jobs/spark/entrypoints/curate_market_trades.py",
        "--raw-integrity-report",
        report_uri,
        "--raw-input",
        raw_uri,
        "--output",
        output_uri,
        "--quarantine-output",
        quarantine_uri,
    ]
    if apply:
        command.append("--apply")
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
    )


def _report(output: str) -> dict[str, object]:
    lines = [
        line for line in output.splitlines() if line.startswith("CURATION_REPORT_JSON=")
    ]
    assert len(lines) == 1, output
    return json.loads(lines[0].partition("=")[2])


@pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_INTEGRATION_TESTS=1 with the local Compose stack available",
)
def test_bounded_minio_snapshot_is_deduplicated_quarantined_and_idempotent(
    tmp_path: Path,
) -> None:
    import boto3
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from jobs.spark.entrypoints.validate_curated_market_trades import validate_manifest

    run_key = str(uuid4())
    raw_prefix = f"integration/curation/{run_key}/raw"
    evidence_prefix = f"evidence/curation-integration/{run_key}"
    output_prefix = f"integration/curation/{run_key}/curated"
    quarantine_prefix = f"integration/curation/{run_key}/quarantine"
    owned_prefix = f"integration/curation/{run_key}/"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    live = _event(f"live-{run_key}", "apps.collector")
    backfill = _event(f"backfill-{run_key}", "apps.historical_backfill")
    invalid = _event(f"invalid-{run_key}", "apps.collector", price="0")
    rows = [
        _raw_row(live, 0),
        _raw_row(live, 1),
        _raw_row(backfill, 2),
        _raw_row(invalid, 3),
    ]
    table = pa.Table.from_pylist(rows)
    parquet = BytesIO()
    pq.write_table(table, parquet)
    report = {
        "status": "passed",
        "topic": "market.trades.raw.v1",
        "invalid_event_values": 0,
        "partitions": [
            {
                "kafka_topic": "market.trades.raw.v1",
                "kafka_partition": 0,
                "earliest_offset": 0,
                "ending_offset_exclusive": 4,
                "kafka_records": 4,
                "parquet_records_in_range": 4,
                "missing_from_parquet": 0,
                "duplicate_parquet_positions": 0,
            }
        ],
    }
    s3.put_object(
        Bucket="crypto-data", Key=f"{raw_prefix}/part.parquet", Body=parquet.getvalue()
    )
    s3.put_object(
        Bucket="crypto-data",
        Key=f"{evidence_prefix}/audit.json",
        Body=json.dumps(report, separators=(",", ":")).encode(),
    )
    report_uri = f"s3a://crypto-data/{evidence_prefix}/audit.json"
    raw_uri = f"s3a://crypto-data/{raw_prefix}"
    output_uri = f"s3a://crypto-data/{output_prefix}"
    quarantine_uri = f"s3a://crypto-data/{quarantine_prefix}"

    try:
        dry_run = _run_job(report_uri, raw_uri, output_uri, quarantine_uri, False)
        assert dry_run.returncode == 2, dry_run.stdout + dry_run.stderr
        dry_report = _report(dry_run.stdout)
        assert dry_report["status"] == "dry_run_ready"
        assert dry_report["curated_logical_trades"] == 2
        assert dry_report["exact_duplicate_deliveries"] == 1
        assert dry_report["quarantined_input_rows"] == 1
        assert s3.list_objects_v2(Bucket="crypto-data", Prefix=output_prefix).get(
            "KeyCount", 0
        ) == 0

        applied = _run_job(report_uri, raw_uri, output_uri, quarantine_uri, True)
        assert applied.returncode == 0, applied.stdout + applied.stderr
        applied_report = _report(applied.stdout)
        assert applied_report["status"] == "published"
        manifest_key = (
            f"{output_prefix}/manifests/{applied_report['snapshot_key']}/manifest.json"
        )
        manifest = json.loads(
            s3.get_object(Bucket="crypto-data", Key=manifest_key)["Body"].read()
        )
        assert manifest["raw_integrity_report_sha256"]

        curated_files: list[str] = []
        quarantine_files: list[str] = []
        local_curated = tmp_path / "curated"
        local_quarantine = tmp_path / "quarantine"
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=owned_prefix
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".parquet"):
                    continue
                if "/curated/runs/" in key:
                    relative = key.partition("/event_date=")[2]
                    destination = (
                        local_curated
                        / "event_date=2026-08-25"
                        / f"part-{len(curated_files)}.parquet"
                    )
                    target_files = curated_files
                elif "/quarantine/runs/" in key:
                    relative = key
                    destination = local_quarantine / f"part-{len(quarantine_files)}.parquet"
                    target_files = quarantine_files
                else:
                    continue
                assert relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(
                    s3.get_object(Bucket="crypto-data", Key=key)["Body"].read()
                )
                target_files.append(str(destination))

        connection = duckdb.connect()
        curated_rows = connection.execute(
            "SELECT event_id, price, size, notional, producer, kafka_offset "
            "FROM read_parquet(?) ORDER BY event_id",
            [curated_files],
        ).fetchall()
        assert len(curated_rows) == 2
        assert {row[1] for row in curated_rows} == {Decimal("100.250000000000000000")}
        assert {row[2] for row in curated_rows} == {Decimal("0.500000000000000000")}
        assert {row[3] for row in curated_rows} == {Decimal("50.125000000000000000")}
        assert next(row for row in curated_rows if row[0] == live["event_id"])[5] == 0
        quarantine_rows = connection.execute(
            "SELECT failure_code FROM read_parquet(?)", [quarantine_files]
        ).fetchall()
        assert quarantine_rows == [("invalid_price",)]
        local_manifest = {**manifest, "curated_output_uri": str(local_curated)}
        local_manifest_path = tmp_path / "manifest.json"
        local_manifest_path.write_text(json.dumps(local_manifest))
        validate_manifest(local_manifest_path, str(backfill["event_id"]))

        rerun = _run_job(report_uri, raw_uri, output_uri, quarantine_uri, True)
        assert rerun.returncode == 0, rerun.stdout + rerun.stderr
        assert _report(rerun.stdout)["status"] == "resolved_existing_snapshot"
        manifests = s3.list_objects_v2(
            Bucket="crypto-data", Prefix=f"{output_prefix}/manifests/"
        ).get("Contents", [])
        assert [item["Key"] for item in manifests] == [manifest_key]
    finally:
        for prefix in (owned_prefix, f"{evidence_prefix}/"):
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket="crypto-data", Prefix=prefix):
                objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket="crypto-data", Delete={"Objects": objects})


@pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_INTEGRATION_TESTS=1 with the local Compose stack available",
)
def test_conflicting_facts_publish_no_manifest_and_quarantine_every_delivery() -> None:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    run_key = str(uuid4())
    raw_prefix = f"integration/curation/{run_key}/raw"
    evidence_prefix = f"evidence/curation-integration/{run_key}"
    output_prefix = f"integration/curation/{run_key}/curated"
    quarantine_prefix = f"integration/curation/{run_key}/quarantine"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    first = _event(f"conflict-{run_key}", "apps.collector", price="10")
    second = json.loads(json.dumps(first))
    second["payload"]["price"] = "11"
    parquet = BytesIO()
    pq.write_table(
        pa.Table.from_pylist([_raw_row(first, 0), _raw_row(second, 1)]), parquet
    )
    audit = {
        "status": "passed",
        "topic": "market.trades.raw.v1",
        "invalid_event_values": 0,
        "partitions": [
            {
                "kafka_topic": "market.trades.raw.v1",
                "kafka_partition": 0,
                "earliest_offset": 0,
                "ending_offset_exclusive": 2,
                "kafka_records": 2,
                "parquet_records_in_range": 2,
                "missing_from_parquet": 0,
                "duplicate_parquet_positions": 0,
            }
        ],
    }
    s3.put_object(
        Bucket="crypto-data", Key=f"{raw_prefix}/part.parquet", Body=parquet.getvalue()
    )
    s3.put_object(
        Bucket="crypto-data",
        Key=f"{evidence_prefix}/audit.json",
        Body=json.dumps(audit, separators=(",", ":")).encode(),
    )
    arguments = (
        f"s3a://crypto-data/{evidence_prefix}/audit.json",
        f"s3a://crypto-data/{raw_prefix}",
        f"s3a://crypto-data/{output_prefix}",
        f"s3a://crypto-data/{quarantine_prefix}",
    )
    try:
        applied = _run_job(*arguments, True)
        assert applied.returncode == 3, applied.stdout + applied.stderr
        report = _report(applied.stdout)
        assert report["status"] == "unresolved_conflicting_duplicates"
        assert report["curated_logical_trades"] == 0
        assert report["conflicting_duplicate_deliveries"] == 2
        assert report["quarantine_records"] == 2
        assert all(
            sample["failure_code"] == "conflicting_duplicate"
            and sample["failure_field"] == "price,notional"
            and "payload" not in json.dumps(sample)
            for sample in report["samples"]
        )
        assert s3.list_objects_v2(
            Bucket="crypto-data", Prefix=f"{output_prefix}/manifests/"
        ).get("KeyCount", 0) == 0
        quarantine_rows = 0
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=f"{quarantine_prefix}/runs/"
        ):
            for item in page.get("Contents", []):
                if item["Key"].endswith(".parquet"):
                    body = s3.get_object(Bucket="crypto-data", Key=item["Key"])[
                        "Body"
                    ].read()
                    quarantine_rows += pq.read_table(BytesIO(body)).num_rows
        assert quarantine_rows == 2
    finally:
        for prefix in (
            f"integration/curation/{run_key}/",
            f"{evidence_prefix}/",
        ):
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket="crypto-data", Prefix=prefix
            ):
                objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket="crypto-data", Delete={"Objects": objects})
