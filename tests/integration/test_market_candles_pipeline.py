import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS") == "1"


def _curated_row(
    *,
    event_id: str,
    symbol: str,
    event_time: datetime,
    price: str,
    size: str,
    offset: int,
    producer: str = "apps.collector",
) -> dict[str, object]:
    quantum = Decimal("0.000000000000000001")
    price_value = Decimal(price).quantize(quantum)
    size_value = Decimal(size).quantize(quantum)
    return {
        "event_id": event_id,
        "exchange": "coinbase",
        "symbol": symbol,
        "source_event_id": f"source-{offset}",
        "event_time": event_time,
        "ingested_at": datetime(2026, 8, 28, 17, offset, tzinfo=UTC),
        "kafka_timestamp": datetime(2026, 8, 28, 17, offset, tzinfo=UTC),
        "price": price_value,
        "size": size_value,
        "notional": (price_value * size_value).quantize(quantum),
        "source_side": "BUY",
        "source_sequence": offset if producer == "apps.collector" else None,
        "producer": producer,
        "trace_id": str(uuid4()),
        "correlation_id": None,
        "causation_id": str(uuid4()) if producer == "apps.historical_backfill" else None,
        "kafka_topic": "market.trades.raw.v1",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "curated_schema_version": "v1",
        "curated_at": datetime(2026, 8, 28, 17, 5, tzinfo=UTC),
        "event_date": event_time.date(),
    }


def _run_job(
    manifest_uri: str,
    manifest_sha256: str,
    output_uri: str,
    known_event_id: str,
    *,
    apply: bool,
):
    command = [
        "podman",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "-e",
        "CANDLE_SOURCE_MANIFEST_PREFIX=s3a://crypto-data/integration/candles",
        "raw-sink",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--conf",
        "spark.jars.ivy=/opt/spark/.ivy2",
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "/opt/spark/work-dir/jobs/spark/entrypoints/build_market_candles.py",
        "--curated-manifest",
        manifest_uri,
        "--curated-manifest-sha256",
        manifest_sha256,
        "--output",
        output_uri,
        "--known-backfill-event-id",
        known_event_id,
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
    lines = [line for line in output.splitlines() if line.startswith("CANDLE_REPORT_JSON=")]
    assert len(lines) == 1, output
    return json.loads(lines[0].partition("=")[2])


@pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_INTEGRATION_TESTS=1 with the local Compose stack available",
)
def test_curated_snapshot_builds_exact_idempotent_minute_candles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import boto3
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from jobs.spark.entrypoints.validate_market_candles import validate_manifest

    run_key = str(uuid4())
    root_prefix = f"integration/candles/{run_key}"
    curated_prefix = f"{root_prefix}/curated/runs/source-run"
    snapshot_key = hashlib.sha256(run_key.encode()).hexdigest()
    curated_manifest_key = (
        f"{root_prefix}/curated/manifests/{snapshot_key}/manifest.json"
    )
    candle_prefix = f"{root_prefix}/analytics/market_candles/v1"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    shared_time = datetime(2026, 8, 25, 14, 0, 10, tzinfo=UTC)
    backfill_event_id = str(uuid4())
    rows = [
        _curated_row(
            event_id=str(uuid4()),
            symbol="BTC-USD",
            event_time=shared_time,
            price="100",
            size="1",
            offset=2,
        ),
        _curated_row(
            event_id=str(uuid4()),
            symbol="BTC-USD",
            event_time=shared_time,
            price="90",
            size="2",
            offset=1,
        ),
        _curated_row(
            event_id=str(uuid4()),
            symbol="BTC-USD",
            event_time=datetime(2026, 8, 25, 14, 0, 50, tzinfo=UTC),
            price="110",
            size="1",
            offset=3,
        ),
        _curated_row(
            event_id=str(uuid4()),
            symbol="ETH-USD",
            event_time=datetime(2026, 8, 25, 14, 0, 20, tzinfo=UTC),
            price="50",
            size="2",
            offset=4,
        ),
        _curated_row(
            event_id=backfill_event_id,
            symbol="BTC-USD",
            event_time=datetime(2026, 8, 25, 14, 1, tzinfo=UTC),
            price="120",
            size="0.5",
            offset=5,
            producer="apps.historical_backfill",
        ),
    ]
    decimal_type = pa.decimal128(38, 18)
    curated_schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("source_event_id", pa.string()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("ingested_at", pa.timestamp("us", tz="UTC")),
            ("kafka_timestamp", pa.timestamp("us", tz="UTC")),
            ("price", decimal_type),
            ("size", decimal_type),
            ("notional", decimal_type),
            ("source_side", pa.string()),
            ("source_sequence", pa.int64()),
            ("producer", pa.string()),
            ("trace_id", pa.string()),
            ("correlation_id", pa.string()),
            ("causation_id", pa.string()),
            ("kafka_topic", pa.string()),
            ("kafka_partition", pa.int32()),
            ("kafka_offset", pa.int64()),
            ("curated_schema_version", pa.string()),
            ("curated_at", pa.timestamp("us", tz="UTC")),
            ("event_date", pa.date32()),
        ]
    )
    parquet = BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=curated_schema), parquet)
    s3.put_object(
        Bucket="crypto-data",
        Key=f"{curated_prefix}/part.parquet",
        Body=parquet.getvalue(),
    )
    curated_manifest = {
        "status": "published",
        "mode": "apply",
        "snapshot_key": snapshot_key,
        "curated_schema_version": "v1",
        "curated_output_uri": f"s3a://crypto-data/{curated_prefix}",
        "curated_logical_trades": len(rows),
    }
    manifest_body = json.dumps(
        curated_manifest, separators=(",", ":"), sort_keys=True
    ).encode()
    manifest_sha256 = hashlib.sha256(manifest_body).hexdigest()
    s3.put_object(
        Bucket="crypto-data", Key=curated_manifest_key, Body=manifest_body
    )
    manifest_uri = f"s3a://crypto-data/{curated_manifest_key}"
    output_uri = f"s3a://crypto-data/{candle_prefix}"

    try:
        dry_run = _run_job(
            manifest_uri,
            manifest_sha256,
            output_uri,
            backfill_event_id,
            apply=False,
        )
        assert dry_run.returncode == 2, dry_run.stdout + dry_run.stderr
        dry_report = _report(dry_run.stdout)
        assert dry_report["status"] == "dry_run_ready"
        assert dry_report["source_curated_trades"] == 5
        assert dry_report["candle_count"] == 3
        assert dry_report["trade_count_sum"] == 5
        assert s3.list_objects_v2(
            Bucket="crypto-data", Prefix=f"{candle_prefix}/runs/"
        ).get("KeyCount", 0) == 0

        applied = _run_job(
            manifest_uri,
            manifest_sha256,
            output_uri,
            backfill_event_id,
            apply=True,
        )
        assert applied.returncode == 0, applied.stdout + applied.stderr
        applied_report = _report(applied.stdout)
        assert applied_report["status"] == "published"
        candle_manifest_key = (
            f"{candle_prefix}/manifests/{applied_report['snapshot_key']}/manifest.json"
        )
        candle_manifest = json.loads(
            s3.get_object(Bucket="crypto-data", Key=candle_manifest_key)["Body"].read()
        )
        published_manifest_path = tmp_path / "published-candle-manifest.json"
        published_manifest_path.write_text(json.dumps(candle_manifest))
        monkeypatch.setenv(
            "CANDLE_S3_ENDPOINT",
            os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        )
        monkeypatch.setenv("CANDLE_S3_ACCESS_KEY", "minioadmin")
        monkeypatch.setenv("CANDLE_S3_SECRET_KEY", "minioadmin")
        validate_manifest(
            published_manifest_path,
            known_backfill_symbol="BTC-USD",
            known_backfill_window_start="2026-08-25T14:01:00Z",
        )

        local_output = tmp_path / "candles"
        run_marker = f"/runs/{applied_report['candle_run_id']}/"
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=f"{candle_prefix}/runs/"
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".parquet"):
                    continue
                relative = key.partition(run_marker)[2]
                destination = local_output / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(
                    s3.get_object(Bucket="crypto-data", Key=key)["Body"].read()
                )

        glob = str(local_output / "**" / "*.parquet").replace("\\", "/")
        relation = (
            f"read_parquet({json.dumps(glob)}, hive_partitioning=true, "
            "hive_types={'interval': VARCHAR, 'event_date': DATE, 'event_hour': VARCHAR})"
        )
        connection = duckdb.connect()
        candles = connection.execute(
            f"""
            SELECT symbol, window_start, open, high, low, close,
                   base_volume, quote_volume, vwap, trade_count,
                   historical_backfill_trade_count
            FROM {relation}
            ORDER BY symbol, window_start
            """
        ).fetchall()
        assert len(candles) == 3
        btc_first = next(row for row in candles if row[0] == "BTC-USD" and row[1].minute == 0)
        assert btc_first[2:6] == (
            Decimal("90.000000000000000000"),
            Decimal("110.000000000000000000"),
            Decimal("90.000000000000000000"),
            Decimal("110.000000000000000000"),
        )
        assert btc_first[6:10] == (
            Decimal("4.000000000000000000"),
            Decimal("390.000000000000000000"),
            Decimal("97.500000000000000000"),
            3,
        )
        historical = next(row for row in candles if row[0] == "BTC-USD" and row[1].minute == 1)
        assert historical[10] == 1

        local_manifest = {
            **candle_manifest,
            "candle_output_uri": str(local_output),
        }
        local_manifest_path = tmp_path / "candle-manifest.json"
        local_manifest_path.write_text(json.dumps(local_manifest))
        validate_manifest(
            local_manifest_path,
            known_backfill_symbol="BTC-USD",
            known_backfill_window_start="2026-08-25T14:01:00Z",
        )

        rerun = _run_job(
            manifest_uri,
            manifest_sha256,
            output_uri,
            backfill_event_id,
            apply=True,
        )
        assert rerun.returncode == 0, rerun.stdout + rerun.stderr
        assert _report(rerun.stdout)["status"] == "resolved_existing_snapshot"
        manifests = s3.list_objects_v2(
            Bucket="crypto-data", Prefix=f"{candle_prefix}/manifests/"
        ).get("Contents", [])
        assert [item["Key"] for item in manifests] == [candle_manifest_key]
    finally:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=f"{root_prefix}/"
        ):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket="crypto-data", Delete={"Objects": objects})
