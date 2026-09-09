import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def _enabled() -> bool:
    return os.getenv("RUN_BACKTEST_INTEGRATION_TESTS") == "1"


def _put_candle_snapshot(s3, root: str) -> tuple[str, str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    start = datetime(2026, 1, 1, tzinfo=UTC)
    decimal_type = pa.decimal128(38, 18)
    schema = pa.schema(
        [
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("us")),
            ("window_end", pa.timestamp("us")),
            ("open", decimal_type),
            ("high", decimal_type),
            ("low", decimal_type),
            ("close", decimal_type),
            ("base_volume", decimal_type),
            ("quote_volume", decimal_type),
            ("vwap", decimal_type),
            ("trade_count", pa.int64()),
            ("source_curated_snapshot_key", pa.string()),
            ("candle_schema_version", pa.string()),
        ]
    )
    partitions: dict[tuple[str, str], list[dict[str, object]]] = {}
    for minute, value in enumerate(["10", "10", "11", "12", "9", "8", "1000"], -2):
        window_start = start + timedelta(minutes=minute)
        price = Decimal(value).quantize(Decimal("0.000000000000000001"))
        partitions.setdefault(
            (window_start.date().isoformat(), f"{window_start.hour:02d}"), []
        ).append(
            {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "window_start": window_start.replace(tzinfo=None),
                "window_end": (window_start + timedelta(minutes=1)).replace(
                    tzinfo=None
                ),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "base_volume": Decimal("1.000000000000000000"),
                "quote_volume": price,
                "vwap": price,
                "trade_count": 1,
                "source_curated_snapshot_key": "b" * 64,
                "candle_schema_version": "v1",
            }
        )

    candle_run_key = f"{root}/candles/runs/source-run"
    for (event_date, event_hour), rows in partitions.items():
        output = BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), output)
        key = (
            f"{candle_run_key}/interval=1m/event_date={event_date}/"
            f"event_hour={event_hour}/part.parquet"
        )
        s3.put_object(Bucket="crypto-data", Key=key, Body=output.getvalue())

    manifest = {
        "candle_count": 7,
        "candle_output_uri": f"s3a://crypto-data/{candle_run_key}",
        "candle_schema_version": "v1",
        "interval": "1m",
        "mode": "apply",
        "snapshot_key": hashlib.sha256(root.encode()).hexdigest(),
        "source_curated_snapshot_key": "b" * 64,
        "status": "published",
    }
    body = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    manifest_key = f"{root}/candles/manifests/source/manifest.json"
    s3.put_object(Bucket="crypto-data", Key=manifest_key, Body=body)
    return f"s3a://crypto-data/{manifest_key}", hashlib.sha256(body).hexdigest()


def _environment(root: str) -> dict[str, str]:
    return {
        **os.environ,
        "BACKTEST_S3_ENDPOINT": os.getenv(
            "INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"
        ),
        "BACKTEST_S3_ACCESS_KEY": "minioadmin",
        "BACKTEST_S3_SECRET_KEY": "minioadmin",
        "BACKTEST_CANDLE_MANIFEST_PREFIX": f"s3a://crypto-data/{root}/candles/manifests",
        "BACKTEST_CANDLE_OUTPUT_PREFIX": f"s3a://crypto-data/{root}/candles/runs",
        "BACKTEST_OUTPUT_PREFIX": f"s3a://crypto-data/{root}/results",
        "BACKTEST_MAXIMUM_INPUT_CANDLES": "100",
    }


def _run(
    manifest_uri: str,
    digest: str,
    output_uri: str,
    environment: dict[str, str],
    *,
    fee_bps: str = "40",
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "crypto_trading_core.backtest",
            "--candle-manifest",
            manifest_uri,
            "--candle-manifest-sha256",
            digest,
            "--exchange",
            "coinbase",
            "--symbol",
            "BTC-USD",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:05:00Z",
            "--starting-cash",
            "1000",
            "--fast-period",
            "2",
            "--slow-period",
            "3",
            "--fee-bps",
            fee_bps,
            "--slippage-bps",
            "5",
            "--output",
            output_uri,
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=180,
    )


def _report(output: str) -> dict[str, object]:
    lines = [
        line for line in output.splitlines() if line.startswith("BACKTEST_REPORT_JSON=")
    ]
    assert len(lines) == 1, output
    return json.loads(lines[0].partition("=")[2])


@pytest.mark.skipif(
    not _enabled(),
    reason="set RUN_BACKTEST_INTEGRATION_TESTS=1 with local MinIO available",
)
def test_pinned_candles_produce_reproducible_s3_backtest() -> None:
    import boto3
    import pyarrow.parquet as pq

    root = f"integration/backtests/{uuid4()}"
    endpoint = os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    manifest_uri, digest = _put_candle_snapshot(s3, root)
    output_uri = f"s3a://crypto-data/{root}/results"
    environment = _environment(root)

    try:
        first = _run(manifest_uri, digest, output_uri, environment)
        assert first.returncode == 0, first.stdout + first.stderr
        report = _report(first.stdout)
        assert report["status"] == "published"
        assert report["execution_mode"] == "simulation"
        summary = report["summary"]
        assert isinstance(summary, dict)
        assert summary["decisions"] == 3
        assert summary["buys"] == summary["sells"] == 1
        assert summary["unfilled_terminal_decisions"] == 1

        fills_uri = report["artifacts"]["fills.parquet"]["uri"]
        assert isinstance(fills_uri, str)
        fills_key = fills_uri.split("crypto-data/", 1)[1]
        fills_body = s3.get_object(Bucket="crypto-data", Key=fills_key)["Body"].read()
        fills = pq.read_table(BytesIO(fills_body)).to_pylist()
        assert [row["side"] for row in fills] == ["BUY", "SELL"]
        assert fills[0]["reference_open_price"] == Decimal("12.000000000000000000")
        assert fills[1]["reference_open_price"] == Decimal("8.000000000000000000")

        validation = subprocess.run(
            [
                sys.executable,
                "-m",
                "crypto_trading_core.validate_backtest",
                "--manifest",
                report["manifest_uri"],
            ],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
            timeout=180,
        )
        assert validation.returncode == 0, validation.stdout + validation.stderr
        assert "BACKTEST_VALIDATION_JSON=" in validation.stdout

        repeated = _run(manifest_uri, digest, output_uri, environment)
        assert repeated.returncode == 0, repeated.stdout + repeated.stderr
        assert _report(repeated.stdout)["status"] == "resolved_existing_backtest"

        changed = _run(manifest_uri, digest, output_uri, environment, fee_bps="41")
        assert changed.returncode == 0, changed.stdout + changed.stderr
        assert _report(changed.stdout)["backtest_key"] != report["backtest_key"]
        manifests = s3.list_objects_v2(
            Bucket="crypto-data", Prefix=f"{root}/results/manifests/"
        ).get("Contents", [])
        assert len(manifests) == 2

        s3.put_object(
            Bucket="crypto-data", Key=fills_key, Body=fills_body + b"tampered"
        )
        tampered = subprocess.run(
            [
                sys.executable,
                "-m",
                "crypto_trading_core.validate_backtest",
                "--manifest",
                report["manifest_uri"],
            ],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
            timeout=180,
        )
        assert tampered.returncode == 4
        assert "byte count changed" in tampered.stderr
    finally:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=f"{root}/"
        ):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket="crypto-data", Delete={"Objects": objects})
