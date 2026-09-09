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
    return os.getenv("RUN_EXPERIMENT_INTEGRATION_TESTS") == "1"


def _put_inputs(s3, root: str) -> tuple[str, str]:
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
    prices = [10, 11, 9, 12, 8, 13, 7, 14, 6, 15, 5, 16, 4, 17, 3, 18, 2]
    for minute, value in enumerate(prices):
        timestamp = start + timedelta(minutes=minute)
        price = Decimal(value).quantize(Decimal("0.000000000000000001"))
        partitions.setdefault(
            (timestamp.date().isoformat(), f"{timestamp.hour:02d}"), []
        ).append(
            {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "window_start": timestamp.replace(tzinfo=None),
                "window_end": (timestamp + timedelta(minutes=1)).replace(tzinfo=None),
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
    candle_run = f"{root}/candles/runs/source"
    for (event_date, event_hour), rows in partitions.items():
        output = BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), output)
        s3.put_object(
            Bucket="crypto-data",
            Key=(
                f"{candle_run}/interval=1m/event_date={event_date}/"
                f"event_hour={event_hour}/part.parquet"
            ),
            Body=output.getvalue(),
        )
    candle_manifest = {
        "candle_count": len(prices),
        "candle_output_uri": f"s3a://crypto-data/{candle_run}",
        "candle_schema_version": "v1",
        "interval": "1m",
        "mode": "apply",
        "snapshot_key": hashlib.sha256(root.encode()).hexdigest(),
        "source_curated_snapshot_key": "b" * 64,
        "status": "published",
    }
    candle_body = json.dumps(
        candle_manifest, separators=(",", ":"), sort_keys=True
    ).encode()
    candle_manifest_key = f"{root}/candles/manifests/source/manifest.json"
    s3.put_object(Bucket="crypto-data", Key=candle_manifest_key, Body=candle_body)

    spec = {
        "experiment_spec_version": "v1",
        "name": "btc-usd-sma-v1",
        "candle_manifest_uri": f"s3a://crypto-data/{candle_manifest_key}",
        "candle_manifest_sha256": hashlib.sha256(candle_body).hexdigest(),
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "starting_cash": "1000.000000000000000000",
        "fee_bps": "40",
        "slippage_bps": "5",
        "ranges": {
            "train": {"start": "2026-01-01T00:03:00Z", "end": "2026-01-01T00:07:00Z"},
            "validation": {
                "start": "2026-01-01T00:08:00Z",
                "end": "2026-01-01T00:12:00Z",
            },
            "test": {"start": "2026-01-01T00:13:00Z", "end": "2026-01-01T00:17:00Z"},
        },
        "candidates": [
            {"candidate_id": "sma-1-2", "fast_period": 1, "slow_period": 2},
            {"candidate_id": "sma-2-3", "fast_period": 2, "slow_period": 3},
        ],
        "selection_policy": {
            "minimum_train_fills": 0,
            "maximum_train_drawdown": "1.000000000000000000",
            "maximum_validation_drawdown": "1.000000000000000000",
        },
    }
    spec_body = json.dumps(spec, separators=(",", ":"), sort_keys=True).encode()
    spec_key = f"{root}/specs/experiment.json"
    s3.put_object(Bucket="crypto-data", Key=spec_key, Body=spec_body)
    return f"s3a://crypto-data/{spec_key}", hashlib.sha256(spec_body).hexdigest()


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
        "BACKTEST_OUTPUT_PREFIX": f"s3a://crypto-data/{root}/results/backtests",
        "BACKTEST_MAXIMUM_INPUT_CANDLES": "1000",
        "EXPERIMENT_SPEC_PREFIX": f"s3a://crypto-data/{root}/specs",
        "EXPERIMENT_OUTPUT_PREFIX": f"s3a://crypto-data/{root}/results",
        "EXPERIMENT_MAXIMUM_CANDIDATE_CANDLE_EVALUATIONS": "10000",
    }


def _run(module: str, arguments: list[str], environment: dict[str, str]):
    return subprocess.run(
        [sys.executable, "-m", module, *arguments],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=240,
    )


def _report(output: str, prefix: str) -> dict[str, object]:
    lines = [line for line in output.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, output
    return json.loads(lines[0].partition("=")[2])


@pytest.mark.skipif(
    not _enabled(),
    reason="set RUN_EXPERIMENT_INTEGRATION_TESTS=1 with local MinIO available",
)
def test_sealed_selection_and_out_of_sample_evaluation_on_minio() -> None:
    import boto3
    import pyarrow.parquet as pq

    root = f"integration/experiments/{uuid4()}"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("INTEGRATION_S3_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    spec_uri, spec_digest = _put_inputs(s3, root)
    environment = _environment(root)
    output_uri = f"s3a://crypto-data/{root}/results"

    try:
        prepared = _run(
            "crypto_trading_core.prepare_experiment",
            ["--spec", spec_uri, "--spec-sha256", spec_digest, "--output", output_uri],
            environment,
        )
        assert prepared.returncode == 0, prepared.stdout + prepared.stderr
        selection = _report(prepared.stdout, "EXPERIMENT_SELECTION_JSON=")
        assert selection["selection_status"] == "selected"
        assert selection["test_data_accessed"] is False

        candidate_uri = selection["artifacts"]["candidate_results.parquet"]["uri"]
        candidate_key = candidate_uri.split("crypto-data/", 1)[1]
        candidate_body = s3.get_object(Bucket="crypto-data", Key=candidate_key)[
            "Body"
        ].read()
        ranges = set(
            pq.read_table(BytesIO(candidate_body)).column("range_name").to_pylist()
        )
        assert ranges == {"train", "validation"}

        selection_uri = selection["manifest_uri"]
        selection_key = selection_uri.split("crypto-data/", 1)[1]
        selection_body = s3.get_object(Bucket="crypto-data", Key=selection_key)[
            "Body"
        ].read()
        selection_digest = hashlib.sha256(selection_body).hexdigest()
        selection_validation = _run(
            "crypto_trading_core.validate_experiment",
            [
                "selection",
                "--manifest",
                selection_uri,
                "--manifest-sha256",
                selection_digest,
            ],
            environment,
        )
        assert selection_validation.returncode == 0, (
            selection_validation.stdout + selection_validation.stderr
        )

        evaluated = _run(
            "crypto_trading_core.evaluate_experiment",
            [
                "--selection-manifest",
                selection_uri,
                "--selection-manifest-sha256",
                selection_digest,
                "--output",
                output_uri,
            ],
            environment,
        )
        assert evaluated.returncode == 0, evaluated.stdout + evaluated.stderr
        evaluation = _report(evaluated.stdout, "EXPERIMENT_EVALUATION_JSON=")
        assert evaluation["summary"]["selection_basis"] == "train_and_validation_only"
        assert evaluation["summary"]["evaluation_range"] == "out_of_sample"

        evaluation_uri = evaluation["manifest_uri"]
        evaluation_key = evaluation_uri.split("crypto-data/", 1)[1]
        evaluation_body = s3.get_object(Bucket="crypto-data", Key=evaluation_key)[
            "Body"
        ].read()
        evaluation_digest = hashlib.sha256(evaluation_body).hexdigest()
        evaluation_validation = _run(
            "crypto_trading_core.validate_experiment",
            [
                "evaluation",
                "--manifest",
                evaluation_uri,
                "--manifest-sha256",
                evaluation_digest,
            ],
            environment,
        )
        assert evaluation_validation.returncode == 0, (
            evaluation_validation.stdout + evaluation_validation.stderr
        )

        repeated_prepare = _run(
            "crypto_trading_core.prepare_experiment",
            ["--spec", spec_uri, "--spec-sha256", spec_digest, "--output", output_uri],
            environment,
        )
        assert _report(repeated_prepare.stdout, "EXPERIMENT_SELECTION_JSON=")[
            "status"
        ] == ("resolved_existing_selection")
        repeated_evaluate = _run(
            "crypto_trading_core.evaluate_experiment",
            [
                "--selection-manifest",
                selection_uri,
                "--selection-manifest-sha256",
                selection_digest,
                "--output",
                output_uri,
            ],
            environment,
        )
        assert _report(repeated_evaluate.stdout, "EXPERIMENT_EVALUATION_JSON=")[
            "status"
        ] == ("resolved_existing_evaluation")

        comparison_uri = evaluation["artifacts"]["comparison.json"]["uri"]
        comparison_key = comparison_uri.split("crypto-data/", 1)[1]
        comparison_body = s3.get_object(Bucket="crypto-data", Key=comparison_key)[
            "Body"
        ].read()
        s3.put_object(
            Bucket="crypto-data", Key=comparison_key, Body=comparison_body + b"tampered"
        )
        tampered = _run(
            "crypto_trading_core.validate_experiment",
            [
                "evaluation",
                "--manifest",
                evaluation_uri,
                "--manifest-sha256",
                evaluation_digest,
            ],
            environment,
        )
        assert tampered.returncode == 4
        assert "digest or size changed" in tampered.stderr
    finally:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="crypto-data", Prefix=f"{root}/"
        ):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket="crypto-data", Delete={"Objects": objects})
