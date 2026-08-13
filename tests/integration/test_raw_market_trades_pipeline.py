import json
import os
import time
from datetime import UTC, datetime
from io import BytesIO
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS") == "1"


@pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_INTEGRATION_TESTS=1 with the local Compose stack running",
)
def test_kafka_record_is_archived_byte_for_byte_in_raw_parquet() -> None:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from confluent_kafka import Producer

    event_id = str(uuid4())
    event = {
        "event_id": event_id,
        "event_type": "market.trade.raw",
        "schema_version": "v1",
        "exchange": "integration-test",
        "symbol": "BTC-USD",
        "event_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "ingested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source_event_id": event_id,
        "source_sequence": None,
        "producer": "apps.collector",
        "trace_id": str(uuid4()),
        "correlation_id": None,
        "causation_id": None,
        "payload": {"fixture": True},
    }
    key = b"integration-test:BTC-USD"
    value = json.dumps(event, separators=(",", ":")).encode()
    headers = [("event_type", b"market.trade.raw"), ("schema_version", b"v1")]

    delivery: list[tuple[str, int, int]] = []
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        "market.trades.raw.v1",
        key=key,
        value=value,
        headers=headers,
        on_delivery=lambda error, message: (
            pytest.fail(str(error))
            if error
            else delivery.append(
                (message.topic(), message.partition(), message.offset())
            )
        ),
    )
    assert producer.flush(10) == 0
    assert len(delivery) == 1
    expected_identity = delivery[0]

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    deadline = time.monotonic() + 90
    matching_rows: list[dict[str, object]] = []

    while time.monotonic() < deadline:
        matching_rows.clear()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket="crypto-data", Prefix="raw/market_trade_raw/v1/"
        ):
            for item in page.get("Contents", []):
                object_key = item["Key"]
                if not object_key.endswith(".parquet"):
                    continue
                body = s3.get_object(Bucket="crypto-data", Key=object_key)[
                    "Body"
                ].read()
                table = pq.read_table(BytesIO(body), partitioning=None)
                table = table.append_column(
                    "event_date",
                    pa.array(
                        [object_key.split("event_date=")[1].split("/")[0]] * len(table)
                    ),
                )
                table = table.append_column(
                    "event_hour",
                    pa.array(
                        [object_key.split("event_hour=")[1].split("/")[0]] * len(table)
                    ),
                )
                matching_rows.extend(
                    row for row in table.to_pylist() if row["kafka_value"] == value
                )

        if matching_rows:
            break
        time.sleep(2)

    assert len(matching_rows) == 1
    row = matching_rows[0]
    assert (
        row["kafka_topic"],
        row["kafka_partition"],
        row["kafka_offset"],
    ) == expected_identity
    assert row["kafka_key"] == key
    assert row["kafka_value"] == value
    assert row["kafka_headers"] == [
        {"key": header_key, "value": header_value}
        for header_key, header_value in headers
    ]
    assert len(str(row["event_date"])) == 10
    assert len(str(row["event_hour"])) == 2
