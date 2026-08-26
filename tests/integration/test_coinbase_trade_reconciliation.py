import json
import os
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from crypto_historical_backfill.reconcile_coinbase_trades import run

pytestmark = pytest.mark.integration
START = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)
END = START + timedelta(minutes=1)


def _integration_enabled() -> bool:
    return os.getenv("RUN_RECONCILIATION_INTEGRATION_TESTS") == "1"


def _event(trade_id: str, event_time: datetime) -> bytes:
    return json.dumps(
        {
            "event_id": f"event-{trade_id}",
            "event_type": "market.trade.raw",
            "schema_version": "v1",
            "exchange": "coinbase",
            "symbol": "BTC-USD",
            "event_time": event_time.isoformat().replace("+00:00", "Z"),
            "ingested_at": event_time.isoformat().replace("+00:00", "Z"),
            "source_event_id": trade_id,
            "source_sequence": None,
            "producer": "apps.collector",
            "trace_id": f"trace-{trade_id}",
            "correlation_id": None,
            "causation_id": None,
            "payload": {},
        }
    ).encode()


@pytest.mark.skipif(
    not _integration_enabled(),
    reason="set RUN_RECONCILIATION_INTEGRATION_TESTS=1",
)
def test_stubbed_rest_only_trade_is_persisted_without_modifying_raw(tmp_path) -> None:
    raw_hour = tmp_path / "raw" / "event_date=2026-08-25" / "event_hour=14"
    raw_hour.mkdir(parents=True)
    raw_file = raw_hour / "part-00000.parquet"
    pq.write_table(
        pa.table(
            {
                "kafka_topic": ["market.trades.raw.v1"],
                "kafka_partition": pa.array([0], type=pa.int32()),
                "kafka_offset": pa.array([10], type=pa.int64()),
                "kafka_value": [_event("archived", START + timedelta(seconds=10))],
            }
        ),
        raw_file,
    )
    original_raw = raw_file.read_bytes()
    integrity_report = tmp_path / "raw-audit.json"
    integrity_report.write_text(
        json.dumps(
            {
                "status": "passed",
                "topic": "market.trades.raw.v1",
                "completed_at": "2026-08-25T14:02:00Z",
            }
        )
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = json.dumps(
                {
                    "trades": [
                        {
                            "trade_id": "archived",
                            "product_id": "BTC-USD",
                            "time": "2026-08-25T14:00:10Z",
                        },
                        {
                            "trade_id": "rest-only",
                            "product_id": "BTC-USD",
                            "time": "2026-08-25T14:00:20Z",
                        },
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        exit_code = run(
            [
                "--symbol",
                "BTC-USD",
                "--start-at",
                "2026-08-25T14:00:00Z",
                "--end-at",
                "2026-08-25T14:01:00Z",
                "--raw-input",
                str(tmp_path / "raw"),
                "--report-output",
                str(tmp_path / "reconciliation"),
                "--raw-integrity-report",
                str(integrity_report),
                "--coinbase-base-url",
                f"http://127.0.0.1:{server.server_port}",
            ]
        )
    finally:
        server.shutdown()
        thread.join()

    assert exit_code == 2
    assert raw_file.read_bytes() == original_raw
    findings_files = list((tmp_path / "reconciliation" / "findings").rglob("*.json"))
    assert len(findings_files) == 1
    findings = json.loads(findings_files[0].read_text())
    assert [item["source_event_id"] for item in findings["missing_from_archive"]] == [
        "rest-only"
    ]
