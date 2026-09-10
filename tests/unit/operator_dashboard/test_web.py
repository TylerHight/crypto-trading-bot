from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.web import create_server


class FakeSource:
    def __init__(self) -> None:
        self.calls = 0

    def read(self) -> dict[str, object]:
        self.calls += 1
        return {
            "draft_plan": {
                "details": {"name": "btc-usd-sma-forward-v1"},
                "raw_sha256": "a" * 64,
                "status": "draft",
            },
            "generated_at": "2026-09-04T12:00:00Z",
            "next_action": {
                "action": "Review the OOS result.",
                "runbook": "apps/trading_core/README.md",
            },
            "pilot": {
                "message": "No real paper pilot has been registered.",
                "source": "PostgreSQL read-only query",
                "status": "not_registered",
            },
            "pipeline": [
                {
                    "detail": "trades=44, sequence gaps=0",
                    "name": "Collector feed",
                    "observed_at": "2026-09-04T12:00:00Z",
                    "source": "Kafka quality topic",
                    "status": "healthy",
                },
                {
                    "detail": "Latest raw archive is older than the freshness window.",
                    "name": "Raw archive",
                    "observed_at": "2026-08-14T12:00:00Z",
                    "source": "MinIO raw Parquet metadata",
                    "status": "stale",
                },
            ],
            "research": {
                "evaluation": {
                    "name": "OOS evaluation",
                    "observed_at": "2026-09-04T12:00:00Z",
                    "sha256": "c" * 64,
                    "source": "MinIO immutable publication",
                    "status": "current",
                    "uri": "s3a://crypto-data/evaluations/manifest.json",
                },
                "explanation": "Out-of-sample results are evidence, not a profitability claim.",
                "oos": {
                    "buy_and_hold_return": "0.01",
                    "candidate": {"candidate_id": "sma-5-20"},
                    "excess_return": "-0.02",
                    "strategy_return": "-0.01",
                },
                "selection": {
                    "name": "Sealed candidate selection",
                    "observed_at": "2026-09-04T12:00:00Z",
                    "sha256": "d" * 64,
                    "source": "MinIO immutable publication",
                    "status": "current",
                    "uri": "s3a://crypto-data/selections/manifest.json",
                },
                "selection_summary": {
                    "candidate": {"candidate_id": "sma-5-20"},
                    "test_data_accessed_before_selection": False,
                    "test_range": {"start": "2026-09-04T10:00:00Z"},
                    "train_range": {"start": "2026-09-04T08:00:00Z"},
                    "validation_range": {"start": "2026-09-04T09:00:00Z"},
                },
                "status": "healthy",
            },
            "trusted_data": {
                "curated_trades": {
                    "details": {"Logical trades": "10"},
                    "name": "Curated trades <script>",
                    "observed_at": "2026-09-04T12:00:00Z",
                    "sha256": "b" * 64,
                    "source": "MinIO immutable publication",
                    "status": "current",
                    "uri": "s3a://crypto-data/curated/manifest.json",
                }
            },
        }


@contextmanager
def _server(source: FakeSource) -> Iterator[str]:
    server = create_server(DashboardSettings(port=0), source=source)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        host, port = server.server_address[:2]
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        worker.join(timeout=2)
        server.server_close()


def test_dashboard_renders_the_not_registered_state_and_escapes_artifacts() -> None:
    source = FakeSource()
    with _server(source) as address:
        with urlopen(address + "/") as response:
            page = response.read().decode("utf-8")
        with urlopen(address + "/api/status") as response:
            document = json.loads(response.read())

    assert "Not registered" in page
    assert "No balance, fills, or assessment are shown because no pilot exists." in page
    assert "Read only. No real orders." in page
    assert "What to do next" in page
    assert "Check the raw sink. It needs a new archive." in page
    assert "Live feed" in page
    assert "SMA 5/20: -0.01% vs 0.01%" in page
    assert 'role="tablist"' in page
    for name in ("research", "trades", "system", "evidence", "paper"):
        assert f'aria-controls="panel-{name}"' in page
        assert f'aria-labelledby="tab-{name}"' in page
    assert 'http-equiv="refresh"' not in page
    assert "Refresh snapshot" in page
    assert "Final test results" in page
    assert "Study evidence" in page
    assert '<script src="/assets/dashboard.js" defer>' in page
    assert "Curated trades &lt;script&gt;" in page
    assert "Curated trades <script>" not in page
    assert document["pilot"]["status"] == "not_registered"
    assert source.calls == 2


def test_dashboard_static_script_is_local_and_does_not_read_sources() -> None:
    source = FakeSource()
    with _server(source) as address:
        with urlopen(address + "/assets/dashboard.js") as response:
            script = response.read().decode("utf-8")
            assert response.headers["Content-Type"].startswith("text/javascript")
        with pytest.raises(HTTPError) as error:
            urlopen(address + "/assets/../config.py")
    assert error.value.code == 404
    assert "trade-marker" in script
    assert "sessionStorage" in script
    assert "setInterval" not in script
    assert source.calls == 0


def test_embedded_chart_json_and_copy_values_are_escaped() -> None:
    from crypto_operator_dashboard.web import _copyable, render_dashboard

    attack = '</script><script>alert("bad")</script>'
    page = render_dashboard({"research": {"visualization": {"label": attack}}}, 30)
    assert attack not in page
    assert "\\u003c/script\\u003e" in page
    assert 'data-copy="a b &quot;c&quot;"' in _copyable("ID", 'a b "c"')


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_dashboard_rejects_mutating_methods_without_reading_a_source(
    method: str,
) -> None:
    source = FakeSource()
    with _server(source) as address:
        request = Request(address + "/api/status", data=b"{}", method=method)
        with pytest.raises(HTTPError) as error:
            urlopen(request)

    assert error.value.code == 405
    assert source.calls == 0
