import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from crypto_historical_backfill.historical_candles import acquire, canonical, load_plan
from crypto_historical_backfill.storage import ObjectStorage, StorageSettings

START = datetime(2026, 6, 10, tzinfo=UTC)


def plan(minutes=301):
    return canonical(
        {
            "plan_version": "historical-candles-v1",
            "name": "fixture",
            "source": "coinbase-exchange",
            "symbol": "BTC-USD",
            "start": START,
            "end": START + timedelta(minutes=minutes),
            "warmup_minutes": 0,
        }
    )


class Client:
    def __init__(self, gap=None):
        self.calls = 0
        self.gap = gap

    def fetch_page(self, start, end):
        self.calls += 1
        rows = []
        while start < end:
            if start != self.gap:
                rows.append([int(start.timestamp()), 1, 4, 2, 3, 5])
            start += timedelta(minutes=1)
        return json.dumps(rows).encode()


def test_acquisition_pages_resumes_and_publishes_hashed_parquet(tmp_path):
    store = ObjectStorage(StorageSettings())
    client = Client()
    result = acquire(plan(), store=store, output=str(tmp_path / "out"), client=client)
    assert client.calls == 2
    assert result["coverage"]["missing_minutes"] == 0
    assert result["coverage"]["status"] == "incomplete"
    for item in result["files"]:
        assert Path(item["uri"]).read_bytes()
        assert pq.ParquetFile(item["uri"]).metadata.num_rows > 0
    second = Client()
    assert (
        acquire(plan(), store=store, output=str(tmp_path / "out"), client=second)
        == result
    )
    assert second.calls == 0


def test_gaps_are_reported_without_creating_candles(tmp_path):
    store = ObjectStorage(StorageSettings())
    result = acquire(
        plan(3),
        store=store,
        output=str(tmp_path / "out"),
        client=Client(START + timedelta(minutes=1)),
    )
    assert result["candle_count"] == 2
    assert result["coverage"]["status"] == "incomplete"
    assert result["coverage"]["missing_minutes"] == 1


def test_changed_cached_page_and_invalid_plan_are_rejected(tmp_path):
    store = ObjectStorage(StorageSettings())
    result = acquire(
        plan(3), store=store, output=str(tmp_path / "out"), client=Client()
    )
    source = json.loads(Path(result["source_archive_manifest_uri"]).read_bytes())
    cached = Path(source["pages"][0]["uri"])
    cached.write_bytes(b"{}")
    with pytest.raises(ValueError, match="cached source page"):
        acquire(plan(3), store=store, output=str(tmp_path / "out"), client=Client())
    with pytest.raises(ValueError):
        load_plan(b"{}")
