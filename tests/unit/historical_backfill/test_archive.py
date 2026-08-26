import json
from datetime import UTC, datetime, timedelta

from crypto_historical_backfill.archive import scan_archived_rows

START = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)


def row(
    offset: int,
    trade_id: str,
    event_time: datetime,
    symbol: str = "BTC-USD",
) -> dict[str, object]:
    value = json.dumps(
        {
            "event_id": f"event-{trade_id}",
            "event_type": "market.trade.raw",
            "schema_version": "v1",
            "exchange": "coinbase",
            "symbol": symbol,
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
    return {
        "kafka_topic": "market.trades.raw.v1",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_value": value,
    }


def test_archive_scan_filters_symbol_and_uses_half_open_time_bounds() -> None:
    scan = scan_archived_rows(
        [
            row(0, "start", START),
            row(1, "inside", START + timedelta(seconds=1)),
            row(2, "end", START + timedelta(seconds=10)),
            row(3, "eth", START, symbol="ETH-USD"),
        ],
        symbol="BTC-USD",
        start_at=START,
        end_at=START + timedelta(seconds=10),
        sample_limit=20,
    )

    assert [trade.source_event_id for trade in scan.trades] == ["inside", "start"]
    assert scan.archived_trade_count == 2
    assert scan.malformed_values == 0


def test_archive_scan_counts_duplicate_source_identities() -> None:
    scan = scan_archived_rows(
        [row(0, "same", START), row(1, "same", START)],
        symbol="BTC-USD",
        start_at=START,
        end_at=START + timedelta(seconds=10),
        sample_limit=20,
    )

    assert len(scan.trades) == 1
    assert scan.archived_trade_count == 2
    assert scan.duplicate_identities == 1


def test_malformed_values_are_counted_and_samples_are_capped() -> None:
    rows = [
        {
            "kafka_topic": "market.trades.raw.v1",
            "kafka_partition": 0,
            "kafka_offset": offset,
            "kafka_value": b"not-json",
        }
        for offset in range(3)
    ]

    scan = scan_archived_rows(
        rows,
        symbol="BTC-USD",
        start_at=START,
        end_at=START + timedelta(seconds=10),
        sample_limit=2,
    )

    assert scan.malformed_values == 3
    assert len(scan.malformed_samples) == 2
    assert all("kafka_value" not in sample for sample in scan.malformed_samples)
