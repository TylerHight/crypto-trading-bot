import json
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any, ClassVar

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from .storage import ObjectStorage, child_uri

REQUIRED_ARCHIVE_FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "exchange",
    "symbol",
    "event_time",
    "ingested_at",
    "source_event_id",
    "source_sequence",
    "producer",
    "trace_id",
    "correlation_id",
    "causation_id",
    "payload",
)


class RetryableArchiveReadError(RuntimeError):
    """Raw archive access failed and may succeed on a bounded retry."""


class PermanentArchiveReadError(RuntimeError):
    """Raw archive structure cannot be reconciled as-is."""


@dataclass(frozen=True)
class ArchivedTrade:
    symbol: str
    source_event_id: str
    event_time: datetime

    @property
    def identity(self) -> tuple[str, str]:
        return self.symbol, self.source_event_id

    def safe_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "source_event_id": self.source_event_id,
            "event_time": self.event_time.isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class ArchiveScan:
    trades: tuple[ArchivedTrade, ...]
    archived_trade_count: int
    duplicate_identities: int
    malformed_values: int
    malformed_samples: tuple[dict[str, object], ...]


def _position_sample(row: Mapping[str, Any], reason: str) -> dict[str, object]:
    return {
        "kafka_topic": row.get("kafka_topic"),
        "kafka_partition": row.get("kafka_partition"),
        "kafka_offset": row.get("kafka_offset"),
        "reason": reason,
    }


def scan_archived_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    symbol: str,
    start_at: datetime,
    end_at: datetime,
    sample_limit: int,
) -> ArchiveScan:
    """Parse raw rows safely and select Coinbase trades in one bounded interval."""

    requested_symbol = symbol.strip().upper()
    accepted: list[ArchivedTrade] = []
    malformed_count = 0
    malformed_samples: list[dict[str, object]] = []

    def malformed(row: Mapping[str, Any], reason: str) -> None:
        nonlocal malformed_count
        malformed_count += 1
        if len(malformed_samples) < sample_limit:
            malformed_samples.append(_position_sample(row, reason))

    for row in rows:
        value = row.get("kafka_value")
        if not isinstance(value, (bytes, bytearray, memoryview)):
            malformed(row, "invalid_binary_value")
            continue
        try:
            event = json.loads(bytes(value).decode("utf-8", errors="strict"))
        except UnicodeDecodeError:
            malformed(row, "invalid_utf8")
            continue
        except json.JSONDecodeError:
            malformed(row, "invalid_json")
            continue
        if not isinstance(event, Mapping):
            malformed(row, "not_object")
            continue
        if any(field not in event for field in REQUIRED_ARCHIVE_FIELDS):
            malformed(row, "missing_required_field")
            continue
        if event["event_type"] != "market.trade.raw" or event["schema_version"] != "v1":
            malformed(row, "invalid_event_contract")
            continue

        exchange = event["exchange"]
        event_symbol = event["symbol"]
        source_event_id = event["source_event_id"]
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                exchange,
                event_symbol,
                source_event_id,
            )
        ):
            malformed(row, "invalid_identity_field")
            continue
        if (
            exchange.strip().lower() != "coinbase"
            or event_symbol.strip().upper() != requested_symbol
        ):
            continue

        try:
            event_time = datetime.fromisoformat(str(event["event_time"]))
        except ValueError:
            malformed(row, "invalid_event_time")
            continue
        if event_time.tzinfo is None or event_time.utcoffset() is None:
            malformed(row, "naive_event_time")
            continue
        event_time = event_time.astimezone(UTC)
        if start_at <= event_time < end_at:
            accepted.append(
                ArchivedTrade(
                    symbol=requested_symbol,
                    source_event_id=source_event_id.strip(),
                    event_time=event_time,
                )
            )

    counts = Counter(trade.identity for trade in accepted)
    unique = {trade.identity: trade for trade in accepted}
    return ArchiveScan(
        trades=tuple(unique[identity] for identity in sorted(unique)),
        archived_trade_count=len(accepted),
        duplicate_identities=sum(count > 1 for count in counts.values()),
        malformed_values=malformed_count,
        malformed_samples=tuple(malformed_samples),
    )


class RawParquetArchive:
    """Read only the raw date/hour partitions overlapping an audit interval."""

    COLUMNS: ClassVar[list[str]] = [
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_value",
    ]

    def __init__(self, storage: ObjectStorage, raw_input: str) -> None:
        self._storage = storage
        self._raw_input = raw_input.rstrip("/")

    def scan(
        self,
        *,
        symbol: str,
        start_at: datetime,
        end_at: datetime,
        sample_limit: int,
    ) -> ArchiveScan:
        parquet_uris: set[str] = set()
        hour = start_at.replace(minute=0, second=0, microsecond=0)
        while hour < end_at:
            prefix = child_uri(
                self._raw_input,
                f"event_date={hour.date().isoformat()}",
                f"event_hour={hour:%H}",
            )
            try:
                parquet_uris.update(self._storage.list_parquet(prefix))
            except Exception as error:
                raise RetryableArchiveReadError("Could not list a raw archive partition") from error
            hour += timedelta(hours=1)

        rows: list[Mapping[str, Any]] = []
        for uri in sorted(parquet_uris):
            try:
                content = self._storage.read_bytes(uri)
            except Exception as error:
                raise RetryableArchiveReadError("Could not read a raw archive object") from error
            try:
                table = pq.read_table(BytesIO(content), columns=self.COLUMNS)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
                raise PermanentArchiveReadError(
                    "Raw archive object has an incompatible Parquet contract"
                ) from error
            rows.extend(table.to_pylist())
        return scan_archived_rows(
            rows,
            symbol=symbol,
            start_at=start_at,
            end_at=end_at,
            sample_limit=sample_limit,
        )
