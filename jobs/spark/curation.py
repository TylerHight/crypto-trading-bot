from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from jobs.spark.schemas.market_trades_v1 import CURATED_SCHEMA_VERSION
from jobs.spark.transforms.curated_market_trades import (
    CANONICAL_TOPIC,
    TRANSFORM_VERSION,
)


class InvalidCurationInput(ValueError):
    """The frozen audit evidence or publication configuration is unsafe."""


@dataclass(frozen=True)
class PartitionBound:
    kafka_topic: str
    kafka_partition: int
    earliest_offset: int
    ending_offset_exclusive: int

    def as_dict(self) -> dict[str, str | int]:
        return {
            "kafka_topic": self.kafka_topic,
            "kafka_partition": self.kafka_partition,
            "earliest_offset": self.earliest_offset,
            "ending_offset_exclusive": self.ending_offset_exclusive,
        }


@dataclass(frozen=True)
class FrozenRawSnapshot:
    report: dict[str, Any]
    report_uri: str
    report_sha256: str
    topic: str
    bounds: tuple[PartitionBound, ...]
    snapshot_key: str

    @property
    def expected_raw_rows(self) -> int:
        return sum(
            bound.ending_offset_exclusive - bound.earliest_offset
            for bound in self.bounds
        )


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        default=_json_default,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidCurationInput(f"{field} must be an integer")
    return value


def _is_local_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    return parsed.scheme in {"", "file"} or (
        len(uri) >= 3 and uri[1] == ":" and uri[2] in {"/", "\\"}
    )


def _normalized_prefix(value: str) -> str:
    return value.strip().replace("\\", "/").rstrip("/")


def validate_distinct_prefixes(
    raw_input: str,
    curated_output: str,
    quarantine_output: str,
) -> None:
    values = [_normalized_prefix(item) for item in (raw_input, curated_output, quarantine_output)]
    if any(not item for item in values) or len(set(values)) != len(values):
        raise InvalidCurationInput(
            "raw, curated, and quarantine prefixes must be non-empty and distinct"
        )


def load_frozen_snapshot(
    report_bytes: bytes,
    *,
    report_uri: str,
    allowed_evidence_prefix: str,
    local_development: bool,
    canonical_topic: str = CANONICAL_TOPIC,
) -> FrozenRawSnapshot:
    """Validate a passing raw-integrity report and derive its deterministic key."""

    if _is_local_uri(report_uri):
        if not local_development:
            raise InvalidCurationInput(
                "local audit evidence requires explicit local-development mode"
            )
    elif not _normalized_prefix(report_uri).startswith(
        _normalized_prefix(allowed_evidence_prefix) + "/"
    ):
        raise InvalidCurationInput("raw-integrity report is outside the evidence prefix")

    digest = hashlib.sha256(report_bytes).hexdigest()
    try:
        report = json.loads(report_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidCurationInput("raw-integrity report is not valid UTF-8 JSON") from error
    if not isinstance(report, dict):
        raise InvalidCurationInput("raw-integrity report must be a JSON object")
    if report.get("status") != "passed":
        raise InvalidCurationInput("raw-integrity report status must be passed")
    if report.get("topic") != canonical_topic:
        raise InvalidCurationInput("raw-integrity report topic is not canonical")
    if _int(report.get("invalid_event_values"), "invalid_event_values") != 0:
        raise InvalidCurationInput("raw-integrity report contains invalid event values")

    partitions = report.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise InvalidCurationInput("raw-integrity report has no partition bounds")
    bounds: list[PartitionBound] = []
    seen: set[int] = set()
    for index, item in enumerate(partitions):
        if not isinstance(item, dict):
            raise InvalidCurationInput(f"partitions[{index}] must be an object")
        topic = item.get("kafka_topic")
        partition = _int(item.get("kafka_partition"), "kafka_partition")
        earliest = _int(item.get("earliest_offset"), "earliest_offset")
        ending = _int(
            item.get("ending_offset_exclusive"), "ending_offset_exclusive"
        )
        missing = _int(item.get("missing_from_parquet"), "missing_from_parquet")
        duplicates = _int(
            item.get("duplicate_parquet_positions"),
            "duplicate_parquet_positions",
        )
        kafka_records = _int(item.get("kafka_records"), "kafka_records")
        parquet_records = _int(
            item.get("parquet_records_in_range"), "parquet_records_in_range"
        )
        if topic != canonical_topic or partition < 0 or partition in seen:
            raise InvalidCurationInput("partition identity is invalid or duplicated")
        if earliest < 0 or ending < earliest:
            raise InvalidCurationInput("partition offsets are invalid")
        expected = ending - earliest
        if missing or duplicates or kafka_records != expected or parquet_records != expected:
            raise InvalidCurationInput("partition counts do not prove an exact raw snapshot")
        seen.add(partition)
        bounds.append(PartitionBound(topic, partition, earliest, ending))

    bounds.sort(key=lambda item: (item.kafka_topic, item.kafka_partition))
    snapshot_identity = {
        "topic": canonical_topic,
        "partition_offset_bounds": [bound.as_dict() for bound in bounds],
        "transform_version": TRANSFORM_VERSION,
        "curated_schema_version": CURATED_SCHEMA_VERSION,
    }
    snapshot_key = hashlib.sha256(canonical_json_bytes(snapshot_identity)).hexdigest()
    return FrozenRawSnapshot(
        report=report,
        report_uri=report_uri,
        report_sha256=digest,
        topic=canonical_topic,
        bounds=tuple(bounds),
        snapshot_key=snapshot_key,
    )
