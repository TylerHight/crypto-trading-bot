from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from jobs.spark.schemas.market_trades_v1 import (
    CURATED_MARKET_TRADE_SCHEMA,
    CURATED_SCHEMA_VERSION,
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    QUARANTINE_SCHEMA,
)

CANONICAL_TOPIC = "market.trades.raw.v1"
EVENT_TYPE = "market.trade.raw"
RAW_SCHEMA_VERSION = "v1"
TRANSFORM_VERSION = "market-trades-curation-v1"
SUPPORTED_PRODUCERS = frozenset({"apps.collector", "apps.historical_backfill"})
SUPPORTED_SIDES = frozenset({"BUY", "SELL"})
ENVELOPE_FIELDS = frozenset(
    {
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
    }
)
LOGICAL_FIELDS = (
    "exchange",
    "symbol",
    "source_event_id",
    "event_time",
    "price",
    "size",
    "notional",
    "source_side",
)
QUANTUM = Decimal(1).scaleb(-DECIMAL_SCALE)

CANDIDATE_SCHEMA = T.StructType(
    [
        *CURATED_MARKET_TRADE_SCHEMA.fields,
        T.StructField("_logical_fingerprint", T.StringType(), nullable=False),
        T.StructField("_value_sha256", T.StringType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class RecordValidation:
    candidate: dict[str, Any] | None
    quarantine: dict[str, Any] | None


@dataclass(frozen=True)
class CurationFrames:
    curated: DataFrame
    quarantine: DataFrame
    candidates: DataFrame
    duplicate_groups: DataFrame


class RecordFailure(ValueError):
    def __init__(self, code: str, field: str | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.field = field


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Row):
        return value.asDict(recursive=True)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _uuid_text(value: Any, field: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise RecordFailure("invalid_envelope_contract", field)
    try:
        return str(UUID(value))
    except (ValueError, AttributeError) as error:
        raise RecordFailure("invalid_envelope_contract", field) from error


def _aware_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise RecordFailure("invalid_envelope_contract", field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RecordFailure("invalid_envelope_contract", field) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecordFailure("invalid_envelope_contract", field)
    return parsed.astimezone(timezone.utc)


def _spark_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise RecordFailure("invalid_envelope_contract", "kafka_timestamp")
    # Spark TimestampType values are timezone-less Python datetimes interpreted
    # in the configured UTC SQL session.
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decimal(value: Any, field: str) -> Decimal:
    code = "invalid_price" if field == "price" else "invalid_size"
    if not isinstance(value, str) or not value.strip():
        raise RecordFailure(code, field)
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise RecordFailure(code, field) from error
    if not parsed.is_finite() or parsed <= 0:
        raise RecordFailure(code, field)
    try:
        scaled = parsed.quantize(QUANTUM)
    except InvalidOperation as error:
        raise RecordFailure("decimal_overflow", field) from error
    if scaled != parsed:
        raise RecordFailure("decimal_overflow", field)
    digits_before_decimal = max(scaled.adjusted() + 1, 0)
    if digits_before_decimal > DECIMAL_PRECISION - DECIMAL_SCALE:
        raise RecordFailure("decimal_overflow", field)
    return scaled


def _notional(price: Decimal, size: Decimal) -> Decimal:
    try:
        value = (price * size).quantize(QUANTUM, rounding=ROUND_HALF_EVEN)
    except InvalidOperation as error:
        raise RecordFailure("decimal_overflow", "notional") from error
    if not value.is_finite() or value <= 0:
        raise RecordFailure("decimal_overflow", "notional")
    if max(value.adjusted() + 1, 0) > DECIMAL_PRECISION - DECIMAL_SCALE:
        raise RecordFailure("decimal_overflow", "notional")
    return value


def _strict_json(value: bytes) -> dict[str, Any]:
    try:
        text = value.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise RecordFailure("invalid_utf8", "kafka_value") from error
    try:
        document = json.loads(
            text,
            parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)),
        )
    except (json.JSONDecodeError, ValueError) as error:
        raise RecordFailure("invalid_json", "kafka_value") from error
    if not isinstance(document, dict):
        raise RecordFailure("invalid_envelope_contract", "kafka_value")
    return document


def _headers(value: Any) -> dict[str, bytes]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise RecordFailure("invalid_headers", "kafka_headers")
    result: dict[str, bytes] = {}
    for raw_header in value:
        header = _mapping(raw_header)
        key = header.get("key")
        raw_value = header.get("value")
        if (
            not isinstance(key, str)
            or key in result
            or not isinstance(raw_value, (bytes, bytearray, memoryview))
        ):
            raise RecordFailure("invalid_headers", "kafka_headers")
        result[key] = bytes(raw_value)
    return result


def _header_text(headers: Mapping[str, bytes], name: str) -> str:
    value = headers.get(name)
    if value is None:
        raise RecordFailure("invalid_headers", name)
    try:
        return value.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise RecordFailure("invalid_headers", name) from error


def _event_id(exchange: str, symbol: str, source_event_id: str) -> str:
    identity = json.dumps(
        [EVENT_TYPE, RAW_SCHEMA_VERSION, exchange, symbol, source_event_id],
        separators=(",", ":"),
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _safe_event_id(document: Mapping[str, Any] | None) -> str | None:
    if document is None:
        return None
    try:
        return _uuid_text(document.get("event_id"), "event_id")
    except RecordFailure:
        return None


def _quarantine(
    record: Mapping[str, Any],
    run_id: str,
    quarantined_at: datetime,
    failure: RecordFailure,
    value_hash: str | None,
    document: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "curation_run_id": run_id,
        "kafka_topic": (
            record.get("kafka_topic")
            if isinstance(record.get("kafka_topic"), str)
            else None
        ),
        "kafka_partition": _safe_int(record.get("kafka_partition")),
        "kafka_offset": _safe_int(record.get("kafka_offset")),
        "kafka_timestamp": (
            record.get("kafka_timestamp")
            if isinstance(record.get("kafka_timestamp"), datetime)
            else None
        ),
        "failure_code": failure.code,
        "failure_field": failure.field,
        "event_id_when_safe": _safe_event_id(document),
        "value_sha256": value_hash,
        "quarantined_at": quarantined_at,
    }


def validate_raw_record(
    raw_record: Mapping[str, Any],
    *,
    run_id: str,
    curated_at: datetime,
    canonical_topic: str = CANONICAL_TOPIC,
) -> RecordValidation:
    """Validate one immutable raw row without retaining unsafe payload diagnostics."""

    record = dict(raw_record)
    raw_value = record.get("kafka_value")
    value = (
        bytes(raw_value)
        if isinstance(raw_value, (bytes, bytearray, memoryview))
        else None
    )
    value_hash = hashlib.sha256(value).hexdigest() if value is not None else None
    document: dict[str, Any] | None = None
    try:
        topic = record.get("kafka_topic")
        partition = _safe_int(record.get("kafka_partition"))
        offset = _safe_int(record.get("kafka_offset"))
        kafka_key = record.get("kafka_key")
        if (
            not isinstance(topic, str)
            or partition is None
            or partition < 0
            or offset is None
            or offset < 0
            or not isinstance(kafka_key, (bytes, bytearray, memoryview))
            or value is None
            or record.get("kafka_headers") is None
            or record.get("kafka_timestamp") is None
        ):
            raise RecordFailure("invalid_envelope_contract", "raw_kafka_record")
        if topic != canonical_topic:
            raise RecordFailure("invalid_envelope_contract", "kafka_topic")
        kafka_timestamp = _spark_utc(record["kafka_timestamp"])
        document = _strict_json(value)
        if set(document) != ENVELOPE_FIELDS:
            raise RecordFailure("invalid_envelope_contract", "envelope_fields")

        event_id = _uuid_text(document.get("event_id"), "event_id")
        trace_id = _uuid_text(document.get("trace_id"), "trace_id")
        correlation_id = _uuid_text(
            document.get("correlation_id"), "correlation_id", nullable=True
        )
        causation_id = _uuid_text(
            document.get("causation_id"), "causation_id", nullable=True
        )
        if document.get("event_type") != EVENT_TYPE:
            raise RecordFailure("invalid_envelope_contract", "event_type")
        if document.get("schema_version") != RAW_SCHEMA_VERSION:
            raise RecordFailure("invalid_envelope_contract", "schema_version")

        exchange_value = document.get("exchange")
        symbol_value = document.get("symbol")
        source_event_id = document.get("source_event_id")
        producer = document.get("producer")
        if not isinstance(exchange_value, str) or not exchange_value.strip():
            raise RecordFailure("invalid_envelope_contract", "exchange")
        exchange = exchange_value.strip().lower()
        if exchange != "coinbase":
            raise RecordFailure("unsupported_exchange", "exchange")
        if not isinstance(symbol_value, str) or not symbol_value.strip():
            raise RecordFailure("invalid_envelope_contract", "symbol")
        symbol = symbol_value.strip().upper()
        if not isinstance(source_event_id, str) or not source_event_id.strip():
            raise RecordFailure("invalid_envelope_contract", "source_event_id")
        source_event_id = source_event_id.strip()
        if producer not in SUPPORTED_PRODUCERS:
            raise RecordFailure("invalid_envelope_contract", "producer")
        source_sequence = document.get("source_sequence")
        if source_sequence is not None and (
            _safe_int(source_sequence) is None or source_sequence < 0
        ):
            raise RecordFailure("invalid_envelope_contract", "source_sequence")

        event_time = _aware_utc(document.get("event_time"), "event_time")
        ingested_at = _aware_utc(document.get("ingested_at"), "ingested_at")
        if event_id != _event_id(exchange, symbol, source_event_id):
            raise RecordFailure("payload_identity_mismatch", "event_id")

        try:
            key_text = bytes(kafka_key).decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise RecordFailure("invalid_kafka_key", "kafka_key") from error
        if key_text != f"{exchange}:{symbol}":
            raise RecordFailure("invalid_kafka_key", "kafka_key")

        headers = _headers(record["kafka_headers"])
        if (
            _header_text(headers, "event_type") != EVENT_TYPE
            or _header_text(headers, "schema_version") != RAW_SCHEMA_VERSION
            or _header_text(headers, "trace_id") != trace_id
        ):
            raise RecordFailure("invalid_headers", "kafka_headers")

        payload = document.get("payload")
        if not isinstance(payload, Mapping):
            raise RecordFailure("invalid_envelope_contract", "payload")
        required_payload = {"trade_id", "product_id", "price", "size", "side", "time"}
        if not required_payload.issubset(payload):
            raise RecordFailure("invalid_envelope_contract", "payload")
        if payload.get("trade_id") != source_event_id:
            raise RecordFailure("payload_identity_mismatch", "payload.trade_id")
        product_id = payload.get("product_id")
        if not isinstance(product_id, str) or product_id.upper() != symbol:
            raise RecordFailure("payload_identity_mismatch", "payload.product_id")
        try:
            payload_time = _aware_utc(payload.get("time"), "payload.time")
        except RecordFailure as error:
            raise RecordFailure("payload_time_mismatch", "payload.time") from error
        if payload_time != event_time:
            raise RecordFailure("payload_time_mismatch", "payload.time")

        price = _decimal(payload.get("price"), "price")
        size = _decimal(payload.get("size"), "size")
        side_value = payload.get("side")
        if not isinstance(side_value, str) or side_value.upper() not in SUPPORTED_SIDES:
            raise RecordFailure("invalid_side", "payload.side")
        source_side = side_value.upper()
        notional = _notional(price, size)
        logical = {
            "exchange": exchange,
            "symbol": symbol,
            "source_event_id": source_event_id,
            "event_time": event_time.isoformat(),
            "price": format(price, "f"),
            "size": format(size, "f"),
            "notional": format(notional, "f"),
            "source_side": source_side,
        }
        fingerprint = hashlib.sha256(
            json.dumps(logical, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        candidate = {
            "event_id": event_id,
            "exchange": exchange,
            "symbol": symbol,
            "source_event_id": source_event_id,
            "event_time": event_time,
            "ingested_at": ingested_at,
            "kafka_timestamp": kafka_timestamp,
            "price": price,
            "size": size,
            "notional": notional,
            "source_side": source_side,
            "source_sequence": source_sequence,
            "producer": producer,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "curated_schema_version": CURATED_SCHEMA_VERSION,
            "curated_at": curated_at,
            "event_date": event_time.date(),
            "_logical_fingerprint": fingerprint,
            "_value_sha256": value_hash,
        }
        return RecordValidation(candidate, None)
    except RecordFailure as failure:
        return RecordValidation(
            None,
            _quarantine(
                record,
                run_id,
                curated_at,
                failure,
                value_hash,
                document,
            ),
        )


def _conflict_fields() -> Any:
    fields = [
        F.when(F.min(F.col(field)) != F.max(F.col(field)), F.lit(field))
        for field in LOGICAL_FIELDS
    ]
    return F.concat_ws(",", F.array_compact(F.array(*fields)))


def curate_market_trades(
    records: DataFrame,
    *,
    run_id: str,
    curated_at: datetime,
    canonical_topic: str = CANONICAL_TOPIC,
) -> CurationFrames:
    """Apply the shared validation, deduplication, and quarantine transformation."""

    spark = records.sparkSession
    validated = records.rdd.map(
        lambda row: validate_raw_record(
            row.asDict(recursive=True),
            run_id=run_id,
            curated_at=curated_at,
            canonical_topic=canonical_topic,
        )
    ).persist(StorageLevel.DISK_ONLY)
    candidates = spark.createDataFrame(
        validated.filter(lambda item: item.candidate is not None).map(
            lambda item: item.candidate
        ),
        CANDIDATE_SCHEMA,
    )
    invalid_quarantine = spark.createDataFrame(
        validated.filter(lambda item: item.quarantine is not None).map(
            lambda item: item.quarantine
        ),
        QUARANTINE_SCHEMA,
    )

    duplicate_groups = candidates.groupBy("event_id").agg(
        F.count(F.lit(1)).alias("deliveries"),
        F.when(
            F.min("_logical_fingerprint") == F.max("_logical_fingerprint"),
            F.lit(1),
        )
        .otherwise(F.lit(2))
        .alias("logical_variants"),
        _conflict_fields().alias("conflicting_fields"),
    )
    non_conflicting = candidates.join(
        duplicate_groups.where(F.col("logical_variants") == 1).select("event_id"),
        on="event_id",
        how="inner",
    )
    selected = (
        non_conflicting.withColumn(
            "_representative",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(
                    "kafka_topic", "kafka_partition", "kafka_offset"
                )
            ),
        )
        .where(F.col("_representative") == 1)
        .select(*CURATED_MARKET_TRADE_SCHEMA.fieldNames())
    )

    conflicting = candidates.join(
        duplicate_groups.where(F.col("logical_variants") > 1).select(
            "event_id", "conflicting_fields"
        ),
        on="event_id",
        how="inner",
    ).select(
        F.lit(run_id).alias("curation_run_id"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.lit("conflicting_duplicate").alias("failure_code"),
        F.col("conflicting_fields").alias("failure_field"),
        F.col("event_id").alias("event_id_when_safe"),
        F.col("_value_sha256").alias("value_sha256"),
        F.lit(curated_at).cast("timestamp").alias("quarantined_at"),
    )
    quarantine = invalid_quarantine.unionByName(conflicting)
    return CurationFrames(selected, quarantine, candidates, duplicate_groups)


def decimal_policy() -> dict[str, Any]:
    """Expose the checked-in numeric policy for reports and tests."""

    return {
        "precision": DECIMAL_PRECISION,
        "scale": DECIMAL_SCALE,
        "notional_rounding": "ROUND_HALF_EVEN",
        "positive_only": True,
    }
