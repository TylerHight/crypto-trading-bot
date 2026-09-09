"""Resumable, append-only acquisition of exchange-produced historical candles."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from crypto_exchange_adapters.coinbase_candles import (
    ENDPOINT,
    MINUTE,
    SCHEMA_VERSION,
    CoinbaseCandlesClient,
    decode_candles,
)
from crypto_exchange_adapters.coinbase_rest import CoinbaseRestError

from .storage import ObjectStorage, StorageSettings, child_uri

VERSION = "historical-candles-v1"
DEFAULT_OUTPUT = "s3a://crypto-data/analytics/historical_candles/v1"
DECIMAL = pa.decimal128(38, 18)
SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("window_start", pa.timestamp("us")),
        ("window_end", pa.timestamp("us")),
        ("open", DECIMAL),
        ("high", DECIMAL),
        ("low", DECIMAL),
        ("close", DECIMAL),
        ("base_volume", DECIMAL),
        ("candle_schema_version", pa.string()),
        ("source_archive_key", pa.string()),
    ]
)


def canonical(document: Any) -> bytes:
    def default(value: Any) -> str:
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
        if isinstance(value, Decimal):
            return format(value, "f")
        raise TypeError(type(value).__name__)

    return json.dumps(document, default=default, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def publish(
    store: ObjectStorage, uri: str, body: bytes, content_type: str = "application/json"
) -> dict[str, Any]:
    store.try_write_bytes_append_only(uri, body, content_type=content_type)
    if store.read_bytes(uri) != body:
        raise ValueError("immutable historical publication conflicts or failed read-back")
    return {"uri": uri, "sha256": sha(body), "bytes": len(body)}


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.utcoffset() != timedelta(0)
        or result.second
        or result.microsecond
    ):
        raise ValueError("historical boundaries must be aligned UTC minutes")
    return result.astimezone(UTC)


def load_plan(body: bytes) -> dict[str, Any]:
    plan = json.loads(body)
    if not isinstance(plan, dict) or set(plan) != {
        "plan_version",
        "name",
        "source",
        "symbol",
        "start",
        "end",
        "warmup_minutes",
    }:
        raise ValueError("historical plan fields are invalid")
    if (
        plan["plan_version"] != VERSION
        or plan["source"] != "coinbase-exchange"
        or plan["symbol"] != "BTC-USD"
    ):
        raise ValueError("only Coinbase Exchange BTC-USD history is supported")
    if not isinstance(plan["name"], str) or not re.fullmatch(
        r"[a-z0-9][a-z0-9-]{0,79}", plan["name"]
    ):
        raise ValueError("invalid historical plan name")
    start, end = timestamp(plan["start"]), timestamp(plan["end"])
    if not MINUTE <= end - start <= timedelta(days=366) or end > datetime.now(UTC):
        raise ValueError("historical range must be closed and at most 366 days")
    warmup = plan["warmup_minutes"]
    if isinstance(warmup, bool) or not isinstance(warmup, int) or not 0 <= warmup <= 10000:
        raise ValueError("warmup must be 0..10000 minutes")
    return plan


def gaps(start: datetime, end: datetime, present: set[datetime]) -> list[dict[str, Any]]:
    missing = []
    cursor = start
    for stamp in sorted(t for t in present if start <= t < end):
        if stamp > cursor:
            missing.append(
                {"start": cursor, "end": stamp, "minutes": int((stamp - cursor) / MINUTE)}
            )
        cursor = stamp + MINUTE
    if cursor < end:
        missing.append({"start": cursor, "end": end, "minutes": int((end - cursor) / MINUTE)})
    return missing


def acquire(
    plan_body: bytes,
    *,
    store: ObjectStorage,
    output: str = DEFAULT_OUTPUT,
    client: CoinbaseCandlesClient | None = None,
    progress: Any = None,
) -> dict[str, Any]:
    plan = load_plan(plan_body)
    key = sha(canonical({"plan": plan, "endpoint": ENDPOINT, "version": VERSION}))
    base = child_uri(output, "runs", key)
    publish(store, child_uri(output, "plans", plan["name"] + ".json"), canonical(plan))
    start, end = timestamp(plan["start"]), timestamp(plan["end"])
    first = start - plan["warmup_minutes"] * MINUTE
    client = client or CoinbaseCandlesClient()
    rows: dict[datetime, dict[str, Any]] = {}
    duplicate_count = 0
    conflicts: set[datetime] = set()
    page_refs = []
    page_start = first
    while page_start < end:
        page_end = min(page_start + 300 * MINUTE, end)
        page_uri = child_uri(base, "pages", str(int(page_start.timestamp())) + ".json")
        cached = store.try_read_bytes(page_uri)
        if cached is None:
            body = client.fetch_page(page_start, page_end)
            # A complete cache object stores exact response bytes plus their digest.
            envelope = {
                "start": page_start,
                "end": page_end,
                "body": body.decode("utf-8"),
                "sha256": sha(body),
            }
            cached = canonical(envelope)
            store.try_write_bytes_append_only(page_uri, cached, content_type="application/json")
            cached = store.read_bytes(page_uri)
        try:
            envelope = json.loads(cached)
            body = envelope["body"].encode("utf-8")
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, AttributeError) as error:
            raise ValueError("cached source page is invalid") from error
        if (
            sha(body) != envelope["sha256"]
            or timestamp(envelope["start"]) != page_start
            or timestamp(envelope["end"]) != page_end
        ):
            raise ValueError("cached source page digest or boundaries changed")
        page_refs.append({"uri": page_uri, "sha256": sha(cached), "response_sha256": sha(body)})
        for row in decode_candles(body):
            stamp = row["window_start"]
            assert isinstance(stamp, datetime)
            # The provider may include buckets outside the requested interval.
            if not page_start <= stamp < page_end:
                continue
            previous = rows.get(stamp)
            if previous is not None:
                if previous == row:
                    duplicate_count += 1
                else:
                    conflicts.add(stamp)
            else:
                rows[stamp] = row
        page_start = page_end
        if progress and (len(page_refs) % 25 == 0 or page_start == end):
            progress(f"Saved {len(page_refs)} pages; {len(rows):,} candle minutes.")
    for stamp in conflicts:
        rows.pop(stamp, None)
    missing = gaps(first, end, set(rows))
    research_missing = gaps(start, end, set(rows))
    expected = int((end - first) / MINUTE)
    ready = not missing and not conflicts and end - start >= timedelta(days=90)
    coverage = {
        "status": "ready" if ready else "incomplete",
        "expected_minutes": expected,
        "available_minutes": len(rows),
        "missing_minutes": sum(x["minutes"] for x in missing),
        "missing_ranges": missing,
        "research_missing_ranges": research_missing,
        "duplicate_minutes": duplicate_count,
        "conflicting_minutes": sorted(conflicts),
        "start": first,
        "end": end,
        "research_start": start,
        "research_minutes": int((end - start) / MINUTE),
        "warmup_minutes": plan["warmup_minutes"],
    }
    archive = {
        "status": "published",
        "archive_key": key,
        "source": "coinbase-exchange",
        "endpoint": ENDPOINT,
        "plan": plan,
        "pages": page_refs,
    }
    archive_ref = publish(store, child_uri(base, "source.json"), canonical(archive))
    output_uri = child_uri(base, "candles")
    partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for stamp, row in sorted(rows.items()):
        partitions[stamp.date().isoformat()].append(
            {
                **row,
                "source_archive_key": key,
                "window_start": stamp.replace(tzinfo=None),
                "window_end": (stamp + MINUTE).replace(tzinfo=None),
            }
        )
    files = []
    for day, items in sorted(partitions.items()):
        target = child_uri(
            output_uri, "interval=1m", f"event_date={day}", "event_hour=00", "part.parquet"
        )
        buffer = BytesIO()
        pq.write_table(
            pa.Table.from_pylist(items, schema=SCHEMA), buffer, compression="zstd", version="2.6"
        )
        files.append(publish(store, target, buffer.getvalue(), "application/vnd.apache.parquet"))
    manifest = {
        "status": "published",
        "mode": "apply",
        "snapshot_key": key,
        "candle_schema_version": SCHEMA_VERSION,
        "interval": "1m",
        "source_kind": "exchange_ohlcv",
        "candle_count": len(rows),
        "candle_output_uri": output_uri,
        "source_archive_key": key,
        "source_archive_manifest_uri": archive_ref["uri"],
        "source_archive_manifest_sha256": archive_ref["sha256"],
        "files": files,
        "coverage": coverage,
        "plan": plan,
    }
    manifest_ref = publish(
        store, child_uri(output, "manifests", key, "manifest.json"), canonical(manifest)
    )
    return {
        **json.loads(canonical(manifest)),
        "manifest_uri": manifest_ref["uri"],
        "manifest_sha256": manifest_ref["sha256"],
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Prepare resumable BTC historical candles, without running strategies."
    )
    parser.add_argument("--plan", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    store = ObjectStorage(
        StorageSettings(
            endpoint_url=os.getenv("HISTORY_S3_ENDPOINT", "http://127.0.0.1:9000"),
            access_key=os.getenv("HISTORY_S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("HISTORY_S3_SECRET_KEY", "minioadmin"),
        )
    )
    try:
        body = store.read_bytes(args.plan)
        if sha(body) != args.plan_sha256:
            raise ValueError("historical acquisition plan SHA-256 does not match")
        result = acquire(
            body,
            store=store,
            output=args.output,
            progress=lambda message: print(message, flush=True),
        )
    except (ValueError, OSError, CoinbaseRestError) as error:
        print(
            f"Historical acquisition stopped: {error}. Saved pages can be resumed.", file=sys.stderr
        )
        raise SystemExit(4) from error
    print(
        "HISTORICAL_DATASET_JSON="
        + canonical(
            {
                key: result[key]
                for key in ("manifest_uri", "manifest_sha256", "coverage", "candle_count")
            }
        ).decode()
    )
    if result["coverage"]["status"] != "ready":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
