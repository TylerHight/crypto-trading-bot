"""Audit missing Coinbase candle minutes against bounded public trade history."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

from crypto_exchange_adapters.coinbase_rest import (
    CoinbaseMarketTradesClient,
    CoinbaseRestError,
    HttpResponse,
    HttpTransport,
    UrllibHttpTransport,
)

from .historical_candles import canonical, publish, timestamp
from .storage import ObjectStorage, StorageSettings, child_uri

MINUTE = timedelta(minutes=1)
VERSION = "coinbase-candle-gap-audit-v1"


class RecordingTransport:
    def __init__(self, upstream: HttpTransport) -> None:
        self.upstream = upstream
        self.responses: list[tuple[str, HttpResponse]] = []

    def request(self, url: str, headers: Mapping[str, str], timeout: float) -> HttpResponse:
        response = self.upstream.request(url, headers, timeout)
        self.responses.append((url, response))
        return response


def _trade_candles(trades: tuple[Any, ...], start: datetime, end: datetime) -> list[dict[str, Any]]:
    grouped: dict[datetime, list[tuple[datetime, str, Decimal, Decimal]]] = defaultdict(list)
    for trade in trades:
        payload = trade.raw_payload
        try:
            price = Decimal(str(payload["price"]))
            size = Decimal(str(payload["size"]))
        except (KeyError, InvalidOperation, TypeError) as error:
            raise ValueError("trade price or size is invalid") from error
        if (
            not price.is_finite()
            or not size.is_finite()
            or price <= 0
            or size <= 0
            or price != price.quantize(Decimal("1e-18"))
            or size != size.quantize(Decimal("1e-18"))
        ):
            raise ValueError("trade price or size exceeds candle bounds")
        minute = trade.event_time.replace(second=0, microsecond=0)
        if not start <= minute < end:
            raise ValueError("trade is outside requested gap")
        grouped[minute].append((trade.event_time, trade.source_event_id, price, size))

    candles = []
    for minute, values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda value: (value[0], value[1]))
        prices = [value[2] for value in ordered]
        candles.append(
            {
                "window_start": minute,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "base_volume": sum((value[3] for value in ordered), Decimal(0)),
                "trade_count": len(ordered),
            }
        )
    return candles


def audit(
    manifest_uri: str,
    manifest_sha256: str,
    *,
    store: ObjectStorage,
    output: str,
    client_factory: Any = None,
) -> dict[str, Any]:
    body = store.read_bytes(manifest_uri)
    if hashlib.sha256(body).hexdigest() != manifest_sha256.lower():
        raise ValueError("candle manifest SHA-256 does not match")
    manifest = json.loads(body)
    if manifest.get("source_kind") != "exchange_ohlcv" or manifest.get("status") != "published":
        raise ValueError("audit requires a published Coinbase historical candle manifest")
    if (
        manifest.get("plan", {}).get("source") != "coinbase-exchange"
        or manifest.get("plan", {}).get("symbol") != "BTC-USD"
    ):
        raise ValueError("audit requires a Coinbase BTC-USD historical plan")
    ranges = manifest.get("coverage", {}).get("missing_ranges")
    if not isinstance(ranges, list) or len(ranges) > 100:
        raise ValueError("missing ranges are invalid or exceed the audit limit")
    if sum(int(item["minutes"]) for item in ranges) > 1500:
        raise ValueError("missing minutes exceed the audit limit")

    run_id = str(uuid4())
    base = child_uri(output, "runs", run_id)
    results: list[dict[str, Any]] = []
    for index, item in enumerate(ranges):
        start, end = timestamp(item["start"]), timestamp(item["end"])
        expected = int((end - start) / MINUTE)
        if expected <= 0 or expected != item["minutes"]:
            raise ValueError("manifest missing range is inconsistent")
        recording = RecordingTransport(UrllibHttpTransport())
        client = (
            client_factory(recording)
            if client_factory is not None
            else CoinbaseMarketTradesClient(transport=recording, max_requests=250)
        )
        error_text = None
        try:
            coverage = client.fetch_interval("BTC-USD", start, end)
            candles = _trade_candles(coverage.trades, start, end)
            complete = coverage.coverage_complete and len(candles) == expected
            reason = coverage.unresolved_reason or (
                "minutes_without_verified_trades" if len(candles) < expected else None
            )
        except (CoinbaseRestError, ValueError) as error:
            coverage = None
            candles = []
            complete = False
            reason = "source_request_failed"
            error_text = str(error)

        response_refs = []
        for response_index, (url, response) in enumerate(recording.responses):
            ref = publish(
                store,
                child_uri(base, "responses", f"{index:03d}-{response_index:04d}.json"),
                canonical(
                    {
                        "url": url,
                        "status": response.status,
                        "body_base64": base64.b64encode(response.body).decode("ascii"),
                        "body_sha256": hashlib.sha256(response.body).hexdigest(),
                    }
                ),
            )
            response_refs.append(ref)
        results.append(
            {
                "start": start,
                "end": end,
                "expected_minutes": expected,
                "verified_minutes": len(candles) if complete else 0,
                "observed_trade_minutes": len(candles),
                "trade_count": len(coverage.trades) if coverage else 0,
                "status": "reconstructable" if complete else "unresolved",
                "reason": reason,
                "error": error_text,
                "candles": candles if complete else [],
                "responses": response_refs,
            }
        )
    report = {
        "version": VERSION,
        "source_manifest_uri": manifest_uri,
        "source_manifest_sha256": manifest_sha256.lower(),
        "run_id": run_id,
        "total_missing_minutes": sum(item["expected_minutes"] for item in results),
        "verified_reconstructable_minutes": sum(item["verified_minutes"] for item in results),
        "complete": all(item["status"] == "reconstructable" for item in results),
        "ranges": results,
    }
    ref = publish(store, child_uri(base, "report.json"), canonical(report))
    return {"report_uri": ref["uri"], "report_sha256": ref["sha256"], **report}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Audit Coinbase candle gaps using public trades")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    store = ObjectStorage(
        StorageSettings(
            endpoint_url=os.getenv("HISTORY_S3_ENDPOINT", "http://127.0.0.1:9000"),
            access_key=os.getenv("HISTORY_S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("HISTORY_S3_SECRET_KEY", "minioadmin"),
        )
    )
    report = audit(
        args.manifest,
        args.manifest_sha256,
        store=store,
        output=args.output,
    )
    print(
        "CANDLE_GAP_AUDIT_JSON="
        + canonical(
            {
                key: report[key]
                for key in (
                    "report_uri",
                    "report_sha256",
                    "total_missing_minutes",
                    "verified_reconstructable_minutes",
                    "complete",
                )
            }
        ).decode("ascii")
    )
    if not report["complete"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
