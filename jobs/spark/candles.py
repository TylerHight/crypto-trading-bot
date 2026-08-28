from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from jobs.spark.curation import canonical_json_bytes
from jobs.spark.schemas.market_candles_v1 import (
    CANDLE_SCHEMA_VERSION,
    CANDLE_TRANSFORM_VERSION,
    SUPPORTED_INTERVAL,
)
from jobs.spark.schemas.market_trades_v1 import CURATED_SCHEMA_VERSION

SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


class InvalidCandleInput(ValueError):
    """The curated source manifest or candle publication configuration is unsafe."""


@dataclass(frozen=True)
class PublishedCuratedSnapshot:
    manifest: dict[str, Any]
    manifest_uri: str
    manifest_sha256: str
    snapshot_key: str
    output_uri: str
    expected_trades: int


def _is_local_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    return parsed.scheme in {"", "file"} or (
        len(uri) >= 3 and uri[1] == ":" and uri[2] in {"/", "\\"}
    )


def normalize_uri(value: str) -> str:
    return value.strip().replace("\\", "/").rstrip("/")


def validate_output_prefix(source_output: str, candle_output: str) -> None:
    source = normalize_uri(source_output)
    output = normalize_uri(candle_output)
    if not source or not output or source == output:
        raise InvalidCandleInput("curated source and candle output must be distinct")
    if output.startswith(source + "/") or source.startswith(output + "/"):
        raise InvalidCandleInput("curated source and candle output must not be nested")


def load_curated_snapshot(
    manifest_bytes: bytes,
    *,
    manifest_uri: str,
    expected_sha256: str,
    allowed_manifest_prefix: str,
    local_development: bool,
) -> PublishedCuratedSnapshot:
    """Validate and pin one successfully published curated-trade snapshot."""

    if _is_local_uri(manifest_uri):
        if not local_development:
            raise InvalidCandleInput(
                "local curated manifests require explicit local-development mode"
            )
    elif not normalize_uri(manifest_uri).startswith(
        normalize_uri(allowed_manifest_prefix) + "/"
    ):
        raise InvalidCandleInput("curated manifest is outside the allowed prefix")

    digest = hashlib.sha256(manifest_bytes).hexdigest()
    expected_digest = expected_sha256.strip().lower()
    if not SHA256_PATTERN.fullmatch(expected_digest) or digest != expected_digest:
        raise InvalidCandleInput("curated manifest SHA-256 digest does not match")
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidCandleInput("curated manifest is not valid UTF-8 JSON") from error
    if not isinstance(manifest, dict):
        raise InvalidCandleInput("curated manifest must be a JSON object")
    if manifest.get("status") != "published" or manifest.get("mode") != "apply":
        raise InvalidCandleInput("curated manifest is not a published apply run")
    if manifest.get("curated_schema_version") != CURATED_SCHEMA_VERSION:
        raise InvalidCandleInput("unsupported curated schema version")

    snapshot_key = manifest.get("snapshot_key")
    output_uri = manifest.get("curated_output_uri")
    expected_trades = manifest.get("curated_logical_trades")
    if not isinstance(snapshot_key, str) or not SHA256_PATTERN.fullmatch(snapshot_key):
        raise InvalidCandleInput("curated snapshot key is invalid")
    if (
        not isinstance(output_uri, str)
        or not output_uri.strip()
        or "/runs/" not in normalize_uri(output_uri)
    ):
        raise InvalidCandleInput("curated output URI is not an immutable run directory")
    if (
        isinstance(expected_trades, bool)
        or not isinstance(expected_trades, int)
        or expected_trades < 0
    ):
        raise InvalidCandleInput("curated trade count is invalid")
    return PublishedCuratedSnapshot(
        manifest=manifest,
        manifest_uri=manifest_uri,
        manifest_sha256=digest,
        snapshot_key=snapshot_key,
        output_uri=output_uri,
        expected_trades=expected_trades,
    )


def candle_snapshot_key(
    source: PublishedCuratedSnapshot,
    *,
    interval: str = SUPPORTED_INTERVAL,
    transform_version: str = CANDLE_TRANSFORM_VERSION,
) -> str:
    if interval != SUPPORTED_INTERVAL:
        raise InvalidCandleInput(f"unsupported candle interval: {interval!r}")
    identity = {
        "source_curated_snapshot_key": source.snapshot_key,
        "source_curated_manifest_sha256": source.manifest_sha256,
        "candle_schema_version": CANDLE_SCHEMA_VERSION,
        "candle_transform_version": transform_version,
        "interval": interval,
    }
    return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
