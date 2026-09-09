"""Immutable human review records for a pinned historical-gap policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from typing import Any

from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes, is_local_uri
from crypto_trading_core.experiments import ExperimentSettings, _safe_output
from crypto_trading_core.storage import ObjectStorage, child_uri

VERSION = "gap-policy-review-v1"
ACTOR = re.compile(r"^[a-z0-9][a-z0-9_-]{1,63}$")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _pinned_json(store: ObjectStorage, uri: str, digest: str, name: str) -> dict[str, Any]:
    body = store.read_bytes(uri)
    if _sha(body) != digest:
        raise InvalidBacktestInput(f"{name} SHA-256 digest does not match")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidBacktestInput(f"{name} is invalid JSON") from error
    if not isinstance(document, dict):
        raise InvalidBacktestInput(f"{name} is not a JSON object")
    return document


def _ranges(value: Any, name: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise InvalidBacktestInput(f"{name} is invalid")
    result: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("start"), str) or not isinstance(
            item.get("end"), str
        ):
            raise InvalidBacktestInput(f"{name} is invalid")
        result.append({"start": item["start"], "end": item["end"]})
    return result


def _evidence(
    store: ObjectStorage,
    *,
    policy_uri: str,
    policy_sha256: str,
    report_uri: str,
    report_sha256: str,
) -> dict[str, Any]:
    policy = _pinned_json(store, policy_uri, policy_sha256, "gap policy")
    required_policy = {
        "gap_policy_version",
        "name",
        "approval_status",
        "source_manifest_sha256",
        "missing_candle_action",
        "indicator_action",
        "pending_order_action",
        "open_position_action",
        "declared_missing_ranges",
    }
    if set(policy) != required_policy or policy.get("gap_policy_version") != "contiguous-source-minutes-v1":
        raise InvalidBacktestInput("gap policy does not have the expected schema")
    if policy.get("approval_status") != "pending_human_review":
        raise InvalidBacktestInput("gap policy is not awaiting human review")
    source_sha = policy.get("source_manifest_sha256")
    if not isinstance(source_sha, str) or len(source_sha) != 64:
        raise InvalidBacktestInput("gap policy source manifest digest is invalid")
    gaps = _ranges(policy["declared_missing_ranges"], "declared_missing_ranges")
    report = _pinned_json(store, report_uri, report_sha256, "gap-safe research report")
    summary = report.get("summary")
    report_policy = report.get("gap_policy")
    coverage_ref = report.get("coverage")
    if (
        report.get("version") != "longer-research-v1"
        or report.get("status") != "published"
        or not isinstance(summary, dict)
        or summary.get("status") != "policy_review_required"
        or not isinstance(report_policy, dict)
        or report_policy.get("sha256") != policy_sha256
        or not isinstance(coverage_ref, dict)
        or not isinstance(coverage_ref.get("uri"), str)
        or not isinstance(coverage_ref.get("sha256"), str)
    ):
        raise InvalidBacktestInput("gap-safe research report does not match the pending policy")
    coverage = _pinned_json(store, coverage_ref["uri"], coverage_ref["sha256"], "coverage report")
    manifests = coverage.get("source_manifests")
    if (
        not isinstance(manifests, list)
        or not manifests
        or not isinstance(manifests[0], dict)
        or manifests[0].get("sha256") != source_sha
        or _ranges(coverage.get("missing_ranges"), "coverage missing_ranges") != gaps
    ):
        raise InvalidBacktestInput("gap policy, report, and source evidence do not agree")
    return {
        "coverage_sha256": coverage_ref["sha256"],
        "coverage_uri": coverage_ref["uri"],
        "gap_policy_sha256": policy_sha256,
        "gap_policy_uri": policy_uri,
        "report_sha256": report_sha256,
        "report_uri": report_uri,
        "source_manifest_sha256": source_sha,
        "source_gaps": gaps,
    }


def record_review(
    arguments: argparse.Namespace,
    settings: ExperimentSettings,
    *,
    store: ObjectStorage | None = None,
) -> dict[str, Any]:
    if arguments.decision not in {"approved", "rejected"}:
        raise InvalidBacktestInput("review decision must be approved or rejected")
    if not ACTOR.fullmatch(arguments.reviewer):
        raise InvalidBacktestInput("reviewer has an invalid format")
    if not arguments.note.strip() or len(arguments.note) > 1000:
        raise InvalidBacktestInput("review note must contain 1 to 1000 characters")
    try:
        parsed_time = datetime.fromisoformat(arguments.decided_at)
    except (AttributeError, ValueError) as error:
        raise InvalidBacktestInput("decision time must be an ISO-8601 UTC timestamp") from error
    if parsed_time.tzinfo is None:
        raise InvalidBacktestInput("decision time must include UTC")
    decided_at = parsed_time.astimezone(UTC)
    storage = store or ObjectStorage(settings.backtest.storage)
    output = arguments.output or child_uri(settings.output_prefix, "gap_policy_reviews")
    _safe_output(output, settings, arguments.local_development)
    if not arguments.local_development and (is_local_uri(arguments.policy) or is_local_uri(arguments.report)):
        raise InvalidBacktestInput("local review evidence requires explicit local-development mode")
    evidence = _evidence(
        storage,
        policy_uri=arguments.policy,
        policy_sha256=arguments.policy_sha256,
        report_uri=arguments.report,
        report_sha256=arguments.report_sha256,
    )
    identity = {
        "decision": arguments.decision,
        "evidence": evidence,
        "note": arguments.note.strip(),
        "reviewer": arguments.reviewer,
        "version": VERSION,
    }
    review_key = _sha(canonical_json_bytes(identity))
    registration_uri = child_uri(
        output, "registrations", f"{arguments.policy_sha256}-{arguments.report_sha256}.json"
    )
    registration = {"review_key": review_key, **identity}
    existing = storage.try_read_bytes(registration_uri)
    if existing is not None and existing != canonical_json_bytes(registration):
        raise InvalidBacktestInput("a conflicting review decision is already recorded for this evidence")
    record = {
        "status": "published",
        "decision": arguments.decision,
        "decided_at": decided_at,
        "evidence": evidence,
        "note": arguments.note.strip(),
        "review_key": review_key,
        "reviewer": arguments.reviewer,
        "version": VERSION,
    }
    record_uri = child_uri(output, "reviews", review_key, "manifest.json")
    body = canonical_json_bytes(record)
    storage.try_write_bytes_append_only(record_uri, body, content_type="application/json")
    if storage.read_bytes(record_uri) != body:
        raise InvalidBacktestInput("review record conflicts or failed read-back")
    storage.try_write_bytes_append_only(
        registration_uri, canonical_json_bytes(registration), content_type="application/json"
    )
    if storage.read_bytes(registration_uri) != canonical_json_bytes(registration):
        raise InvalidBacktestInput("review registration conflicts or failed read-back")
    return {**record, "manifest_uri": record_uri, "manifest_sha256": _sha(body)}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Record an immutable review of a historical gap policy.")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--policy-sha256", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--report-sha256", required=True)
    parser.add_argument("--decision", required=True, choices=("approved", "rejected"))
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--note", required=True)
    parser.add_argument("--decided-at", required=True)
    parser.add_argument("--output")
    parser.add_argument("--local-development", action="store_true")
    try:
        review = record_review(parser.parse_args(argv), ExperimentSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Review rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("GAP_POLICY_REVIEW_JSON=" + canonical_json_bytes(review).decode("ascii"))


if __name__ == "__main__":
    main()
