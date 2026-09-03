from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from crypto_trading_core.contracts import SHA256_PATTERN, canonical_json_bytes, parse_utc_minute
from crypto_trading_core.paper_contracts import (
    PAPER_ENGINE_VERSION,
    PAPER_SCHEMA_VERSION,
    InvalidPaperTrading,
    decimal38,
)

PILOT_PLAN_VERSION = "v1"
PILOT_SCHEMA_VERSION = "v1"
ASSESSMENT_POLICY_VERSION = "paper-pilot-assessment-v1"
PILOT_NAME_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")
PILOT_ID_PATTERN = SHA256_PATTERN

PLAN_FIELDS = {
    "pilot_plan_version",
    "name",
    "evaluation_manifest_uri",
    "evaluation_manifest_sha256",
    "start_not_before",
    "minimum_calendar_days",
    "minimum_processed_candles",
    "minimum_fills",
    "maximum_drawdown",
    "minimum_excess_return_over_buy_and_hold",
    "maximum_data_gap_events",
    "maximum_conflict_events",
    "maximum_unplanned_pauses",
}


class PilotState(StrEnum):
    REGISTERED = "registered"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INCONCLUSIVE = "inconclusive"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            PilotState.COMPLETED,
            PilotState.FAILED,
            PilotState.INCONCLUSIVE,
            PilotState.CANCELLED,
        }


def _integer(value: Any, field: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise InvalidPaperTrading(f"{field} must be an integer from {minimum} through {maximum}")
    return value


def _decimal(value: Any, field: str, *, allow_negative: bool = False) -> Decimal:
    if not isinstance(value, str):
        raise InvalidPaperTrading(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InvalidPaperTrading(f"{field} is not a decimal") from error
    return decimal38(parsed, field, allow_negative=allow_negative)


@dataclass(frozen=True)
class PilotPlan:
    name: str
    evaluation_manifest_uri: str
    evaluation_manifest_sha256: str
    start_not_before: datetime
    minimum_calendar_days: int
    minimum_processed_candles: int
    minimum_fills: int
    maximum_drawdown: Decimal
    minimum_excess_return_over_buy_and_hold: Decimal
    maximum_data_gap_events: int
    maximum_conflict_events: int
    maximum_unplanned_pauses: int
    raw_sha256: str
    canonical_sha256: str
    pilot_plan_version: str = PILOT_PLAN_VERSION

    def document(self) -> dict[str, Any]:
        return {
            "evaluation_manifest_sha256": self.evaluation_manifest_sha256,
            "evaluation_manifest_uri": self.evaluation_manifest_uri,
            "maximum_conflict_events": self.maximum_conflict_events,
            "maximum_data_gap_events": self.maximum_data_gap_events,
            "maximum_drawdown": format(self.maximum_drawdown, "f"),
            "maximum_unplanned_pauses": self.maximum_unplanned_pauses,
            "minimum_calendar_days": self.minimum_calendar_days,
            "minimum_excess_return_over_buy_and_hold": format(
                self.minimum_excess_return_over_buy_and_hold, "f"
            ),
            "minimum_fills": self.minimum_fills,
            "minimum_processed_candles": self.minimum_processed_candles,
            "name": self.name,
            "pilot_plan_version": self.pilot_plan_version,
            "start_not_before": self.start_not_before,
        }

    def identity(self) -> dict[str, Any]:
        return {
            "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
            "canonical_plan_sha256": self.canonical_sha256,
            "evaluation_manifest_sha256": self.evaluation_manifest_sha256,
            "paper_engine_version": PAPER_ENGINE_VERSION,
            "paper_schema_version": PAPER_SCHEMA_VERSION,
            "pilot_plan_version": self.pilot_plan_version,
            "pilot_schema_version": PILOT_SCHEMA_VERSION,
            "raw_plan_sha256": self.raw_sha256,
            "thresholds": {
                key: value
                for key, value in self.document().items()
                if key.startswith(("minimum_", "maximum_"))
            },
        }

    @property
    def pilot_id(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.identity())).hexdigest()


@dataclass(frozen=True)
class Pilot:
    plan: PilotPlan
    session_id: str
    state: PilotState
    approved_by: str
    approval_note: str
    created_at: datetime
    local_development: bool
    finalized_at: datetime | None = None
    assessment: dict[str, Any] | None = None

    @property
    def pilot_id(self) -> str:
        return self.plan.pilot_id


def load_pilot_plan(
    body: bytes,
    *,
    expected_sha256: str,
    maximum_processed_candles: int,
    maximum_fills: int,
) -> PilotPlan:
    raw_sha256 = hashlib.sha256(body).hexdigest()
    if not SHA256_PATTERN.fullmatch(expected_sha256) or raw_sha256 != expected_sha256:
        raise InvalidPaperTrading("pilot plan SHA-256 digest does not match")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidPaperTrading("pilot plan is not strict UTF-8 JSON") from error
    if not isinstance(value, dict) or set(value) != PLAN_FIELDS:
        raise InvalidPaperTrading("pilot plan fields do not match the v1 contract")
    if value["pilot_plan_version"] != PILOT_PLAN_VERSION:
        raise InvalidPaperTrading("pilot plan version is unsupported")
    name = value["name"]
    if not isinstance(name, str) or not PILOT_NAME_PATTERN.fullmatch(name):
        raise InvalidPaperTrading("pilot name must be conservative lowercase kebab-case")
    evaluation_uri = value["evaluation_manifest_uri"]
    evaluation_digest = value["evaluation_manifest_sha256"]
    if not isinstance(evaluation_uri, str) or not evaluation_uri.strip():
        raise InvalidPaperTrading("evaluation manifest URI is empty")
    if not isinstance(evaluation_digest, str) or not SHA256_PATTERN.fullmatch(evaluation_digest):
        raise InvalidPaperTrading("evaluation manifest digest is invalid")
    try:
        start = parse_utc_minute(value["start_not_before"], "start_not_before")
    except (TypeError, ValueError) as error:
        raise InvalidPaperTrading(str(error)) from error
    drawdown = _decimal(value["maximum_drawdown"], "maximum_drawdown")
    if not Decimal(0) < drawdown <= Decimal(1):
        raise InvalidPaperTrading("maximum_drawdown must be in (0, 1]")
    excess = _decimal(
        value["minimum_excess_return_over_buy_and_hold"],
        "minimum_excess_return_over_buy_and_hold",
        allow_negative=True,
    )
    document = {
        **value,
        "maximum_drawdown": format(drawdown, "f"),
        "minimum_excess_return_over_buy_and_hold": format(excess, "f"),
        "start_not_before": start,
    }
    canonical_sha256 = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    return PilotPlan(
        name=name,
        evaluation_manifest_uri=evaluation_uri,
        evaluation_manifest_sha256=evaluation_digest,
        start_not_before=start.astimezone(UTC),
        minimum_calendar_days=_integer(
            value["minimum_calendar_days"], "minimum_calendar_days", minimum=7, maximum=90
        ),
        minimum_processed_candles=_integer(
            value["minimum_processed_candles"],
            "minimum_processed_candles",
            minimum=1,
            maximum=maximum_processed_candles,
        ),
        minimum_fills=_integer(
            value["minimum_fills"], "minimum_fills", minimum=0, maximum=maximum_fills
        ),
        maximum_drawdown=drawdown,
        minimum_excess_return_over_buy_and_hold=excess,
        maximum_data_gap_events=_integer(
            value["maximum_data_gap_events"],
            "maximum_data_gap_events",
            minimum=0,
            maximum=1_000_000,
        ),
        maximum_conflict_events=_integer(
            value["maximum_conflict_events"],
            "maximum_conflict_events",
            minimum=0,
            maximum=1_000_000,
        ),
        maximum_unplanned_pauses=_integer(
            value["maximum_unplanned_pauses"],
            "maximum_unplanned_pauses",
            minimum=0,
            maximum=1_000_000,
        ),
        raw_sha256=raw_sha256,
        canonical_sha256=canonical_sha256,
    )


def pilot_plan_from_document(document: dict[str, Any], raw_sha256: str) -> PilotPlan:
    body = canonical_json_bytes(document)
    # Stored plans are already canonical; this also reapplies every v1 constraint.
    stored = load_pilot_plan(
        body,
        expected_sha256=hashlib.sha256(body).hexdigest(),
        maximum_processed_candles=100_000_000,
        maximum_fills=100_000_000,
    )
    return PilotPlan(**{**stored.__dict__, "raw_sha256": raw_sha256})
