from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_trading_core.contracts import (
    SHA256_PATTERN,
    canonical_json_bytes,
    load_candle_snapshot,
)
from crypto_trading_core.paper import (
    PaperSettings,
    _bounds,
    create_paper_session,
    paper_session_status,
    process_paper_candles,
    set_paper_session_state,
)
from crypto_trading_core.paper_contracts import (
    PAPER_ENGINE_VERSION,
    PAPER_SCHEMA_VERSION,
    InvalidPaperTrading,
    PaperSessionState,
    validate_actor,
    validate_command_id,
    validate_reason,
)
from crypto_trading_core.paper_repository import PaperRepository
from crypto_trading_core.pilot_contracts import (
    ASSESSMENT_POLICY_VERSION,
    PILOT_ID_PATTERN,
    PILOT_SCHEMA_VERSION,
    Pilot,
    PilotPlan,
    PilotState,
    load_pilot_plan,
)
from crypto_trading_core.pilot_repository import PilotRepository
from crypto_trading_core.storage import ObjectStorage, child_uri


@dataclass(frozen=True)
class PilotSettings:
    paper: PaperSettings
    output_prefix: str
    maximum_plan_processed_candles: int
    maximum_plan_fills: int

    @classmethod
    def from_env(cls) -> PilotSettings:
        candles = int(os.getenv("PAPER_PILOT_MAXIMUM_PLAN_CANDLES", "1000000"))
        fills = int(os.getenv("PAPER_PILOT_MAXIMUM_PLAN_FILLS", "100000"))
        if candles <= 0 or fills <= 0:
            raise InvalidPaperTrading("pilot plan resource limits must be positive")
        return cls(
            paper=PaperSettings.from_env(),
            output_prefix=os.getenv(
                "PAPER_PILOT_OUTPUT_PREFIX",
                "s3a://crypto-data/analytics/paper_pilots/v1",
            ),
            maximum_plan_processed_candles=candles,
            maximum_plan_fills=fills,
        )


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _readback_publish(store: ObjectStorage, uri: str, body: bytes) -> None:
    created = store.try_write_bytes_append_only(uri, body, content_type="application/json")
    existing = store.read_bytes(uri)
    if existing != body:
        noun = "existing" if not created else "published"
        raise InvalidPaperTrading(f"{noun} pilot artifact conflicts with canonical content")


def _publish_manifested_json(
    store: ObjectStorage,
    *,
    artifact_uri: str,
    manifest_uri: str,
    document: dict[str, Any],
    kind: str,
    identity: dict[str, Any],
) -> dict[str, Any]:
    artifact = canonical_json_bytes(document)
    artifact_sha256 = hashlib.sha256(artifact).hexdigest()
    _readback_publish(store, artifact_uri, artifact)
    manifest = {
        "artifact": {
            "sha256": artifact_sha256,
            "uri": artifact_uri,
        },
        "identity": identity,
        "kind": kind,
        "manifest_uri": manifest_uri,
        "pilot_schema_version": PILOT_SCHEMA_VERSION,
        "status": "published",
    }
    manifest_body = canonical_json_bytes(manifest)
    _readback_publish(store, manifest_uri, manifest_body)
    return {
        "artifact_sha256": artifact_sha256,
        "artifact_uri": artifact_uri,
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(manifest_body).hexdigest(),
        "manifest_uri": manifest_uri,
    }


def validate_pilot_publication(
    manifest_uri: str,
    expected_sha256: str,
    *,
    store: ObjectStorage,
    expected_kind: str | None = None,
) -> dict[str, Any]:
    body = store.read_bytes(manifest_uri)
    digest = hashlib.sha256(body).hexdigest()
    if not SHA256_PATTERN.fullmatch(expected_sha256) or digest != expected_sha256:
        raise InvalidPaperTrading("pilot publication manifest digest does not match")
    try:
        manifest = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidPaperTrading("pilot publication manifest is invalid UTF-8 JSON") from error
    if not isinstance(manifest, dict) or set(manifest) != {
        "artifact",
        "identity",
        "kind",
        "manifest_uri",
        "pilot_schema_version",
        "status",
    }:
        raise InvalidPaperTrading("pilot publication manifest fields are invalid")
    if (
        manifest["status"] != "published"
        or manifest["pilot_schema_version"] != PILOT_SCHEMA_VERSION
        or manifest["manifest_uri"] != manifest_uri
        or (expected_kind is not None and manifest["kind"] != expected_kind)
    ):
        raise InvalidPaperTrading("pilot publication identity is invalid")
    artifact = manifest["artifact"]
    if not isinstance(artifact, dict) or set(artifact) != {"sha256", "uri"}:
        raise InvalidPaperTrading("pilot artifact reference is invalid")
    artifact_body = store.read_bytes(artifact["uri"])
    if hashlib.sha256(artifact_body).hexdigest() != artifact["sha256"]:
        raise InvalidPaperTrading("pilot artifact digest does not match")
    try:
        document = json.loads(artifact_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidPaperTrading("pilot artifact is invalid JSON") from error
    if not isinstance(document, dict):
        raise InvalidPaperTrading("pilot artifact must be a JSON object")
    if document.get("pilot_id") != manifest["identity"].get("pilot_id"):
        raise InvalidPaperTrading("pilot artifact identity does not match its manifest")
    required_fields = {
        "paper_pilot_registration": {
            "approval",
            "created_at",
            "evaluation_manifest_sha256",
            "evaluation_manifest_uri",
            "execution_mode",
            "pilot_id",
            "plan",
            "plan_canonical_sha256",
            "plan_raw_artifact",
            "plan_raw_sha256",
            "session_id",
            "state",
        },
        "paper_pilot_snapshot": {
            "as_of",
            "assessment_policy_version",
            "criteria",
            "evaluation_manifest_sha256",
            "execution_mode",
            "interim_only",
            "metrics",
            "observation_start",
            "paper_engine_version",
            "paper_schema_version",
            "pilot_id",
            "pilot_state",
            "plan_canonical_sha256",
            "plan_raw_sha256",
            "selection_manifest_sha256",
            "session_id",
        },
        "paper_pilot_assessment": {
            "assessment_policy_version",
            "criteria",
            "eligibility",
            "execution_mode",
            "finalized_at",
            "live_trading_enabled",
            "metrics",
            "operator",
            "pilot_id",
            "session_id",
            "terminal_stop",
            "verdict",
        },
    }
    expected_fields = required_fields.get(str(manifest["kind"]))
    if expected_fields is None or set(document) != expected_fields:
        raise InvalidPaperTrading("pilot artifact fields do not match its contract")
    if document.get("execution_mode") != "paper_simulation":
        raise InvalidPaperTrading("pilot artifact is not simulation-only")
    if manifest["kind"] == "paper_pilot_registration":
        plan_artifact = document.get("plan_raw_artifact")
        if not isinstance(plan_artifact, dict) or set(plan_artifact) != {"sha256", "uri"}:
            raise InvalidPaperTrading("raw pilot plan reference is invalid")
        raw_plan = store.read_bytes(plan_artifact["uri"])
        plan = load_pilot_plan(
            raw_plan,
            expected_sha256=plan_artifact["sha256"],
            maximum_processed_candles=100_000_000,
            maximum_fills=100_000_000,
        )
        stored_plan = document.get("plan")
        if (
            not isinstance(stored_plan, dict)
            or canonical_json_bytes(plan.document()) != canonical_json_bytes(stored_plan)
            or plan.canonical_sha256 != document.get("plan_canonical_sha256")
            or plan.raw_sha256 != document.get("plan_raw_sha256")
        ):
            raise InvalidPaperTrading("published pilot plan identity does not reproduce")
    if manifest["kind"] == "paper_pilot_assessment" and (
        document.get("live_trading_enabled") is not False
        or document.get("verdict") not in {"pass", "fail", "inconclusive"}
        or document.get("assessment_policy_version") != ASSESSMENT_POLICY_VERSION
    ):
        raise InvalidPaperTrading("pilot assessment safety identity is invalid")
    return {
        "artifact_sha256": artifact["sha256"],
        "kind": manifest["kind"],
        "manifest_sha256": digest,
        "pilot_id": document["pilot_id"],
        "status": "valid",
    }


def start_paper_pilot(
    plan_uri: str,
    plan_sha256: str,
    *,
    approved_by: str,
    approval_note: str,
    settings: PilotSettings,
    paper_repository: PaperRepository,
    pilot_repository: PilotRepository,
    local_development: bool,
    store: ObjectStorage | None = None,
    command_id: str | None = None,
    now: datetime | None = None,
    evaluation_validator=None,
    range_loader=None,
) -> dict[str, Any]:
    storage = store or ObjectStorage(settings.paper.experiment.backtest.storage)
    body = storage.read_bytes(plan_uri)
    plan = load_pilot_plan(
        body,
        expected_sha256=plan_sha256,
        maximum_processed_candles=settings.maximum_plan_processed_candles,
        maximum_fills=settings.maximum_plan_fills,
    )
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    if plan.start_not_before < timestamp:
        raise InvalidPaperTrading("pilot must be registered no later than start_not_before")
    actor = validate_actor(approved_by)
    note = validate_reason(approval_note)
    actual_command = validate_command_id(command_id or f"register-{plan.pilot_id[:24]}")
    kwargs: dict[str, Any] = {}
    if evaluation_validator is not None:
        kwargs["validator"] = evaluation_validator
    if range_loader is not None:
        kwargs["range_loader"] = range_loader
    session_report = create_paper_session(
        plan.evaluation_manifest_uri,
        plan.evaluation_manifest_sha256,
        approved_by=actor,
        approval_note=note,
        maximum_drawdown=plan.maximum_drawdown,
        settings=settings.paper,
        repository=paper_repository,
        local_development=local_development,
        store=storage,
        command_id=f"pilot-session-{plan.pilot_id[:24]}",
        now=timestamp,
        pilot_id=plan.pilot_id,
        forward_start=plan.start_not_before,
        **kwargs,
    )
    session = paper_repository.get_session(str(session_report["session_id"]))
    if session.spec.maximum_drawdown > plan.maximum_drawdown:
        raise InvalidPaperTrading("paper-session drawdown limit is weaker than the pilot plan")
    if session.spec.evaluation_manifest_sha256 != plan.evaluation_manifest_sha256:
        raise InvalidPaperTrading("paper session evaluation lineage does not match the pilot")
    if session.spec.pilot_id != plan.pilot_id:
        raise InvalidPaperTrading("paper session identity does not match the pilot")
    if session.spec.forward_start != plan.start_not_before:
        raise InvalidPaperTrading("paper session forward boundary does not match the pilot")
    if plan.start_not_before < session.spec.test_end:
        raise InvalidPaperTrading("pilot start precedes the sealed evaluation boundary")
    if session.last_candle_time is not None and session.last_candle_time >= plan.start_not_before:
        raise InvalidPaperTrading("pilot registration followed forward candle processing")
    pilot = Pilot(
        plan=plan,
        session_id=session.session_id,
        state=PilotState.REGISTERED,
        approved_by=actor,
        approval_note=note,
        created_at=timestamp,
        local_development=local_development,
    )
    payload_digest = _digest(
        {
            "action": "register",
            "approval_note": note,
            "approved_by": actor,
            "local_development": local_development,
            "pilot_id": pilot.pilot_id,
            "session_id": pilot.session_id,
        }
    )
    stored, created = pilot_repository.create_pilot(
        pilot, command_id=actual_command, payload_digest=payload_digest
    )
    base = child_uri(settings.output_prefix, "pilots", pilot.pilot_id)
    plan_artifact_uri = child_uri(base, "plan.json")
    _readback_publish(storage, plan_artifact_uri, body)
    publication = _publish_manifested_json(
        storage,
        artifact_uri=child_uri(base, "pilot.json"),
        manifest_uri=child_uri(base, "manifest.json"),
        document={
            "approval": {"note": note, "operator": actor},
            "created_at": stored.created_at,
            "evaluation_manifest_sha256": plan.evaluation_manifest_sha256,
            "evaluation_manifest_uri": plan.evaluation_manifest_uri,
            "execution_mode": "paper_simulation",
            "pilot_id": pilot.pilot_id,
            "plan": plan.document(),
            "plan_canonical_sha256": plan.canonical_sha256,
            "plan_raw_artifact": {
                "sha256": plan.raw_sha256,
                "uri": plan_artifact_uri,
            },
            "plan_raw_sha256": plan.raw_sha256,
            "session_id": pilot.session_id,
            "state": PilotState.REGISTERED.value,
        },
        kind="paper_pilot_registration",
        identity={"pilot_id": pilot.pilot_id, "plan_raw_sha256": plan.raw_sha256},
    )
    return {
        "execution_mode": "paper_simulation",
        "manifest_sha256": publication["manifest_sha256"],
        "manifest_uri": publication["manifest_uri"],
        "pilot_id": pilot.pilot_id,
        "session_id": pilot.session_id,
        "state": stored.state.value,
        "status": "created" if created else "resolved_existing_pilot",
    }


def run_paper_pilot_cycle(
    pilot_id: str,
    candle_manifest_uri: str,
    candle_manifest_sha256: str,
    *,
    command_id: str,
    settings: PilotSettings,
    paper_repository: PaperRepository,
    pilot_repository: PilotRepository,
    store: ObjectStorage | None = None,
    now: datetime | None = None,
    range_loader=None,
) -> dict[str, object]:
    if not PILOT_ID_PATTERN.fullmatch(pilot_id):
        raise InvalidPaperTrading("paper pilot ID is invalid")
    command_id = validate_command_id(command_id)
    pilot = pilot_repository.get_pilot(pilot_id)
    storage = store or ObjectStorage(settings.paper.experiment.backtest.storage)
    body = storage.read_bytes(candle_manifest_uri)
    snapshot = load_candle_snapshot(
        body,
        manifest_uri=candle_manifest_uri,
        expected_sha256=candle_manifest_sha256,
        allowed_manifest_prefix=settings.paper.candle_manifest_prefix,
        local_development=pilot.local_development,
    )
    first, last = _bounds(snapshot)
    session = paper_repository.get_session(pilot.session_id)
    if session.last_candle_time is None:
        warmup_start = pilot.plan.start_not_before - timedelta(minutes=session.spec.slow_period - 1)
        if first > warmup_start:
            raise InvalidPaperTrading("first candle publication does not cover forward warm-up")
        if last <= pilot.plan.start_not_before:
            raise InvalidPaperTrading("candle publication has no forward pilot candles")
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    if timestamp < pilot.plan.start_not_before:
        raise InvalidPaperTrading("paper pilot has not reached start_not_before")
    payload_digest = _digest(
        {
            "action": "cycle",
            "manifest_sha256": candle_manifest_sha256,
            "manifest_uri": candle_manifest_uri,
            "pilot_id": pilot_id,
            "snapshot_key": snapshot.snapshot_key,
        }
    )

    def runner() -> dict[str, object]:
        kwargs: dict[str, Any] = {}
        if range_loader is not None:
            kwargs["range_loader"] = range_loader
        return process_paper_candles(
            pilot.session_id,
            candle_manifest_uri,
            candle_manifest_sha256,
            settings=settings.paper,
            repository=paper_repository,
            local_development=pilot.local_development,
            store=storage,
            command_id=f"pilot-{command_id}",
            now=timestamp,
            **kwargs,
        )

    report = pilot_repository.run_cycle(
        pilot_id,
        command_id=command_id,
        payload_digest=payload_digest,
        manifest_uri=candle_manifest_uri,
        manifest_sha256=candle_manifest_sha256,
        now=timestamp,
        runner=runner,
    )
    return {**report, "pilot_id": pilot_id}


def _criteria(plan: PilotPlan, metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    values = {
        "calendar_days": (metrics["calendar_days"], plan.minimum_calendar_days, ">="),
        "processed_candles": (metrics["processed_candles"], plan.minimum_processed_candles, ">="),
        "fills": (metrics["fills"], plan.minimum_fills, ">="),
        "maximum_drawdown": (metrics["maximum_drawdown"], plan.maximum_drawdown, "<="),
        "excess_return_over_buy_and_hold": (
            metrics["excess_return_over_buy_and_hold"],
            plan.minimum_excess_return_over_buy_and_hold,
            ">=",
        ),
        "data_gap_events": (metrics["data_gap_events"], plan.maximum_data_gap_events, "<="),
        "conflict_events": (metrics["conflict_events"], plan.maximum_conflict_events, "<="),
        "unplanned_pauses": (metrics["unplanned_pauses"], plan.maximum_unplanned_pauses, "<="),
    }
    return {
        name: {
            "actual": actual,
            "operator": operator,
            "passes": actual >= threshold if operator == ">=" else actual <= threshold,
            "threshold": threshold,
        }
        for name, (actual, threshold, operator) in values.items()
    }


def pilot_snapshot_document(
    pilot: Pilot,
    *,
    as_of: datetime,
    paper_repository: PaperRepository,
    pilot_repository: PilotRepository,
) -> dict[str, Any]:
    if (
        as_of.tzinfo is None
        or as_of.utcoffset() != timedelta(0)
        or as_of.second
        or as_of.microsecond
    ):
        raise InvalidPaperTrading("snapshot as-of must be an aligned UTC minute")
    as_of = as_of.astimezone(UTC)
    if as_of < pilot.plan.start_not_before:
        raise InvalidPaperTrading("snapshot as-of precedes the pilot observation start")
    session = paper_repository.get_session(pilot.session_id)
    if session.updated_at > as_of:
        raise InvalidPaperTrading("snapshot as-of precedes the latest paper-session mutation")
    status = paper_session_status(pilot.session_id, paper_repository)
    operations = pilot_repository.evidence(pilot.pilot_id, as_of)
    expected = int((as_of - pilot.plan.start_not_before) / timedelta(minutes=1))
    processed = int(status["processed_candles"])
    strategy_return = Decimal(status["net_percentage_return"])
    baseline_return = Decimal(status["baseline"]["percentage_return"])
    metrics = {
        **operations,
        "calendar_days": int((as_of.date() - pilot.plan.start_not_before.date()).days),
        "cash": status["cash"],
        "discovered_candles": operations["discovered_candles"],
        "expected_candles": expected,
        "excess_return_over_buy_and_hold": strategy_return - baseline_return,
        "fills": int(status["trades"]["buys"]) + int(status["trades"]["sells"]),
        "marked_equity": status["marked_equity"],
        "maximum_drawdown": status["maximum_drawdown"],
        "missing_candles": max(0, expected - processed),
        "pending_target": status["pending_target"],
        "position_quantity": status["position_quantity"],
        "processed_candles": processed,
        "rejected_candles": operations["rejected_candles"],
        "strategy_return": strategy_return,
        "total_fees": status["total_fees"],
        "forward_buy_and_hold_return": baseline_return,
    }
    return {
        "as_of": as_of,
        "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
        "criteria": _criteria(pilot.plan, metrics),
        "evaluation_manifest_sha256": pilot.plan.evaluation_manifest_sha256,
        "execution_mode": "paper_simulation",
        "interim_only": not pilot.state.terminal,
        "metrics": metrics,
        "observation_start": pilot.plan.start_not_before,
        "paper_engine_version": PAPER_ENGINE_VERSION,
        "paper_schema_version": PAPER_SCHEMA_VERSION,
        "pilot_id": pilot.pilot_id,
        "pilot_state": pilot.state.value,
        "plan_canonical_sha256": pilot.plan.canonical_sha256,
        "plan_raw_sha256": pilot.plan.raw_sha256,
        "selection_manifest_sha256": status["selection_manifest_sha256"],
        "session_id": pilot.session_id,
    }


def report_paper_pilot(
    pilot_id: str,
    as_of: datetime,
    *,
    settings: PilotSettings,
    paper_repository: PaperRepository,
    pilot_repository: PilotRepository,
    store: ObjectStorage | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    pilot = pilot_repository.get_pilot(pilot_id)
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    if as_of > timestamp:
        raise InvalidPaperTrading("snapshot as-of cannot be in the future")
    document = pilot_snapshot_document(
        pilot,
        as_of=as_of,
        paper_repository=paper_repository,
        pilot_repository=pilot_repository,
    )
    storage = store or ObjectStorage(settings.paper.experiment.backtest.storage)
    date = as_of.astimezone(UTC).date().isoformat()
    base = child_uri(settings.output_prefix, "snapshots", pilot_id, f"event_date={date}")
    publication = _publish_manifested_json(
        storage,
        artifact_uri=child_uri(base, "snapshot.json"),
        manifest_uri=child_uri(base, "manifest.json"),
        document=document,
        kind="paper_pilot_snapshot",
        identity={"event_date": date, "pilot_id": pilot_id},
    )
    record = {
        **publication,
        "as_of": as_of,
        "created_at": timestamp,
        "document": document,
    }
    _, created = pilot_repository.store_snapshot(pilot_id, date, record)
    return {
        "event_date": date,
        "manifest_sha256": publication["manifest_sha256"],
        "manifest_uri": publication["manifest_uri"],
        "pilot_id": pilot_id,
        "status": "published" if created else "resolved_existing_snapshot",
    }


def _verify_evidence(
    pilot: Pilot,
    *,
    pilot_repository: PilotRepository,
    store: ObjectStorage,
) -> bool:
    try:
        for cycle in pilot_repository.cycle_records(pilot.pilot_id):
            body = store.try_read_bytes(cycle["manifest_uri"])
            if body is None:
                return False
            if hashlib.sha256(body).hexdigest() != cycle["manifest_sha256"]:
                raise InvalidPaperTrading("referenced candle manifest digest changed")
        for snapshot in pilot_repository.snapshot_records(pilot.pilot_id):
            manifest_body = store.try_read_bytes(snapshot["manifest_uri"])
            if manifest_body is None:
                return False
            try:
                manifest = json.loads(manifest_body)
                artifact_uri = manifest["artifact"]["uri"]
            except (KeyError, TypeError, json.JSONDecodeError):
                return False
            if not isinstance(artifact_uri, str) or store.try_read_bytes(artifact_uri) is None:
                return False
            validate_pilot_publication(
                snapshot["manifest_uri"],
                snapshot["manifest_sha256"],
                store=store,
                expected_kind="paper_pilot_snapshot",
            )
    except (InvalidPaperTrading, OSError):
        return False
    return True


def finalize_paper_pilot(
    pilot_id: str,
    *,
    reviewed_by: str,
    review_note: str,
    settings: PilotSettings,
    paper_repository: PaperRepository,
    pilot_repository: PilotRepository,
    store: ObjectStorage | None = None,
    command_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    pilot = pilot_repository.get_pilot(pilot_id)
    actor = validate_actor(reviewed_by)
    note = validate_reason(review_note)
    storage = store or ObjectStorage(settings.paper.experiment.backtest.storage)
    if pilot.state.terminal:
        if pilot.assessment is None:
            raise InvalidPaperTrading("terminal pilot has no assessment")
        if pilot.assessment.get("operator") != {"note": note, "reviewed_by": actor}:
            raise InvalidPaperTrading("terminal pilot assessment arguments changed")
        manifest_uri = child_uri(settings.output_prefix, "assessments", pilot_id, "manifest.json")
        manifest_sha256 = hashlib.sha256(storage.read_bytes(manifest_uri)).hexdigest()
        return {
            **pilot.assessment,
            "manifest_sha256": manifest_sha256,
            "manifest_uri": manifest_uri,
            "status": "resolved_existing_assessment",
        }
    timestamp = (now or datetime.now(UTC)).astimezone(UTC).replace(second=0, microsecond=0)
    evidence_available = _verify_evidence(pilot, pilot_repository=pilot_repository, store=storage)
    snapshot = pilot_snapshot_document(
        pilot,
        as_of=timestamp,
        paper_repository=paper_repository,
        pilot_repository=pilot_repository,
    )
    criteria = snapshot["criteria"]
    criteria["immutable_evidence"] = {
        "actual": evidence_available,
        "operator": "==",
        "passes": evidence_available,
        "threshold": True,
    }
    paper_status = paper_session_status(pilot.session_id, paper_repository)
    definitive = {"maximum_drawdown", "data_gap_events", "conflict_events", "unplanned_pauses"}
    terminal_stop = (
        paper_status["state"] == PaperSessionState.STOPPED.value
        and paper_status["last_state_reason"] != "pilot_finalized"
    )
    hard_failure = terminal_stop or any(not criteria[name]["passes"] for name in definitive)
    minimum_end = pilot.plan.start_not_before + timedelta(days=pilot.plan.minimum_calendar_days)
    if timestamp < minimum_end and not hard_failure:
        raise InvalidPaperTrading("pilot observation window has not reached its minimum duration")
    incomplete = (
        any(
            not criteria[name]["passes"] for name in ("calendar_days", "processed_candles", "fills")
        )
        or not evidence_available
    )
    if hard_failure or (
        not incomplete and not criteria["excess_return_over_buy_and_hold"]["passes"]
    ):
        verdict = "fail"
        state = PilotState.FAILED
    elif incomplete:
        verdict = "inconclusive"
        state = PilotState.INCONCLUSIVE
    else:
        verdict = "pass"
        state = PilotState.COMPLETED
    assessment = {
        "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
        "criteria": criteria,
        "eligibility": "eligible_for_execution_design_review"
        if verdict == "pass"
        else "not_eligible",
        "execution_mode": "paper_simulation",
        "finalized_at": timestamp,
        "live_trading_enabled": False,
        "metrics": snapshot["metrics"],
        "operator": {"note": note, "reviewed_by": actor},
        "pilot_id": pilot_id,
        "session_id": pilot.session_id,
        "terminal_stop": terminal_stop,
        "verdict": verdict,
    }
    base = child_uri(settings.output_prefix, "assessments", pilot_id)
    publication = _publish_manifested_json(
        storage,
        artifact_uri=child_uri(base, "assessment.json"),
        manifest_uri=child_uri(base, "manifest.json"),
        document=assessment,
        kind="paper_pilot_assessment",
        identity={
            "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
            "pilot_id": pilot_id,
            "verdict": verdict,
        },
    )
    actual_command = validate_command_id(command_id or f"finalize-{pilot_id[:24]}")
    payload_digest = _digest(
        {
            "action": "finalize",
            "assessment_sha256": publication["artifact_sha256"],
            "operator": assessment["operator"],
            "pilot_id": pilot_id,
        }
    )
    if paper_status["state"] != PaperSessionState.STOPPED.value:
        set_paper_session_state(
            pilot.session_id,
            PaperSessionState.STOPPED,
            actor=actor,
            reason="pilot_finalized",
            repository=paper_repository,
            command_id=f"pilot-stop-{pilot_id[:24]}",
            now=timestamp,
        )
    _, created = pilot_repository.finalize(
        pilot_id,
        command_id=actual_command,
        payload_digest=payload_digest,
        state=state,
        assessment=assessment,
        now=timestamp,
    )
    return {
        **assessment,
        "manifest_sha256": publication["manifest_sha256"],
        "manifest_uri": publication["manifest_uri"],
        "status": "finalized" if created else "resolved_existing_assessment",
    }
