from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from threading import RLock
from typing import Any, Protocol

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import MemoryPaperRepository, PostgresPaperRepository
from crypto_trading_core.pilot_contracts import Pilot, PilotState, pilot_plan_from_document

CycleRunner = Callable[[], dict[str, object]]


class PilotRepository(Protocol):
    def migrate(self) -> None: ...
    def create_pilot(
        self, pilot: Pilot, *, command_id: str, payload_digest: str
    ) -> tuple[Pilot, bool]: ...
    def get_pilot(self, pilot_id: str) -> Pilot: ...
    def run_cycle(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        manifest_uri: str,
        manifest_sha256: str,
        now: datetime,
        runner: CycleRunner,
    ) -> dict[str, object]: ...
    def evidence(self, pilot_id: str, as_of: datetime) -> dict[str, Any]: ...
    def store_snapshot(
        self, pilot_id: str, event_date: str, record: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]: ...
    def snapshot_records(self, pilot_id: str) -> list[dict[str, Any]]: ...
    def cycle_records(self, pilot_id: str) -> list[dict[str, Any]]: ...
    def finalize(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        state: PilotState,
        assessment: dict[str, Any],
        now: datetime,
    ) -> tuple[Pilot, bool]: ...


def _pilot_document(pilot: Pilot) -> dict[str, Any]:
    return {
        "approved_by": pilot.approved_by,
        "approval_note": pilot.approval_note,
        "assessment": pilot.assessment,
        "created_at": pilot.created_at,
        "finalized_at": pilot.finalized_at,
        "local_development": pilot.local_development,
        "pilot_id": pilot.pilot_id,
        "plan": pilot.plan.document(),
        "raw_plan_sha256": pilot.plan.raw_sha256,
        "session_id": pilot.session_id,
        "state": pilot.state.value,
    }


def _json_document(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(canonical_json_bytes(value))


def _pilot_from_row(row: Mapping[str, Any]) -> Pilot:
    plan = pilot_plan_from_document(dict(row["plan"]), row["raw_plan_sha256"])
    if plan.canonical_sha256 != row["canonical_plan_sha256"]:
        raise InvalidPaperTrading("stored pilot plan digest does not match")
    pilot = Pilot(
        plan=plan,
        session_id=row["session_id"],
        state=PilotState(row["state"]),
        approved_by=row["approved_by"],
        approval_note=row["approval_note"],
        created_at=row["created_at"].astimezone(UTC),
        local_development=row["local_development"],
        finalized_at=(row["finalized_at"].astimezone(UTC) if row["finalized_at"] else None),
        assessment=dict(row["assessment"]) if row["assessment"] else None,
    )
    if pilot.pilot_id != row["pilot_id"]:
        raise InvalidPaperTrading("stored pilot identity does not match")
    return pilot


def _pause_summary(events: list[dict[str, Any]], as_of: datetime) -> dict[str, Any]:
    planned_seconds = 0
    unplanned_seconds = 0
    planned_reasons: list[str] = []
    unplanned_reasons: list[str] = []
    open_pause: tuple[datetime, bool] | None = None
    gaps = 0
    conflicts = 0
    unplanned_count = 0
    for event in events:
        reason = str(event["reason"])
        state = str(event["resulting_state"])
        action = str(event["action"])
        created = event["created_at"].astimezone(UTC)
        if reason == "candle_sequence_gap":
            gaps += 1
        if reason in {"processed_candle_conflict", "candle_identity_changed"}:
            conflicts += 1
        if state == "paused" and open_pause is None:
            planned = action == "set_state"
            open_pause = (created, planned)
            if planned:
                planned_reasons.append(reason)
            else:
                unplanned_count += 1
                unplanned_reasons.append(reason)
        elif open_pause is not None and state != "paused":
            seconds = max(0, int((min(created, as_of) - open_pause[0]).total_seconds()))
            if open_pause[1]:
                planned_seconds += seconds
            else:
                unplanned_seconds += seconds
            open_pause = None
    if open_pause is not None:
        seconds = max(0, int((as_of - open_pause[0]).total_seconds()))
        if open_pause[1]:
            planned_seconds += seconds
        else:
            unplanned_seconds += seconds
    return {
        "conflict_events": conflicts,
        "data_gap_events": gaps,
        "planned_pause_seconds": planned_seconds,
        "planned_pause_reasons": planned_reasons,
        "unplanned_pause_seconds": unplanned_seconds,
        "unplanned_pause_reasons": unplanned_reasons,
        "unplanned_pauses": unplanned_count,
    }


class MemoryPilotRepository:
    def __init__(self, paper: MemoryPaperRepository) -> None:
        self.paper = paper
        self.pilots: dict[str, Pilot] = {}
        self.commands: dict[str, tuple[str, str, dict[str, Any]]] = {}
        self.cycles: dict[str, list[dict[str, Any]]] = {}
        self.snapshots: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = RLock()

    def migrate(self) -> None:
        return None

    def create_pilot(
        self, pilot: Pilot, *, command_id: str, payload_digest: str
    ) -> tuple[Pilot, bool]:
        with self._lock:
            command = self.commands.get(command_id)
            if command and (command[0] != pilot.pilot_id or command[1] != payload_digest):
                raise InvalidPaperTrading("pilot command ID was reused with different arguments")
            existing = self.pilots.get(pilot.pilot_id)
            if existing:
                if (
                    existing.session_id != pilot.session_id
                    or existing.plan.identity() != pilot.plan.identity()
                    or existing.approved_by != pilot.approved_by
                    or existing.approval_note != pilot.approval_note
                    or existing.local_development != pilot.local_development
                ):
                    raise InvalidPaperTrading("pilot registration conflicts with stored approval")
                return existing, False
            self.pilots[pilot.pilot_id] = pilot
            self.cycles[pilot.pilot_id] = []
            self.commands[command_id] = (pilot.pilot_id, payload_digest, _pilot_document(pilot))
            return pilot, True

    def get_pilot(self, pilot_id: str) -> Pilot:
        try:
            return self.pilots[pilot_id]
        except KeyError as error:
            raise InvalidPaperTrading("paper pilot does not exist") from error

    def run_cycle(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        manifest_uri: str,
        manifest_sha256: str,
        now: datetime,
        runner: CycleRunner,
    ) -> dict[str, object]:
        with self._lock:
            command = self.commands.get(command_id)
            if command:
                if command[0] != pilot_id or command[1] != payload_digest:
                    raise InvalidPaperTrading(
                        "pilot command ID was reused with different arguments"
                    )
                if command[2].get("status") == "failed":
                    raise InvalidPaperTrading("paper pilot cycle processing was rejected")
                return {**command[2], "status": "resolved_existing_command"}
            pilot = self.get_pilot(pilot_id)
            if pilot.state.terminal:
                raise InvalidPaperTrading("terminal paper pilot cannot process candles")
            try:
                report = dict(runner())
            except (InvalidPaperTrading, OSError, ValueError) as error:
                report = {
                    "failure_reason": "cycle_processing_rejected",
                    "status": "failed",
                }
                record = {
                    "command_id": command_id,
                    "completed_at": now,
                    "manifest_sha256": manifest_sha256,
                    "manifest_uri": manifest_uri,
                    "result": report,
                    "started_at": now,
                    "success": False,
                }
                self.cycles[pilot_id].append(record)
                self.commands[command_id] = (pilot_id, payload_digest, report)
                raise InvalidPaperTrading("paper pilot cycle processing was rejected") from error
            record = {
                "command_id": command_id,
                "completed_at": now,
                "manifest_sha256": manifest_sha256,
                "manifest_uri": manifest_uri,
                "result": report,
                "started_at": now,
                "success": True,
            }
            self.cycles[pilot_id].append(record)
            if pilot.state is PilotState.REGISTERED:
                self.pilots[pilot_id] = replace(pilot, state=PilotState.RUNNING)
            self.commands[command_id] = (pilot_id, payload_digest, report)
            return report

    def evidence(self, pilot_id: str, as_of: datetime) -> dict[str, Any]:
        pilot = self.get_pilot(pilot_id)
        cycles = [row for row in self.cycles[pilot_id] if row["completed_at"] <= as_of]
        events = [
            {**event, "created_at": event.get("created_at", pilot.created_at)}
            for event in self.paper.events.get(pilot.session_id, [])
        ]
        result = _pause_summary(events, as_of)
        result.update(
            {
                "cycles_failed": sum(not row["success"] for row in cycles),
                "cycles_succeeded": sum(row["success"] for row in cycles),
                "discovered_candles": sum(
                    int(row["result"].get("discovered", 0)) for row in cycles
                ),
                "latest_successful_cycle_at": max(
                    (row["completed_at"] for row in cycles if row["success"]), default=None
                ),
                "rejected_candles": sum(int(row["result"].get("rejected", 0)) for row in cycles),
            }
        )
        return result

    def store_snapshot(
        self, pilot_id: str, event_date: str, record: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        with self._lock:
            key = (pilot_id, event_date)
            existing = self.snapshots.get(key)
            if existing:
                if existing["manifest_sha256"] != record["manifest_sha256"]:
                    raise InvalidPaperTrading("daily pilot snapshot already has different content")
                return existing, False
            self.snapshots[key] = record
            return record, True

    def snapshot_records(self, pilot_id: str) -> list[dict[str, Any]]:
        return [
            row for (stored_id, _), row in sorted(self.snapshots.items()) if stored_id == pilot_id
        ]

    def cycle_records(self, pilot_id: str) -> list[dict[str, Any]]:
        self.get_pilot(pilot_id)
        return list(self.cycles[pilot_id])

    def finalize(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        state: PilotState,
        assessment: dict[str, Any],
        now: datetime,
    ) -> tuple[Pilot, bool]:
        with self._lock:
            command = self.commands.get(command_id)
            if command:
                if command[0] != pilot_id or command[1] != payload_digest:
                    raise InvalidPaperTrading(
                        "pilot command ID was reused with different arguments"
                    )
                return self.get_pilot(pilot_id), False
            pilot = self.get_pilot(pilot_id)
            if pilot.state.terminal:
                if pilot.assessment == assessment:
                    return pilot, False
                raise InvalidPaperTrading("paper pilot is already terminal")
            updated = replace(pilot, state=state, finalized_at=now, assessment=assessment)
            self.pilots[pilot_id] = updated
            self.commands[command_id] = (pilot_id, payload_digest, assessment)
            return updated, True


class PostgresPilotRepository:
    def __init__(self, paper: PostgresPaperRepository) -> None:
        self.paper = paper

    def migrate(self) -> None:
        self.paper.migrate()

    def _connect(self):
        return self.paper._connect()

    def _set_timeout(self, connection: Any) -> None:
        self.paper._set_timeout(connection)

    @staticmethod
    def _jsonb():
        return PostgresPaperRepository._psycopg()[2]

    def create_pilot(
        self, pilot: Pilot, *, command_id: str, payload_digest: str
    ) -> tuple[Pilot, bool]:
        Jsonb = self._jsonb()
        try:
            with self._connect() as connection:
                self._set_timeout(connection)
                previous = connection.execute(
                    "SELECT pilot_id, payload_digest FROM paper_pilot_commands WHERE command_id=%s",
                    [command_id],
                ).fetchone()
                if previous and (
                    previous["pilot_id"] != pilot.pilot_id
                    or previous["payload_digest"] != payload_digest
                ):
                    raise InvalidPaperTrading(
                        "pilot command ID was reused with different arguments"
                    )
                inserted = connection.execute(
                    """INSERT INTO paper_pilots
                    (pilot_id,session_id,state,plan,raw_plan_sha256,canonical_plan_sha256,
                     approved_by,approval_note,local_development,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pilot_id) DO NOTHING RETURNING pilot_id""",
                    [
                        pilot.pilot_id,
                        pilot.session_id,
                        pilot.state.value,
                        Jsonb(_json_document(pilot.plan.document())),
                        pilot.plan.raw_sha256,
                        pilot.plan.canonical_sha256,
                        pilot.approved_by,
                        pilot.approval_note,
                        pilot.local_development,
                        pilot.created_at,
                        pilot.created_at,
                    ],
                ).fetchone()
                row = connection.execute(
                    "SELECT * FROM paper_pilots WHERE pilot_id=%s", [pilot.pilot_id]
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("pilot registration did not persist")
                stored = _pilot_from_row(row)
                if (
                    stored.session_id != pilot.session_id
                    or stored.plan.identity() != pilot.plan.identity()
                    or stored.approved_by != pilot.approved_by
                    or stored.approval_note != pilot.approval_note
                    or stored.local_development != pilot.local_development
                ):
                    raise InvalidPaperTrading("pilot identity conflicts with stored state")
                if inserted and not previous:
                    connection.execute(
                        "INSERT INTO paper_pilot_commands VALUES (%s,%s,'register',%s,%s,%s)",
                        [
                            command_id,
                            pilot.pilot_id,
                            payload_digest,
                            Jsonb(_json_document(_pilot_document(pilot))),
                            pilot.created_at,
                        ],
                    )
                return stored, inserted is not None
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper pilot registration failed") from error

    def get_pilot(self, pilot_id: str) -> Pilot:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT * FROM paper_pilots WHERE pilot_id=%s", [pilot_id]
                ).fetchone()
            if row is None:
                raise InvalidPaperTrading("paper pilot does not exist")
            return _pilot_from_row(row)
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper pilot lookup failed") from error

    def run_cycle(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        manifest_uri: str,
        manifest_sha256: str,
        now: datetime,
        runner: CycleRunner,
    ) -> dict[str, object]:
        Jsonb = self._jsonb()
        try:
            failure: Exception | None = None
            with self._connect() as connection:
                self._set_timeout(connection)
                row = connection.execute(
                    "SELECT * FROM paper_pilots WHERE pilot_id=%s FOR UPDATE", [pilot_id]
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("paper pilot does not exist")
                pilot = _pilot_from_row(row)
                previous = connection.execute(
                    "SELECT pilot_id,payload_digest,result FROM paper_pilot_commands WHERE command_id=%s",
                    [command_id],
                ).fetchone()
                if previous:
                    if (
                        previous["pilot_id"] != pilot_id
                        or previous["payload_digest"] != payload_digest
                    ):
                        raise InvalidPaperTrading(
                            "pilot command ID was reused with different arguments"
                        )
                    if previous["result"].get("status") == "failed":
                        raise InvalidPaperTrading("paper pilot cycle processing was rejected")
                    return {**dict(previous["result"]), "status": "resolved_existing_command"}
                if pilot.state.terminal:
                    raise InvalidPaperTrading("terminal paper pilot cannot process candles")
                try:
                    report = dict(runner())
                    success = True
                except (InvalidPaperTrading, OSError, ValueError) as error:
                    failure = error
                    success = False
                    report = {
                        "failure_reason": "cycle_processing_rejected",
                        "status": "failed",
                    }
                connection.execute(
                    """INSERT INTO paper_pilot_cycles
                    (pilot_id,command_id,payload_digest,manifest_uri,manifest_sha256,
                     started_at,completed_at,success,result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [
                        pilot_id,
                        command_id,
                        payload_digest,
                        manifest_uri,
                        manifest_sha256,
                        now,
                        now,
                        success,
                        Jsonb(report),
                    ],
                )
                connection.execute(
                    "INSERT INTO paper_pilot_commands VALUES (%s,%s,'cycle',%s,%s,%s)",
                    [command_id, pilot_id, payload_digest, Jsonb(report), now],
                )
                if success and pilot.state is PilotState.REGISTERED:
                    connection.execute(
                        "UPDATE paper_pilots SET state='running',updated_at=%s WHERE pilot_id=%s",
                        [now, pilot_id],
                    )
            if failure is not None:
                raise InvalidPaperTrading("paper pilot cycle processing was rejected") from failure
            return report
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper pilot cycle failed") from error

    def evidence(self, pilot_id: str, as_of: datetime) -> dict[str, Any]:
        pilot = self.get_pilot(pilot_id)
        try:
            with self._connect() as connection:
                cycles = connection.execute(
                    "SELECT * FROM paper_pilot_cycles WHERE pilot_id=%s AND completed_at<=%s "
                    "ORDER BY completed_at,command_id",
                    [pilot_id, as_of],
                ).fetchall()
                events = connection.execute(
                    "SELECT action,reason,resulting_state,created_at FROM paper_session_events "
                    "WHERE session_id=%s AND created_at<=%s ORDER BY created_at,event_id",
                    [pilot.session_id, as_of],
                ).fetchall()
            result = _pause_summary([dict(row) for row in events], as_of)
            result.update(
                {
                    "cycles_failed": sum(not row["success"] for row in cycles),
                    "cycles_succeeded": sum(row["success"] for row in cycles),
                    "discovered_candles": sum(
                        int(row["result"].get("discovered", 0)) for row in cycles
                    ),
                    "latest_successful_cycle_at": max(
                        (row["completed_at"] for row in cycles if row["success"]), default=None
                    ),
                    "rejected_candles": sum(
                        int(row["result"].get("rejected", 0)) for row in cycles
                    ),
                }
            )
            return result
        except Exception as error:
            raise InvalidPaperTrading("paper pilot evidence query failed") from error

    def store_snapshot(
        self, pilot_id: str, event_date: str, record: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        Jsonb = self._jsonb()
        try:
            with self._connect() as connection:
                inserted = connection.execute(
                    """INSERT INTO paper_pilot_snapshots
                    (pilot_id,event_date,as_of,artifact_uri,artifact_sha256,manifest_uri,
                     manifest_sha256,document,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pilot_id,event_date) DO NOTHING RETURNING pilot_id""",
                    [
                        pilot_id,
                        event_date,
                        record["as_of"],
                        record["artifact_uri"],
                        record["artifact_sha256"],
                        record["manifest_uri"],
                        record["manifest_sha256"],
                        Jsonb(_json_document(record["document"])),
                        record["created_at"],
                    ],
                ).fetchone()
                existing = connection.execute(
                    "SELECT * FROM paper_pilot_snapshots WHERE pilot_id=%s AND event_date=%s",
                    [pilot_id, event_date],
                ).fetchone()
                if existing["manifest_sha256"] != record["manifest_sha256"]:
                    raise InvalidPaperTrading("daily pilot snapshot already has different content")
                return record, inserted is not None
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper pilot snapshot persistence failed") from error

    def snapshot_records(self, pilot_id: str) -> list[dict[str, Any]]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM paper_pilot_snapshots WHERE pilot_id=%s ORDER BY event_date",
                    [pilot_id],
                ).fetchall()
            return [dict(row) for row in rows]
        except Exception as error:
            raise InvalidPaperTrading("paper pilot snapshot lookup failed") from error

    def cycle_records(self, pilot_id: str) -> list[dict[str, Any]]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM paper_pilot_cycles WHERE pilot_id=%s ORDER BY completed_at,command_id",
                    [pilot_id],
                ).fetchall()
            return [dict(row) for row in rows]
        except Exception as error:
            raise InvalidPaperTrading("paper pilot cycle lookup failed") from error

    def finalize(
        self,
        pilot_id: str,
        *,
        command_id: str,
        payload_digest: str,
        state: PilotState,
        assessment: dict[str, Any],
        now: datetime,
    ) -> tuple[Pilot, bool]:
        Jsonb = self._jsonb()
        try:
            with self._connect() as connection:
                self._set_timeout(connection)
                row = connection.execute(
                    "SELECT * FROM paper_pilots WHERE pilot_id=%s FOR UPDATE", [pilot_id]
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("paper pilot does not exist")
                pilot = _pilot_from_row(row)
                previous = connection.execute(
                    "SELECT pilot_id,payload_digest FROM paper_pilot_commands WHERE command_id=%s",
                    [command_id],
                ).fetchone()
                if previous:
                    if (
                        previous["pilot_id"] != pilot_id
                        or previous["payload_digest"] != payload_digest
                    ):
                        raise InvalidPaperTrading(
                            "pilot command ID was reused with different arguments"
                        )
                    return pilot, False
                if pilot.state.terminal:
                    raise InvalidPaperTrading("paper pilot is already terminal")
                connection.execute(
                    "UPDATE paper_pilots SET state=%s,assessment=%s,finalized_at=%s,updated_at=%s WHERE pilot_id=%s",
                    [state.value, Jsonb(_json_document(assessment)), now, now, pilot_id],
                )
                connection.execute(
                    "INSERT INTO paper_pilot_commands VALUES (%s,%s,'finalize',%s,%s,%s)",
                    [command_id, pilot_id, payload_digest, Jsonb(_json_document(assessment)), now],
                )
                updated = connection.execute(
                    "SELECT * FROM paper_pilots WHERE pilot_id=%s", [pilot_id]
                ).fetchone()
                return _pilot_from_row(updated), True
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper pilot finalization failed") from error
