from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files
from threading import RLock
from typing import Any, Protocol

from crypto_trading_domain.backtest import (
    IncrementalBacktestState,
    PortfolioState,
    StrategyDecision,
    TargetPosition,
)

from crypto_trading_core.paper_contracts import (
    InvalidPaperTrading,
    PaperMutation,
    PaperSession,
    PaperSessionSpec,
    PaperSessionState,
    StoredPaperCandle,
)

PaperMutator = Callable[
    [PaperSession, Mapping[object, StoredPaperCandle]], PaperMutation
]


class PaperRepository(Protocol):
    def migrate(self) -> None: ...

    def create_session(
        self,
        session: PaperSession,
        *,
        command_id: str,
        payload_digest: str,
    ) -> tuple[PaperSession, bool]: ...

    def get_session(self, session_id: str) -> PaperSession: ...

    def execute(
        self,
        session_id: str,
        *,
        command_id: str,
        payload_digest: str,
        actor: str,
        action: str,
        reason: str,
        mutator: PaperMutator,
    ) -> dict[str, object]: ...


@dataclass(frozen=True)
class _Command:
    payload_digest: str
    report: dict[str, object]


class MemoryPaperRepository:
    """Transactional in-memory repository used for domain and service tests."""

    def __init__(self) -> None:
        self.sessions: dict[str, PaperSession] = {}
        self.candles: dict[str, dict[object, StoredPaperCandle]] = {}
        self.commands: dict[tuple[str, str], _Command] = {}
        self.decisions: dict[str, list[object]] = {}
        self.fills: dict[str, list[object]] = {}
        self.equity: dict[str, list[object]] = {}
        self.events: dict[str, list[dict[str, object]]] = {}
        self._lock = RLock()

    def migrate(self) -> None:
        return None

    def create_session(
        self,
        session: PaperSession,
        *,
        command_id: str,
        payload_digest: str,
    ) -> tuple[PaperSession, bool]:
        with self._lock:
            command_key = (session.session_id, command_id)
            existing_command = self.commands.get(command_key)
            if existing_command is not None and existing_command.payload_digest != payload_digest:
                raise InvalidPaperTrading("command ID was reused with different arguments")
            existing = self.sessions.get(session.session_id)
            if existing is not None:
                return existing, False
            self.sessions[session.session_id] = session
            self.candles[session.session_id] = {}
            self.decisions[session.session_id] = []
            self.fills[session.session_id] = []
            self.equity[session.session_id] = []
            report: dict[str, object] = {
                "session_id": session.session_id,
                "state": session.state.value,
                "status": "created",
            }
            self.commands[command_key] = _Command(payload_digest, report)
            self.events[session.session_id] = [
                {
                    "action": "create",
                    "actor": session.spec.approved_by,
                    "command_id": command_id,
                    "reason": session.spec.approval_note,
                    "resulting_state": session.state.value,
                }
            ]
            return session, True

    def get_session(self, session_id: str) -> PaperSession:
        try:
            return self.sessions[session_id]
        except KeyError as error:
            raise InvalidPaperTrading("paper session does not exist") from error

    def execute(
        self,
        session_id: str,
        *,
        command_id: str,
        payload_digest: str,
        actor: str,
        action: str,
        reason: str,
        mutator: PaperMutator,
    ) -> dict[str, object]:
        with self._lock:
            key = (session_id, command_id)
            existing = self.commands.get(key)
            if existing is not None:
                if existing.payload_digest != payload_digest:
                    raise InvalidPaperTrading("command ID was reused with different arguments")
                return {**existing.report, "status": "resolved_existing_command"}
            session = self.get_session(session_id)
            mutation = mutator(session, dict(self.candles[session_id]))
            self.sessions[session_id] = mutation.session
            for candle in mutation.candles:
                self.candles[session_id][candle.window_start] = candle
            self.decisions[session_id].extend(mutation.decisions)
            self.fills[session_id].extend(mutation.fills)
            self.equity[session_id].extend(mutation.equity)
            report = dict(mutation.report)
            self.commands[key] = _Command(payload_digest, report)
            self.events.setdefault(session_id, []).append(
                {
                    "action": action,
                    "actor": actor,
                    "command_id": command_id,
                    "reason": mutation.report.get("pause_reason") or reason,
                    "resulting_state": mutation.session.state.value,
                }
            )
            return report


def _time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def _decision_document(value: StrategyDecision | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "decision_time": _time(value.decision_time),
        "fast_sma": format(value.fast_sma, "f"),
        "new_target": value.new_target.value,
        "observed_candle_window_start": _time(value.observed_candle_window_start),
        "previous_target": value.previous_target.value,
        "slow_sma": format(value.slow_sma, "f"),
        "strategy_version": value.strategy_version,
    }


def _decision_from_document(value: dict[str, Any] | None) -> StrategyDecision | None:
    if value is None:
        return None
    return StrategyDecision(
        decision_time=_parse_time(value["decision_time"]),
        observed_candle_window_start=_parse_time(value["observed_candle_window_start"]),
        fast_sma=Decimal(value["fast_sma"]),
        slow_sma=Decimal(value["slow_sma"]),
        previous_target=TargetPosition(value["previous_target"]),
        new_target=TargetPosition(value["new_target"]),
        strategy_version=value["strategy_version"],
    )


def _spec_document(spec: PaperSessionSpec) -> dict[str, Any]:
    return {
        "approval_note": spec.approval_note,
        "approved_by": spec.approved_by,
        "backtest_engine_version": spec.backtest_engine_version,
        "candidate_id": spec.candidate_id,
        "candle_manifest_sha256": spec.candle_manifest_sha256,
        "candle_manifest_uri": spec.candle_manifest_uri,
        "evaluation_key": spec.evaluation_key,
        "evaluation_manifest_sha256": spec.evaluation_manifest_sha256,
        "evaluation_manifest_uri": spec.evaluation_manifest_uri,
        "exchange": spec.exchange,
        "experiment_engine_version": spec.experiment_engine_version,
        "fast_period": spec.fast_period,
        "fee_bps": format(spec.fee_bps, "f"),
        "maximum_drawdown": format(spec.maximum_drawdown, "f"),
        "paper_engine_version": spec.paper_engine_version,
        "paper_schema_version": spec.paper_schema_version,
        "selection_key": spec.selection_key,
        "selection_manifest_sha256": spec.selection_manifest_sha256,
        "selection_manifest_uri": spec.selection_manifest_uri,
        "slow_period": spec.slow_period,
        "slippage_bps": format(spec.slippage_bps, "f"),
        "starting_cash": format(spec.starting_cash, "f"),
        "strategy_version": spec.strategy_version,
        "symbol": spec.symbol,
        "test_end": _time(spec.test_end),
    }


def _spec_from_document(value: dict[str, Any]) -> PaperSessionSpec:
    return PaperSessionSpec(
        evaluation_manifest_uri=value["evaluation_manifest_uri"],
        evaluation_manifest_sha256=value["evaluation_manifest_sha256"],
        selection_manifest_uri=value["selection_manifest_uri"],
        selection_manifest_sha256=value["selection_manifest_sha256"],
        candle_manifest_uri=value["candle_manifest_uri"],
        candle_manifest_sha256=value["candle_manifest_sha256"],
        evaluation_key=value["evaluation_key"],
        selection_key=value["selection_key"],
        exchange=value["exchange"],
        symbol=value["symbol"],
        candidate_id=value["candidate_id"],
        fast_period=value["fast_period"],
        slow_period=value["slow_period"],
        starting_cash=Decimal(value["starting_cash"]),
        fee_bps=Decimal(value["fee_bps"]),
        slippage_bps=Decimal(value["slippage_bps"]),
        test_end=_parse_time(value["test_end"]),
        approved_by=value["approved_by"],
        approval_note=value["approval_note"],
        maximum_drawdown=Decimal(value["maximum_drawdown"]),
        strategy_version=value["strategy_version"],
        backtest_engine_version=value["backtest_engine_version"],
        experiment_engine_version=value["experiment_engine_version"],
        paper_engine_version=value["paper_engine_version"],
        paper_schema_version=value["paper_schema_version"],
    )


def _portfolio_document(value: PortfolioState | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {"base_quantity": format(value.base_quantity, "f"), "cash": format(value.cash, "f")}


def _portfolio_from_document(value: dict[str, str] | None) -> PortfolioState | None:
    if value is None:
        return None
    return PortfolioState(cash=Decimal(value["cash"]), base_quantity=Decimal(value["base_quantity"]))


def _session_document(session: PaperSession) -> dict[str, Any]:
    strategy = session.strategy_state
    return {
        "baseline_equity": (
            format(session.baseline_equity, "f") if session.baseline_equity is not None else None
        ),
        "baseline_fee": format(session.baseline_fee, "f"),
        "baseline_maximum_drawdown": format(session.baseline_maximum_drawdown, "f"),
        "baseline_peak_equity": (
            format(session.baseline_peak_equity, "f")
            if session.baseline_peak_equity is not None
            else None
        ),
        "baseline_portfolio": _portfolio_document(session.baseline_portfolio),
        "buys": session.buys,
        "created_at": _time(session.created_at),
        "current_equity": (
            format(session.current_equity, "f") if session.current_equity is not None else None
        ),
        "decisions": session.decisions,
        "first_candle_time": (
            _time(session.first_candle_time) if session.first_candle_time is not None else None
        ),
        "last_candle_time": (
            _time(session.last_candle_time) if session.last_candle_time is not None else None
        ),
        "last_state_changed_at": (
            _time(session.last_state_changed_at)
            if session.last_state_changed_at is not None
            else None
        ),
        "last_state_reason": session.last_state_reason,
        "maximum_drawdown": format(session.maximum_drawdown, "f"),
        "processed_candles": session.processed_candles,
        "sells": session.sells,
        "state": session.state.value,
        "strategy_state": {
            "base_quantity": format(strategy.portfolio.base_quantity, "f"),
            "cash": format(strategy.portfolio.cash, "f"),
            "peak_equity": format(strategy.peak_equity, "f"),
            "pending_decision": _decision_document(strategy.pending_decision),
            "recent_closes": [format(value, "f") for value in strategy.recent_closes],
            "target": strategy.target.value,
        },
        "total_fees": format(session.total_fees, "f"),
        "updated_at": _time(session.updated_at),
    }


def _session_from_documents(
    spec_document: dict[str, Any], session_document: dict[str, Any]
) -> PaperSession:
    state = session_document["strategy_state"]
    return PaperSession(
        spec=_spec_from_document(spec_document),
        state=PaperSessionState(session_document["state"]),
        strategy_state=IncrementalBacktestState(
            recent_closes=tuple(Decimal(value) for value in state["recent_closes"]),
            target=TargetPosition(state["target"]),
            portfolio=PortfolioState(
                cash=Decimal(state["cash"]),
                base_quantity=Decimal(state["base_quantity"]),
            ),
            pending_decision=_decision_from_document(state["pending_decision"]),
            peak_equity=Decimal(state["peak_equity"]),
        ),
        created_at=_parse_time(session_document["created_at"]),
        updated_at=_parse_time(session_document["updated_at"]),
        first_candle_time=(
            _parse_time(session_document["first_candle_time"])
            if session_document["first_candle_time"] is not None
            else None
        ),
        last_candle_time=(
            _parse_time(session_document["last_candle_time"])
            if session_document["last_candle_time"] is not None
            else None
        ),
        processed_candles=session_document["processed_candles"],
        decisions=session_document["decisions"],
        buys=session_document["buys"],
        sells=session_document["sells"],
        total_fees=Decimal(session_document["total_fees"]),
        current_equity=(
            Decimal(session_document["current_equity"])
            if session_document["current_equity"] is not None
            else None
        ),
        maximum_drawdown=Decimal(session_document["maximum_drawdown"]),
        baseline_portfolio=_portfolio_from_document(session_document["baseline_portfolio"]),
        baseline_peak_equity=(
            Decimal(session_document["baseline_peak_equity"])
            if session_document["baseline_peak_equity"] is not None
            else None
        ),
        baseline_equity=(
            Decimal(session_document["baseline_equity"])
            if session_document["baseline_equity"] is not None
            else None
        ),
        baseline_maximum_drawdown=Decimal(session_document["baseline_maximum_drawdown"]),
        baseline_fee=Decimal(session_document["baseline_fee"]),
        last_state_reason=session_document["last_state_reason"],
        last_state_changed_at=(
            _parse_time(session_document["last_state_changed_at"])
            if session_document["last_state_changed_at"] is not None
            else None
        ),
    )


class PostgresPaperRepository:
    """PostgreSQL implementation with one serializable transaction per command."""

    def __init__(self, database_url: str, *, transaction_timeout_seconds: int = 30) -> None:
        if not database_url.strip():
            raise InvalidPaperTrading("paper database URL is empty")
        if transaction_timeout_seconds <= 0:
            raise InvalidPaperTrading("paper transaction timeout must be positive")
        self._database_url = database_url
        self._timeout_ms = transaction_timeout_seconds * 1000

    @staticmethod
    def _psycopg():
        try:
            import psycopg
            from psycopg.rows import dict_row
            from psycopg.types.json import Jsonb
        except ImportError as error:
            raise InvalidPaperTrading("PostgreSQL paper support is not installed") from error
        return psycopg, dict_row, Jsonb

    def _connect(self):
        psycopg, dict_row, _ = self._psycopg()
        try:
            return psycopg.connect(self._database_url, row_factory=dict_row)
        except Exception as error:
            raise InvalidPaperTrading("paper database connection failed") from error

    def migrate(self) -> None:
        sql = files("crypto_trading_core").joinpath("migrations/001_paper_trading.sql").read_text()
        try:
            with self._connect() as connection:
                connection.execute(sql)
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper database migration failed") from error

    def _set_timeout(self, connection: Any) -> None:
        timeout = str(self._timeout_ms)
        connection.execute("SELECT set_config('statement_timeout', %s, true)", [timeout])
        connection.execute("SELECT set_config('lock_timeout', %s, true)", [timeout])

    @staticmethod
    def _row_session(row: Mapping[str, Any]) -> PaperSession:
        return _session_from_documents(row["spec"], row["session_document"])

    def create_session(
        self,
        session: PaperSession,
        *,
        command_id: str,
        payload_digest: str,
    ) -> tuple[PaperSession, bool]:
        _, _, Jsonb = self._psycopg()
        try:
            with self._connect() as connection:
                self._set_timeout(connection)
                event = connection.execute(
                    "SELECT payload_digest FROM paper_session_events "
                    "WHERE session_id = %s AND command_id = %s",
                    [session.session_id, command_id],
                ).fetchone()
                if event is not None and event["payload_digest"] != payload_digest:
                    raise InvalidPaperTrading("command ID was reused with different arguments")
                inserted = connection.execute(
                    """
                    INSERT INTO paper_sessions (
                        session_id, state, spec, session_document, cash, base_quantity,
                        current_equity, peak_equity, total_fees, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO NOTHING
                    RETURNING session_id
                    """,
                    [
                        session.session_id,
                        session.state.value,
                        Jsonb(_spec_document(session.spec)),
                        Jsonb(_session_document(session)),
                        session.strategy_state.portfolio.cash,
                        session.strategy_state.portfolio.base_quantity,
                        session.current_equity,
                        session.strategy_state.peak_equity,
                        session.total_fees,
                        session.created_at,
                        session.updated_at,
                    ],
                ).fetchone()
                row = connection.execute(
                    "SELECT spec, session_document FROM paper_sessions WHERE session_id = %s",
                    [session.session_id],
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("paper session creation did not persist")
                stored = self._row_session(row)
                if stored.spec.identity() != session.spec.identity():
                    raise InvalidPaperTrading("paper session identity conflicts with stored state")
                created = inserted is not None
                if created:
                    report = {
                        "session_id": session.session_id,
                        "state": session.state.value,
                        "status": "created",
                    }
                    connection.execute(
                        """
                        INSERT INTO paper_session_events (
                            session_id, command_id, payload_digest, actor, action, reason,
                            resulting_state, result, created_at
                        ) VALUES (%s, %s, %s, %s, 'create', %s, %s, %s, %s)
                        """,
                        [
                            session.session_id,
                            command_id,
                            payload_digest,
                            session.spec.approved_by,
                            session.spec.approval_note,
                            session.state.value,
                            Jsonb(report),
                            session.created_at,
                        ],
                    )
                return stored, created
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper database session creation failed") from error

    def get_session(self, session_id: str) -> PaperSession:
        try:
            with self._connect() as connection:
                connection.execute("SET TRANSACTION READ ONLY")
                self._set_timeout(connection)
                row = connection.execute(
                    "SELECT spec, session_document FROM paper_sessions WHERE session_id = %s",
                    [session_id],
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("paper session does not exist")
                return self._row_session(row)
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper database read failed") from error

    def execute(
        self,
        session_id: str,
        *,
        command_id: str,
        payload_digest: str,
        actor: str,
        action: str,
        reason: str,
        mutator: PaperMutator,
    ) -> dict[str, object]:
        _, _, Jsonb = self._psycopg()
        try:
            with self._connect() as connection:
                self._set_timeout(connection)
                row = connection.execute(
                    "SELECT spec, session_document FROM paper_sessions "
                    "WHERE session_id = %s FOR UPDATE",
                    [session_id],
                ).fetchone()
                if row is None:
                    raise InvalidPaperTrading("paper session does not exist")
                existing_command = connection.execute(
                    "SELECT payload_digest, result FROM paper_session_events "
                    "WHERE session_id = %s AND command_id = %s",
                    [session_id, command_id],
                ).fetchone()
                if existing_command is not None:
                    if existing_command["payload_digest"] != payload_digest:
                        raise InvalidPaperTrading("command ID was reused with different arguments")
                    return {
                        **existing_command["result"],
                        "status": "resolved_existing_command",
                    }
                session = self._row_session(row)
                candle_rows = connection.execute(
                    """
                    SELECT window_start, manifest_uri, manifest_sha256, snapshot_key,
                           open, high, low, close
                    FROM paper_candle_inputs WHERE session_id = %s
                    """,
                    [session_id],
                ).fetchall()
                existing = {
                    item["window_start"].astimezone(UTC): StoredPaperCandle(
                        window_start=item["window_start"].astimezone(UTC),
                        manifest_uri=item["manifest_uri"],
                        manifest_sha256=item["manifest_sha256"],
                        snapshot_key=item["snapshot_key"],
                        open=item["open"],
                        high=item["high"],
                        low=item["low"],
                        close=item["close"],
                    )
                    for item in candle_rows
                }
                mutation = mutator(session, existing)
                for candle in mutation.candles:
                    connection.execute(
                        """
                        INSERT INTO paper_candle_inputs (
                            session_id, window_start, manifest_uri, manifest_sha256,
                            snapshot_key, exchange, symbol, open, high, low, close
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            session_id,
                            candle.window_start,
                            candle.manifest_uri,
                            candle.manifest_sha256,
                            candle.snapshot_key,
                            session.spec.exchange,
                            session.spec.symbol,
                            candle.open,
                            candle.high,
                            candle.low,
                            candle.close,
                        ],
                    )
                for decision in mutation.decisions:
                    connection.execute(
                        """
                        INSERT INTO paper_decisions (
                            session_id, decision_time, observed_candle_window_start,
                            fast_sma, slow_sma, previous_target, new_target, strategy_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            session_id,
                            decision.decision_time,
                            decision.observed_candle_window_start,
                            decision.fast_sma,
                            decision.slow_sma,
                            decision.previous_target.value,
                            decision.new_target.value,
                            decision.strategy_version,
                        ],
                    )
                for fill in mutation.fills:
                    connection.execute(
                        """
                        INSERT INTO paper_fills (
                            session_id, decision_time, fill_time, side, base_quantity,
                            reference_open_price, execution_price, gross_notional, fee,
                            cash_after, base_after
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            session_id,
                            fill.decision_time,
                            fill.fill_time,
                            fill.side.value,
                            fill.base_quantity,
                            fill.reference_open_price,
                            fill.execution_price,
                            fill.gross_notional,
                            fill.fee,
                            fill.cash_after,
                            fill.base_after,
                        ],
                    )
                for equity in mutation.equity:
                    connection.execute(
                        """
                        INSERT INTO paper_equity (
                            session_id, window_start, close, cash, base_quantity,
                            position, equity, drawdown
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            session_id,
                            equity.window_start,
                            equity.close,
                            equity.cash,
                            equity.base_quantity,
                            equity.position.value,
                            equity.equity,
                            equity.drawdown,
                        ],
                    )
                updated = mutation.session
                connection.execute(
                    """
                    UPDATE paper_sessions
                    SET state = %s, session_document = %s, cash = %s,
                        base_quantity = %s, current_equity = %s, peak_equity = %s,
                        total_fees = %s, updated_at = %s
                    WHERE session_id = %s
                    """,
                    [
                        updated.state.value,
                        Jsonb(_session_document(updated)),
                        updated.strategy_state.portfolio.cash,
                        updated.strategy_state.portfolio.base_quantity,
                        updated.current_equity,
                        updated.strategy_state.peak_equity,
                        updated.total_fees,
                        updated.updated_at,
                        session_id,
                    ],
                )
                report = dict(mutation.report)
                connection.execute(
                    """
                    INSERT INTO paper_session_events (
                        session_id, command_id, payload_digest, actor, action, reason,
                        resulting_state, result, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        session_id,
                        command_id,
                        payload_digest,
                        actor,
                        action,
                        report.get("pause_reason") or reason,
                        updated.state.value,
                        Jsonb(report),
                        updated.updated_at,
                    ],
                )
                return report
        except InvalidPaperTrading:
            raise
        except Exception as error:
            raise InvalidPaperTrading("paper database command failed") from error
