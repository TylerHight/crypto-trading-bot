import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext

import pytest
from crypto_exchange_adapters.coinbase_rest import RetryableCoinbaseRestError
from crypto_trading_core import daily_breakout as study
from crypto_trading_core.daily_momentum import DailyCandle

DAY = timedelta(days=1)
START = datetime(2026, 9, 25, tzinfo=UTC)
REGISTERED = START - timedelta(hours=10)


@pytest.fixture
def experiment(tmp_path, monkeypatch):
    spec = {
        "version": study.VERSION, "name": "btc-usd-daily-breakout-v1",
        "source": "coinbase-exchange", "symbol": "BTC-USD", "granularity_seconds": 86400,
        "warmup_start": "2026-09-05T00:00:00Z", "start": "2026-09-25T00:00:00Z",
        "end": "2027-03-24T00:00:00Z", "entry_days": 20, "exit_days": 10,
        "starting_cash": "10000", "fee_bps": "40", "slippage_bps": "5",
        "cost_multipliers": [1, 2, 3], "minimum_round_trips": 5,
        "maximum_drawdown": "0.25", "minimum_excess_return_pct": "0",
        "settlement_delay_seconds": 3600,
    }
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(study, "_now", lambda: REGISTERED)
    return path, digest, tmp_path / "artifacts"


def source(start, end):
    rows = [
        [int((start + day * DAY).timestamp()), 100, 100, 100, 100, 1]
        for day in range((end - start).days)
    ]
    body = json.dumps(rows).encode()
    return study._decode(body, start, end), body


def test_registration_seals_exact_spec_and_rejects_after_start(experiment, monkeypatch):
    path, digest, output = experiment
    monkeypatch.setattr(study, "fetch_candles", lambda *_: pytest.fail("registration fetched prices"))
    first = study.register(*experiment)
    root = output / first["name"]
    assert (root / f"spec-{digest}.json").read_bytes() == path.read_bytes()
    assert first["registered_at"] == REGISTERED.isoformat()
    monkeypatch.setattr(study, "_now", lambda: START)
    assert study.register(*experiment) == first
    with pytest.raises(ValueError, match="precede study start"):
        study.register(path, digest, output / "late")


def test_registration_rejects_changed_rules_and_tampered_artifact(experiment):
    path, digest, output = experiment
    registration = study.register(*experiment)
    original = path.read_bytes()
    changed = json.loads(original)
    changed["entry_days"] = 19
    path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="registration differs"):
        study.register(path, hashlib.sha256(path.read_bytes()).hexdigest(), output)
    path.write_bytes(original)
    artifact = next((output / registration["name"] / "registration").glob("*.json"))
    artifact.write_bytes(artifact.read_bytes() + b" ")
    with pytest.raises(ValueError, match="SHA-256"):
        study.register(path, digest, output)


def test_capture_is_incremental_and_respects_settlement_boundary(experiment, monkeypatch):
    study.register(*experiment)
    calls = []

    def tracked(start, end):
        calls.append((start, end))
        return source(start, end)

    monkeypatch.setattr(study, "fetch_candles", tracked)
    first = study.collect(*experiment)
    assert first["collected_days"] == first["sealed_days"] == 19
    assert first["sealed_end"] == "2026-09-24T00:00:00+00:00"
    assert study.collect(*experiment)["collected_days"] == 0
    monkeypatch.setattr(study, "_now", lambda: START + timedelta(minutes=59))
    assert study.collect(*experiment)["collected_days"] == 0
    monkeypatch.setattr(study, "_now", lambda: START + timedelta(hours=1))
    assert study.collect(*experiment)["collected_days"] == 1
    assert calls == [
        (datetime(2026, 9, 5, tzinfo=UTC), datetime(2026, 9, 24, tzinfo=UTC)),
        (datetime(2026, 9, 24, tzinfo=UTC), START),
    ]


def test_capture_validates_source_and_chain_before_refetch(experiment, monkeypatch):
    registration = study.register(*experiment)
    monkeypatch.setattr(study, "fetch_candles", source)
    study.collect(*experiment)
    root = experiment[2] / registration["name"]
    artifact = next((root / "sources").glob("*.json"))
    body = artifact.read_bytes()
    artifact.write_bytes(body + b" ")
    monkeypatch.setattr(study, "fetch_candles", lambda *_: pytest.fail("tampered source refetched"))
    with pytest.raises(ValueError, match="source SHA-256"):
        study.collect(*experiment)
    artifact.write_bytes(body)
    manifest = next((root / "captures").glob("*.json"))
    changed = json.loads(manifest.read_bytes())
    changed["start"] = "2026-09-06T00:00:00+00:00"
    new_body = study.canonical_json_bytes(changed)
    manifest.unlink()
    (root / "captures" / f"000001-{study._hash(new_body)}.json").write_bytes(new_body)
    with pytest.raises(ValueError, match="continuity"):
        study.collect(*experiment)


def test_capture_does_not_pad_missing_days(experiment, monkeypatch):
    study.register(*experiment)

    def missing(start, end):
        _, body = source(start, end)
        return (), json.dumps(json.loads(body)[1:]).encode()

    monkeypatch.setattr(study, "fetch_candles", missing)
    with pytest.raises(ValueError, match="missing candle days"):
        study.collect(*experiment)
    assert not list(experiment[2].rglob("captures/*.json"))


def test_cli_transport_failure_preserves_registration_and_can_resume(experiment, monkeypatch, capsys):
    path, digest, output = experiment
    study.register(*experiment)
    before = {str(path): path.read_bytes() for path in output.rglob("*.json")}

    def unavailable(*_):
        raise RetryableCoinbaseRestError("network unavailable")

    monkeypatch.setattr(study, "fetch_candles", unavailable)
    with pytest.raises(SystemExit) as error:
        study.main(["collect", "--spec", str(path), "--spec-sha256", digest,
                    "--output", str(output)])
    assert error.value.code == 4
    assert "network unavailable" in capsys.readouterr().err
    assert {str(path): path.read_bytes() for path in output.rglob("*.json")} == before
    monkeypatch.setattr(study, "fetch_candles", source)
    assert study.collect(*experiment)["collected_days"] == 19


@pytest.mark.parametrize("value", ["NaN", "Infinity", True, None, {}, []])
def test_decoder_rejects_malformed_and_nonfinite_rows(value):
    row = [int(START.timestamp()), 100, 100, 100, value, 1]
    with pytest.raises(ValueError):
        study._decode(json.dumps([row]).encode(), START, START + DAY)


def candle(day, opening, close, high=None, low=None):
    opening, close = Decimal(opening), Decimal(close)
    return DailyCandle(
        START + day * DAY, opening,
        Decimal(high) if high is not None else max(opening, close),
        Decimal(low) if low is not None else min(opening, close), close, Decimal(1),
    )


def run(candles, **overrides):
    kwargs = {
        "start": START, "end": candles[-1].start + DAY, "entry_days": 2, "exit_days": 1,
        "cash": Decimal(1000), "fee_bps": Decimal(40), "slippage_bps": Decimal(5),
    }
    kwargs.update(overrides)
    return study.simulate(tuple(candles), **kwargs)


def test_channel_uses_previous_highs_next_open_and_completed_exits():
    candles = [
        candle(-2, "10", "10", high="12"), candle(-1, "10", "10"),
        candle(0, "11", "11"),  # Above prior closes, but not prior high: no entry.
        candle(1, "11", "12", high="20"),  # Current high excluded: entry signal.
        candle(2, "15", "9"),  # Buy at 15, then signal exit below previous low.
        candle(3, "8", "8"),
    ]
    result = run(candles)
    assert result["fill_count"] == 2
    assert result["completed_round_trips"] == 1
    assert result["open_position"] is False
    with localcontext() as context:
        context.prec = 50
        quantity = Decimal(1000) / (Decimal(15) * Decimal("1.0005") * Decimal("1.004"))
        expected = quantity * Decimal(8) * Decimal("0.9995") * Decimal("0.996")
        fees = quantity * Decimal(15) * Decimal("1.0005") * Decimal("0.004")
        fees += quantity * Decimal(8) * Decimal("0.9995") * Decimal("0.004")
    assert abs(result["ending_equity"] - expected) < Decimal("1e-45")
    assert abs(result["total_fees"] - fees) < Decimal("1e-45")
    assert result["maximum_drawdown"] > Decimal("0.45")


def test_equal_channels_do_not_trade_and_final_signal_is_not_a_fill():
    candles = [candle(-2, "10", "10"), candle(-1, "10", "10"),
               candle(0, "10", "10"), candle(1, "10", "11")]
    result = run(candles)
    assert result["fill_count"] == result["completed_round_trips"] == 0
    assert result["ending_equity"] == 1000
    assert result["unfilled_final_signal"] == "buy"


def test_current_low_is_excluded_from_exit_and_final_holding_is_marked():
    candles = [candle(-2, "10", "10"), candle(-1, "10", "10"),
               candle(0, "10", "11"), candle(1, "12", "9", low="1")]
    result = run(candles)
    assert result["fill_count"] == 1
    assert result["completed_round_trips"] == 0
    assert result["open_position"] is True
    assert result["unfilled_final_signal"] == "sell"
    assert result["ending_equity"] < 750


def test_buy_hold_uses_multiplicative_costs_and_no_forced_exit():
    result = run([candle(0, "100", "200")], hold=True)
    with localcontext() as context:
        context.prec = 50
        expected = Decimal(1000) / (Decimal(100) * Decimal("1.0005") * Decimal("1.004")) * 200
    assert result["ending_equity"] == expected
    assert result["fill_count"] == 1
    assert result["completed_round_trips"] == 0


def test_early_evaluation_cannot_fetch_or_simulate(experiment, monkeypatch):
    study.register(*experiment)
    monkeypatch.setattr(study, "fetch_candles", lambda *_: pytest.fail("evaluation fetched prices"))
    monkeypatch.setattr(study, "simulate", lambda *_args, **_kwargs: pytest.fail("early simulation"))
    result = study.evaluate(*experiment)
    assert result["status"] == "waiting"
    assert "scenarios" not in result
    assert not list(experiment[2].rglob("reports/*.json"))
    monkeypatch.setattr(study, "_now", lambda: datetime(2027, 3, 24, 0, 59, 59, tzinfo=UTC))
    assert study.evaluate(*experiment)["reason"] == "study_not_finished"
    monkeypatch.setattr(study, "_now", lambda: datetime(2027, 3, 24, 1, tzinfo=UTC))
    assert study.evaluate(*experiment)["reason"] == "missing_sealed_candles"


def test_complete_evaluation_is_offline_reproducible_and_rejects_no_trades(experiment, monkeypatch):
    study.register(*experiment)
    monkeypatch.setattr(study, "fetch_candles", source)
    end = datetime(2027, 3, 24, 1, tzinfo=UTC)
    monkeypatch.setattr(study, "_now", lambda: end)
    assert study.collect(*experiment)["complete"] is True
    monkeypatch.setattr(study, "fetch_candles", lambda *_: pytest.fail("evaluation fetched prices"))
    first = study.evaluate(*experiment)
    assert first["status"] == "evaluated"
    assert first["research_gate_passed"] is False
    assert first["paper_trial_eligible"] is False
    assert "1x_insufficient_completed_round_trips" in first["research_gate_failure_reasons"]
    assert study.evaluate(*experiment) == first
    artifact = next(experiment[2].rglob("reports/*.json"))
    artifact.write_bytes(artifact.read_bytes() + b" ")
    with pytest.raises(ValueError, match="SHA-256"):
        study.evaluate(*experiment)


def test_passing_research_still_cannot_promote_to_paper(experiment, monkeypatch):
    path, _, output = experiment
    spec = json.loads(path.read_bytes())
    spec.update(entry_days=2, exit_days=1)
    path.write_text(json.dumps(spec), encoding="utf-8")
    args = path, hashlib.sha256(path.read_bytes()).hexdigest(), output
    study.register(*args)

    def profitable_cycles(start, end):
        pattern = [(100, 100), (100, 101), (100, 400), (400, 400),
                   (400, 399), (400, 100), (100, 100)]
        rows = []
        for day in range((end - start).days):
            stamp = start + day * DAY
            opening, close = pattern[(stamp - START).days % 7] if stamp >= START else (100, 100)
            rows.append([int(stamp.timestamp()), min(opening, close), max(opening, close),
                         opening, close, 1])
        return (), json.dumps(rows).encode()

    monkeypatch.setattr(study, "fetch_candles", profitable_cycles)
    monkeypatch.setattr(study, "_now", lambda: datetime(2027, 3, 24, 1, tzinfo=UTC))
    study.collect(*args)
    report = study.evaluate(*args)
    assert report["research_gate_passed"] is True
    assert report["research_gate_failure_reasons"] == []
    assert report["paper_trial_eligible"] is False
    assert all(item["strategy"]["completed_round_trips"] >= 5 for item in report["scenarios"])


def test_cli_waiting_and_invalid_exit_codes(experiment):
    path, digest, output = experiment
    args = ["--spec", str(path), "--spec-sha256", digest, "--output", str(output)]
    study.main(["register", *args])
    with pytest.raises(SystemExit) as error:
        study.main(["evaluate", *args])
    assert error.value.code == 2
    args[3] = "0" * 64
    with pytest.raises(SystemExit) as error:
        study.main(["collect", *args])
    assert error.value.code == 4


@pytest.mark.parametrize("operation", [study.register, study.collect, study.evaluate])
def test_lifecycle_rejects_overlap_without_changing_evidence(experiment, operation, monkeypatch):
    registration = study.register(*experiment)
    root = experiment[2] / registration["name"]
    before = {str(path): path.read_bytes() for path in root.rglob("*.json")}
    monkeypatch.setattr(study, "fetch_candles", lambda *_: pytest.fail("overlapping fetch"))
    with study._study_lock(root), pytest.raises(ValueError, match="another study operation"):
        operation(*experiment)
    assert {str(path): path.read_bytes() for path in root.rglob("*.json")} == before
    assert study.register(*experiment) == registration


def test_os_lock_blocks_other_process_and_recovers_after_crash(tmp_path):
    code = (
        "import os, sys\n"
        "from pathlib import Path\n"
        "from crypto_trading_core.daily_breakout import _study_lock\n"
        "with _study_lock(Path(sys.argv[1])):\n"
        "    os._exit(0)\n"
    )
    command = [sys.executable, "-c", code, str(tmp_path)]
    with study._study_lock(tmp_path):
        blocked = subprocess.run(command, capture_output=True, text=True, timeout=15, check=False)
    assert blocked.returncode != 0
    assert "another study operation" in blocked.stderr
    crashed = subprocess.run(command, capture_output=True, text=True, timeout=15, check=False)
    assert crashed.returncode == 0, crashed.stderr
    with study._study_lock(tmp_path):
        assert (tmp_path / ".study.lock").exists()
