import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
from crypto_trading_core import daily_momentum

DAY = timedelta(days=1)


def candle(day, opening, close):
    start = datetime(2026, 1, 1, tzinfo=UTC) + day * DAY
    return daily_momentum.DailyCandle(
        start, Decimal(opening), max(Decimal(opening), Decimal(close)),
        min(Decimal(opening), Decimal(close)), Decimal(close), Decimal(1),
    )


def test_daily_signal_fills_at_next_open_with_costs():
    candles = (
        candle(0, "10", "10"),
        candle(1, "12", "12"),
        candle(2, "15", "11"),
        candle(3, "8", "8"),
    )
    result = daily_momentum.simulate(
        candles, start=candles[1].start, end=candles[3].start + DAY,
        fast=1, slow=2, cash=Decimal(1000),
        fee_bps=Decimal(40), slippage_bps=Decimal(5),
    )
    assert result["fill_count"] == 2
    assert result["completed_round_trips"] == 1
    buy_price = Decimal("15.0075")
    sell_price = Decimal("7.996")
    with localcontext() as context:
        context.prec = 50
        expected = Decimal(1000) / (buy_price * Decimal("1.004")) * sell_price * Decimal("0.996")
    assert result["ending_equity"] == expected


def test_prepare_seals_selection_without_fetching_test_prices(tmp_path, monkeypatch):
    spec_path = Path("experiments/btc-usd-daily-momentum-v1.json")
    digest = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    called = []

    def source(start, end):
        called.append((start, end))
        assert end <= datetime(2026, 6, 21, tzinfo=UTC)
        days = (end - start).days
        rows = [
            [int((start + index * DAY).timestamp()), 100 + index, 100 + index,
             100 + index, 100 + index, 1]
            for index in range(days)
        ]
        body = json.dumps(rows).encode()
        return daily_momentum.decode_candles(body, start, end), body

    monkeypatch.setattr(daily_momentum, "fetch_candles", source)
    result = daily_momentum.prepare(spec_path, digest, tmp_path)
    assert result["status"] == "selected"
    assert result["test_prices_accessed"] is False
    assert len(called) == 1
    assert called[0][1] == datetime(2026, 6, 21, tzinfo=UTC)


def test_daily_decoder_requires_every_day():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    body = json.dumps([[int(start.timestamp()), 10, 10, 10, 10, 1]]).encode()
    with pytest.raises(ValueError, match="missing candle days"):
        daily_momentum.decode_candles(body, start, start + 2 * DAY)


def test_operational_review_vetoes_zero_trade_validation_and_one_entry_test():
    selection = {
        "selected_candidate": {"id": "sma-7-28"},
        "candidate_results": [{
            "candidate": {"id": "sma-7-28"},
            "train": {"percentage_return": "-6.54"},
            "validation": {"fill_count": 0, "percentage_return": "0"},
        }],
    }
    evaluation = {"paper_trial_eligible": True, "scenarios": [{"strategy": {"fill_count": 1}}]}
    review = daily_momentum.review_paper_trial(selection, evaluation)
    assert review["recommendation"] == "do_not_start_paper_pilot"
    assert "validation_has_no_fills" in review["reasons"]
    assert "legacy_evidence_requires_audit_only" in review["reasons"]
    assert daily_momentum.PAPER_UNSUPPORTED in review["reasons"]


def test_buy_and_hold_charges_fees_on_the_slipped_execution_price():
    candles = (candle(0, "100", "100"), candle(1, "100", "100"))
    result = daily_momentum.buy_and_hold(
        candles, start=candles[0].start, end=candles[-1].start + DAY,
        cash=Decimal(1000), fee_bps=Decimal(100), slippage_bps=Decimal(100),
    )
    with localcontext() as context:
        context.prec = 50
        quantity = Decimal(1000) / Decimal("102.01")
        expected = quantity * 100
        expected_fee = quantity * Decimal("1.01")
    assert result["ending_equity"] == expected
    assert result["total_fees"] == expected_fee
    assert result["completed_round_trips"] == 0


def increasing_source(start, end):
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for index in range((end - start).days):
        day = start + index * DAY
        price = 100 + (day - origin).days
        rows.append([int(day.timestamp()), price, price, price, price, 1])
    body = json.dumps(rows).encode()
    return daily_momentum.decode_candles(body, start, end), body


@pytest.fixture
def selected_study(tmp_path, monkeypatch):
    spec_path = Path("experiments/btc-usd-daily-momentum-v1.json")
    spec = json.loads(spec_path.read_bytes())
    spec_digest = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    monkeypatch.setattr(daily_momentum, "fetch_candles", increasing_source)
    selection = daily_momentum.prepare(spec_path, spec_digest, tmp_path)
    key = hashlib.sha256((daily_momentum.VERSION + spec_digest).encode()).hexdigest()
    path = tmp_path / "selections" / key / "selection.json"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return spec, selection, path, digest, tmp_path


def no_price_access(*args, **kwargs):
    pytest.fail("invalid or cached evidence must not fetch new market prices")


def test_prepare_rejects_idle_validation_even_when_cash_beats_losing_candidates(tmp_path, monkeypatch):
    spec_path = Path("experiments/btc-usd-daily-momentum-v1.json")
    digest = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    monkeypatch.setattr(daily_momentum, "fetch_candles", increasing_source)

    def simulation(candles, *, start, fast, **kwargs):
        validation = start == datetime(2026, 5, 22, tzinfo=UTC)
        return {
            "fill_count": 0 if validation and fast == 7 else 2,
            "percentage_return": Decimal(0 if fast == 7 else -1) if validation else Decimal(10),
            "maximum_drawdown": Decimal("0.01"),
        }

    monkeypatch.setattr(daily_momentum, "simulate", simulation)
    result = daily_momentum.prepare(spec_path, digest, tmp_path)
    assert result["status"] == "no_candidate_selected"
    assert result["selected_candidate"] is None
    assert result["version"] == "daily-momentum-study-v2"
    assert result["policy_version"] == daily_momentum.POLICY_VERSION
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    assert daily_momentum.prepare(spec_path, digest, tmp_path)["status"] == "no_candidate_selected"


@pytest.mark.parametrize("legacy", [False, True])
def test_invalid_selection_cannot_evaluate_even_with_cached_passing_report(
    selected_study, monkeypatch, legacy,
):
    _, selection, path, _, output = selected_study
    for row in selection["candidate_results"]:
        row["validation"]["fill_count"] = 0
        row["validation"]["percentage_return"] = Decimal(0)
    if legacy:
        selection["version"] = daily_momentum.SPEC_VERSION
        selection.pop("policy_version")
    invalid = output / "invalid-selection.json"
    digest = daily_momentum._write(invalid, selection)
    key = hashlib.sha256((digest + daily_momentum.VERSION).encode()).hexdigest()
    daily_momentum._write(output / "evaluations" / key / "report.json", {"paper_trial_eligible": True})
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    with pytest.raises(ValueError, match="legacy policy|participation and performance"):
        daily_momentum.evaluate(invalid, digest, output)
    assert path.read_bytes() != invalid.read_bytes()


def passing_scenarios():
    return [
        {
            "cost_multiplier": multiplier,
            "strategy": {
                "percentage_return": "5", "maximum_drawdown": "0.01",
                "fill_count": 2, "completed_round_trips": 1,
            },
            "buy_and_hold": {"percentage_return": "1"},
            "excess_return_pct": "4",
        }
        for multiplier in (1, 2, 3)
    ]


def test_positive_completed_trades_pass_research_but_daily_execution_remains_ineligible(selected_study):
    spec, selection, _, _, _ = selected_study
    decision = daily_momentum._promotion_decision(passing_scenarios(), spec)
    assert decision["research_gate_passed"] is True
    assert decision["research_gate_reasons"] == []
    assert decision["paper_trial_eligible"] is False
    assert decision["paper_trial_reasons"] == [daily_momentum.PAPER_UNSUPPORTED]
    review = daily_momentum.review_paper_trial(selection, {
        "version": daily_momentum.VERSION,
        "policy_version": daily_momentum.POLICY_VERSION,
        "scenarios": passing_scenarios(), **decision,
    })
    assert review["recommendation"] == "do_not_start_paper_pilot"
    assert review["reasons"] == [daily_momentum.PAPER_UNSUPPORTED]


@pytest.mark.parametrize("field,value,reason", [
    ("completed_round_trips", 0, "test_has_no_completed_round_trip"),
    ("percentage_return", "-1", "test_return_not_positive"),
    ("percentage_return", "1", "test_excess_return_below_threshold"),
    ("maximum_drawdown", "0.26", "test_drawdown_exceeds_limit"),
])
def test_research_gate_requires_each_stressed_scenario_to_pass(selected_study, field, value, reason):
    spec, _, _, _, _ = selected_study
    scenarios = passing_scenarios()
    scenarios[1]["strategy"][field] = value
    scenarios[1]["excess_return_pct"] = Decimal(scenarios[1]["strategy"]["percentage_return"]) - 1
    decision = daily_momentum._promotion_decision(scenarios, spec)
    assert decision["research_gate_passed"] is False
    assert "2x_" + reason in decision["research_gate_reasons"]
    assert decision["paper_trial_eligible"] is False


def test_evaluation_with_only_one_entry_fails_research_gate_and_cache_is_revalidated(
    selected_study, monkeypatch,
):
    _, _, path, digest, output = selected_study
    report = daily_momentum.evaluate(path, digest, output)
    assert report["version"] == daily_momentum.VERSION
    assert report["research_gate_passed"] is False
    assert "1x_test_has_no_completed_round_trip" in report["research_gate_reasons"]
    assert report["paper_trial_eligible"] is False
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    cached = daily_momentum.evaluate(path, digest, output)
    assert cached["paper_trial_eligible"] is False
    key = hashlib.sha256((digest + daily_momentum.VERSION).encode()).hexdigest()
    report_path = output / "evaluations" / key / "report.json"
    cached["paper_trial_eligible"] = True
    report_path.write_text(json.dumps(cached))
    with pytest.raises(ValueError, match="inconsistent promotion"):
        daily_momentum.evaluate(path, digest, output)


def test_evaluation_rejects_legacy_report_at_current_cache_key(selected_study, monkeypatch):
    _, _, path, digest, output = selected_study
    key = hashlib.sha256((digest + daily_momentum.VERSION).encode()).hexdigest()
    daily_momentum._write(output / "evaluations" / key / "report.json", {
        "version": daily_momentum.SPEC_VERSION, "paper_trial_eligible": True,
    })
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    with pytest.raises(ValueError, match="legacy calculations or policy"):
        daily_momentum.evaluate(path, digest, output)


@pytest.mark.parametrize("legacy_selection", [True, False])
def test_forward_rejects_legacy_evidence_before_cache_or_market_access(
    selected_study, monkeypatch, legacy_selection,
):
    spec, selection, _, _, output = selected_study
    if legacy_selection:
        selection["version"] = daily_momentum.SPEC_VERSION
        for row in selection["candidate_results"]:
            row["validation"]["fill_count"] = 0
    selection_path = output / "forward-selection.json"
    selection_digest = daily_momentum._write(selection_path, selection)
    evaluation_path = output / "legacy-evaluation.json"
    evaluation_digest = daily_momentum._write(evaluation_path, {
        "version": daily_momentum.SPEC_VERSION, "paper_trial_eligible": True,
    })
    forward_spec = {
        "version": "daily-momentum-forward-v1",
        "selection_sha256": selection_digest,
        "evaluation_sha256": evaluation_digest,
        "candidate_id": selection["selected_candidate"]["id"],
        "start": spec["ranges"]["test"]["end"],
        "end": "2026-09-19T00:00:00Z",
        "cost_multipliers": [1, 2, 3],
    }
    forward_path = output / "forward-spec.json"
    forward_digest = daily_momentum._write(forward_path, forward_spec)
    key = hashlib.sha256((forward_digest + daily_momentum.VERSION).encode()).hexdigest()
    daily_momentum._write(output / "forward" / key / "report.json", {"paper_trial_eligible": True})
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    with pytest.raises(ValueError, match="legacy policy|legacy calculations or policy"):
        daily_momentum.forward(forward_path, forward_digest, selection_path, evaluation_path, output)


def test_profitable_completed_test_trade_passes_research_gate_without_paper_promotion(
    tmp_path, monkeypatch,
):
    spec = json.loads(Path("experiments/btc-usd-daily-momentum-v1.json").read_bytes())
    spec["candidates"] = [
        {"id": "a", "fast_days": 1, "slow_days": 2},
        {"id": "b", "fast_days": 1, "slow_days": 3},
    ]
    spec_path = tmp_path / "synthetic-spec.json"
    spec_digest = daily_momentum._write(spec_path, spec)
    test_start = datetime(2026, 6, 21, tzinfo=UTC)

    def source(start, end):
        candles, _ = increasing_source(start, end)
        rows = []
        for item in candles:
            index = (item.start - test_start).days
            opening, close = item.open, item.close
            if index >= 0:
                opening, close = {
                    0: (650, 650), 1: (650, 700), 2: (710, 600), 3: (710, 600),
                    29: (600, 650),
                }.get(index, (600, 600))
            rows.append([
                int(item.start.timestamp()), int(min(opening, close)), int(max(opening, close)),
                int(opening), int(close), 1,
            ])
        body = json.dumps(rows).encode()
        return daily_momentum.decode_candles(body, start, end), body

    monkeypatch.setattr(daily_momentum, "fetch_candles", source)
    selection = daily_momentum.prepare(spec_path, spec_digest, tmp_path)
    assert selection["selected_candidate"]["id"] == "a"
    path = tmp_path / "sealed-selection.json"
    digest = daily_momentum._write(path, selection)
    report = daily_momentum.evaluate(path, digest, tmp_path)
    assert all(item["strategy"]["completed_round_trips"] == 1 for item in report["scenarios"])
    assert report["research_gate_passed"] is True
    assert report["paper_trial_eligible"] is False
    assert report["paper_trial_reasons"] == [daily_momentum.PAPER_UNSUPPORTED]
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    assert daily_momentum.evaluate(path, digest, tmp_path)["research_gate_passed"] is True


def test_forward_uses_current_policy_and_revalidates_cached_promotion(selected_study, monkeypatch):
    spec, selection, selection_path, selection_digest, output = selected_study
    evaluation = daily_momentum.evaluate(selection_path, selection_digest, output)
    evaluation_path = output / "pinned-evaluation.json"
    evaluation_digest = daily_momentum._write(evaluation_path, evaluation)
    forward_spec = {
        "version": "daily-momentum-forward-v1",
        "selection_sha256": selection_digest,
        "evaluation_sha256": evaluation_digest,
        "candidate_id": selection["selected_candidate"]["id"],
        "start": spec["ranges"]["test"]["end"],
        "end": "2026-09-19T00:00:00Z",
        "cost_multipliers": [1, 2, 3],
    }
    forward_path = output / "forward-spec.json"
    forward_digest = daily_momentum._write(forward_path, forward_spec)
    report = daily_momentum.forward(
        forward_path, forward_digest, selection_path, evaluation_path, output,
    )
    assert report["version"] == daily_momentum.FORWARD_VERSION
    assert report["policy_version"] == daily_momentum.POLICY_VERSION
    assert report["paper_trial_eligible"] is False
    monkeypatch.setattr(daily_momentum, "fetch_candles", no_price_access)
    cached = daily_momentum.forward(
        forward_path, forward_digest, selection_path, evaluation_path, output,
    )
    key = hashlib.sha256((forward_digest + daily_momentum.VERSION).encode()).hexdigest()
    report_path = output / "forward" / key / "report.json"
    cached["paper_trial_eligible"] = True
    report_path.write_text(json.dumps(cached))
    with pytest.raises(ValueError, match="inconsistent promotion"):
        daily_momentum.forward(
            forward_path, forward_digest, selection_path, evaluation_path, output,
        )
