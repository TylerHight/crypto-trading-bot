from __future__ import annotations

import html
import json
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import DashboardSettings
from .sources import DashboardSource, LiveDashboardSource


def _text(value: object) -> str:
    if value is None or value == "":
        return "—"
    return str(value)


def _escape(value: object) -> str:
    return html.escape(_text(value))


def _status_class(value: object) -> str:
    status = _text(value).lower().replace("_", "-")
    return "status-" + "".join(
        character for character in status if character.isalnum() or character == "-"
    )


def _status_badge(value: object) -> str:
    label = _text(value).replace("_", " ").capitalize()
    return f'<span class="status {_status_class(value)}">{_escape(label)}</span>'


def _label_badge(label: str, state: str) -> str:
    return f'<span class="status {_status_class(state)}">{_escape(label)}</span>'


def _details(details: Mapping[str, object] | None) -> str:
    if not details:
        return ""
    rows = "".join(
        f"<dt>{_escape(key)}</dt><dd>{_escape(value)}</dd>" for key, value in details.items()
    )
    return f'<dl class="details">{rows}</dl>'


def _copyable(label: str, value: object) -> str:
    if value is None:
        return ""
    text = _text(value)
    escaped = _escape(text)
    encoded = html.escape(text, quote=True)
    return (
        f'<div class="copyable"><span>{_escape(label)}</span><code>{escaped}</code>'
        f'<button type="button" data-copy="{encoded}">Copy</button></div>'
    )


def _artifact_card(artifact: Mapping[str, Any]) -> str:
    return "".join(
        (
            '<article class="card">',
            f"<h3>{_escape(artifact.get('name'))} {_status_badge(artifact.get('status'))}</h3>",
            f'<p class="muted">{_escape(artifact.get("source"))}</p>',
            f"<p>Observed: {_escape(artifact.get('observed_at'))}</p>",
            _details(artifact.get("details")),
            _copyable("Canonical URI", artifact.get("uri")),
            _copyable("SHA-256", artifact.get("sha256")),
            "</article>",
        )
    )


def _pipeline_table(items: list[Mapping[str, Any]]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{_escape(item.get('name'))}</td>"
        f"<td>{_status_badge(item.get('status'))}</td>"
        f"<td>{_escape(item.get('observed_at'))}</td>"
        f"<td>{_escape(item.get('detail'))}</td>"
        "</tr>"
        for item in items
    )
    return (
        "<table><thead><tr><th>Component</th><th>Status</th><th>Observed</th>"
        "<th>Evidence</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def _named_item(items: list[Mapping[str, Any]], name: str) -> Mapping[str, Any]:
    return next((item for item in items if item.get("name") == name), {})


def _short_date(value: object) -> str:
    text = _text(value)
    return text.split("T", maxsplit=1)[0] if text != "—" else text


def _candidate_name(value: object) -> str:
    if isinstance(value, Mapping):
        value = value.get("candidate_id")
    candidate_id = _text(value)
    parts = candidate_id.split("-")
    if len(parts) == 3 and parts[0].lower() == "sma":
        return f"SMA {parts[1]}/{parts[2]}"
    return candidate_id


def _percentage(value: object) -> str:
    try:
        return f"{float(_text(value)):.2f}%"
    except ValueError:
        return _text(value)


def _overview_card(title: str, label: str, state: str, detail: str) -> str:
    return "".join(
        (
            '<article class="overview-card">',
            f"<h3>{_escape(title)}</h3>",
            _label_badge(label, state),
            f"<p>{_escape(detail)}</p>",
            "</article>",
        )
    )


def _operator_summary(snapshot: Mapping[str, Any]) -> str:
    pipeline = snapshot.get("pipeline")
    pipeline_items = pipeline if isinstance(pipeline, list) else []
    collector = _named_item(pipeline_items, "Collector feed")
    raw_archive = _named_item(pipeline_items, "Raw archive")
    trusted_data = snapshot.get("trusted_data")
    trusted_items = (
        [item for item in trusted_data.values() if isinstance(item, Mapping)]
        if isinstance(trusted_data, Mapping)
        else []
    )
    research = snapshot.get("research")
    research_status = (
        _text(research.get("status")) if isinstance(research, Mapping) else "unavailable"
    )
    history = snapshot.get("history")
    history_status = _text(history.get("status")) if isinstance(history, Mapping) else "missing"
    pilot = snapshot.get("pilot")
    pilot_status = _text(pilot.get("status")) if isinstance(pilot, Mapping) else "unavailable"
    collector_status = _text(collector.get("status"))
    raw_status = _text(raw_archive.get("status"))
    stale_artifacts = [item for item in trusted_items if item.get("status") == "stale"]
    oos = research.get("oos") if isinstance(research, Mapping) else None
    research_detail = "No saved result"
    if isinstance(research, Mapping) and research.get("explanation"):
        research_detail = _text(research.get("explanation"))
    if isinstance(oos, Mapping):
        research_detail = (
            f"{_candidate_name(oos.get('candidate'))}: "
            f"{_percentage(oos.get('strategy_return'))} vs "
            f"{_percentage(oos.get('buy_and_hold_return'))} buy-and-hold. "
            f"Difference: {_percentage(oos.get('excess_return'))}."
        )
    actions: list[str] = []
    if collector_status in {"unavailable", "missing", "invalid", "failed"}:
        actions.append("Fix the live feed first.")
    elif raw_status == "stale":
        actions.append("Check the raw sink. It needs a new archive.")
    if stale_artifacts:
        actions.append("Then publish new audit, trade, and candle reports.")
    if pilot_status == "not_registered":
        if isinstance(research, Mapping) and research.get("recommendation"):
            actions.append(_text(research.get("recommendation")))
            if research_status == "policy_review_required":
                actions.append("Review the gap-safe policy before strategy selection.")
            elif research_status == "gap_policy_approved":
                actions.append("Implement and validate the gap-aware selection engine.")
            elif research_status == "inconclusive":
                if history_status == "gaps_found":
                    actions.append(
                        "Define and test how strategy research should handle the five Coinbase outage minutes."
                    )
                elif history_status == "ready":
                    actions.append("Create a new fixed experiment using the historical dataset.")
                else:
                    actions.append(
                        "Prepare 90 days of BTC history, then run a new fixed experiment."
                    )
        else:
            actions.append("Review strategy evidence before considering a paper trial.")
    action_list = "".join(f"<li>{_escape(action)}</li>" for action in actions)
    if not action_list:
        action_list = "<li>Nothing needs attention now.</li>"
    collector_detail = _text(collector.get("detail"))
    raw_detail = f"Latest archive: {_short_date(raw_archive.get('observed_at'))}"
    research_label = "Saved result" if research_status in {"current", "stale"} else research_status
    research_label = {
        "inconclusive": "Not enough data",
        "policy_review_required": "Policy review needed",
        "gap_policy_approved": "Policy approved",
        "gap_policy_rejected": "Policy rejected",
        "no_candidate": "No strategy selected",
        "evaluated": "Test complete",
        "segmented_evaluated": "Segmented research complete",
    }.get(research_status, research_label)
    research_state = "historical" if research_label == "Saved result" else research_status
    pilot_label = "Not started" if pilot_status == "not_registered" else pilot_status
    history_detail = (
        _text(history.get("message")) if isinstance(history, Mapping) else "No historical dataset"
    )
    history_label = {"ready": "Ready", "gaps_found": "5 source gaps"}.get(
        history_status, history_status
    )
    return "".join(
        (
            '<section class="operator-summary">',
            "<h2>What to do next</h2>",
            f'<ol class="summary-actions">{action_list}</ol>',
            '<div class="overview-grid">',
            _overview_card("Live feed", collector_status, collector_status, collector_detail),
            _overview_card(
                "Data files",
                "Needs update" if raw_status == "stale" else raw_status,
                raw_status,
                raw_detail,
            ),
            _overview_card("Historical data", history_label, history_status, history_detail),
            _overview_card("Research", research_label, research_state, research_detail),
            _overview_card(
                "Paper trial",
                pilot_label,
                pilot_status,
                "Approval required" if pilot_status == "not_registered" else "Forward test",
            ),
            "</div>",
            "</section>",
        )
    )


def _study_controls(research: Mapping[str, Any]) -> str:
    visualization = research.get("visualization")
    candidates = visualization.get("candidates", {}) if isinstance(visualization, Mapping) else {}
    if not candidates:
        return '<p class="muted">No chart data was published for this study.</p>'
    options = "".join(
        f'<option value="{_escape(candidate_id)}">{_escape(_candidate_name(candidate_id))}</option>'
        for candidate_id in sorted(candidates)
    )
    return f"""<label>Strategy<select id="research-candidate">{options}</select></label>
<label>Period<select id="research-range"><option value="train">Training</option><option value="validation" selected>Validation</option></select></label>
<label>Data segment<select id="research-segment"></select></label>"""


def _research_charts(research: Mapping[str, Any]) -> str:
    visualization = research.get("visualization")
    if not isinstance(visualization, Mapping) or not visualization.get("candidates"):
        return ""
    return """<div id="research-metrics" class="metrics"></div>
<article class="card"><h3>Account value</h3>
<p class="muted">Simulated money over time. Each line starts a new account after a data gap.</p>
<div class="chart-scroll"><svg id="research-equity-chart" class="chart" role="img" aria-label="Separate simulated account-value lines"></svg></div>
<p id="research-chart-summary" class="muted"></p>
<details><summary>About this chart</summary><p id="equity-sampling" class="muted"></p></details></article>
<article class="card"><h3>Strategy comparison</h3>
<div class="chart-scroll"><svg id="research-comparison-chart" class="chart" role="img" aria-label="Candidate returns and drawdowns"></svg></div>
<p class="muted">Return: gain or loss. Drawdown: largest drop from a peak. The dashed line is the study's 20% limit.</p></article>"""


def _trades_view() -> str:
    return """<h2>Trades</h2><p>Where the simulation bought and sold BTC. These are not real orders.</p>
<article class="card"><h3>Buy and sell prices</h3>
<p><span class="buy">▲ Buy</span> &nbsp; <span class="sell">▼ Sell</span> <span class="muted">· Hover or focus a marker for details.</span></p>
<p id="trade-coverage" class="muted">No trade data was published for this study.</p>
<div class="chart-controls"><label>Show<select id="trade-side"><option value="all">Buys and sells</option><option value="BUY">Buys only</option><option value="SELL">Sells only</option></select></label>
<label>Find a trade<input id="trade-search" type="search" placeholder="Date (YYYY-MM-DD) or price"></label></div>
<div class="chart-scroll"><svg id="research-trades-chart" class="chart" role="img" aria-label="Buy and sell execution prices over time"></svg></div>
<div class="table-scroll"><table><caption class="sr-only">Published simulated trades</caption><thead><tr><th>Time (UTC)</th><th>Side</th><th>Price</th><th>Fee</th><th>Segment</th></tr></thead><tbody id="trade-rows"></tbody></table></div>
<div class="pagination"><button id="trades-prev" disabled>Previous</button><span id="trade-page" aria-live="polite">No trades to display</span><button id="trades-next" disabled>Next</button></div>
</article>"""


def _research_view(research: Mapping[str, Any], *, evidence_only: bool = False) -> str:
    selection = research.get("selection")
    evaluation = research.get("evaluation")
    detail_cards = ""
    publication = research.get("publication")
    if isinstance(publication, Mapping):
        detail_cards += _artifact_card(publication)
    coverage = research.get("coverage")
    if isinstance(coverage, Mapping):
        detail_cards += (
            '<article class="card"><h3>History coverage</h3>'
            + _details(
                {
                    "Available minutes": coverage.get("available_minutes"),
                    "Required minutes": coverage.get("expected_minutes"),
                    "Missing minutes": coverage.get("missing_minutes"),
                }
            )
            + "</article>"
        )
    gap_policy = research.get("gap_policy")
    segment_counts = research.get("segment_counts")
    if isinstance(segment_counts, Mapping):
        detail_cards += (
            '<article class="card"><h3>Source segments</h3>'
            + _details(
                {
                    "Train": segment_counts.get("train"),
                    "Validation": segment_counts.get("validation"),
                    "Test": segment_counts.get("test"),
                    "Meaning": "Each segment resets SMA, orders, and simulated cash.",
                }
            )
            + "</article>"
        )
    valid_minutes = research.get("policy_valid_minutes_by_range")
    if isinstance(gap_policy, Mapping):
        detail_cards += (
            '<article class="card"><h3>Gap-safe policy</h3>'
            + _details(
                {
                    "Approval": gap_policy.get("approval_status"),
                    "Missing candles": gap_policy.get("missing_candle_action"),
                    "Indicators": gap_policy.get("indicator_action"),
                    "Selection-valid minutes": (
                        valid_minutes.get("selection")
                        if isinstance(valid_minutes, Mapping)
                        else None
                    ),
                    "Test-valid minutes": (
                        valid_minutes.get("test") if isinstance(valid_minutes, Mapping) else None
                    ),
                }
            )
            + "</article>"
        )
    review = research.get("review")
    if isinstance(review, Mapping):
        detail_cards += (
            '<article class="card"><h3>Human review</h3>'
            + _details(
                {
                    "Decision": review.get("decision"),
                    "Reviewer": review.get("reviewer"),
                    "Time": review.get("decided_at"),
                    "Reason": review.get("note"),
                }
            )
            + "</article>"
        )
    if isinstance(selection, Mapping):
        detail_cards += _artifact_card(selection)
    if isinstance(evaluation, Mapping):
        detail_cards += _artifact_card(evaluation)
    selection_summary = research.get("selection_summary")
    if isinstance(selection_summary, Mapping):
        detail_cards += "".join(
            (
                '<article class="card">',
                "<h3>Selection evidence</h3>",
                _details(
                    {
                        "Selected candidate": selection_summary.get("candidate"),
                        "Train range": selection_summary.get("train_range"),
                        "Validation range": selection_summary.get("validation_range"),
                        "Test range": selection_summary.get("test_range"),
                        "Test accessed before selection": selection_summary.get(
                            "test_data_accessed_before_selection"
                        ),
                    }
                ),
                "</article>",
            )
        )
    oos = research.get("oos")
    summary = {
        "Selected candidate": oos.get("candidate") if isinstance(oos, Mapping) else None,
        "Strategy return": oos.get("strategy_return") if isinstance(oos, Mapping) else None,
        "Buy-and-hold return": oos.get("buy_and_hold_return") if isinstance(oos, Mapping) else None,
        "Excess return": oos.get("excess_return") if isinstance(oos, Mapping) else None,
    }
    if evidence_only:
        return (
            f'<div class="grid">{detail_cards}</div>'
            if detail_cards
            else "<p>No evidence available.</p>"
        )
    comparison = (
        '<article class="card"><h3>Final test results</h3>' + _details(summary) + "</article>"
        if isinstance(oos, Mapping)
        else ""
    )
    if research.get("status") == "no_candidate":
        return (
            '<article class="card"><h3>No strategy passed</h3>'
            "<p>The strategies missed the study rules. No strategy advanced to the final test.</p>"
            '<p class="muted">Next: review the losses below, then decide whether to retire or replace this strategy idea.</p></article>'
            + _research_charts(research)
        )
    return (
        f"<p>{_escape(research.get('explanation'))}</p>"
        f"<p>{_escape(research.get('recommendation', ''))}</p>"
        f"<p>Research status: {_status_badge(research.get('status'))}</p>"
        f"{comparison}"
        f"{_research_charts(research)}"
    )


def _pilot_view(pilot: Mapping[str, Any], draft_plan: Mapping[str, Any]) -> str:
    status = _text(pilot.get("status"))
    if status == "not_registered":
        plan = _details(draft_plan.get("details") if isinstance(draft_plan, Mapping) else None)
        digest = _copyable(
            "Draft plan SHA-256",
            draft_plan.get("raw_sha256") if isinstance(draft_plan, Mapping) else None,
        )
        return "".join(
            (
                f"<p>{_status_badge(status)} {_escape(pilot.get('message'))}</p>",
                "<p>No balance, fills, or assessment are shown because no pilot exists.</p>",
                '<article class="card"><h3>Draft pre-registered plan</h3>',
                f"{_status_badge(draft_plan.get('status')) if isinstance(draft_plan, Mapping) else ''}",
                plan,
                digest,
                "</article>",
            )
        )
    return "".join(
        (
            f"<p>Pilot status: {_status_badge(status)}</p>",
            _details(
                {
                    "Pilot ID": pilot.get("pilot_id"),
                    "Session state": pilot.get("session_state"),
                    "Created": pilot.get("created_at"),
                    "Last update": pilot.get("updated_at"),
                    "Assessment verdict": pilot.get("assessment_verdict"),
                }
            ),
            "<h3>Forward metrics</h3>",
            _details(pilot.get("metrics") if isinstance(pilot.get("metrics"), Mapping) else None),
            "<h3>Precommitted criteria</h3>",
            _details(pilot.get("plan") if isinstance(pilot.get("plan"), Mapping) else None),
            _copyable("Pilot plan SHA-256", pilot.get("plan_raw_sha256")),
        )
    )


def render_dashboard(snapshot: Mapping[str, Any], refresh_seconds: int) -> str:
    pipeline = snapshot.get("pipeline")
    pipeline_items = pipeline if isinstance(pipeline, list) else []
    trusted_data = snapshot.get("trusted_data")
    artifacts = (
        [item for item in trusted_data.values() if isinstance(item, Mapping)]
        if isinstance(trusted_data, Mapping)
        else []
    )
    research_value = snapshot.get("research")
    research: Mapping[str, Any] = research_value if isinstance(research_value, Mapping) else {}
    pilot_value = snapshot.get("pilot")
    pilot: Mapping[str, Any] = pilot_value if isinstance(pilot_value, Mapping) else {}
    draft_plan_value = snapshot.get("draft_plan")
    draft_plan: Mapping[str, Any] = (
        draft_plan_value if isinstance(draft_plan_value, Mapping) else {}
    )
    visualization = research.get("visualization")
    chart_json = (
        json.dumps(visualization, default=str, separators=(",", ":"))
        if isinstance(visualization, Mapping)
        else "{}"
    )
    chart_json = chart_json.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Crypto platform operator dashboard</title>
  <style>
    :root {{ color-scheme: dark; font-family: system-ui, sans-serif; }}
    body {{ background: #10151f; color: #e8edf5; margin: 0; }}
    main {{ margin: auto; max-width: 1200px; padding: 2rem; }}
    h1 {{ margin-bottom: .25rem; }} h2 {{ margin-top: 1.5rem; }}
    .muted {{ color: #aab6c7; }} .grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }}
    .card {{ background: #1a2230; border: 1px solid #344258; border-radius: .6rem; padding: 1rem; }}
    table {{ border-collapse: collapse; width: 100%; }} th, td {{ border-bottom: 1px solid #344258; padding: .65rem; text-align: left; vertical-align: top; }}
    .status {{ border-radius: 999px; display: inline-block; font-size: .8rem; font-weight: 650; padding: .15rem .5rem; text-transform: uppercase; }}
    .status-healthy, .status-current, .status-available, .status-running, .status-draft {{ background: #164e35; color: #c8ffe0; }}
    .status-stale, .status-missing, .status-not-registered {{ background: #684a13; color: #fff0b8; }}
    .status-unavailable, .status-invalid, .status-failed {{ background: #702437; color: #ffd7df; }}
    dl.details {{ display: grid; gap: .3rem .75rem; grid-template-columns: minmax(9rem, auto) 1fr; }} dt {{ color: #aab6c7; }} dd {{ margin: 0; overflow-wrap: anywhere; }}
    .copyable {{ margin-top: .55rem; }} .copyable span {{ color: #aab6c7; display: block; font-size: .85rem; }} code {{ overflow-wrap: anywhere; }} button {{ margin-left: .5rem; }}
    .operator-summary {{ background: #172d47; border: 1px solid #3d6d99; border-radius: .6rem; margin: 1.25rem 0; padding: 1rem; }}
    .operator-summary h2 {{ margin: 0 0 .5rem; }} .summary-actions {{ margin: 0 0 1rem; padding-left: 1.3rem; }} .summary-actions li + li {{ margin-top: .35rem; }}
    .overview-grid {{ display: grid; gap: .75rem; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); }}
    .overview-card {{ background: #1a2230; border: 1px solid #344258; border-radius: .5rem; padding: .8rem; }} .overview-card h3 {{ font-size: .95rem; margin: 0 0 .45rem; }} .overview-card p {{ color: #aab6c7; margin: .55rem 0 0; }}
    .disclosure {{ background: #1a2230; border: 1px solid #344258; border-radius: .6rem; margin-top: 1rem; padding: .8rem 1rem; }}
    .disclosure summary {{ cursor: pointer; font-weight: 650; }} .disclosure .grid {{ margin-top: 1rem; }}
    .status-historical {{ background: #254667; color: #cce9ff; }}
    * {{ box-sizing: border-box; }} [hidden] {{ display: none !important; }}
    main {{ max-width: 1280px; padding: 1.5rem 2rem; }}
    h1 {{ font-size: 1.6rem; margin: 0; }} h3 {{ margin-top: 0; }}
    p {{ line-height: 1.5; }} .card {{ margin: 1rem 0; min-width: 0; }}
    .page-header {{ display: flex; align-items: center; justify-content: space-between; gap: 1rem; }}
    .page-header p {{ margin: .3rem 0; }} .snapshot {{ text-align: right; font-size: .8rem; }}
    button, select, input {{ font: inherit; padding: .6rem .8rem; border: 1px solid #42536a; border-radius: .4rem; background: #1a283b; color: #e8edf5; }}
    button {{ cursor: pointer; margin: 0; }} button:disabled {{ opacity: .4; cursor: default; }}
    :focus-visible {{ outline: 2px solid #69b6ff; outline-offset: 3px; }}
    .tabs {{ display: flex; gap: .3rem; margin: 1.5rem 0; border-bottom: 1px solid #344258; overflow-x: auto; padding-bottom: .3rem; }}
    .tabs button {{ background: transparent; border: 0; border-radius: .3rem .3rem 0 0; padding: .8rem 1.2rem; white-space: nowrap; }}
    .tabs [aria-selected="true"] {{ background: #203956; color: #b8dcff; box-shadow: inset 0 -3px #69b6ff; }}
    .chart-controls {{ display: flex; flex-wrap: wrap; gap: 1rem; margin: 1rem 0; }}
    .chart-controls label {{ display: grid; gap: .4rem; font-size: .85rem; color: #aab6c7; }}
    .chart-controls input {{ width: 260px; max-width: 100%; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1rem 0; }}
    .metric {{ background: #1a2230; padding: 1rem; border: 1px solid #344258; border-radius: .6rem; }}
    .metric span {{ display: block; color: #aab6c7; font-size: .85rem; }}
    .metric strong {{ display: block; font-size: 1.7rem; margin-top: .5rem; }}
    .chart-scroll, .table-scroll {{ overflow-x: auto; max-width: 100%; }}
    #panel-trades .table-scroll {{ max-height: 360px; margin-top: 1rem; }}
    #panel-trades thead {{ position: sticky; top: 0; background: #1a2230; }}
    .chart {{ background: #10151f; border-radius: .4rem; display: block; width: 100%; min-width: 650px; }}
    .trade-marker {{ cursor: crosshair; }} .trade-marker:hover, .trade-marker:focus {{ stroke: white; stroke-width: 3; }}
    .buy {{ color: #6ee7b7; }} .sell {{ color: #ff8e9d; }}
    .pagination {{ display: flex; align-items: center; justify-content: space-between; gap: .5rem; margin-top: 1rem; font-size: .85rem; }}
    #chart-tooltip {{ position: fixed; bottom: 1rem; left: 50%; transform: translateX(-50%); background: #263e5c; padding: .8rem 1rem; border: 1px solid #69b6ff; border-radius: .5rem; max-width: 95vw; z-index: 10; pointer-events: none; }}
    summary {{ cursor: pointer; color: #b8dcff; }} .sr-only {{ position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }}
    @media(max-width: 650px) {{ main {{ padding: 1rem; }} .page-header {{ display: block; }} .snapshot {{ text-align: left; margin-top: 1rem; }} .metrics {{ grid-template-columns: repeat(2, 1fr); gap: .5rem; }} .metric strong {{ font-size: 1.3rem; }} .tabs button {{ padding: .7rem; }} .grid {{ grid-template-columns: minmax(0, 1fr); }} dl.details {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <main>
    <header class="page-header"><div><h1>Research workspace</h1><p class="muted">Read only. No real orders.</p></div>
    <div class="snapshot"><button id="refresh-dashboard">Refresh snapshot</button><p class="muted">Snapshot: {_escape(snapshot.get("generated_at"))}<br>Manual refresh · Your view stays put.</p></div></header>
    <nav class="tabs" role="tablist" aria-label="Dashboard pages">
      <button id="tab-research" role="tab" data-tab="research" aria-controls="panel-research" aria-selected="true">Research</button>
      <button id="tab-trades" role="tab" data-tab="trades" aria-controls="panel-trades" aria-selected="false" tabindex="-1">Trades</button>
      <button id="tab-system" role="tab" data-tab="system" aria-controls="panel-system" aria-selected="false" tabindex="-1">System</button>
      <button id="tab-evidence" role="tab" data-tab="evidence" aria-controls="panel-evidence" aria-selected="false" tabindex="-1">Evidence</button>
      <button id="tab-paper" role="tab" data-tab="paper" aria-controls="panel-paper" aria-selected="false" tabindex="-1">Paper trial</button>
    </nav>
    <div id="study-controls" class="chart-controls">{_study_controls(research)}</div>
    <section id="panel-research" role="tabpanel" aria-labelledby="tab-research"><h2>Study results</h2>{_research_view(research)}</section>
    <section id="panel-trades" role="tabpanel" aria-labelledby="tab-trades" hidden>{_trades_view()}</section>
    <section id="panel-system" role="tabpanel" aria-labelledby="tab-system" hidden>
    {_operator_summary(snapshot)}<h2>Pipeline</h2><div class="table-scroll">{_pipeline_table(pipeline_items)}</div>
    <h2>Data publications</h2><p class="muted">Stale means a file has not been updated recently. Saved research does not need to be fresh.</p><div class="grid">{"".join(_artifact_card(item) for item in artifacts)}</div></section>
    <section id="panel-evidence" role="tabpanel" aria-labelledby="tab-evidence" hidden><h2>Study evidence</h2><p class="muted">Source files, approval records, and checksums behind these results.</p>{_research_view(research, evidence_only=True)}</section>
    <section id="panel-paper" role="tabpanel" aria-labelledby="tab-paper" hidden><h2>Paper trial</h2>{_pilot_view(pilot, draft_plan)}</section>
    <div id="chart-tooltip" role="status" hidden></div>
    <noscript>This dashboard needs JavaScript for tabs and charts. The read-only snapshot is available at <a href="/api/status">/api/status</a>.</noscript>
  </main>
  <script id="research-visualization" type="application/json">{chart_json}</script>
  <script src="/assets/dashboard.js" defer></script>
</body>
</html>"""


class DashboardService:
    def __init__(self, source: DashboardSource) -> None:
        self._source = source

    def snapshot(self) -> dict[str, Any]:
        try:
            return self._source.read()
        except Exception:  # noqa: BLE001 - source failures render a safe unavailable page.
            return {
                "draft_plan": {"status": "unavailable"},
                "generated_at": None,
                "next_action": {
                    "action": "Dashboard sources could not be read. Use the local pipeline runbook.",
                    "runbook": "docs/runbooks/local-market-data-pipeline.md",
                },
                "pilot": {"status": "unavailable"},
                "pipeline": [],
                "research": {
                    "explanation": "Out-of-sample results are evidence, not a profitability claim.",
                    "status": "unavailable",
                },
                "trusted_data": {},
            }


def create_server(
    settings: DashboardSettings,
    *,
    source: DashboardSource | None = None,
) -> ThreadingHTTPServer:
    service = DashboardService(source or LiveDashboardSource(settings))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/assets/dashboard.js":
                body = Path(__file__).with_name("dashboard.js").read_bytes()
                self._send(HTTPStatus.OK, "text/javascript; charset=utf-8", body)
                return
            if path == "/":
                snapshot = service.snapshot()
                body = render_dashboard(snapshot, settings.refresh_seconds).encode("utf-8")
                self._send(HTTPStatus.OK, "text/html; charset=utf-8", body)
                return
            if path == "/api/status":
                snapshot = service.snapshot()
                body = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
                self._send(HTTPStatus.OK, "application/json; charset=utf-8", body)
                return
            self._send(HTTPStatus.NOT_FOUND, "text/plain; charset=utf-8", b"Not found\n")

        def do_POST(self) -> None:
            self._method_not_allowed()

        def do_PUT(self) -> None:
            self._method_not_allowed()

        def do_PATCH(self) -> None:
            self._method_not_allowed()

        def do_DELETE(self) -> None:
            self._method_not_allowed()

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Allow", "GET, OPTIONS")
            self.end_headers()

        def _method_not_allowed(self) -> None:
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.send_header("Allow", "GET, OPTIONS")
            self.end_headers()

        def _send(self, status: HTTPStatus, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return None

    return ThreadingHTTPServer((settings.host, settings.port), Handler)
