from __future__ import annotations

import html
import json
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
    return "status-" + "".join(character for character in status if character.isalnum() or character == "-")


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
        f'<button type="button" data-copy={encoded}>Copy</button></div>'
    )


def _artifact_card(artifact: Mapping[str, Any]) -> str:
    return "".join(
        (
            '<article class="card">',
            f"<h3>{_escape(artifact.get('name'))} {_status_badge(artifact.get('status'))}</h3>",
            f"<p class=\"muted\">{_escape(artifact.get('source'))}</p>",
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
    research_status = _text(research.get("status")) if isinstance(research, Mapping) else "unavailable"
    pilot = snapshot.get("pilot")
    pilot_status = _text(pilot.get("status")) if isinstance(pilot, Mapping) else "unavailable"
    collector_status = _text(collector.get("status"))
    raw_status = _text(raw_archive.get("status"))
    stale_artifacts = [item for item in trusted_items if item.get("status") == "stale"]
    oos = research.get("oos") if isinstance(research, Mapping) else None
    research_detail = "No saved result"
    if isinstance(oos, Mapping):
        research_detail = (
            f"{_candidate_name(oos.get('candidate'))}: "
            f"{_percentage(oos.get('strategy_return'))} vs "
            f"{_percentage(oos.get('buy_and_hold_return'))}"
        )
    actions: list[str] = []
    if collector_status in {"unavailable", "missing", "invalid", "failed"}:
        actions.append("Fix the live feed first.")
    elif raw_status == "stale":
        actions.append("Check the raw sink. It needs a new archive.")
    if stale_artifacts:
        actions.append("Then publish new audit, trade, and candle reports.")
    if pilot_status == "not_registered":
        actions.append("Then decide: start the paper trial or stop here.")
    action_list = "".join(f"<li>{_escape(action)}</li>" for action in actions)
    if not action_list:
        action_list = "<li>Nothing needs attention now.</li>"
    collector_detail = _text(collector.get("detail"))
    raw_detail = f"Latest archive: {_short_date(raw_archive.get('observed_at'))}"
    research_label = "Saved result" if research_status in {"current", "stale"} else research_status
    research_state = "historical" if research_label == "Saved result" else research_status
    pilot_label = "Not started" if pilot_status == "not_registered" else pilot_status
    return "".join(
        (
            '<section class="operator-summary">',
            "<h2>What to do next</h2>",
            f'<ol class="summary-actions">{action_list}</ol>',
            '<div class="overview-grid">',
            _overview_card("Live feed", collector_status, collector_status, collector_detail),
            _overview_card("Data files", "Needs update" if raw_status == "stale" else raw_status, raw_status, raw_detail),
            _overview_card("Research", research_label, research_state, research_detail),
            _overview_card("Paper trial", pilot_label, pilot_status, "Approval required" if pilot_status == "not_registered" else "Forward test"),
            "</div>",
            "</section>",
        )
    )


def _research_view(research: Mapping[str, Any]) -> str:
    selection = research.get("selection")
    evaluation = research.get("evaluation")
    detail_cards = ""
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
    details = (
        '<details class="disclosure"><summary>Show research details and publication identities</summary>'
        f'<div class="grid">{detail_cards}</div></details>'
        if detail_cards
        else ""
    )
    return (
        f"<p>{_escape(research.get('explanation'))}</p>"
        f"<p>Research status: {_status_badge(research.get('status'))}</p>"
        '<article class="card research-summary"><h3>Out-of-sample comparison</h3>'
        f"{_details(summary)}</article>"
        f"{details}"
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
                "<article class=\"card\"><h3>Draft pre-registered plan</h3>",
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
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{refresh_seconds}">
  <title>Crypto platform operator dashboard</title>
  <style>
    :root {{ color-scheme: light dark; font-family: system-ui, sans-serif; }}
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
  </style>
</head>
<body>
  <main>
    <h1>Crypto platform operator dashboard</h1>
    <p class="muted">Read only. No real orders.</p>
    <p class="muted">Updated: {_escape(snapshot.get('generated_at'))}. Refreshes every {refresh_seconds} seconds.</p>
    {_operator_summary(snapshot)}
    <details class="disclosure section-details"><summary>Pipeline details</summary>{_pipeline_table(pipeline_items)}</details>
    <details class="disclosure section-details"><summary>Data details</summary><p class="muted">Old means the saved evidence is older than the freshness window. It may still be valid.</p><div class="grid">{''.join(_artifact_card(item) for item in artifacts)}</div></details>
    <details class="disclosure section-details"><summary>Research details</summary>{_research_view(research)}</details>
    <details class="disclosure section-details"><summary>Paper trial details</summary>{_pilot_view(pilot, draft_plan)}</details>
  </main>
  <script>
    document.querySelectorAll('[data-copy]').forEach((button) => button.addEventListener('click', () => {{
      navigator.clipboard?.writeText(button.dataset.copy || '');
      button.textContent = 'Copied';
    }}));
  </script>
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
            snapshot = service.snapshot()
            if path == "/":
                body = render_dashboard(snapshot, settings.refresh_seconds).encode("utf-8")
                self._send(HTTPStatus.OK, "text/html; charset=utf-8", body)
                return
            if path == "/api/status":
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
