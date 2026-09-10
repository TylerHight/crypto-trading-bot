"""Optional real-browser checks. Set DASHBOARD_CHROME to a Chrome executable.

Set DASHBOARD_LIVE_URL to also exercise a read-only snapshot from a running server.
No backtest, publication, or trading commands are invoked.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime, timedelta
from urllib.request import urlopen

import pytest
from crypto_operator_dashboard.config import DashboardSettings
from crypto_operator_dashboard.web import create_server

pytestmark = pytest.mark.skipif(
    not os.environ.get("DASHBOARD_CHROME"),
    reason="Set DASHBOARD_CHROME for browser tests",
)


def _snapshot():
    def period(count):
        segments = []
        for index in range(count):
            start = datetime(2026, 8, 1 + index * 10, tzinfo=UTC)
            trades = [
                {
                    "time": (start + timedelta(minutes=i)).isoformat(),
                    "side": "BUY" if i % 2 == 0 else "SELL",
                    "execution_price": str(70000 + i * 20),
                    "fee": "2.5",
                }
                for i in range(30)
            ]
            segments.append(
                {
                    "source_segment": {"start": start.isoformat()},
                    "chart": {
                        "equity_points": [
                            {
                                "time": trade["time"],
                                "equity": str(10000 - i * 5),
                                "position": "LONG",
                                "drawdown": str(i * 0.0005),
                            }
                            for i, trade in enumerate(trades)
                        ],
                        "trade_markers": trades,
                        "trade_marker_total": 40,
                    },
                }
            )
        return {
            "segments": segments,
            "aggregate": {
                "segment_count": count,
                "fill_count": count * 40,
                "total_fees": str(count * 100),
                "percentage_return": "-1.45",
                "maximum_drawdown": ".0145",
            },
        }

    return {
        "generated_at": "2026-09-09T12:00:00Z",
        "research": {
            "status": "no_candidate",
            "visualization": {
                "candidates": {
                    name: {"train": period(2), "validation": period(1)}
                    for name in ("sma-5-20", "sma-15-60")
                }
            },
        },
        "pilot": {"status": "not_registered"},
    }


@pytest.fixture
def chrome():
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(executable_path=os.environ["DASHBOARD_CHROME"])
        yield browser
        browser.close()


@pytest.fixture
def serve():
    servers = []

    def start(snapshot):
        class Source:
            def read(self):
                return snapshot

        server = create_server(DashboardSettings(port=0), source=Source())
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        servers.append((server, worker))
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start
    for server, worker in servers:
        server.shutdown()
        worker.join(timeout=2)
        server.server_close()


def test_tabs_trades_filters_refresh_and_small_screen(chrome, serve):
    page = chrome.new_page(viewport={"width": 1440, "height": 1000})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(serve(_snapshot()))
    assert page.locator("#panel-research").is_visible()
    assert not page.locator("#panel-system").is_visible()
    assert page.locator(".equity-series").count() == 1
    page.select_option("#research-range", "train")
    assert page.locator(".equity-series").count() == 2
    page.select_option("#research-segment", "1")
    assert page.locator(".equity-series").count() == 1
    page.select_option("#research-candidate", "sma-5-20")
    assert page.locator("#research-segment").input_value() == "all"
    page.select_option("#research-segment", "1")
    page.click("#tab-trades")
    assert page.locator(".trade-marker").count() == 30
    assert page.locator("#trade-rows tr").count() == 20
    assert (
        "30 published trade markers of 40"
        in page.locator("#trade-coverage").inner_text()
    )
    page.locator(".trade-marker").first.focus()
    assert "Price $70,000.00" in page.locator("#chart-tooltip").inner_text()
    assert "Fee $2.50" in page.locator("#chart-tooltip").inner_text()
    page.click("#trades-next")
    assert page.locator("#trade-rows tr").count() == 10
    page.select_option("#trade-side", "BUY")
    assert page.locator(".trade-marker").count() == 15
    assert "SELL" not in page.locator("#trade-rows").inner_text()
    page.fill("#trade-search", "70000")
    assert page.locator("#trade-rows tr").count() == 1
    page.fill("#trade-search", "nothing matches")
    assert page.locator("#trade-rows tr").count() == 0
    assert (
        "No published trades" in page.locator("#research-trades-chart").text_content()
    )
    page.fill("#trade-search", "")
    page.select_option("#trade-side", "all")
    page.click("#tab-research")
    page.click("summary")
    assert page.locator("details").first.get_attribute("open") is not None
    page.click("#tab-trades")
    page.select_option("#trade-side", "BUY")
    page.fill("#trade-search", "70000")
    # The old bug reloaded the entire document after 30 seconds.
    navigations = []
    page.on("framenavigated", lambda frame: navigations.append(frame.url))
    page.click("#research-segment")
    page.wait_for_timeout(32000)
    assert not navigations
    assert page.locator("#research-segment").input_value() == "1"
    assert page.locator("#research-segment").evaluate(
        "(el) => document.activeElement === el"
    )
    page.keyboard.press("Escape")
    page.click("#refresh-dashboard")
    page.wait_for_load_state()
    assert page.locator("#panel-trades").is_visible()
    assert page.locator("#research-segment").input_value() == "1"
    assert page.locator("#research-candidate").input_value() == "sma-5-20"
    assert page.locator("#research-range").input_value() == "train"
    assert page.locator("#trade-side").input_value() == "BUY"
    assert page.locator("#trade-search").input_value() == "70000"
    assert page.locator("details").first.get_attribute("open") is not None
    page.click("#tab-system")
    assert not page.locator("#study-controls").is_visible()
    page.keyboard.press("ArrowRight")
    assert page.locator("#panel-evidence").is_visible()
    page.keyboard.press("Home")
    assert page.locator("#panel-research").is_visible()
    page.set_viewport_size({"width": 390, "height": 844})
    for tab in ("research", "trades", "system", "evidence", "paper"):
        page.click(f"#tab-{tab}")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert not errors
    page.close()


def test_storage_disabled_or_invalid_does_not_break_tabs(chrome, serve):
    page = chrome.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.add_init_script("sessionStorage.setItem('research-view', 'null')")
    page.goto(serve(_snapshot()))
    page.click("#tab-trades")
    assert page.locator(".trade-marker").count() == 30
    page.close()
    page = chrome.new_page()
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.add_init_script(
        "Object.defineProperty(window, 'sessionStorage', {get() { throw Error('disabled'); }})"
    )
    page.goto(serve(_snapshot()))
    page.click("#tab-system")
    assert page.locator("#panel-system").is_visible()
    assert not errors
    page.close()


def test_missing_and_empty_evidence_do_not_break_navigation(chrome, serve):
    page = chrome.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    for snapshot in ({}, {"research": {"visualization": {"candidates": {}}}}):
        page.goto(serve(snapshot))
        for tab in ("trades", "system", "evidence", "paper", "research"):
            page.click(f"#tab-{tab}")
            assert page.locator(f"#panel-{tab}").is_visible()
    assert not errors
    page.close()


def test_live_published_snapshot(chrome, serve):
    live = os.environ.get("DASHBOARD_LIVE_URL")
    if not live:
        pytest.skip("Set DASHBOARD_LIVE_URL for saved-study browser verification")
    with urlopen(live + "/api/status", timeout=30) as response:
        snapshot = json.load(response)
    page = chrome.new_page(viewport={"width": 1440, "height": 1000})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(serve(snapshot))
    candidates = snapshot["research"]["visualization"]["candidates"]
    assert candidates, "Live source must contain a saved visual study"
    for name, periods in candidates.items():
        page.select_option("#research-candidate", name)
        for period, data in periods.items():
            if period not in ("train", "validation"):
                continue
            page.select_option("#research-range", period)
            page.click("#tab-trades")
            count = sum(
                len(item["chart"]["trade_markers"]) for item in data["segments"]
            )
            assert page.locator(".trade-marker").count() == count
            assert "NaN" not in page.locator("#panel-trades").inner_text()
            assert "undefined" not in page.locator("#panel-trades").inner_text()
            if count:
                page.locator(".trade-marker").first.focus()
                assert "Price $" in page.locator("#chart-tooltip").inner_text()
    screenshots = os.environ.get("DASHBOARD_SCREENSHOTS")
    if screenshots:
        page.select_option("#research-candidate", "sma-15-60")
        page.select_option("#research-range", "validation")
        page.screenshot(path=f"{screenshots}/trades.png", full_page=True)
        page.click("#tab-research")
        page.screenshot(path=f"{screenshots}/research.png", full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        page.screenshot(path=f"{screenshots}/mobile.png", full_page=True)
    assert not errors
    page.close()
