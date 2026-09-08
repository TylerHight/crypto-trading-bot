from __future__ import annotations

from pathlib import Path

import pytest
from crypto_operator_dashboard.config import DashboardSettings


def test_dashboard_defaults_to_loopback_and_hides_secret_fields_from_repr() -> None:
    settings = DashboardSettings(pilot_plan_path=Path("pilot.json"))

    assert settings.host == "127.0.0.1"
    assert "minioadmin" not in repr(settings)
    assert "postgresql://" not in repr(settings)


@pytest.mark.parametrize("host", ["0.0.0.0", "192.168.1.10", "example.com"])
def test_dashboard_rejects_non_loopback_bindings(host: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        DashboardSettings(host=host)


def test_dashboard_rejects_stale_threshold_shorter_than_refresh_interval() -> None:
    with pytest.raises(ValueError, match="stale threshold"):
        DashboardSettings(refresh_seconds=30, stale_after_seconds=29)
