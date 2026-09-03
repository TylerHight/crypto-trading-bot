from __future__ import annotations

from pathlib import Path

import tomllib

ROOT = Path(__file__).parents[2]
PACKAGE_PROJECTS = (
    "apps/collector/pyproject.toml",
    "apps/historical_backfill/pyproject.toml",
    "apps/trading_core/pyproject.toml",
    "packages/domain/pyproject.toml",
    "packages/exchange_adapters/pyproject.toml",
)


def test_supported_python_runtime_is_consistent_across_build_and_ci() -> None:
    runtime = (ROOT / ".python-version").read_text(encoding="utf-8").strip()

    assert runtime in {"3.11", "3.12"}
    assert (ROOT / "Dockerfile").read_text(encoding="utf-8").startswith(
        f"FROM python:{runtime}-slim\n"
    )
    assert f'python-version: "{runtime}"' in (
        ROOT / ".github/workflows/ci.yml"
    ).read_text(encoding="utf-8")

    for project_path in PACKAGE_PROJECTS:
        project = tomllib.loads((ROOT / project_path).read_text(encoding="utf-8"))
        assert project["project"]["requires-python"] == ">=3.11,<3.13"
