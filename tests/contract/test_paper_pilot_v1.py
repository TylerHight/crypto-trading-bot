import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).parents[2]
SCHEMA_ROOT = ROOT / "apps" / "trading_core" / "src" / "crypto_trading_core"


def test_example_pilot_plan_matches_checked_in_v1_schema() -> None:
    schema = json.loads((SCHEMA_ROOT / "pilot_plan.schema.json").read_text())
    plan = json.loads((ROOT / "pilots" / "pilot-plan.example.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(plan)


def test_assessment_schema_requires_safe_simulation_identity() -> None:
    schema = json.loads((SCHEMA_ROOT / "pilot_assessment.schema.json").read_text())
    assessment = {
        "assessment_policy_version": "paper-pilot-assessment-v1",
        "criteria": {},
        "eligibility": "eligible_for_execution_design_review",
        "execution_mode": "paper_simulation",
        "finalized_at": "2026-10-15T00:00:00Z",
        "live_trading_enabled": False,
        "metrics": {},
        "operator": {"note": "Reviewed evidence", "reviewed_by": "operator"},
        "pilot_id": "a" * 64,
        "session_id": "b" * 64,
        "terminal_stop": False,
        "verdict": "pass",
    }
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(assessment)
