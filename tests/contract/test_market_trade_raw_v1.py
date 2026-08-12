import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIRECTORY = REPOSITORY_ROOT / "schemas" / "events" / "market.trade.raw.v1"


def validator() -> Draft202012Validator:
    schema = json.loads(
        (CONTRACT_DIRECTORY / "schema.json").read_text(encoding="utf-8")
    )
    return Draft202012Validator(schema, format_checker=FormatChecker())


def valid_fixture() -> dict[str, object]:
    return json.loads(
        (CONTRACT_DIRECTORY / "valid-example.json").read_text(encoding="utf-8")
    )


def test_valid_fixture_matches_json_schema() -> None:
    errors = sorted(
        validator().iter_errors(valid_fixture()),
        key=lambda error: error.json_path,
    )

    assert errors == []


def test_schema_rejects_unknown_envelope_field() -> None:
    fixture = valid_fixture()
    fixture["unexpected"] = "mistake"

    assert list(validator().iter_errors(fixture))


def test_schema_rejects_naive_timestamp() -> None:
    fixture = valid_fixture()
    fixture["event_time"] = "2026-08-11T15:30:00"

    assert list(validator().iter_errors(fixture))
