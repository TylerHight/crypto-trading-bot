import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIRECTORY = REPOSITORY_ROOT / "schemas" / "events" / "market.data.quality.v1"


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


def test_schema_rejects_unbounded_malformed_excerpt() -> None:
    fixture = valid_fixture()
    fixture["message_excerpt"] = "x" * 1025

    assert list(validator().iter_errors(fixture))
