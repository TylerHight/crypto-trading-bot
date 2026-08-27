import json

import pytest

from jobs.spark.curation import (
    InvalidCurationInput,
    load_frozen_snapshot,
    validate_distinct_prefixes,
)


def audit_report(*, ending: int = 3, status: str = "passed") -> bytes:
    return json.dumps(
        {
            "status": status,
            "topic": "market.trades.raw.v1",
            "invalid_event_values": 0,
            "partitions": [
                {
                    "kafka_topic": "market.trades.raw.v1",
                    "kafka_partition": 0,
                    "earliest_offset": 0,
                    "ending_offset_exclusive": ending,
                    "kafka_records": ending,
                    "parquet_records_in_range": ending,
                    "missing_from_parquet": 0,
                    "duplicate_parquet_positions": 0,
                }
            ],
        },
        separators=(",", ":"),
    ).encode()


def load(value: bytes, *, local: bool = True):
    return load_frozen_snapshot(
        value,
        report_uri="audit.json",
        allowed_evidence_prefix="s3a://crypto-data/evidence",
        local_development=local,
    )


def test_snapshot_key_is_deterministic_and_changes_with_ending_offset() -> None:
    first = load(audit_report())
    same = load(audit_report())
    later = load(audit_report(ending=4))

    assert first.snapshot_key == same.snapshot_key
    assert first.snapshot_key != later.snapshot_key
    assert first.expected_raw_rows == 3


@pytest.mark.parametrize(
    "mutation",
    [
        lambda report: report.update(status="failed"),
        lambda report: report.update(topic="other"),
        lambda report: report.update(invalid_event_values=1),
        lambda report: report["partitions"][0].update(earliest_offset=-1),
        lambda report: report["partitions"][0].update(missing_from_parquet=1),
        lambda report: report["partitions"][0].update(duplicate_parquet_positions=1),
        lambda report: report["partitions"][0].update(parquet_records_in_range=2),
    ],
)
def test_invalid_audit_evidence_is_rejected(mutation) -> None:
    report = json.loads(audit_report())
    mutation(report)
    with pytest.raises(InvalidCurationInput):
        load(json.dumps(report).encode())


def test_local_evidence_requires_explicit_development_mode() -> None:
    with pytest.raises(InvalidCurationInput, match="local-development"):
        load(audit_report(), local=False)


def test_remote_evidence_must_be_beneath_configured_prefix() -> None:
    with pytest.raises(InvalidCurationInput, match="outside"):
        load_frozen_snapshot(
            audit_report(),
            report_uri="s3a://other/audit.json",
            allowed_evidence_prefix="s3a://crypto-data/evidence",
            local_development=False,
        )


def test_raw_curated_and_quarantine_prefixes_are_distinct() -> None:
    with pytest.raises(InvalidCurationInput, match="distinct"):
        validate_distinct_prefixes("s3a://bucket/raw", "s3a://bucket/raw/", "q")
