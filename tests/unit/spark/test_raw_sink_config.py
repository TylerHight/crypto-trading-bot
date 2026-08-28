import pytest

from jobs.spark.config import (
    CandleSettings,
    CurationSettings,
    RawAuditSettings,
    RawSinkSettings,
)


def test_raw_sink_defaults_match_local_compose() -> None:
    settings = RawSinkSettings.from_env({})

    assert settings.kafka_bootstrap_servers == "kafka:29092"
    assert settings.kafka_topic == "market.trades.raw.v1"
    assert settings.output_path == "s3a://crypto-data/raw/market_trade_raw/v1"
    assert (
        settings.checkpoint_path == "s3a://crypto-data/checkpoints/raw-market-trades-v1"
    )
    assert settings.s3_endpoint == "http://minio:9000"


def test_raw_sink_normalizes_trailing_path_slashes() -> None:
    settings = RawSinkSettings.from_env(
        {
            "RAW_SINK_OUTPUT_PATH": "s3a://bucket/raw/",
            "RAW_SINK_CHECKPOINT_PATH": "s3a://bucket/checkpoints/",
        }
    )

    assert settings.output_path == "s3a://bucket/raw"
    assert settings.checkpoint_path == "s3a://bucket/checkpoints"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RAW_SINK_OUTPUT_PATH", "file:///tmp/raw"),
        ("RAW_SINK_CHECKPOINT_PATH", "file:///tmp/checkpoint"),
    ],
)
def test_raw_sink_requires_s3a_paths(name: str, value: str) -> None:
    with pytest.raises(ValueError, match="s3a://"):
        RawSinkSettings.from_env({name: value})


def test_raw_sink_rejects_shared_output_and_checkpoint_path() -> None:
    with pytest.raises(ValueError, match="must be different"):
        RawSinkSettings.from_env(
            {
                "RAW_SINK_OUTPUT_PATH": "s3a://bucket/same",
                "RAW_SINK_CHECKPOINT_PATH": "s3a://bucket/same/",
            }
        )


def test_raw_audit_defaults_match_local_compose() -> None:
    settings = RawAuditSettings.from_env({})

    assert settings.kafka_bootstrap_servers == "kafka:29092"
    assert settings.kafka_topic == "market.trades.raw.v1"
    assert settings.input_path == "s3a://crypto-data/raw/market_trade_raw/v1"
    assert settings.s3_endpoint == "http://minio:9000"
    assert settings.sample_limit == 20


def test_raw_audit_normalizes_input_path() -> None:
    settings = RawAuditSettings.from_env(
        {"RAW_AUDIT_INPUT_PATH": "s3a://bucket/raw/"}
    )

    assert settings.input_path == "s3a://bucket/raw"


@pytest.mark.parametrize("value", ["0", "-1", "not-a-number"])
def test_raw_audit_requires_positive_sample_limit(value: str) -> None:
    with pytest.raises(ValueError, match="RAW_AUDIT_SAMPLE_LIMIT"):
        RawAuditSettings.from_env({"RAW_AUDIT_SAMPLE_LIMIT": value})


def test_curation_defaults_are_bounded_and_use_the_evidence_prefix() -> None:
    settings = CurationSettings.from_env({})

    assert settings.evidence_prefix == "s3a://crypto-data/reconciliation/raw-integrity"
    assert settings.sample_limit == 20
    assert settings.maximum_input_rows is None


@pytest.mark.parametrize("value", ["0", "-1", "not-a-number"])
def test_curation_requires_a_positive_maximum_when_configured(value: str) -> None:
    with pytest.raises(ValueError, match="CURATION_MAXIMUM_INPUT_ROWS"):
        CurationSettings.from_env({"CURATION_MAXIMUM_INPUT_ROWS": value})


def test_candle_defaults_use_curated_manifest_evidence() -> None:
    settings = CandleSettings.from_env({})

    assert settings.source_manifest_prefix.endswith(
        "/curated/market_trades/v1/manifests"
    )
    assert settings.sample_limit == 20
    assert settings.maximum_input_rows is None


@pytest.mark.parametrize("value", ["0", "-1", "not-a-number"])
def test_candles_require_a_positive_maximum_when_configured(value: str) -> None:
    with pytest.raises(ValueError, match="CANDLE_MAXIMUM_INPUT_ROWS"):
        CandleSettings.from_env({"CANDLE_MAXIMUM_INPUT_ROWS": value})
