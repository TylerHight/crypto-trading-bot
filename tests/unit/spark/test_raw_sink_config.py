import pytest

from jobs.spark.config import RawSinkSettings


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
