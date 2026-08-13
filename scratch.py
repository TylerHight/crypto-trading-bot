from jobs.spark.config import RawSinkSettings

def test_raw_sink_defaults_match_local_compose() -> None:
    settings = RawSinkSettings.from_env({})