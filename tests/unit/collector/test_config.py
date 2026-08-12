import pytest
from crypto_trading_collector.config import CollectorSettings
from pydantic import ValidationError


def test_symbols_are_cleaned_deduplicated_and_uppercased() -> None:
    settings = CollectorSettings(symbols=[" btc-usd ", "BTC-USD", "eth-usd"])

    assert settings.symbols == ["BTC-USD", "ETH-USD"]


def test_empty_symbol_list_is_rejected() -> None:
    with pytest.raises(ValidationError, match="At least one"):
        CollectorSettings(symbols=[])


def test_sasl_requires_complete_credentials() -> None:
    with pytest.raises(ValidationError, match="username, and password"):
        CollectorSettings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="SCRAM-SHA-512",
        )
