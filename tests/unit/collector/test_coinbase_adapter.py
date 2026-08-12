import json
from pathlib import Path

import pytest
from crypto_exchange_adapters.coinbase import CoinbasePublicTradeClient

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = (
    REPOSITORY_ROOT / "tests" / "contract" / "fixtures" / "coinbase_market_trades.json"
)


def test_parses_recorded_market_trade_message() -> None:
    message = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    trades = CoinbasePublicTradeClient.parse_message(message)

    assert len(trades) == 1
    assert trades[0].exchange == "coinbase"
    assert trades[0].symbol == "BTC-USD"
    assert trades[0].source_event_id == "123456789"
    assert trades[0].source_sequence == 42


def test_ignores_other_channels() -> None:
    assert CoinbasePublicTradeClient.parse_message({"channel": "heartbeats"}) == []


def test_rejects_non_integer_sequence_number() -> None:
    message = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    message["sequence_num"] = "42"

    with pytest.raises(TypeError, match="sequence_num"):
        CoinbasePublicTradeClient.parse_message(message)
