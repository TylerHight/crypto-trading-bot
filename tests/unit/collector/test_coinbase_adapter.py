import json
import logging
from pathlib import Path
from uuid import UUID

import pytest
from crypto_exchange_adapters.coinbase import (
    ENVELOPE_SEQUENCE_STREAM,
    HEARTBEAT_COUNTER_STREAM,
    CoinbaseFeedContinuityMonitor,
    CoinbasePublicTradeClient,
    report_continuity_observation,
)

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


def test_envelope_sequence_advances_across_products_and_channels() -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))
    messages = [
        {
            "channel": "market_trades",
            "sequence_num": 0,
            "events": [{"trades": [{"product_id": "BTC-USD"}]}],
        },
        {
            "channel": "market_trades",
            "sequence_num": 1,
            "events": [{"trades": [{"product_id": "ETH-USD"}]}],
        },
        {"channel": "subscriptions", "sequence_num": 2, "events": []},
        {
            "channel": "heartbeats",
            "sequence_num": 3,
            "events": [{"heartbeat_counter": "900"}],
        },
        {
            "channel": "market_trades",
            "sequence_num": 4,
            "events": [{"trades": [{"product_id": "BTC-USD"}]}],
        },
    ]

    observations = [monitor.observe_message(message) for message in messages]

    assert observations[0][0][0] == ENVELOPE_SEQUENCE_STREAM
    assert observations[0][0][1].status == "initialized"
    assert observations[1][0][1].status == "ok"
    assert observations[2][0][1].status == "ok"
    assert observations[3][0][1].status == "ok"
    assert observations[4][0][1].status == "ok"


def test_batched_trades_observe_the_envelope_only_once() -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))
    message = {
        "channel": "market_trades",
        "sequence_num": 10,
        "events": [
            {
                "trades": [
                    {"product_id": "BTC-USD", "trade_id": "1"},
                    {"product_id": "BTC-USD", "trade_id": "2"},
                ]
            }
        ],
    }

    observations = monitor.observe_message(message)

    assert len(observations) == 1
    assert observations[0][0] == ENVELOPE_SEQUENCE_STREAM


def test_missing_envelope_sequence_reports_the_exact_gap() -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))
    monitor.observe_message(
        {"channel": "market_trades", "sequence_num": 10, "events": []}
    )

    observations = monitor.observe_message(
        {"channel": "market_trades", "sequence_num": 13, "events": []}
    )

    result = observations[0][1]
    assert result.status == "gap"
    assert result.missing_from == 11
    assert result.missing_to == 12


def test_heartbeat_counter_is_independent_from_envelope_sequence() -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))
    first = monitor.observe_message(
        {
            "channel": "heartbeats",
            "sequence_num": 0,
            "events": [{"heartbeat_counter": "700"}],
        }
    )
    monitor.observe_message(
        {"channel": "market_trades", "sequence_num": 1, "events": []}
    )
    second = monitor.observe_message(
        {
            "channel": "heartbeats",
            "sequence_num": 2,
            "events": [{"heartbeat_counter": "702"}],
        }
    )

    assert first[0][0] == ENVELOPE_SEQUENCE_STREAM
    assert first[0][1].status == "initialized"
    assert first[1][0] == HEARTBEAT_COUNTER_STREAM
    assert first[1][1].status == "initialized"
    assert second[0][1].status == "ok"
    assert second[1][1].status == "gap"
    assert second[1][1].missing_from == 701
    assert second[1][1].missing_to == 701


def test_rejects_non_integer_heartbeat_counter() -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))

    with pytest.raises(TypeError, match="heartbeat_counter"):
        monitor.observe_message(
            {
                "channel": "heartbeats",
                "sequence_num": 0,
                "events": [{"heartbeat_counter": "not-a-number"}],
            }
        )


def test_new_connection_uses_a_fresh_sequence_baseline() -> None:
    first_connection = CoinbaseFeedContinuityMonitor(UUID(int=1))
    second_connection = CoinbaseFeedContinuityMonitor(UUID(int=2))
    first_connection.observe_message(
        {"channel": "market_trades", "sequence_num": 100, "events": []}
    )

    observations = second_connection.observe_message(
        {"channel": "market_trades", "sequence_num": 0, "events": []}
    )

    assert observations[0][1].status == "initialized"
    assert observations[0][1].current == 0


def test_gap_is_logged_without_raising(caplog: pytest.LogCaptureFixture) -> None:
    monitor = CoinbaseFeedContinuityMonitor(UUID(int=1))
    monitor.observe_message(
        {"channel": "market_trades", "sequence_num": 10, "events": []}
    )
    stream, result = monitor.observe_message(
        {"channel": "market_trades", "sequence_num": 12, "events": []}
    )[0]

    with caplog.at_level(logging.ERROR):
        report_continuity_observation(monitor.connection_id, stream, result)

    assert "missing_from=11" in caplog.text
    assert "missing_to=11" in caplog.text
