from crypto_trading_collector.models import event_id_for_trade


def test_same_source_trade_has_same_event_id() -> None:
    first = event_id_for_trade("coinbase", "BTC-USD", "123")
    second = event_id_for_trade("coinbase", "BTC-USD", "123")

    assert first == second


def test_different_source_trades_have_different_event_ids() -> None:
    first = event_id_for_trade("coinbase", "BTC-USD", "123")
    second = event_id_for_trade("coinbase", "BTC-USD", "124")

    assert first != second


def test_identity_normalizes_exchange_and_symbol_case() -> None:
    first = event_id_for_trade(" Coinbase ", "btc-usd", "123")
    second = event_id_for_trade("coinbase", "BTC-USD", "123")

    assert first == second
