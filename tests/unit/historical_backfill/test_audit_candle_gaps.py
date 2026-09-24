import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta

from crypto_exchange_adapters.coinbase_rest import (
    CoinbaseMarketTradesClient,
    HttpResponse,
)
from crypto_historical_backfill.audit_candle_gaps import audit
from crypto_historical_backfill.storage import ObjectStorage, StorageSettings

START = datetime(2026, 6, 29, 13, 10, tzinfo=UTC)


class Responses:
    def __init__(self, payloads):
        self.payloads = iter(payloads)

    def request(self, url, headers, timeout):
        return HttpResponse(200, {}, json.dumps({"trades": next(self.payloads)}).encode())


def trade(identity, second, price, size="1"):
    return {
        "trade_id": identity,
        "product_id": "BTC-USD",
        "time": (START + timedelta(seconds=second)).isoformat().replace("+00:00", "Z"),
        "price": price,
        "size": size,
    }


def manifest(tmp_path, minutes):
    body = json.dumps(
        {
            "status": "published",
            "source_kind": "exchange_ohlcv",
            "plan": {"source": "coinbase-exchange", "symbol": "BTC-USD"},
            "coverage": {
                "missing_ranges": [
                    {
                        "start": START.isoformat(),
                        "end": (START + timedelta(minutes=minutes)).isoformat(),
                        "minutes": minutes,
                    }
                ]
            },
        }
    ).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(body)
    return str(path), hashlib.sha256(body).hexdigest()


def test_complete_trade_window_produces_exact_candle_and_hashed_response(tmp_path):
    source, digest = manifest(tmp_path, 1)
    payloads = [[trade("2", 30, "12", "0.5"), trade("1", 1, "10", "1.25")]]

    def client(recording):
        recording.upstream = Responses(payloads)
        return CoinbaseMarketTradesClient(transport=recording)

    report = audit(
        source, digest, store=ObjectStorage(StorageSettings()),
        output=str(tmp_path / "audit"), client_factory=client,
    )
    assert report["complete"] is True
    assert report["verified_reconstructable_minutes"] == 1
    candle = report["ranges"][0]["candles"][0]
    assert candle["open"] == 10
    assert candle["close"] == 12
    assert candle["base_volume"] == 1.75
    response_ref = report["ranges"][0]["responses"][0]
    response = json.loads(ObjectStorage(StorageSettings()).read_bytes(response_ref["uri"]))
    assert hashlib.sha256(base64.b64decode(response["body_base64"])).hexdigest() == response["body_sha256"]


def test_empty_trade_window_remains_unresolved(tmp_path):
    source, digest = manifest(tmp_path, 1)

    def client(recording):
        recording.upstream = Responses([[]])
        return CoinbaseMarketTradesClient(transport=recording)

    report = audit(
        source, digest, store=ObjectStorage(StorageSettings()),
        output=str(tmp_path / "audit"), client_factory=client,
    )
    assert report["complete"] is False
    assert report["verified_reconstructable_minutes"] == 0
    assert report["ranges"][0]["status"] == "unresolved"
    assert report["ranges"][0]["reason"] == "source_history_unavailable"
