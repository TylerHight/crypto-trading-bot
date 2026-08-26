import argparse
import json
import logging
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from crypto_exchange_adapters.coinbase_rest import (
    CoinbaseMarketTradesClient,
    PermanentCoinbaseRestError,
    RetryableCoinbaseRestError,
)

from .archive import PermanentArchiveReadError, RawParquetArchive, RetryableArchiveReadError
from .backfill import (
    RAW_TOPIC,
    BackfillService,
    BackfillStateStore,
    InvalidBackfillInput,
    exit_code,
    load_backfill_input,
)
from .kafka import AcknowledgedKafkaPublisher
from .reconciliation import utc_text
from .storage import ObjectStorage, StorageSettings

LOGGER = logging.getLogger(__name__)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply one confirmed Coinbase reconciliation backfill"
    )
    parser.add_argument("--reconciliation-report", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--allow-legacy-findings", action="store_true")
    parser.add_argument(
        "--findings-prefix",
        default=os.getenv(
            "BACKFILL_FINDINGS_PREFIX",
            "s3a://crypto-data/reconciliation/coinbase-trades/findings",
        ),
    )
    parser.add_argument(
        "--state-output",
        default=os.getenv(
            "BACKFILL_STATE_OUTPUT", "s3a://crypto-data/backfill/coinbase-trades"
        ),
    )
    parser.add_argument(
        "--raw-input",
        default=os.getenv(
            "RECONCILIATION_RAW_INPUT", "s3a://crypto-data/raw/market_trade_raw/v1"
        ),
    )
    parser.add_argument(
        "--maximum-window-seconds",
        type=_positive_int,
        default=int(os.getenv("RECONCILIATION_MAXIMUM_WINDOW_SECONDS", "900")),
    )
    parser.add_argument(
        "--rest-limit",
        type=_positive_int,
        default=int(os.getenv("RECONCILIATION_REST_LIMIT", "1000")),
    )
    parser.add_argument(
        "--maximum-rest-requests",
        type=_positive_int,
        default=int(os.getenv("RECONCILIATION_MAXIMUM_REST_REQUESTS", "100")),
    )
    parser.add_argument(
        "--minimum-split-seconds",
        type=_positive_int,
        default=int(os.getenv("RECONCILIATION_MINIMUM_SPLIT_SECONDS", "1")),
    )
    parser.add_argument(
        "--rest-timeout-seconds",
        type=_positive_float,
        default=float(os.getenv("RECONCILIATION_TIMEOUT_SECONDS", "10")),
    )
    parser.add_argument(
        "--maximum-retries",
        type=_nonnegative_int,
        default=int(os.getenv("RECONCILIATION_MAXIMUM_RETRIES", "3")),
    )
    parser.add_argument(
        "--maximum-retry-delay-seconds",
        type=float,
        default=float(os.getenv("RECONCILIATION_MAXIMUM_RETRY_DELAY_SECONDS", "30")),
    )
    parser.add_argument(
        "--sample-limit",
        type=_positive_int,
        default=int(os.getenv("BACKFILL_SAMPLE_LIMIT", "20")),
    )
    parser.add_argument(
        "--verification-timeout-seconds",
        type=float,
        default=float(os.getenv("BACKFILL_VERIFICATION_TIMEOUT_SECONDS", "30")),
    )
    parser.add_argument(
        "--verification-poll-seconds",
        type=_positive_float,
        default=float(os.getenv("BACKFILL_VERIFICATION_POLL_SECONDS", "1")),
    )
    parser.add_argument(
        "--coinbase-base-url",
        default=os.getenv(
            "RECONCILIATION_COINBASE_BASE_URL",
            "https://api.coinbase.com/api/v3/brokerage/market",
        ),
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.maximum_retry_delay_seconds < 0 or args.verification_timeout_seconds < 0:
        raise SystemExit("retry delay and verification timeout must not be negative")
    storage = ObjectStorage(
        StorageSettings(
            endpoint_url=os.getenv("RECONCILIATION_S3_ENDPOINT"),
            access_key=os.getenv("RECONCILIATION_S3_ACCESS_KEY"),
            secret_key=os.getenv("RECONCILIATION_S3_SECRET_KEY"),
            region=os.getenv("RECONCILIATION_S3_REGION", "us-east-1"),
        )
    )
    report: dict[str, object]
    try:
        value = load_backfill_input(
            storage=storage,
            report_uri=args.reconciliation_report,
            findings_prefix=args.findings_prefix,
            maximum_window=timedelta(seconds=args.maximum_window_seconds),
            allow_legacy_findings=args.allow_legacy_findings,
            apply=args.apply,
        )
    except (InvalidBackfillInput, FileNotFoundError, OSError) as error:
        report = {"status": "failed_permanent", "reason": str(error)[:500]}
        print("BACKFILL_REPORT_JSON=" + json.dumps(report, sort_keys=True))
        return 4

    kafka_topic = os.getenv("BACKFILL_KAFKA_TOPIC", RAW_TOPIC)
    if kafka_topic != RAW_TOPIC:
        report = {
            "status": "failed_permanent",
            "reason": f"BACKFILL_KAFKA_TOPIC must be {RAW_TOPIC}",
        }
        print("BACKFILL_REPORT_JSON=" + json.dumps(report, sort_keys=True))
        return 4

    bootstrap_servers = os.getenv(
        "BACKFILL_KAFKA_BOOTSTRAP_SERVERS",
        os.getenv("COLLECTOR_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    security_protocol = os.getenv("BACKFILL_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism = os.getenv("BACKFILL_KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("BACKFILL_KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("BACKFILL_KAFKA_SASL_PASSWORD")
    try:
        kafka_ack_timeout = float(os.getenv("BACKFILL_KAFKA_ACK_TIMEOUT_SECONDS", "10"))
        kafka_queue_retries = int(os.getenv("BACKFILL_KAFKA_QUEUE_FULL_RETRIES", "3"))
    except ValueError:
        report = {"status": "failed_permanent", "reason": "invalid Kafka numeric setting"}
        print("BACKFILL_REPORT_JSON=" + json.dumps(report, sort_keys=True))
        return 4
    invalid_kafka_config = (
        not bootstrap_servers.strip()
        or security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
        or kafka_ack_timeout <= 0
        or kafka_queue_retries < 0
        or (
            security_protocol.startswith("SASL")
            and (not sasl_mechanism or not sasl_username or not sasl_password)
        )
    )
    if invalid_kafka_config:
        report = {"status": "failed_permanent", "reason": "invalid Kafka configuration"}
        print("BACKFILL_REPORT_JSON=" + json.dumps(report, sort_keys=True))
        return 4

    state = BackfillStateStore(storage, args.state_output)
    backfill_run_id = uuid4()
    started_at = datetime.now(UTC)
    state.write_attempt(value, backfill_run_id, apply=args.apply)

    client = CoinbaseMarketTradesClient(
        base_url=args.coinbase_base_url,
        bearer_token=os.getenv("COINBASE_API_BEARER_TOKEN"),
        limit=args.rest_limit,
        max_requests=args.maximum_rest_requests,
        minimum_split_duration=timedelta(seconds=args.minimum_split_seconds),
        timeout_seconds=args.rest_timeout_seconds,
        max_retries=args.maximum_retries,
        max_retry_delay_seconds=args.maximum_retry_delay_seconds,
    )
    try:
        coverage = client.fetch_interval(value.symbol, value.start_at, value.end_at)
        archive = RawParquetArchive(storage, args.raw_input)
        def publisher_factory() -> AcknowledgedKafkaPublisher:
            return AcknowledgedKafkaPublisher(
                bootstrap_servers=bootstrap_servers,
                topic=kafka_topic,
                client_id=os.getenv("BACKFILL_KAFKA_CLIENT_ID", "coinbase-trade-backfill"),
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
                acknowledgement_timeout_seconds=kafka_ack_timeout,
                queue_full_retries=kafka_queue_retries,
            )

        report = BackfillService(
            archive=archive,
            state=state,
            publisher_factory=publisher_factory,
            sample_limit=args.sample_limit,
            verification_timeout_seconds=args.verification_timeout_seconds,
            verification_poll_seconds=args.verification_poll_seconds,
        ).run(
            value,
            coverage,
            apply=args.apply,
            backfill_run_id=backfill_run_id,
            attempt_recorded=True,
        )
    except (RetryableCoinbaseRestError, RetryableArchiveReadError) as error:
        report = {"status": "failed_retryable", "reason": str(error)[:500]}
    except (PermanentCoinbaseRestError, PermanentArchiveReadError) as error:
        report = {"status": "failed_permanent", "reason": str(error)[:500]}

    if "backfill_run_id" not in report:
        report.update(
            {
                "backfill_run_id": str(backfill_run_id),
                "reconciliation_run_id": str(value.reconciliation_run_id),
                "reconciliation_key": value.reconciliation_key,
                "mode": "apply" if args.apply else "dry_run",
                "symbol": value.symbol,
                "started_at": utc_text(started_at),
                "completed_at": utc_text(datetime.now(UTC)),
            }
        )
        report["report_uri"] = state.write_run(report)

    print(f"Coinbase trade backfill: {str(report['status']).upper()}")
    print("BACKFILL_REPORT_JSON=" + json.dumps(report, sort_keys=True))
    return exit_code(report)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(run())


if __name__ == "__main__":
    main()
