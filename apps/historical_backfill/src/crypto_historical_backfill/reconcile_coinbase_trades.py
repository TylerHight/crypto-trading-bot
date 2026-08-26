import argparse
import json
import logging
import os
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

from crypto_exchange_adapters.coinbase_rest import (
    CoinbaseMarketTradesClient,
    CoinbaseTradeCoverage,
    PermanentCoinbaseRestError,
    RetryableCoinbaseRestError,
)

from .archive import (
    ArchiveScan,
    PermanentArchiveReadError,
    RawParquetArchive,
    RetryableArchiveReadError,
)
from .reconciliation import (
    RawIntegrityEvidence,
    ReconciliationOutcome,
    persist_outcome,
    reconcile_trades,
)
from .storage import ObjectStorage, StorageSettings

LOGGER = logging.getLogger(__name__)
GAPS_FOUND_EXIT_CODE = 2
UNRESOLVED_EXIT_CODE = 3
COINBASE_PRODUCT_PATTERN = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)+$")


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("must include a timezone")
    return parsed.astimezone(UTC)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile bounded Coinbase REST trades with raw Parquet",
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-at", required=True, type=_aware_datetime)
    parser.add_argument("--end-at", required=True, type=_aware_datetime)
    parser.add_argument(
        "--raw-input",
        default=os.getenv(
            "RECONCILIATION_RAW_INPUT",
            "s3a://crypto-data/raw/market_trade_raw/v1",
        ),
    )
    parser.add_argument(
        "--report-output",
        default=os.getenv(
            "RECONCILIATION_REPORT_OUTPUT",
            "s3a://crypto-data/reconciliation/coinbase-trades",
        ),
    )
    parser.add_argument(
        "--raw-integrity-report",
        default=os.getenv("RECONCILIATION_RAW_INTEGRITY_REPORT"),
        required=os.getenv("RECONCILIATION_RAW_INTEGRITY_REPORT") is None,
    )
    parser.add_argument("--incident-id", type=UUID)
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
        "--timeout-seconds",
        type=float,
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
        default=int(os.getenv("RECONCILIATION_SAMPLE_LIMIT", "20")),
    )
    parser.add_argument(
        "--coinbase-base-url",
        default=os.getenv(
            "RECONCILIATION_COINBASE_BASE_URL",
            "https://api.coinbase.com/api/v3/brokerage/market",
        ),
    )
    return parser


def _empty_rest(reason: str | None = None) -> CoinbaseTradeCoverage:
    return CoinbaseTradeCoverage(
        trades=(),
        requests=(),
        raw_trade_count=0,
        duplicate_identities=0,
        coverage_complete=reason is None,
        unresolved_reason=reason,
    )


def _empty_archive() -> ArchiveScan:
    return ArchiveScan(
        trades=(),
        archived_trade_count=0,
        duplicate_identities=0,
        malformed_values=0,
        malformed_samples=(),
    )


def _qualify_raw_integrity(
    evidence: RawIntegrityEvidence,
    *,
    interval_end: datetime,
) -> RawIntegrityEvidence:
    if evidence.status != "passed":
        return evidence
    if evidence.topic != "market.trades.raw.v1":
        return RawIntegrityEvidence(
            status="topic_mismatch",
            topic=evidence.topic,
            completed_at=evidence.completed_at,
        )
    if evidence.completed_at is None:
        return RawIntegrityEvidence(
            status="missing_completion_time",
            topic=evidence.topic,
            completed_at=None,
        )
    try:
        completed_at = _aware_datetime(evidence.completed_at)
    except argparse.ArgumentTypeError:
        return RawIntegrityEvidence(
            status="invalid_completion_time",
            topic=evidence.topic,
            completed_at=evidence.completed_at,
        )
    if completed_at < interval_end:
        return RawIntegrityEvidence(
            status="stale_for_requested_interval",
            topic=evidence.topic,
            completed_at=evidence.completed_at,
        )
    return evidence


def _failure_outcome(
    *,
    status: str,
    reason: str,
    args: argparse.Namespace,
    raw_integrity: RawIntegrityEvidence,
    started_at: datetime,
) -> ReconciliationOutcome:
    base = reconcile_trades(
        symbol=args.symbol,
        start_at=args.start_at,
        end_at=args.end_at,
        raw_integrity=raw_integrity,
        rest=_empty_rest("source_history_unavailable"),
        archive=_empty_archive(),
        sample_limit=args.sample_limit,
        incident_id=args.incident_id,
        started_at=started_at,
    )
    report = dict(base.report)
    report["status"] = status
    report["failure_reason"] = reason[:500]
    return ReconciliationOutcome(report=report, confirmed_missing=())


def _print_summary(outcome: ReconciliationOutcome) -> None:
    report = outcome.report
    print(f"Coinbase trade reconciliation: {str(report['status']).upper()}")
    print(f"  symbol={report['symbol']} range=[{report['start_at']},{report['end_at_exclusive']})")
    print(
        "  "
        f"rest={report['rest_trades']} archived={report['archived_trades']} "
        f"missing={report['missing_from_archive']} "
        f"archive_only={report['archive_only']}"
    )
    print(
        "  "
        f"duplicate_rest={report['duplicate_rest_identities']} "
        f"duplicate_archived={report['duplicate_archived_identities']} "
        f"malformed_archive={report['malformed_archive_values']}"
    )
    print("RECONCILIATION_REPORT_JSON=" + json.dumps(report, sort_keys=True))


def run(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.symbol = args.symbol.strip().upper()
    if not COINBASE_PRODUCT_PATTERN.fullmatch(args.symbol):
        parser.error("--symbol must be a Coinbase product ID such as BTC-USD")
    if args.start_at >= args.end_at:
        parser.error("--start-at must be before --end-at")
    if (args.end_at - args.start_at).total_seconds() > args.maximum_window_seconds:
        parser.error("requested interval exceeds --maximum-window-seconds")
    if args.timeout_seconds <= 0 or args.maximum_retry_delay_seconds < 0:
        parser.error("timeout must be positive and retry delay must not be negative")

    started_at = datetime.now(UTC)
    storage = ObjectStorage(
        StorageSettings(
            endpoint_url=os.getenv("RECONCILIATION_S3_ENDPOINT"),
            access_key=os.getenv("RECONCILIATION_S3_ACCESS_KEY"),
            secret_key=os.getenv("RECONCILIATION_S3_SECRET_KEY"),
            region=os.getenv("RECONCILIATION_S3_REGION", "us-east-1"),
        )
    )
    try:
        raw_integrity = _qualify_raw_integrity(
            RawIntegrityEvidence.from_bytes(storage.read_bytes(args.raw_integrity_report)),
            interval_end=args.end_at,
        )
    except Exception:
        LOGGER.exception("Could not validate the raw integrity prerequisite")
        raw_integrity = RawIntegrityEvidence(
            status="evidence_unavailable",
            topic=None,
            completed_at=None,
        )

    if raw_integrity.status != "passed":
        outcome = reconcile_trades(
            symbol=args.symbol,
            start_at=args.start_at,
            end_at=args.end_at,
            raw_integrity=raw_integrity,
            rest=_empty_rest(),
            archive=_empty_archive(),
            sample_limit=args.sample_limit,
            incident_id=args.incident_id,
            started_at=started_at,
        )
    else:
        client = CoinbaseMarketTradesClient(
            base_url=args.coinbase_base_url,
            bearer_token=os.getenv("COINBASE_API_BEARER_TOKEN"),
            limit=args.rest_limit,
            max_requests=args.maximum_rest_requests,
            minimum_split_duration=timedelta(seconds=args.minimum_split_seconds),
            timeout_seconds=args.timeout_seconds,
            max_retries=args.maximum_retries,
            max_retry_delay_seconds=args.maximum_retry_delay_seconds,
        )
        try:
            rest = client.fetch_interval(args.symbol, args.start_at, args.end_at)
            archive = RawParquetArchive(storage, args.raw_input).scan(
                symbol=args.symbol,
                start_at=args.start_at,
                end_at=args.end_at,
                sample_limit=args.sample_limit,
            )
            outcome = reconcile_trades(
                symbol=args.symbol,
                start_at=args.start_at,
                end_at=args.end_at,
                raw_integrity=raw_integrity,
                rest=rest,
                archive=archive,
                sample_limit=args.sample_limit,
                incident_id=args.incident_id,
                started_at=started_at,
            )
        except (RetryableCoinbaseRestError, RetryableArchiveReadError) as error:
            outcome = _failure_outcome(
                status="failed_retryable",
                reason=str(error),
                args=args,
                raw_integrity=raw_integrity,
                started_at=started_at,
            )
        except (PermanentCoinbaseRestError, PermanentArchiveReadError) as error:
            outcome = _failure_outcome(
                status="failed_permanent",
                reason=str(error),
                args=args,
                raw_integrity=raw_integrity,
                started_at=started_at,
            )

    persisted = persist_outcome(storage, args.report_output, outcome)
    _print_summary(persisted)
    if persisted.status == "passed":
        return 0
    if persisted.status == "gaps_found":
        return GAPS_FOUND_EXIT_CODE
    return UNRESOLVED_EXIT_CODE


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(run())


if __name__ == "__main__":
    main()
