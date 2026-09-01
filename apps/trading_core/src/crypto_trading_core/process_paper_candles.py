from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper import PaperSettings, process_paper_candles
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Advance a paper session from one pinned candle publication."
    )
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--candle-manifest", required=True)
    parser.add_argument("--candle-manifest-sha256", required=True)
    parser.add_argument("--command-id")
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        settings = PaperSettings.from_env()
        repository = PostgresPaperRepository(
            settings.database_url,
            transaction_timeout_seconds=settings.transaction_timeout_seconds,
        )
        report = process_paper_candles(
            arguments.session_id,
            arguments.candle_manifest,
            arguments.candle_manifest_sha256,
            settings=settings,
            repository=repository,
            local_development=arguments.local_development,
            command_id=arguments.command_id,
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper candle processing rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper candle processing complete")
    print("PAPER_PROCESSING_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
