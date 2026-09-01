from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper import PaperSettings, paper_session_status
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show a paper session without mutating it.")
    parser.add_argument("--session-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        settings = PaperSettings.from_env()
        repository = PostgresPaperRepository(
            settings.database_url,
            transaction_timeout_seconds=settings.transaction_timeout_seconds,
        )
        report = paper_session_status(arguments.session_id, repository)
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper session read failed: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("PAPER_SESSION_STATUS_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
