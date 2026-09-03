from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes, parse_utc_minute
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_core.pilot_repository import PostgresPilotRepository
from crypto_trading_core.pilots import PilotSettings, report_paper_pilot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish one immutable daily pilot snapshot.")
    parser.add_argument("--pilot-id", required=True)
    parser.add_argument("--as-of", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        settings = PilotSettings.from_env()
        paper = PostgresPaperRepository(
            settings.paper.database_url,
            transaction_timeout_seconds=settings.paper.transaction_timeout_seconds,
        )
        result = report_paper_pilot(
            args.pilot_id,
            parse_utc_minute(args.as_of, "as-of"),
            settings=settings,
            paper_repository=paper,
            pilot_repository=PostgresPilotRepository(paper),
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper pilot snapshot rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper pilot snapshot ready")
    print("PAPER_PILOT_SNAPSHOT_JSON=" + canonical_json_bytes(result).decode("ascii"))


if __name__ == "__main__":
    main()
