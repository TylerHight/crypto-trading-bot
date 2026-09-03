from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_core.pilot_repository import PostgresPilotRepository
from crypto_trading_core.pilots import PilotSettings, finalize_paper_pilot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Finalize a paper pilot from stored criteria.")
    parser.add_argument("--pilot-id", required=True)
    parser.add_argument("--reviewed-by", required=True)
    parser.add_argument("--review-note", required=True)
    parser.add_argument("--command-id")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        settings = PilotSettings.from_env()
        paper = PostgresPaperRepository(
            settings.paper.database_url,
            transaction_timeout_seconds=settings.paper.transaction_timeout_seconds,
        )
        result = finalize_paper_pilot(
            args.pilot_id,
            reviewed_by=args.reviewed_by,
            review_note=args.review_note,
            settings=settings,
            paper_repository=paper,
            pilot_repository=PostgresPilotRepository(paper),
            command_id=args.command_id,
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper pilot finalization rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper pilot assessment ready")
    print("PAPER_PILOT_ASSESSMENT_JSON=" + canonical_json_bytes(result).decode("ascii"))


if __name__ == "__main__":
    main()
