from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_core.pilot_repository import PostgresPilotRepository
from crypto_trading_core.pilots import PilotSettings, start_paper_pilot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Register a pinned forward paper-trading pilot.")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approval-note", required=True)
    parser.add_argument("--command-id")
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        settings = PilotSettings.from_env()
        paper = PostgresPaperRepository(
            settings.paper.database_url,
            transaction_timeout_seconds=settings.paper.transaction_timeout_seconds,
        )
        repository = PostgresPilotRepository(paper)
        repository.migrate()
        result = start_paper_pilot(
            args.plan,
            args.plan_sha256,
            approved_by=args.approved_by,
            approval_note=args.approval_note,
            settings=settings,
            paper_repository=paper,
            pilot_repository=repository,
            local_development=args.local_development,
            command_id=args.command_id,
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper pilot registration rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper pilot ready")
    print("PAPER_PILOT_JSON=" + canonical_json_bytes(result).decode("ascii"))


if __name__ == "__main__":
    main()
