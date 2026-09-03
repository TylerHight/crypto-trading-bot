from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository
from crypto_trading_core.pilot_repository import PostgresPilotRepository
from crypto_trading_core.pilots import PilotSettings, run_paper_pilot_cycle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one bounded paper-pilot candle cycle.")
    parser.add_argument("--pilot-id", required=True)
    parser.add_argument("--candle-manifest", required=True)
    parser.add_argument("--candle-manifest-sha256", required=True)
    parser.add_argument("--command-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        settings = PilotSettings.from_env()
        paper = PostgresPaperRepository(
            settings.paper.database_url,
            transaction_timeout_seconds=settings.paper.transaction_timeout_seconds,
        )
        result = run_paper_pilot_cycle(
            args.pilot_id,
            args.candle_manifest,
            args.candle_manifest_sha256,
            command_id=args.command_id,
            settings=settings,
            paper_repository=paper,
            pilot_repository=PostgresPilotRepository(paper),
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper pilot cycle rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper pilot cycle complete")
    print("PAPER_PILOT_CYCLE_JSON=" + canonical_json_bytes(result).decode("ascii"))


if __name__ == "__main__":
    main()
