from __future__ import annotations

import argparse
import sys
from decimal import Decimal, InvalidOperation

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper import PaperSettings, create_paper_session
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a durable paper session from a sealed evaluation."
    )
    parser.add_argument("--evaluation-manifest", required=True)
    parser.add_argument("--evaluation-manifest-sha256", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approval-note", required=True)
    parser.add_argument("--maximum-drawdown", required=True)
    parser.add_argument("--command-id")
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        maximum_drawdown = Decimal(arguments.maximum_drawdown)
    except InvalidOperation as error:
        print("Paper session rejected: maximum drawdown is not a decimal", file=sys.stderr)
        raise SystemExit(4) from error
    try:
        settings = PaperSettings.from_env()
        repository = PostgresPaperRepository(
            settings.database_url,
            transaction_timeout_seconds=settings.transaction_timeout_seconds,
        )
        report = create_paper_session(
            arguments.evaluation_manifest,
            arguments.evaluation_manifest_sha256,
            approved_by=arguments.approved_by,
            approval_note=arguments.approval_note,
            maximum_drawdown=maximum_drawdown,
            settings=settings,
            repository=repository,
            local_development=arguments.local_development,
            command_id=arguments.command_id,
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper session rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper session ready")
    print("PAPER_SESSION_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
