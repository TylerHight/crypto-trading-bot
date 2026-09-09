from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper import PaperSettings, set_paper_session_state
from crypto_trading_core.paper_contracts import (
    InvalidPaperTrading,
    PaperSessionState,
)
from crypto_trading_core.paper_repository import PostgresPaperRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Change a paper session lifecycle state.")
    parser.add_argument("--session-id", required=True)
    parser.add_argument(
        "--state", choices=[state.value for state in PaperSessionState], required=True
    )
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--command-id")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        settings = PaperSettings.from_env()
        repository = PostgresPaperRepository(
            settings.database_url,
            transaction_timeout_seconds=settings.transaction_timeout_seconds,
        )
        report = set_paper_session_state(
            arguments.session_id,
            PaperSessionState(arguments.state),
            actor=arguments.actor,
            reason=arguments.reason,
            repository=repository,
            command_id=arguments.command_id,
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper session state change rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("PAPER_SESSION_STATE_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
