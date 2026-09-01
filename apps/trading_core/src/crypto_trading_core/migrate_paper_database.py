from __future__ import annotations

import argparse
import sys

from crypto_trading_core.paper import PaperSettings
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.paper_repository import PostgresPaperRepository


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description="Apply paper-trading PostgreSQL migrations.")


def main(argv: list[str] | None = None) -> None:
    build_parser().parse_args(argv)
    try:
        settings = PaperSettings.from_env()
        repository = PostgresPaperRepository(
            settings.database_url,
            transaction_timeout_seconds=settings.transaction_timeout_seconds,
        )
        repository.migrate()
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper database migration failed: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper database migration complete")


if __name__ == "__main__":
    main()
