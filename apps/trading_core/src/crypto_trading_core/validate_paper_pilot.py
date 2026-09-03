from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import canonical_json_bytes
from crypto_trading_core.paper_contracts import InvalidPaperTrading
from crypto_trading_core.pilots import PilotSettings, validate_pilot_publication
from crypto_trading_core.storage import ObjectStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independently validate a pilot publication.")
    parser.add_argument("stage", choices=("registration", "snapshot", "assessment"))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    kinds = {
        "registration": "paper_pilot_registration",
        "snapshot": "paper_pilot_snapshot",
        "assessment": "paper_pilot_assessment",
    }
    try:
        settings = PilotSettings.from_env()
        result = validate_pilot_publication(
            args.manifest,
            args.manifest_sha256,
            store=ObjectStorage(settings.paper.experiment.backtest.storage),
            expected_kind=kinds[args.stage],
        )
    except (InvalidPaperTrading, OSError, ValueError) as error:
        print(f"Paper pilot validation failed: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print("Paper pilot publication valid")
    print("PAPER_PILOT_VALIDATION_JSON=" + canonical_json_bytes(result).decode("ascii"))


if __name__ == "__main__":
    main()
