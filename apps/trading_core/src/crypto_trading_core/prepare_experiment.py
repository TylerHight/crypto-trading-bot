from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes
from crypto_trading_core.experiments import ExperimentSettings, prepare_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Select and seal an SMA candidate without reading test candles."
    )
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--output")
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        report = prepare_experiment(arguments, ExperimentSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Experiment preparation rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print(f"Strategy experiment preparation: {str(report['status']).upper()}")
    print(f"  selection_status={report['selection_status']}")
    print(f"  selection_key={report['selection_key']}")
    print("EXPERIMENT_SELECTION_JSON=" + canonical_json_bytes(report).decode("ascii"))
    if report["selection_status"] == "no_candidate_selected":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
