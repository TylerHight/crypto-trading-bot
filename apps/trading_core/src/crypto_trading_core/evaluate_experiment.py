from __future__ import annotations

import argparse
import sys

from crypto_trading_core.contracts import InvalidBacktestInput, canonical_json_bytes
from crypto_trading_core.experiments import ExperimentSettings, evaluate_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate one sealed SMA selection on its out-of-sample range."
    )
    parser.add_argument("--selection-manifest", required=True)
    parser.add_argument("--selection-manifest-sha256", required=True)
    parser.add_argument("--output")
    parser.add_argument("--local-development", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    try:
        report = evaluate_experiment(arguments, ExperimentSettings.from_env())
    except (InvalidBacktestInput, OSError, ValueError) as error:
        print(f"Out-of-sample evaluation rejected: {error}", file=sys.stderr)
        raise SystemExit(4) from error
    print(f"Out-of-sample strategy evaluation: {str(report['status']).upper()}")
    print(f"  evaluation_key={report['evaluation_key']}")
    print("EXPERIMENT_EVALUATION_JSON=" + canonical_json_bytes(report).decode("ascii"))


if __name__ == "__main__":
    main()
