"""Run the DQN model-upgrade grid on cached official experiment data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

from src.evaluation.model_upgrade import ModelUpgradeRunConfig, run_model_upgrade_grid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DQN model upgrade grid without rerunning crawlers.")
    parser.add_argument("--target-symbol", default="002475")
    parser.add_argument("--target-company", default="立讯精密")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2026-04-30")
    parser.add_argument("--max-seeds", type=int, default=20)
    parser.add_argument("--quick-test", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = ModelUpgradeRunConfig(
        target_symbol=args.target_symbol,
        target_company=args.target_company,
        start_date=args.start_date,
        end_date=args.end_date,
        max_seeds=args.max_seeds,
        quick_test=bool(args.quick_test),
    )
    paths = run_model_upgrade_grid(config)
    print("Model upgrade grid finished.")
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
