"""Run the DQN model-upgrade grid on cached official experiment data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

from src.config.paths import OUTPUTS_ROOT
from src.evaluation.model_upgrade import ModelUpgradeRunConfig, run_model_upgrade_grid
from src.evaluation.model_upgrade_diagnostics import generate_model_upgrade_diagnostic_reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DQN model upgrade grid without rerunning crawlers.")
    parser.add_argument("--target-symbol", default="002475")
    parser.add_argument("--target-company", default="立讯精密")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2026-04-30")
    parser.add_argument("--max-seeds", type=int, default=20)
    parser.add_argument("--quick-test", action="store_true")
    parser.add_argument("--medium-test", action="store_true")
    parser.add_argument("--batch-file", help="CSV with target_symbol,target_company columns. Runs each row and continues after failures.")
    args = parser.parse_args()
    if args.quick_test and args.medium_test:
        parser.error("--quick-test and --medium-test cannot be used together.")
    return args


def main() -> None:
    args = parse_args()
    if args.batch_file:
        batch = pd.read_csv(args.batch_file)
        required = {"target_symbol", "target_company"}
        missing = required.difference(batch.columns)
        if missing:
            raise SystemExit("--batch-file is missing required columns: " + ", ".join(sorted(missing)))
        failures: list[str] = []
        all_paths: dict[str, Path] = {}
        for _, row in batch.iterrows():
            symbol = str(row["target_symbol"]).zfill(6)
            company = str(row["target_company"])
            try:
                config = _config_from_args(args, target_symbol=symbol, target_company=company)
                paths = run_model_upgrade_grid(config)
                all_paths.update(paths)
                print(f"Model upgrade grid finished for {symbol} {company}.")
            except Exception as exc:  # continue batch after individual stock failures
                message = f"{symbol},{company},failed,{type(exc).__name__}: {exc}"
                failures.append(message)
                print(message)
        if failures:
            log_path = OUTPUTS_ROOT / "model_upgrade" / "upgrade_run_log.md"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write("\n## Batch Failures\n")
                for failure in failures:
                    handle.write(f"- {failure}\n")
        all_paths.update({f"diagnostic_{name}": path for name, path in generate_model_upgrade_diagnostic_reports().items()})
        print("Batch model upgrade grid finished.")
        for name, path in all_paths.items():
            print(f"{name}: {path}")
        return

    paths = run_model_upgrade_grid(_config_from_args(args, target_symbol=args.target_symbol, target_company=args.target_company))
    print("Model upgrade grid finished.")
    for name, path in paths.items():
        print(f"{name}: {path}")


def _config_from_args(args: argparse.Namespace, *, target_symbol: str, target_company: str) -> ModelUpgradeRunConfig:
    return ModelUpgradeRunConfig(
        target_symbol=target_symbol,
        target_company=target_company,
        start_date=args.start_date,
        end_date=args.end_date,
        max_seeds=args.max_seeds,
        quick_test=bool(args.quick_test),
        medium_test=bool(args.medium_test),
    )


if __name__ == "__main__":
    main()
