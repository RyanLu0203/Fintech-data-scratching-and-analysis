#!/usr/bin/env python3
"""Optional DQN hyperparameter sensitivity mode.

Default behavior is dry-run only. Pass --run-training to actually train a few
small cached-data experiments.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import stock_reports_dir, stock_results_dir
from src.evaluation.ablation import run_ablation_study


SAFE_COMBINATIONS = [
    {"learning_rate": 0.001, "gamma": 0.99, "batch_size": 64, "target_update_freq": 200, "epsilon_decay": 0.995},
    {"learning_rate": 0.0005, "gamma": 0.99, "batch_size": 64, "target_update_freq": 200, "epsilon_decay": 0.995},
    {"learning_rate": 0.001, "gamma": 0.95, "batch_size": 32, "target_update_freq": 100, "epsilon_decay": 0.99},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optional cached-data DQN hyperparameter sensitivity mode.")
    parser.add_argument("--symbol", default="002475")
    parser.add_argument("--input-csv", default="")
    parser.add_argument("--sentiment-csv", default="")
    parser.add_argument("--episodes", type=int, default=20)
    parser.add_argument("--run-training", action="store_true", help="Actually run small training jobs. Default is dry-run only.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = PROJECT_ROOT / "reports" / "tables" / "hyperparameter_sensitivity_plan.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    plan = pd.DataFrame(SAFE_COMBINATIONS)
    plan.insert(0, "symbol", args.symbol)
    plan["episodes"] = args.episodes
    plan["status"] = "planned_not_run"
    plan.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"Saved dry-run sensitivity plan: {output}")
    if not args.run_training:
        print("Dry run only. Pass --run-training to execute cached-data sensitivity training.")
        return 0

    input_csv = Path(args.input_csv) if args.input_csv else _latest_csv(PROJECT_ROOT / "outputs" / "stocks" / args.symbol / "data", "*_finance_text_*.csv")
    sentiment_csv = Path(args.sentiment_csv) if args.sentiment_csv else _latest_csv(stock_reports_dir(args.symbol), "*_daily_sentiment.csv")
    if input_csv is None or sentiment_csv is None:
        raise SystemExit("Missing cached input or sentiment CSV. This script does not scrape data.")
    rows = []
    for index, combo in enumerate(SAFE_COMBINATIONS, start=1):
        result_dir = stock_results_dir(args.symbol) / f"sensitivity_{index}"
        reports_dir = stock_reports_dir(args.symbol) / f"sensitivity_{index}"
        outputs = run_ablation_study(
            input_csv=input_csv,
            sentiment_csv=sentiment_csv,
            output_dir=result_dir,
            reports_dir=reports_dir,
            episodes=args.episodes,
            seeds=[42, 123],
        )
        metrics = outputs["metrics"].copy()
        for key, value in combo.items():
            metrics[key] = value
        metrics["sensitivity_run"] = index
        rows.append(metrics)
    final = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    result_path = PROJECT_ROOT / "reports" / "tables" / "hyperparameter_sensitivity_results.csv"
    final.to_csv(result_path, index=False, encoding="utf-8-sig")
    print(f"Saved sensitivity results: {result_path}")
    return 0


def _latest_csv(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if path.is_file() and not path.name.endswith("_master.csv")]
    return sorted(files, key=lambda path: path.stat().st_mtime)[-1] if files else None


if __name__ == "__main__":
    raise SystemExit(main())
