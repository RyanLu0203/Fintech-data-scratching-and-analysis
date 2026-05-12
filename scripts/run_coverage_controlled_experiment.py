#!/usr/bin/env python3
"""Explicitly run coverage-controlled DQN ablation for one local stock.

This script trains DQN agents, so it is intentionally opt-in. The dashboard and
result overview notebook only load cached outputs and never call this script.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import normalize_symbol_for_path, stock_data_dir, stock_reports_dir
from src.evaluation.ablation import run_coverage_controlled_ablation_study


def latest_file(directory: Path, pattern: str, exclude_suffix: str | None = None) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if exclude_suffix is None or not path.name.endswith(exclude_suffix)]
    return sorted(files, key=lambda item: item.stat().st_mtime)[-1] if files else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run opt-in coverage-controlled NLP-DQN ablation from local cached data.")
    parser.add_argument("--symbol", required=True, help="Six-digit stock code, e.g. 002475")
    parser.add_argument("--episodes", type=int, default=200)
    parser.add_argument("--seeds", default="42,123,2024,2025,3407")
    parser.add_argument("--reward-mode", default="portfolio_return")
    args = parser.parse_args()

    symbol = normalize_symbol_for_path(args.symbol)
    input_csv = latest_file(stock_data_dir(symbol), "*_finance_text_*.csv", exclude_suffix="_master.csv")
    sentiment_csv = latest_file(stock_reports_dir(symbol), "*_daily_sentiment.csv")
    if input_csv is None:
        raise FileNotFoundError(f"No local market/news CSV found for {symbol}.")
    if sentiment_csv is None:
        raise FileNotFoundError(f"No local daily sentiment CSV found for {symbol}.")
    seeds = [int(token.strip()) for token in args.seeds.split(",") if token.strip()]
    outputs = run_coverage_controlled_ablation_study(
        input_csv=input_csv,
        sentiment_csv=sentiment_csv,
        episodes=args.episodes,
        seeds=seeds,
        reward_mode=args.reward_mode,
    )
    for name, value in outputs.items():
        if isinstance(value, Path):
            print(f"{name}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
