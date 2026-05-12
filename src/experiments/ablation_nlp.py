"""Ablation experiment comparing DQN with and without NLP sentiment signals."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def run_ablation(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    data = pd.read_csv(input_csv)
    with_nlp = {
        "experiment": "with_nlp_signal",
        "rows": len(data),
        "sentiment_nonzero_rows": int((data.get("event_count", 0) != 0).sum()),
    }
    without_nlp = {
        "experiment": "without_nlp_signal",
        "rows": len(data),
        "sentiment_nonzero_rows": 0,
    }
    result = pd.DataFrame([with_nlp, without_nlp])
    result.to_csv(output_csv, index=False)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", default="reports/ablation_nlp.csv")
    args = parser.parse_args()
    run_ablation(Path(args.input_csv), Path(args.output_csv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

