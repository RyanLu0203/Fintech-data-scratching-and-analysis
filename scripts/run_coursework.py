"""Offline smoke or source-bound full evaluation; never substitutes demo data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.evaluation.coursework import CourseworkConfig, run_coursework  # noqa: E402


def synthetic_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Deterministic, explicitly synthetic path fixture, not market evidence."""
    dates = pd.bdate_range("2025-01-02", periods=70)

    def market(symbol: str, phase: float) -> pd.DataFrame:
        close = 20 + np.sin(np.arange(len(dates)) * 0.6 + phase) * 2
        opening = np.r_[close[0], close[:-1]]
        return pd.DataFrame(
            {
                "symbol": symbol,
                "date": dates,
                "open": opening,
                "close": close,
                "high": np.maximum(opening, close) + 0.2,
                "low": np.minimum(opening, close) - 0.2,
                "volume": 10000,
                "source": "synthetic_fixture",
            }
        )

    def news(symbol: str, target: bool = False) -> pd.DataFrame:
        rows = []
        for i, date in enumerate(dates[::3] if target else dates):
            text = [
                "profit growth improves",
                "loss risk weak",
                "company meeting scheduled",
            ][i % 3]
            rows.append(
                {
                    "news_id": f"{symbol}-{i}",
                    "symbol": symbol,
                    "text": f"{text} synthetic story {symbol} {i}",
                    "available_at": (
                        date.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=18)
                    ).isoformat(),
                    "source": "synthetic_fixture",
                }
            )
        return pd.DataFrame(rows)

    return (
        market("002475", 0),
        news("002475", True),
        pd.concat([news("002241"), news("601138")]),
        pd.concat([market("002241", 0.5), market("601138", 1.2)]),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["smoke", "full"], default="full")
    parser.add_argument("--target-symbol", default="002475")
    for name in ["market", "target-news", "peer-news", "peer-market"]:
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--output-root", type=Path, default=ROOT / "outputs/runs")
    parser.add_argument(
        "--showcase",
        action="store_true",
        help="Select this completed Full run for the default reader",
    )
    args = parser.parse_args()
    if args.mode == "smoke":
        if (
            any([args.market, args.target_news, args.peer_news, args.peer_market])
            or args.showcase
        ):
            parser.error(
                "Smoke always uses the labelled fixture and cannot be promoted to showcase"
            )
        inputs = synthetic_inputs()
        config = CourseworkConfig(
            mode="smoke",
            episodes=2,
            seeds=(42,),
            nlp_days=20,
            train_days=20,
            test_days=10,
            folds=2,
        )
    else:
        paths = [args.market, args.target_news, args.peer_news, args.peer_market]
        if any(path is None or not path.is_file() for path in paths):
            parser.error(
                "Full needs four existing input CSVs; see docs/data.md. No fixture fallback is used."
            )
        inputs = tuple(pd.read_csv(path, dtype={"symbol": str}) for path in paths)
        config = CourseworkConfig(target_symbol=args.target_symbol)
    manifest_path = run_coursework(*inputs, config, args.output_root)
    if args.showcase:
        destination = ROOT / "outputs/showcase/manifest.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        manifest = json.loads(manifest_path.read_text())
        manifest["run_directory"] = str(manifest_path.parent.relative_to(ROOT))
        destination.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
