"""Freeze the predetermined four electronics companies for real-data evaluation."""

from pathlib import Path
import argparse
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.data.historical_sources import freeze_sources  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2025-03-31")
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    print(
        freeze_sources(
            ("002475", "601138", "002241", "601231"),
            args.start,
            args.end,
            args.destination,
        )
    )
