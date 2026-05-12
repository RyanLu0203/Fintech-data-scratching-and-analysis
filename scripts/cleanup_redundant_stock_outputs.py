#!/usr/bin/env python3
"""Clean redundant per-stock output files while preserving reproducible artifacts.

This script is intentionally conservative:

- Keep each stock's canonical ``*_finance_text_master.csv``
- Remove range CSVs in ``data/`` only when the master already covers that range
- Remove scraper diagnostic JSON files in ``data/``
- Keep ``reports/``, ``results/``, and ``models/`` intact
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import STOCK_OUTPUT_ROOT
from src.data_ingestion.cache import build_master_csv


RANGE_PATTERN = re.compile(r"(?P<symbol>\d{6})_finance_text_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.csv$")


@dataclass
class CleanupStats:
    removed_range_csvs: int = 0
    removed_diagnostic_jsons: int = 0
    kept_range_csvs: int = 0
    symbols_processed: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove redundant per-stock data files covered by master CSVs.")
    parser.add_argument("--dry-run", action="store_true", help="List what would be deleted without deleting anything.")
    args = parser.parse_args()

    stats = CleanupStats()
    for stock_dir in sorted(path for path in STOCK_OUTPUT_ROOT.glob("*") if path.is_dir()):
        cleanup_stock_dir(stock_dir, stats, dry_run=args.dry_run)

    print(
        "Cleanup summary:",
        {
            "symbols_processed": stats.symbols_processed,
            "removed_range_csvs": stats.removed_range_csvs,
            "removed_diagnostic_jsons": stats.removed_diagnostic_jsons,
            "kept_range_csvs": stats.kept_range_csvs,
            "dry_run": args.dry_run,
        },
    )
    return 0


def cleanup_stock_dir(stock_dir: Path, stats: CleanupStats, *, dry_run: bool) -> None:
    symbol = stock_dir.name
    data_dir = stock_dir / "data"
    if not data_dir.exists():
        return

    stats.symbols_processed += 1
    master = build_master_csv(symbol)
    master_dates = None
    if master is not None and not master.empty and "date" in master.columns:
        master_dates = pd.to_datetime(master["date"], errors="coerce").dropna()

    for path in sorted(data_dir.iterdir()):
        if path.name.endswith("_finance_text_master.csv"):
            continue

        if path.suffix.lower() == ".json" and "_diagnostic_" in path.name:
            remove(path, dry_run=dry_run)
            stats.removed_diagnostic_jsons += 1
            continue

        match = RANGE_PATTERN.fullmatch(path.name)
        if not match:
            continue

        if master_dates is None or master_dates.empty:
            stats.kept_range_csvs += 1
            continue

        start = pd.Timestamp(match.group("start"))
        end = pd.Timestamp(match.group("end"))
        if master_covers_range(master_dates, start, end):
            remove(path, dry_run=dry_run)
            stats.removed_range_csvs += 1
        else:
            stats.kept_range_csvs += 1


def master_covers_range(master_dates: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if master_dates.empty:
        return False
    has_rows_in_window = ((master_dates >= start) & (master_dates <= end)).any()
    if not has_rows_in_window:
        return False
    return master_dates.min() <= end and master_dates.max() >= start


def remove(path: Path, *, dry_run: bool) -> None:
    action = "Would remove" if dry_run else "Removed"
    print(f"{action}: {path}")
    if not dry_run:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
