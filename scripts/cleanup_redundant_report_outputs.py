#!/usr/bin/env python3
"""Remove older duplicated per-range report artifacts while keeping the latest set."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import STOCK_OUTPUT_ROOT


STEM_PATTERN = re.compile(r"(?P<stem>\d{6}_finance_text_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2})_")


@dataclass
class CleanupStats:
    symbols_processed: int = 0
    removed_files: int = 0
    kept_stems: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Keep only the latest per-range report set for each stock.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting anything.")
    args = parser.parse_args()

    stats = CleanupStats()
    for stock_dir in sorted(path for path in STOCK_OUTPUT_ROOT.glob("*") if path.is_dir()):
        cleanup_reports(stock_dir, stats, dry_run=args.dry_run)

    print(
        "Cleanup summary:",
        {
            "symbols_processed": stats.symbols_processed,
            "removed_files": stats.removed_files,
            "kept_stems": stats.kept_stems,
            "dry_run": args.dry_run,
        },
    )
    return 0


def cleanup_reports(stock_dir: Path, stats: CleanupStats, *, dry_run: bool) -> None:
    reports_dir = stock_dir / "reports"
    if not reports_dir.exists():
        return

    groups: dict[str, list[Path]] = {}
    for path in reports_dir.iterdir():
        match = STEM_PATTERN.match(path.name)
        if match:
            groups.setdefault(match.group("stem"), []).append(path)

    if not groups:
        return

    stats.symbols_processed += 1
    keep_stem = newest_stem(groups)
    stats.kept_stems += 1
    print(f"Keeping latest report stem for {stock_dir.name}: {keep_stem}")

    for stem, files in sorted(groups.items()):
        if stem == keep_stem:
            continue
        for path in sorted(files):
            remove(path, dry_run=dry_run)
            stats.removed_files += 1


def newest_stem(groups: dict[str, list[Path]]) -> str:
    def stem_key(item: tuple[str, list[Path]]) -> float:
        stem, files = item
        summary = next((path for path in files if path.name.endswith("_analysis_summary.json")), None)
        anchor = summary or max(files, key=lambda path: path.stat().st_mtime)
        return anchor.stat().st_mtime

    return max(groups.items(), key=stem_key)[0]


def remove(path: Path, *, dry_run: bool) -> None:
    action = "Would remove" if dry_run else "Removed"
    print(f"{action}: {path}")
    if not dry_run:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
