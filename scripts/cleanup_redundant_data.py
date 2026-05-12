#!/usr/bin/env python3
"""Clean redundant generated data while preserving canonical program inputs.

The cleanup policy is intentionally conservative for source code and aggressive
for generated scraper clutter:

- build/update one ``*_finance_text_master.csv`` per stock in ``outputs/stocks/<symbol>/data/``;
- remove old diagnostics, source-specific event CSVs, and dated duplicate CSVs;
- remove stale per-run report files that can be regenerated;
- keep per-stock outputs under ``outputs/stocks/<symbol>/``.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORT))

from src.config.paths import LEGACY_INTEGRATED_DIR, LEGACY_RESULT_DIR, STOCK_OUTPUT_ROOT, stock_data_dir, stock_reports_dir
from src.config.settings import PROJECT_ROOT
from src.data_ingestion.cache import build_master_csv, plain_symbol


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean redundant generated data artifacts.")
    parser.add_argument("--apply", action="store_true", help="Actually delete files. Without this flag, only prints a dry run.")
    parser.add_argument(
        "--keep-reports",
        action="store_true",
        help="Keep per-run report artifacts. By default stale reports are removed because they can be regenerated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = _discover_finance_text_symbols()
    for symbol in symbols:
        build_master_csv(symbol)

    files_to_delete: list[Path] = []
    dirs_to_delete: list[Path] = []

    files_to_delete.extend(_result_files_to_delete())
    data_ds_store = PROJECT_ROOT / "data" / ".DS_Store"
    if data_ds_store.exists():
        files_to_delete.append(data_ds_store)
    if not args.keep_reports:
        files_to_delete.extend(_report_files_to_delete())

    for directory in [
        PROJECT_ROOT / "Library",
        PROJECT_ROOT / ".pytest_cache",
        PROJECT_ROOT / ".matplotlib_cache",
        PROJECT_ROOT / "program" / "__pycache__",
    ]:
        if directory.exists():
            dirs_to_delete.append(directory)

    _print_plan(files_to_delete, dirs_to_delete, args.apply)
    if args.apply:
        for path in files_to_delete:
            if path.exists():
                path.unlink()
        for path in dirs_to_delete:
            if path.exists():
                shutil.rmtree(path)
    return 0


def _discover_finance_text_symbols() -> list[str]:
    symbols = set()
    for root in [LEGACY_INTEGRATED_DIR, LEGACY_RESULT_DIR]:
        if root.exists():
            for path in root.glob("*_finance_text*.csv"):
                symbols.add(plain_symbol(path.name.split("_finance_text")[0]))
    if STOCK_OUTPUT_ROOT.exists():
        for folder in STOCK_OUTPUT_ROOT.iterdir():
            if folder.is_dir():
                symbols.add(plain_symbol(folder.name))
    return sorted(symbols)


def _result_files_to_delete() -> list[Path]:
    files: list[Path] = []
    for symbol in _discover_finance_text_symbols():
        data_dir = stock_data_dir(symbol)
        if not data_dir.exists():
            continue
        for path in data_dir.iterdir():
            if path.is_dir():
                continue
            name = path.name
            if name == ".DS_Store":
                files.append(path)
            elif "_diagnostic_" in name and path.suffix == ".json":
                files.append(path)
            elif name.endswith("_news_events_2026-04-15_2026-04-22.csv"):
                files.append(path)
    return sorted(set(files))


def _report_files_to_delete() -> list[Path]:
    files: list[Path] = []
    for symbol in _discover_finance_text_symbols():
        reports_dir = stock_reports_dir(symbol)
        if not reports_dir.exists():
            continue
        for path in reports_dir.iterdir():
            if path.is_file():
                files.append(path)
    return sorted(set(files))


def _print_plan(files: list[Path], dirs: list[Path], apply: bool) -> None:
    mode = "DELETE" if apply else "DRY RUN"
    print(f"{mode}: {len(files)} files and {len(dirs)} directories")
    for path in files[:200]:
        print(f"file: {path.relative_to(PROJECT_ROOT)}")
    if len(files) > 200:
        print(f"... {len(files) - 200} more files")
    for path in dirs:
        print(f"dir:  {path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    raise SystemExit(main())
