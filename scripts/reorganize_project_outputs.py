#!/usr/bin/env python3
"""Move generated artifacts into the final per-stock output layout.

Final layout:

```
outputs/
  stocks/
    000625/
      data/
      reports/
      results/
      models/
  system/
  database/
```
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import sys
from pathlib import Path

PROJECT_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORT))

from src.config.paths import (  # noqa: E402
    DATABASE_DIR,
    LEGACY_INTEGRATED_DIR,
    LEGACY_REPORTS_DIR,
    LEGACY_RESULT_DIR,
    LEGACY_RESULTS_DIR,
    SYSTEM_OUTPUT_DIR,
    database_path,
    ensure_stock_dirs,
    normalize_symbol_for_path,
    stock_data_dir,
    stock_reports_dir,
    stock_results_dir,
)
from src.config.settings import PROJECT_ROOT  # noqa: E402
from src.data_ingestion.cache import build_master_csv  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reorganize generated artifacts into outputs/stocks/<symbol>/.")
    parser.add_argument("--apply", action="store_true", help="Move/delete files. Without this flag, prints a dry run.")
    parser.add_argument("--remove-caches", action="store_true", help="Remove .pytest_cache and stray .DS_Store files.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    moves: list[tuple[Path, Path]] = []
    deletes: list[Path] = []

    moves.extend(_finance_text_moves())
    moves.extend(_report_moves())
    moves.extend(_latest_result_moves())
    moves.extend(_database_moves())

    if args.remove_caches:
        deletes.extend(_cache_paths())

    _print_plan(moves, deletes, args.apply)
    if not args.apply:
        return 0

    for src, dst in moves:
        _move_or_merge(src, dst)

    for symbol in sorted(_discover_symbols_from_outputs()):
        build_master_csv(symbol)

    for path in deletes:
        _remove_path(path)

    _remove_empty_legacy_dirs()
    return 0


def _finance_text_moves() -> list[tuple[Path, Path]]:
    moves: list[tuple[Path, Path]] = []
    for directory in [LEGACY_INTEGRATED_DIR, LEGACY_RESULT_DIR]:
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.is_dir() or path.name == ".DS_Store":
                continue
            symbol = _symbol_from_name(path.name)
            if not symbol:
                continue
            ensure_stock_dirs(symbol)
            moves.append((path, stock_data_dir(symbol) / path.name))
    return moves


def _report_moves() -> list[tuple[Path, Path]]:
    moves: list[tuple[Path, Path]] = []
    if not LEGACY_REPORTS_DIR.exists():
        return moves
    for path in LEGACY_REPORTS_DIR.iterdir():
        if path.is_dir():
            continue
        if path.name == "system_architecture.mmd":
            moves.append((path, SYSTEM_OUTPUT_DIR / path.name))
            continue
        symbol = _symbol_from_name(path.name)
        if not symbol:
            continue
        ensure_stock_dirs(symbol)
        moves.append((path, stock_reports_dir(symbol) / path.name))
    return moves


def _latest_result_moves() -> list[tuple[Path, Path]]:
    if not LEGACY_RESULTS_DIR.exists():
        return []
    symbol = _latest_report_symbol()
    if not symbol:
        return []
    ensure_stock_dirs(symbol)
    moves: list[tuple[Path, Path]] = []
    for path in LEGACY_RESULTS_DIR.iterdir():
        if path.is_dir():
            continue
        target_dir = stock_results_dir(symbol)
        if path.suffix == ".pt":
            target_dir = ensure_stock_dirs(symbol)["models"]
        moves.append((path, target_dir / path.name))
    return moves


def _database_moves() -> list[tuple[Path, Path]]:
    legacy = PROJECT_ROOT / "data" / "trading_platform.db"
    if legacy.exists():
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        return [(legacy, database_path())]
    return []


def _cache_paths() -> list[Path]:
    paths = [PROJECT_ROOT / ".pytest_cache"]
    paths.extend(PROJECT_ROOT.glob("**/.DS_Store"))
    return [path for path in paths if path.exists()]


def _symbol_from_name(name: str) -> str:
    match = re.match(r"(\d{6})", name)
    return normalize_symbol_for_path(match.group(1)) if match else ""


def _latest_report_symbol() -> str:
    if not LEGACY_REPORTS_DIR.exists():
        return ""
    candidates = []
    for path in LEGACY_REPORTS_DIR.glob("*_ablation_metrics.csv"):
        symbol = _symbol_from_name(path.name)
        if symbol:
            candidates.append((path.stat().st_mtime, symbol))
    if not candidates:
        return ""
    return sorted(candidates)[-1][1]


def _discover_symbols_from_outputs() -> set[str]:
    root = PROJECT_ROOT / "outputs" / "stocks"
    if not root.exists():
        return set()
    return {path.name for path in root.iterdir() if path.is_dir()}


def _move_or_merge(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if src.is_file() and dst.is_file() and _sha256(src) == _sha256(dst):
            src.unlink()
            return
        alternate = dst.with_name(f"{dst.stem}_legacy{dst.suffix}")
        counter = 1
        while alternate.exists():
            alternate = dst.with_name(f"{dst.stem}_legacy_{counter}{dst.suffix}")
            counter += 1
        shutil.move(str(src), str(alternate))
        return
    shutil.move(str(src), str(dst))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _remove_empty_legacy_dirs() -> None:
    for path in [LEGACY_RESULT_DIR, LEGACY_REPORTS_DIR, LEGACY_RESULTS_DIR, LEGACY_INTEGRATED_DIR, PROJECT_ROOT / "data"]:
        try:
            path.rmdir()
        except OSError:
            pass


def _print_plan(moves: list[tuple[Path, Path]], deletes: list[Path], apply: bool) -> None:
    mode = "APPLY" if apply else "DRY RUN"
    print(f"{mode}: {len(moves)} moves, {len(deletes)} removals")
    for src, dst in moves[:250]:
        print(f"move: {src.relative_to(PROJECT_ROOT)} -> {dst.relative_to(PROJECT_ROOT)}")
    if len(moves) > 250:
        print(f"... {len(moves) - 250} more moves")
    for path in deletes:
        print(f"remove: {path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    raise SystemExit(main())
