"""Canonical filesystem layout for generated project artifacts."""

from __future__ import annotations

import re
from pathlib import Path

from src.config.settings import PROJECT_ROOT


OUTPUTS_ROOT = PROJECT_ROOT / "outputs"
STOCK_OUTPUT_ROOT = OUTPUTS_ROOT / "stocks"
SYSTEM_OUTPUT_DIR = OUTPUTS_ROOT / "system"
DATABASE_DIR = OUTPUTS_ROOT / "database"

LEGACY_INTEGRATED_DIR = PROJECT_ROOT / "data" / "integrated"
LEGACY_RESULT_DIR = PROJECT_ROOT / "result"
LEGACY_REPORTS_DIR = PROJECT_ROOT / "reports"
LEGACY_RESULTS_DIR = PROJECT_ROOT / "results"


def normalize_symbol_for_path(symbol: str) -> str:
    """Return a stable stock-code folder name such as ``000625``."""

    text = str(symbol or "").strip().upper()
    match = re.search(r"\d{6}", text)
    return match.group(0) if match else text.replace(".", "_").replace("/", "_")


def stock_root_dir(symbol: str) -> Path:
    return STOCK_OUTPUT_ROOT / normalize_symbol_for_path(symbol)


def stock_data_dir(symbol: str) -> Path:
    return stock_root_dir(symbol) / "data"


def stock_reports_dir(symbol: str) -> Path:
    return stock_root_dir(symbol) / "reports"


def stock_results_dir(symbol: str) -> Path:
    return stock_root_dir(symbol) / "results"


def stock_models_dir(symbol: str) -> Path:
    return stock_root_dir(symbol) / "models"


def database_path() -> Path:
    return DATABASE_DIR / "trading_platform.db"


def ensure_stock_dirs(symbol: str) -> dict[str, Path]:
    paths = {
        "root": stock_root_dir(symbol),
        "data": stock_data_dir(symbol),
        "reports": stock_reports_dir(symbol),
        "results": stock_results_dir(symbol),
        "models": stock_models_dir(symbol),
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    SYSTEM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_empty_legacy_dirs()
    return paths


def legacy_data_dirs() -> list[Path]:
    return [LEGACY_INTEGRATED_DIR]


def cleanup_empty_legacy_dirs() -> None:
    """Remove empty legacy artifact directories that should no longer be used."""

    for path in [LEGACY_RESULT_DIR]:
        try:
            if path.exists() and path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        except OSError:
            continue
