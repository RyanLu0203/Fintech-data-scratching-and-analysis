"""CSV cache and master timeline utilities for integrated finance text data."""

from __future__ import annotations

import logging
import re
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.config.settings import PROJECT_ROOT
from src.config.paths import legacy_data_dirs, stock_data_dir

LOGGER = logging.getLogger(__name__)
INTEGRATED_DIR = PROJECT_ROOT / "outputs" / "stocks"
BOUNDARY_TOLERANCE_DAYS = 10
_STOCK_ALIAS_CACHE: dict[str, str] | None = None

CANONICAL_INTEGRATED_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adjclose",
    "volume",
    "dividend",
    "split",
    "data_source",
    "company_name",
    "exchange",
    "currency",
    "instrument_type",
    "market_cap_latest",
    "trailing_pe_latest",
    "forward_pe_latest",
    "fifty_two_week_high_latest",
    "fifty_two_week_low_latest",
    "event_count",
    "event_titles",
    "event_summaries",
    "event_publishers",
    "event_links",
    "event_source_types",
    "external_event_count",
    "generated_event_count",
    "has_external_text",
    "keywords",
    "amount",
    "amplitude_pct",
    "change_pct",
    "change",
    "turnover_pct",
]


@dataclass(frozen=True)
class CacheResolution:
    path: Path
    source: str
    master_path: Path | None = None
    rows: int = 0


def plain_symbol(symbol: str) -> str:
    """Normalize symbols such as ``002475.SZ`` and ``002475`` to ``002475``."""

    text = str(symbol).strip().upper()
    match = re.search(r"\d{6}", text)
    return match.group(0) if match else text.replace(".", "_")


def safe_symbol(symbol: str) -> str:
    return plain_symbol(symbol).replace(".", "_")


def _load_stock_aliases() -> dict[str, str]:
    global _STOCK_ALIAS_CACHE
    if _STOCK_ALIAS_CACHE is not None:
        return _STOCK_ALIAS_CACHE
    aliases: dict[str, str] = {}
    mapping_path = PROJECT_ROOT / "config" / "stock_sector_mapping.csv"
    if mapping_path.exists():
        try:
            mapping = pd.read_csv(mapping_path, dtype=str)
            if {"symbol", "company_name"}.issubset(mapping.columns):
                for _, row in mapping.iterrows():
                    symbol = plain_symbol(row.get("symbol", ""))
                    name = str(row.get("company_name", "") or "").strip()
                    if symbol and name and not _looks_like_symbol_name(name, symbol):
                        aliases[symbol] = name
        except Exception:
            aliases = {}

    path = PROJECT_ROOT / "config" / "stock_aliases.json"
    if not path.exists():
        _STOCK_ALIAS_CACHE = aliases
        return _STOCK_ALIAS_CACHE
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        _STOCK_ALIAS_CACHE = aliases
        return _STOCK_ALIAS_CACHE
    aliases.update({plain_symbol(key): str(value).strip() for key, value in payload.items() if str(value).strip()})
    _STOCK_ALIAS_CACHE = aliases
    return _STOCK_ALIAS_CACHE


def _looks_like_symbol_name(value: str, symbol: str) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return True
    normalized_symbol = plain_symbol(symbol)
    stripped_symbol = normalized_symbol.lstrip("0") or normalized_symbol
    symbol_variants = {
        normalized_symbol.upper(),
        normalized_symbol,
        stripped_symbol.upper(),
        stripped_symbol,
        f"{normalized_symbol}.SZ",
        f"{normalized_symbol}.SS",
        f"SZ{normalized_symbol}",
        f"SH{normalized_symbol}",
    }
    if text in {variant.upper() for variant in symbol_variants}:
        return True
    return bool(text.isdigit() and (text == stripped_symbol or text.zfill(6) == normalized_symbol))


def master_csv_path(symbol: str, integrated_dir: Path | None = None) -> Path:
    return _effective_integrated_dir(symbol, integrated_dir) / f"{safe_symbol(symbol)}_finance_text_master.csv"


def default_range_csv(symbol: str, start_date: str, end_date: str, integrated_dir: Path | None = None) -> Path:
    return _effective_integrated_dir(symbol, integrated_dir) / f"{safe_symbol(symbol)}_finance_text_{start_date}_{end_date}.csv"


def resolve_cached_csv(
    symbol: str,
    start_date: str,
    end_date: str,
    output_csv: Path | None = None,
    integrated_dir: Path | None = None,
    build_master: bool = True,
) -> CacheResolution | None:
    """Return a cached CSV for the requested interval when coverage exists.

    If a master or previous scrape covers the requested interval, this function
    materializes only the requested rows to ``output_csv`` and returns that path.
    It does not perform network scraping.
    """

    integrated_dir = _effective_integrated_dir(symbol, integrated_dir)
    integrated_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_csv or default_range_csv(symbol, start_date, end_date, integrated_dir)
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)

    if output_csv.exists():
        frame = _read_integrated_csv(output_csv)
        if _covers(frame, requested_start, requested_end):
            return CacheResolution(output_csv, "exact_csv", master_csv_path(symbol, integrated_dir), len(frame))

    master = build_master_csv(symbol, integrated_dir=integrated_dir) if build_master else _read_master_if_exists(symbol, integrated_dir)
    if master is None or master.empty:
        return None

    if not _covers(master, requested_start, requested_end):
        return None

    sliced = _slice_range(master, requested_start, requested_end)
    if sliced.empty:
        return None
    sliced = normalize_integrated_frame(sliced, symbol=symbol)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    sliced.to_csv(output_csv, index=False, encoding="utf-8-sig")
    LOGGER.info(
        "Resolved %s %s..%s from cached master: %s rows -> %s",
        symbol,
        start_date,
        end_date,
        len(sliced),
        output_csv,
    )
    return CacheResolution(output_csv, "master_slice", master_csv_path(symbol, integrated_dir), len(sliced))


def cache_covers_range(symbol: str, start_date: str, end_date: str, integrated_dir: Path | None = None) -> bool:
    """Return True when existing same-stock data covers the requested interval."""

    output_csv = default_range_csv(symbol, start_date, end_date, integrated_dir)
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)
    if output_csv.exists():
        try:
            if _covers(_read_integrated_csv(output_csv), requested_start, requested_end):
                return True
        except Exception:
            pass
    master = build_master_csv(symbol, integrated_dir=integrated_dir)
    return bool(master is not None and _covers(master, requested_start, requested_end))


def build_master_csv(symbol: str, integrated_dir: Path | None = None) -> pd.DataFrame | None:
    """Merge all same-stock integrated CSV files into one canonical timeline."""

    integrated_dir = _effective_integrated_dir(symbol, integrated_dir)
    integrated_dir.mkdir(parents=True, exist_ok=True)
    files = list(find_integrated_csvs(symbol, integrated_dir=integrated_dir))
    if not files:
        return None

    frames = []
    for path in files:
        try:
            frame = _read_integrated_csv(path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable integrated CSV %s: %s", path, exc)
            continue
        if frame.empty or "date" not in frame.columns:
            continue
        if not _same_symbol_frame(frame, symbol):
            continue
        frame = normalize_integrated_frame(frame, symbol=symbol)
        frame["_source_path"] = path.name
        frame["_quality_score"] = _row_quality(frame)
        frames.append(frame)

    if not frames:
        return None

    merged = _merge_frames(frames)

    master_path = master_csv_path(symbol, integrated_dir)
    merged.to_csv(master_path, index=False, encoding="utf-8-sig")
    LOGGER.info(
        "Built master CSV for %s: %s rows, %s..%s -> %s",
        symbol,
        len(merged),
        merged["date"].min().date(),
        merged["date"].max().date(),
        master_path,
    )
    return merged


def find_integrated_csvs(symbol: str, integrated_dir: Path | None = None) -> Iterable[Path]:
    wanted = safe_symbol(symbol)
    seen: set[Path] = set()
    for directory in _candidate_data_dirs(symbol, integrated_dir):
        if not directory.exists():
            continue
        for path in sorted(directory.glob(f"{wanted}_finance_text*.csv")):
            if path.name.endswith("_tmp.csv") or path in seen:
                continue
            seen.add(path)
            yield path


def materialize_range_from_master(
    symbol: str,
    start_date: str,
    end_date: str,
    output_csv: Path | None = None,
    integrated_dir: Path | None = None,
) -> Path:
    resolved = resolve_cached_csv(symbol, start_date, end_date, output_csv, integrated_dir=integrated_dir, build_master=True)
    if resolved is None:
        raise FileNotFoundError(f"No cached data covers {symbol} from {start_date} to {end_date}.")
    return resolved.path


def merge_csvs_into_master(
    symbol: str,
    csv_paths: Iterable[Path],
    integrated_dir: Path | None = None,
    *,
    delete_sources: bool = False,
) -> Path | None:
    """Merge one or more newly created CSVs into the canonical master timeline.

    This keeps one authoritative same-stock timeline while allowing callers to
    fetch only the missing date gaps into temporary files.
    """

    integrated_dir = _effective_integrated_dir(symbol, integrated_dir)
    frames = []

    existing_master = _read_master_if_exists(symbol, integrated_dir)
    if existing_master is not None and not existing_master.empty:
        master_frame = existing_master.copy()
        master_frame["_source_path"] = master_csv_path(symbol, integrated_dir).name
        master_frame["_quality_score"] = _row_quality(master_frame)
        frames.append(master_frame)

    source_paths = [Path(path) for path in csv_paths]
    for path in source_paths:
        if not path.exists():
            continue
        try:
            frame = _read_integrated_csv(path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable merge source %s: %s", path, exc)
            continue
        if frame.empty or "date" not in frame.columns:
            continue
        if not _same_symbol_frame(frame, symbol):
            continue
        frame = normalize_integrated_frame(frame, symbol=symbol)
        frame["_source_path"] = path.name
        frame["_quality_score"] = _row_quality(frame)
        frames.append(frame)

    if not frames:
        return None

    merged = _merge_frames(frames)
    target = master_csv_path(symbol, integrated_dir)
    merged.to_csv(target, index=False, encoding="utf-8-sig")
    LOGGER.info(
        "Merged %s source CSV(s) into master for %s: %s rows, %s..%s -> %s",
        len(source_paths),
        symbol,
        len(merged),
        merged["date"].min().date() if not merged.empty else "n/a",
        merged["date"].max().date() if not merged.empty else "n/a",
        target,
    )

    if delete_sources:
        for path in source_paths:
            if path.exists():
                try:
                    path.unlink()
                except OSError as exc:
                    LOGGER.warning("Failed to delete temporary merge source %s: %s", path, exc)

    return target


def _read_master_if_exists(symbol: str, integrated_dir: Path) -> pd.DataFrame | None:
    path = master_csv_path(symbol, integrated_dir)
    return _read_integrated_csv(path) if path.exists() else None


def _merge_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = normalize_integrated_frame(merged)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.dropna(subset=["date"]).sort_values(["date", "_quality_score"])
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged.drop(columns=[col for col in ["_source_path", "_quality_score"] if col in merged.columns])


def _effective_integrated_dir(symbol: str, integrated_dir: Path | None = None) -> Path:
    if integrated_dir is not None:
        return integrated_dir
    return stock_data_dir(symbol)


def _candidate_data_dirs(symbol: str, integrated_dir: Path | None = None) -> list[Path]:
    primary = _effective_integrated_dir(symbol, integrated_dir)
    if integrated_dir is not None:
        return [primary]
    candidates = [primary]
    for path in legacy_data_dirs():
        if path not in candidates:
            candidates.append(path)
    return candidates


def _read_integrated_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    return normalize_integrated_frame(frame)


def _same_symbol_frame(frame: pd.DataFrame, symbol: str) -> bool:
    wanted = plain_symbol(symbol)
    if "symbol" in frame.columns and frame["symbol"].notna().any():
        symbols = {plain_symbol(value) for value in frame["symbol"].dropna().astype(str).head(20)}
        return wanted in symbols
    return True


def _covers(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if frame.empty or "date" not in frame.columns:
        return False
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return False
    first_date = dates.min()
    last_date = dates.max()
    in_range = ((dates >= start) & (dates <= end)).any()
    if not in_range:
        return False

    start_gap_days = max(0, (first_date - start).days)
    end_gap_days = max(0, (end - last_date).days)
    return (
        first_date <= start or start_gap_days <= BOUNDARY_TOLERANCE_DAYS
    ) and (
        last_date >= end or end_gap_days <= BOUNDARY_TOLERANCE_DAYS
    )


def _slice_range(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data[(data["date"] >= start) & (data["date"] <= end)].copy()
    return data.sort_values("date").reset_index(drop=True)


def _row_quality(frame: pd.DataFrame) -> pd.Series:
    score = pd.Series(0.0, index=frame.index)
    if "external_event_count" in frame.columns:
        score += pd.to_numeric(frame["external_event_count"], errors="coerce").fillna(0.0) * 1000
    if "event_count" in frame.columns:
        score += pd.to_numeric(frame["event_count"], errors="coerce").fillna(0.0) * 10
    if "has_external_text" in frame.columns:
        score += pd.to_numeric(frame["has_external_text"], errors="coerce").fillna(0.0) * 100
    for column in ["amount", "change_pct", "turnover_pct", "keywords", "event_summaries"]:
        if column in frame.columns:
            score += frame[column].notna().astype(float)
    return score


def normalize_integrated_frame(
    frame: pd.DataFrame,
    symbol: str | None = None,
    preferred_company_name: str | None = None,
) -> pd.DataFrame:
    """Force integrated finance_text CSVs into one stable schema.

    This makes future single-stock and cross-stock analysis operate on the same
    column set and column order, even when upstream source payloads differ.
    """

    data = frame.copy()
    rename_map: dict[str, str] = {}
    if "instrumentType" in data.columns and "instrument_type" not in data.columns:
        rename_map["instrumentType"] = "instrument_type"
    if "quoteType" in data.columns and "instrument_type" not in data.columns:
        rename_map["quoteType"] = "instrument_type"
    if "longName" in data.columns and "company_name" not in data.columns:
        rename_map["longName"] = "company_name"
    if rename_map:
        data = data.rename(columns=rename_map)

    if "symbol" not in data.columns:
        data["symbol"] = plain_symbol(symbol or "")
    elif symbol:
        data["symbol"] = data["symbol"].fillna(plain_symbol(symbol)).replace("", plain_symbol(symbol))

    if "date" in data.columns:
        data["date"] = pd.to_datetime(data["date"], errors="coerce")

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "adjclose",
        "volume",
        "dividend",
        "split",
        "market_cap_latest",
        "trailing_pe_latest",
        "forward_pe_latest",
        "fifty_two_week_high_latest",
        "fifty_two_week_low_latest",
        "event_count",
        "external_event_count",
        "generated_event_count",
        "has_external_text",
        "amount",
        "amplitude_pct",
        "change_pct",
        "change",
        "turnover_pct",
    ]
    text_defaults = {
        "data_source": "",
        "company_name": "",
        "exchange": "",
        "currency": "",
        "instrument_type": "",
        "event_titles": "",
        "event_summaries": "",
        "event_publishers": "",
        "event_links": "",
        "event_source_types": "",
        "keywords": "",
    }
    for column in CANONICAL_INTEGRATED_COLUMNS:
        if column not in data.columns:
            if column in text_defaults:
                data[column] = text_defaults[column]
            else:
                data[column] = pd.NA

    for column in numeric_columns:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")

    for column, default in text_defaults.items():
        if column in data.columns:
            data[column] = data[column].fillna(default)

    normalized_symbol = plain_symbol(symbol or "")
    if not normalized_symbol and "symbol" in data.columns and data["symbol"].notna().any():
        normalized_symbol = plain_symbol(str(data["symbol"].dropna().iloc[0]))
    preferred_name = str(preferred_company_name or "").strip() or _load_stock_aliases().get(normalized_symbol, "")
    if "company_name" in data.columns:
        data["company_name"] = data["company_name"].astype(str)
        company_series = data["company_name"].astype(str).str.strip()
        invalid_mask = company_series.eq("") | company_series.apply(lambda value: _looks_like_symbol_name(value, normalized_symbol))
        if preferred_name:
            data.loc[invalid_mask, "company_name"] = preferred_name

    canonical = [column for column in CANONICAL_INTEGRATED_COLUMNS if column in data.columns]
    extras = [column for column in data.columns if column not in canonical]
    return data[canonical + extras]
