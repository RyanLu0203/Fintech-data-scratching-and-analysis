"""Function-based data ingestion interface built around the existing scraper.

The project already has a working A-share scraper in ``program/run_scraper.py``.
This module keeps that workflow intact and exposes clean functions for the rest
of the platform.
"""

from __future__ import annotations

import logging
import math
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from src.config.paths import database_path
from src.config.settings import PROJECT_ROOT
from src.data_ingestion.cache import (
    BOUNDARY_TOLERANCE_DAYS,
    build_master_csv,
    default_range_csv,
    merge_csvs_into_master,
    normalize_integrated_frame,
    resolve_cached_csv,
    safe_symbol,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    symbol: str
    start_date: str
    end_date: str
    company_name: str = ""
    sources: str = "tencent"
    news_count: int = 5000
    reuse_existing_csv: bool = True
    require_news: bool = False
    output_csv: Path | None = None
    use_sqlite: bool = False
    sqlite_path: Path | None = None


def _as_config(config: IngestionConfig | Mapping[str, Any]) -> IngestionConfig:
    if isinstance(config, IngestionConfig):
        return config
    return IngestionConfig(
        symbol=str(config["symbol"]),
        company_name=str(config.get("company_name", "")),
        start_date=str(config["start_date"]),
        end_date=str(config["end_date"]),
        sources=str(config.get("sources", "tencent")),
        news_count=int(config.get("news_count", 5000)),
        reuse_existing_csv=bool(config.get("reuse_existing_csv", True)),
        require_news=bool(config.get("require_news", False)),
        output_csv=Path(config["output_csv"]) if config.get("output_csv") else None,
        use_sqlite=bool(config.get("use_sqlite", False)),
        sqlite_path=Path(config["sqlite_path"]) if config.get("sqlite_path") else None,
    )


def default_output_csv(symbol: str, start_date: str, end_date: str) -> Path:
    path = default_range_csv(symbol, start_date, end_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def run_ingestion(config: IngestionConfig | Mapping[str, Any]) -> Path:
    """Run or reuse the existing scraper and return the integrated CSV path."""

    cfg = _as_config(config)
    output_csv = cfg.output_csv or default_output_csv(cfg.symbol, cfg.start_date, cfg.end_date)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    if cfg.reuse_existing_csv:
        cached = resolve_cached_csv(cfg.symbol, cfg.start_date, cfg.end_date, output_csv=output_csv)
        if cached is not None:
            LOGGER.info("Using cached integrated CSV via %s: %s", cached.source, cached.path)
            _log_csv_health(cached.path)
            output_csv = cached.path
        else:
            output_csv = _fetch_missing_ranges_and_materialize(cfg, output_csv)
    else:
        output_csv = _run_existing_scraper(cfg, output_csv)
        build_master_csv(cfg.symbol)

    if cfg.use_sqlite:
        from src.storage.database import initialize_database, save_market_data, save_news_data

        sqlite_path = cfg.sqlite_path or database_path()
        initialize_database(sqlite_path)
        save_market_data(fetch_market_data(cfg.symbol, cfg.start_date, cfg.end_date, output_csv), sqlite_path)
        save_news_data(fetch_news_data(cfg.symbol, cfg.company_name, cfg.start_date, cfg.end_date, cfg.sources, cfg.news_count, output_csv), sqlite_path)

    return output_csv


def _fetch_missing_ranges_and_materialize(cfg: IngestionConfig, output_csv: Path) -> Path:
    """Fetch only the uncovered date gaps, merge into master, then slice the request.

    This avoids accumulating redundant same-stock files when a longer local
    timeline already exists for the requested symbol.
    """

    missing_ranges = _missing_ranges_against_master(cfg.symbol, cfg.start_date, cfg.end_date)
    if not missing_ranges:
        resolved = resolve_cached_csv(cfg.symbol, cfg.start_date, cfg.end_date, output_csv=output_csv)
        if resolved is not None:
            return resolved.path
        return output_csv

    temp_paths: list[Path] = []
    gap_failures: list[str] = []
    for start_date, end_date in missing_ranges:
        if pd.Timestamp(end_date) < pd.Timestamp(start_date):
            continue
        temp_path = output_csv.parent / f"{safe_symbol(cfg.symbol)}_finance_text_{start_date}_{end_date}_tmp.csv"
        gap_cfg = IngestionConfig(
            symbol=cfg.symbol,
            company_name=cfg.company_name,
            start_date=start_date,
            end_date=end_date,
            sources=cfg.sources,
            news_count=cfg.news_count,
            reuse_existing_csv=False,
            require_news=cfg.require_news,
            output_csv=temp_path,
            use_sqlite=False,
            sqlite_path=cfg.sqlite_path,
        )
        LOGGER.info("Fetching uncovered gap for %s: %s -> %s", cfg.symbol, start_date, end_date)
        if temp_path.exists() and _csv_covers_market_range(temp_path, start_date, end_date):
            LOGGER.info("Reusing previously fetched temporary gap CSV: %s", temp_path)
            temp_paths.append(temp_path)
            continue
        try:
            temp_paths.append(_run_existing_scraper(gap_cfg, temp_path))
        except Exception as exc:
            if temp_path.exists() and _csv_covers_market_range(temp_path, start_date, end_date):
                LOGGER.warning(
                    "Scraper failed for %s %s..%s, but an existing temporary gap CSV is usable: %s",
                    cfg.symbol,
                    start_date,
                    end_date,
                    temp_path,
                )
                temp_paths.append(temp_path)
                continue
            message = f"{start_date}..{end_date}: {exc}"
            gap_failures.append(message)
            LOGGER.warning("Failed to fetch uncovered gap for %s: %s", cfg.symbol, message)

    if temp_paths:
        merge_csvs_into_master(cfg.symbol, temp_paths, delete_sources=True)

    resolved = resolve_cached_csv(cfg.symbol, cfg.start_date, cfg.end_date, output_csv=output_csv)
    if resolved is None:
        overlap_path = _materialize_available_overlap(cfg.symbol, cfg.start_date, cfg.end_date, output_csv)
        if overlap_path is not None:
            coverage_note = _market_coverage_note(cfg.symbol)
            failure_note = f" Gap fetch failures: {' | '.join(gap_failures)}." if gap_failures else ""
            LOGGER.warning(
                "Requested range for %s could not be fully covered, but usable overlapping market data was materialized. %s%s",
                cfg.symbol,
                coverage_note,
                failure_note,
            )
            _log_csv_health(overlap_path)
            return overlap_path
        coverage_note = _market_coverage_note(cfg.symbol)
        failure_note = f" Gap fetch failures: {' | '.join(gap_failures)}." if gap_failures else ""
        raise FileNotFoundError(
            f"After fetching uncovered gaps, requested range still could not be materialized for "
            f"{cfg.symbol} from {cfg.start_date} to {cfg.end_date}. {coverage_note}{failure_note}"
        )
    _log_csv_health(resolved.path)
    return resolved.path


def _run_existing_scraper(cfg: IngestionConfig, output_csv: Path) -> Path:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "program" / "run_scraper.py"),
        cfg.symbol,
        "--start-date",
        cfg.start_date,
        "--end-date",
        cfg.end_date,
        "--sources",
        cfg.sources,
        "--news-count",
        str(cfg.news_count),
        "-o",
        str(output_csv),
    ]
    if cfg.company_name:
        cmd.extend(["--company-name", cfg.company_name])
    if cfg.require_news:
        cmd.append("--require-news")
    LOGGER.info("Running existing scraper: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))
    frame = pd.read_csv(output_csv)
    frame = normalize_integrated_frame(frame, symbol=cfg.symbol, preferred_company_name=cfg.company_name)
    frame.to_csv(output_csv, index=False, encoding="utf-8-sig")
    _log_csv_health(output_csv)
    return output_csv


def _csv_covers_market_range(path: Path, start_date: str, end_date: str) -> bool:
    try:
        frame = pd.read_csv(path)
    except Exception:
        return False
    if frame.empty or "date" not in frame.columns:
        return False
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    if "close" in data.columns:
        data["close"] = pd.to_numeric(data["close"], errors="coerce")
        data = data.dropna(subset=["date", "close"])
    else:
        data = data.dropna(subset=["date"])
    if data.empty:
        return False
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)
    first_date = data["date"].min()
    last_date = data["date"].max()
    has_overlap = ((data["date"] >= requested_start) & (data["date"] <= requested_end)).any()
    if not has_overlap:
        return False
    start_gap = max(0, (first_date - requested_start).days)
    end_gap = max(0, (requested_end - last_date).days)
    return (first_date <= requested_start or start_gap <= BOUNDARY_TOLERANCE_DAYS) and (
        last_date >= requested_end or end_gap <= BOUNDARY_TOLERANCE_DAYS
    )


def _market_coverage_note(symbol: str) -> str:
    master = build_master_csv(symbol)
    if master is None or master.empty or "date" not in master.columns or "close" not in master.columns:
        return "No local market coverage is available."
    data = master.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    market_dates = data.dropna(subset=["date", "close"])["date"]
    if market_dates.empty:
        return "No local market rows with close price are available."
    return f"Available market coverage is {market_dates.min().date()} to {market_dates.max().date()}."


def _materialize_available_overlap(symbol: str, start_date: str, end_date: str, output_csv: Path) -> Path | None:
    """Write the locally available overlap when the requested left side is unfillable.

    Recently listed stocks can legitimately have no rows before their listing
    date. In that case the scraper reports "No rows to write" for the pre-listing
    gap; returning the available overlap keeps the dashboard usable while
    downstream diagnostics mark the date-range gap as not fully reliable.
    """

    master = build_master_csv(symbol)
    if master is None or master.empty or "date" not in master.columns:
        return None
    data = master.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    if "close" in data.columns:
        data["close"] = pd.to_numeric(data["close"], errors="coerce")
        data = data.dropna(subset=["date", "close"])
    else:
        data = data.dropna(subset=["date"])
    if data.empty:
        return None
    mask = (data["date"] >= pd.Timestamp(start_date)) & (data["date"] <= pd.Timestamp(end_date))
    overlap = data.loc[mask].copy().sort_values("date").reset_index(drop=True)
    if overlap.empty:
        return None
    overlap = normalize_integrated_frame(overlap, symbol=symbol)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    overlap.to_csv(output_csv, index=False, encoding="utf-8-sig")
    LOGGER.warning(
        "Materialized partial available range for %s: %s..%s into requested output %s.",
        symbol,
        overlap["date"].min().date(),
        overlap["date"].max().date(),
        output_csv,
    )
    return output_csv


def _missing_ranges_against_master(symbol: str, start_date: str, end_date: str) -> list[tuple[str, str]]:
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)
    master = build_master_csv(symbol)
    if master is None or master.empty or "date" not in master.columns:
        return [(start_date, end_date)]

    dates = pd.to_datetime(master["date"], errors="coerce").dropna().sort_values()
    if dates.empty:
        return [(start_date, end_date)]

    first_date = dates.min()
    last_date = dates.max()
    gaps: list[tuple[str, str]] = []

    # Missing left side beyond a small boundary tolerance (for holidays/weekends).
    if first_date > requested_start and (first_date - requested_start).days > BOUNDARY_TOLERANCE_DAYS:
        left_end = min(requested_end, first_date - pd.Timedelta(days=1))
        if left_end >= requested_start:
            gaps.append((requested_start.date().isoformat(), left_end.date().isoformat()))

    # Requested period entirely after current master.
    elif requested_start > last_date and (requested_start - last_date).days > 0:
        gaps.append((requested_start.date().isoformat(), requested_end.date().isoformat()))
        return gaps

    # Missing right side beyond tolerance.
    if last_date < requested_end and (requested_end - last_date).days > BOUNDARY_TOLERANCE_DAYS:
        right_start = max(requested_start, last_date + pd.Timedelta(days=1))
        if requested_end >= right_start:
            gaps.append((right_start.date().isoformat(), requested_end.date().isoformat()))

    # Requested period entirely before current master.
    if requested_end < first_date and (first_date - requested_end).days > 0:
        return [(requested_start.date().isoformat(), requested_end.date().isoformat())]

    return gaps


def fetch_market_data(
    symbol: str,
    start_date: str,
    end_date: str,
    input_csv: Path | None = None,
) -> pd.DataFrame:
    """Load market OHLCV data from the integrated scraper CSV."""

    csv_path = input_csv or default_output_csv(symbol, start_date, end_date)
    if not csv_path.exists():
        raise FileNotFoundError(f"Market CSV not found: {csv_path}. Run ingestion first.")
    frame = pd.read_csv(csv_path)
    frame["date"] = pd.to_datetime(frame["date"])
    for column in ["open", "high", "low", "close", "volume"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    market = frame[["symbol", "date", "open", "high", "low", "close", "volume"]].copy()
    market = market.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    LOGGER.info("Loaded market rows=%s source=%s path=%s", len(market), symbol, csv_path)
    return market


def fetch_news_data(
    symbol: str,
    company_name: str,
    start_date: str,
    end_date: str,
    sources: str = "tencent",
    news_count: int = 5000,
    input_csv: Path | None = None,
) -> pd.DataFrame:
    """Extract item-level news/events from the integrated scraper CSV."""

    csv_path = input_csv or default_output_csv(symbol, start_date, end_date)
    if not csv_path.exists():
        raise FileNotFoundError(f"News source CSV not found: {csv_path}. Run ingestion first.")
    data = pd.read_csv(csv_path)
    data["date"] = pd.to_datetime(data["date"])
    records: list[dict[str, Any]] = []
    for _, row in data.iterrows():
        titles = _split_events(row.get("event_titles", ""))
        summaries = _split_events(row.get("event_summaries", ""))
        publishers = _split_events(row.get("event_publishers", ""))
        links = _split_events(row.get("event_links", ""))
        event_count = pd.to_numeric(pd.Series([row.get("event_count", 0)]), errors="coerce").fillna(0).iloc[0]
        count = max(len(titles), len(summaries), int(event_count))
        if count == 0:
            continue
        for index in range(count):
            title = titles[index] if index < len(titles) else ""
            content = summaries[index] if index < len(summaries) else title
            source = publishers[index] if index < len(publishers) else str(row.get("data_source", sources))
            link = links[index] if index < len(links) else ""
            records.append(
                {
                    "news_id": f"{safe_symbol(symbol)}-{row['date']:%Y%m%d}-{index}",
                    "ticker": row.get("symbol", symbol),
                    "date": row["date"],
                    "title": title,
                    "content": content,
                    "source": source,
                    "url": link,
                    "company_name": company_name or row.get("company_name", ""),
                }
            )
    news = pd.DataFrame(records)
    if not news.empty and news_count > 0:
        unique_days = max(int(news["date"].nunique()), 1)
        per_day_cap = max(1, min(50, math.ceil(news_count / unique_days * 3)))
        news = news.sort_values(["date", "news_id"]).reset_index(drop=True)
        news["_rank_in_day"] = news.groupby("date").cumcount()
        news = news.loc[news["_rank_in_day"] < per_day_cap].copy()
        if len(news) > news_count:
            # Round-robin by within-day rank so a recency spike does not consume
            # the whole NLP budget and erase older dates from the experiment.
            news = news.sort_values(["_rank_in_day", "date", "news_id"]).head(news_count)
        news = news.drop(columns=["_rank_in_day"]).sort_values(["date", "news_id"]).reset_index(drop=True)
    LOGGER.info(
        "Loaded news/event rows=%s symbol=%s sources=%s news_count_cap=%s path=%s",
        len(news),
        symbol,
        sources,
        news_count,
        csv_path,
    )
    return news


def _split_events(value: Any) -> list[str]:
    if pd.isna(value) or value is None:
        return []
    return [part.strip() for part in str(value).split(" || ") if part and part.strip()]


def _log_csv_health(csv_path: Path) -> None:
    frame = pd.read_csv(csv_path)
    missing = frame.isna().sum().to_dict()
    LOGGER.info("Integrated CSV path=%s rows=%s columns=%s", csv_path, len(frame), len(frame.columns))
    LOGGER.info("Missing values by column: %s", {k: v for k, v in missing.items() if v})
