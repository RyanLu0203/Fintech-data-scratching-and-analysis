"""Database helpers.

SQLite is optional in this project. The platform writes CSV artifacts by
default, but these helpers make it easy to persist the core tables when
``--use-sqlite`` is enabled.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.paths import database_path
from src.config.settings import PROJECT_ROOT, settings


engine = create_engine(settings.database_url, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


SCHEMA_PATH = PROJECT_ROOT / "src" / "storage" / "schema.sql"


def initialize_database(db_path: Path | str | None = None) -> Path:
    """Create SQLite tables if they do not already exist."""

    path = Path(db_path) if db_path else database_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return path


def save_news_data(news: pd.DataFrame, db_path: Path | str | None = None) -> None:
    if news.empty:
        return
    path = initialize_database(db_path)
    frame = news.rename(columns={"symbol": "ticker"}).copy()
    columns = ["news_id", "ticker", "date", "title", "content", "source"]
    frame = _prepare_dates(frame, "date")
    _upsert_frame(path, "news_table", frame[columns])


def save_market_data(market: pd.DataFrame, db_path: Path | str | None = None) -> None:
    if market.empty:
        return
    path = initialize_database(db_path)
    frame = market.rename(columns={"symbol": "ticker"}).copy()
    columns = ["ticker", "date", "open", "high", "low", "close", "volume"]
    frame = _prepare_dates(frame, "date")
    _upsert_frame(path, "market_table", frame[columns])


def save_sentiment_data(sentiment: pd.DataFrame, db_path: Path | str | None = None) -> None:
    if sentiment.empty:
        return
    path = initialize_database(db_path)
    frame = sentiment.copy()
    canonical = pd.DataFrame()
    canonical["ticker"] = _first_series(frame, ["ticker", "symbol"], default="")
    canonical["date"] = _first_series(frame, ["date"])
    canonical["method"] = _first_series(frame, ["method", "sentiment_method"], default="unknown").fillna("unknown")

    score = _first_series(frame, ["sentiment_score", "daily_sentiment_score"])
    canonical["sentiment_score"] = pd.to_numeric(score, errors="coerce").fillna(0.0)

    canonical = _prepare_dates(canonical, "date")
    _upsert_frame(path, "sentiment_table", canonical[["ticker", "date", "method", "sentiment_score"]])


def save_trading_logs(logs: pd.DataFrame, db_path: Path | str | None = None) -> None:
    if logs.empty:
        return
    path = initialize_database(db_path)
    frame = _prepare_dates(logs.copy(), "date")
    columns = ["episode", "date", "action", "reward", "position", "cash", "portfolio_value", "experiment"]
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    with sqlite3.connect(path) as conn:
        frame[columns].to_sql("trading_log_table", conn, if_exists="append", index=False)


def save_nlp_signals(signals: pd.DataFrame, db_path: Path | str | None = None, *, source: str = "pipeline") -> None:
    """Persist peer sentiment and market-impact daily signals in long form."""

    if signals.empty or "date" not in signals.columns:
        return
    path = initialize_database(db_path)
    frame = signals.copy()
    ticker = _first_series(frame, ["ticker", "symbol"], default="").astype(str)
    date = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    signal_specs = [
        ("sector_sentiment_score", "sector_sentiment", "sector_sentiment_method", "sector_corpus_status", "target_news_count", "sector_sentiment_missing_flag"),
        ("marketwide_sentiment_score", "marketwide_sentiment", "marketwide_sentiment_method", "marketwide_corpus_status", "target_news_count", "marketwide_sentiment_missing_flag"),
        ("sector_impact_score", "sector_impact", "sector_impact_method", "sector_impact_corpus_status", "target_news_count", "sector_impact_missing_flag"),
        ("marketwide_impact_score", "marketwide_impact", "marketwide_impact_method", "marketwide_impact_corpus_status", "target_news_count", "marketwide_impact_missing_flag"),
    ]
    rows = []
    for value_col, signal_name, method_col, status_col, count_col, missing_col in signal_specs:
        if value_col not in frame.columns:
            continue
        rows.append(
            pd.DataFrame(
                {
                    "ticker": ticker,
                    "date": date,
                    "signal_name": signal_name,
                    "signal_value": pd.to_numeric(frame[value_col], errors="coerce"),
                    "method": _first_series(frame, [method_col], default="").astype(str),
                    "corpus_status": _first_series(frame, [status_col], default="").astype(str),
                    "target_news_count": pd.to_numeric(_first_series(frame, [count_col], default=0), errors="coerce").fillna(0).astype(int),
                    "missing_flag": pd.to_numeric(_first_series(frame, [missing_col], default=1), errors="coerce").fillna(1).astype(int),
                    "source": source,
                }
            )
        )
    if not rows:
        return
    canonical = pd.concat(rows, ignore_index=True)
    canonical = canonical.dropna(subset=["date"])
    canonical = canonical[canonical["ticker"].astype(str).str.strip().ne("")]
    if canonical.empty:
        return
    _upsert_frame(path, "nlp_signal_table", canonical)


def save_experiment_metrics(metrics: pd.DataFrame, db_path: Path | str | None = None, *, ticker: str, source: str) -> None:
    """Persist wide experiment metric tables as normalized metric rows."""

    if metrics.empty or "experiment" not in metrics.columns:
        return
    path = initialize_database(db_path)
    id_columns = {"experiment", "official_experiment", "legacy_experiment_excluded"}
    metric_columns = [column for column in metrics.columns if column not in id_columns]
    rows = []
    for _, row in metrics.iterrows():
        experiment = str(row.get("experiment", "") or "")
        if not experiment:
            continue
        for column in metric_columns:
            value = row.get(column)
            numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            rows.append(
                {
                    "ticker": str(ticker),
                    "experiment": experiment,
                    "metric_name": str(column),
                    "metric_value": None if pd.isna(numeric) else float(numeric),
                    "metric_text": "" if pd.notna(numeric) or pd.isna(value) else str(value),
                    "metric_source": source,
                }
            )
    if rows:
        _upsert_frame(path, "experiment_metrics_table", pd.DataFrame(rows))


def load_table(table: str, db_path: Path | str | None = None) -> pd.DataFrame:
    path = initialize_database(db_path)
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table}", conn)


def _prepare_dates(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in frame.columns:
        frame[column] = pd.to_datetime(frame[column]).dt.strftime("%Y-%m-%d")
    return frame


def _upsert_frame(db_path: Path, table: str, frame: pd.DataFrame) -> None:
    frame = frame.loc[:, ~frame.columns.duplicated()].copy()
    temp_table = f"_{table}_staging"
    with sqlite3.connect(db_path) as conn:
        frame.to_sql(temp_table, conn, if_exists="replace", index=False)
        columns = list(frame.columns)
        column_sql = ", ".join(columns)
        select_sql = ", ".join(columns)
        conn.execute(f"INSERT OR REPLACE INTO {table} ({column_sql}) SELECT {select_sql} FROM {temp_table}")
        conn.execute(f"DROP TABLE {temp_table}")


def _first_series(frame: pd.DataFrame, candidates: list[str], default: object | None = None) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            return frame[column]
    return pd.Series([default] * len(frame), index=frame.index, dtype="object")
