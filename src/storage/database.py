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
    with sqlite3.connect(path) as conn:
        frame[columns].to_sql("trading_log_table", conn, if_exists="append", index=False)


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
