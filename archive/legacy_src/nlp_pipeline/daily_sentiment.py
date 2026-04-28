"""Aggregate article-level sentiment into daily sentiment features."""

from __future__ import annotations

import pandas as pd


def aggregate_daily_sentiment(events: pd.DataFrame, score_col: str = "sentiment_score") -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["date", "sentiment_score", "news_count"])
    grouped = events.groupby("date")[score_col].agg(["mean", "count"]).reset_index()
    return grouped.rename(columns={"mean": "sentiment_score", "count": "news_count"})

