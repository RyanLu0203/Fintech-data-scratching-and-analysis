"""Run item-level sentiment models and aggregate to daily ticker signals."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from src.data_ingestion.ingestion import fetch_news_data
from src.nlp.finbert_sentiment import FinBERTSentiment
from src.nlp.lexicon_sentiment import score_texts
from src.nlp.logistic_sentiment import score_with_logistic

LOGGER = logging.getLogger(__name__)
LOW_COVERAGE_WARNING_THRESHOLD = 0.2


def build_news_frame(input_csv: Path, symbol: str, company_name: str, start_date: str, end_date: str, sources: str, news_count: int) -> pd.DataFrame:
    news = fetch_news_data(symbol, company_name, start_date, end_date, sources, news_count, input_csv)
    if news.empty:
        return pd.DataFrame(columns=["news_id", "ticker", "date", "title", "content", "source", "text"])
    news["date"] = pd.to_datetime(news["date"], errors="coerce")
    news["text"] = (news["title"].fillna("") + " " + news["content"].fillna("")).str.strip()
    return news.dropna(subset=["date"]).reset_index(drop=True)


def run_nlp_pipeline(
    input_csv: Path,
    reports_dir: Path,
    symbol: str,
    company_name: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    sources: str = "tencent",
    news_count: int = 5000,
    methods: Iterable[str] = ("lexicon", "logistic", "finbert"),
) -> dict[str, Path | pd.DataFrame | str | float]:
    """Create daily sentiment CSVs and an NLP evaluation table."""

    reports_dir.mkdir(parents=True, exist_ok=True)
    market = pd.read_csv(input_csv)
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    market = market.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    trading_dates = pd.Series(pd.to_datetime(market["date"]).drop_duplicates().sort_values().tolist())
    start = start_date or str(trading_dates.min().date())
    end = end_date or str(trading_dates.max().date())
    news = build_news_frame(input_csv, symbol, company_name, start, end, sources, news_count)

    item_scores = news.copy()
    evaluations: list[dict[str, object]] = []
    available_methods: list[str] = []
    main_method = "event_count_proxy"

    if "lexicon" in methods:
        item_scores["lexicon_score"] = score_texts(item_scores.get("text", pd.Series(dtype=str)).fillna("").astype(str).tolist())
        evaluations.append({"method": "lexicon", "status": "ok", "accuracy": np.nan, "precision": np.nan, "recall": np.nan, "f1": np.nan})
        available_methods.append("lexicon")

    if "logistic" in methods and not item_scores.empty:
        result = score_with_logistic(item_scores, text_column="text")
        item_scores["logistic_score"] = result.scores
        evaluations.append(result.evaluation)
        available_methods.append("logistic")

    finbert_status = "skipped"
    finbert_warning = ""
    if "finbert" in methods and not item_scores.empty:
        result = FinBERTSentiment().score(item_scores["text"].fillna("").astype(str).tolist())
        item_scores["finbert_label"] = result.labels
        item_scores["finbert_positive_prob"] = result.positive_probs
        item_scores["finbert_neutral_prob"] = result.neutral_probs
        item_scores["finbert_negative_prob"] = result.negative_probs
        item_scores["finbert_score"] = result.scores
        finbert_status = result.status
        finbert_warning = result.warning
        evaluations.append(
            {
                "method": "finbert",
                "status": result.status,
                "error": result.error,
                "warning": result.warning,
                "model_name": result.model_name,
                "allow_download": result.allow_download,
                "local_files_only": result.local_files_only,
                "accuracy": np.nan,
                "precision": np.nan,
                "recall": np.nan,
                "f1": np.nan,
            }
        )
        if result.status == "ok":
            available_methods.append("finbert")

    if "finbert" in available_methods:
        item_scores["sentiment_score"] = item_scores["finbert_score"]
        main_method = "finbert"
    elif "logistic" in available_methods:
        item_scores["sentiment_score"] = item_scores["logistic_score"]
        main_method = "logistic"
    elif "lexicon" in available_methods:
        item_scores["sentiment_score"] = item_scores["lexicon_score"]
        main_method = "lexicon"

    alignment_rule = "next_trading_day_no_intraday_timestamp"
    if item_scores.empty or "sentiment_score" not in item_scores.columns:
        LOGGER.warning("No usable news sentiment. Falling back to event_count proxy.")
        daily = _proxy_sentiment(market)
        item_scores = pd.DataFrame()
        main_method = "event_count_proxy"
    else:
        item_scores["original_news_date"] = pd.to_datetime(item_scores["date"], errors="coerce")
        # Assign positionally so filtered news frames do not lose valid aligned
        # trading dates because of pandas index-label alignment.
        item_scores["tradable_date"] = align_news_to_trading_dates(item_scores["original_news_date"], trading_dates).to_numpy()
        item_scores["alignment_rule"] = alignment_rule
        item_scores["sentiment_method"] = main_method
        item_scores["sentiment_score"] = pd.to_numeric(item_scores["sentiment_score"], errors="coerce").fillna(0.0).clip(-1, 1)
        grouped = (
            item_scores.groupby(["ticker", "tradable_date"], as_index=False)
            .agg(sentiment_score=("sentiment_score", "mean"), news_count=("news_id", "count"))
            .rename(columns={"tradable_date": "date"})
        )
        daily = pd.DataFrame({"date": trading_dates})
        ticker_value = str(item_scores["ticker"].dropna().iloc[0]) if item_scores["ticker"].notna().any() else symbol
        daily["ticker"] = ticker_value
        daily = daily.merge(grouped, on=["ticker", "date"], how="left")
        daily["sentiment_score"] = pd.to_numeric(daily["sentiment_score"], errors="coerce").fillna(0.0)
        daily["news_count"] = pd.to_numeric(daily["news_count"], errors="coerce").fillna(0).astype(int)
        daily["sentiment_method"] = main_method
        daily["sentiment_fallback_used"] = main_method != "finbert"
        daily["finbert_status"] = finbert_status
        daily["alignment_rule"] = alignment_rule
        daily["daily_sentiment_score"] = daily["sentiment_score"]

    if "daily_sentiment_score" not in daily.columns:
        daily["daily_sentiment_score"] = daily["sentiment_score"]
    if "news_count" not in daily.columns:
        daily["news_count"] = 0
    if "sentiment_method" not in daily.columns:
        daily["sentiment_method"] = main_method
    if "alignment_rule" not in daily.columns:
        daily["alignment_rule"] = alignment_rule
    if "sentiment_fallback_used" not in daily.columns:
        daily["sentiment_fallback_used"] = main_method != "finbert"
    if "finbert_status" not in daily.columns:
        daily["finbert_status"] = finbert_status

    sentiment_coverage = float((pd.to_numeric(daily["news_count"], errors="coerce").fillna(0) > 0).mean()) if not daily.empty else 0.0
    if sentiment_coverage < LOW_COVERAGE_WARNING_THRESHOLD:
        LOGGER.warning(
            "Low sentiment coverage for %s: %.1f%% of trading days have news-derived sentiment.",
            symbol,
            sentiment_coverage * 100,
        )

    stem = input_csv.stem
    daily_path = reports_dir / f"{stem}_daily_sentiment.csv"
    item_path = reports_dir / f"{stem}_item_sentiment.csv"
    evaluation_path = reports_dir / f"{stem}_nlp_evaluation.csv"
    proxy_path = reports_dir / f"{stem}_daily_sentiment_proxy.csv"

    evaluation_frame = pd.DataFrame(evaluations)
    if not evaluation_frame.empty:
        evaluation_frame["main_experiment_method"] = main_method
        evaluation_frame["sentiment_coverage"] = sentiment_coverage
        evaluation_frame["alignment_rule"] = alignment_rule
        evaluation_frame["finbert_status"] = finbert_status
        evaluation_frame["finbert_warning"] = finbert_warning
        evaluation_frame["sentiment_fallback_used"] = main_method != "finbert"

    daily.to_csv(daily_path, index=False, encoding="utf-8-sig")
    daily.rename(columns={"daily_sentiment_score": "sentiment_score_proxy"}).to_csv(proxy_path, index=False, encoding="utf-8-sig")
    item_scores.to_csv(item_path, index=False, encoding="utf-8-sig")
    evaluation_frame.to_csv(evaluation_path, index=False, encoding="utf-8-sig")

    return {
        "daily_sentiment": daily,
        "item_sentiment": item_scores,
        "nlp_evaluation": evaluation_frame,
        "daily_sentiment_csv": daily_path,
        "item_sentiment_csv": item_path,
        "nlp_evaluation_csv": evaluation_path,
        "daily_sentiment_proxy_csv": proxy_path,
        "sentiment_method": main_method,
        "alignment_rule": alignment_rule,
        "sentiment_coverage": sentiment_coverage,
    }


def align_news_to_trading_dates(news_dates: pd.Series, trading_dates: pd.Series) -> pd.Series:
    """Map calendar news dates to the first tradable market date.

    Conservative rule:
    - no intraday timestamp or after-market timestamp -> next trading day
    - pre-market timestamp -> same trading day
    """

    trading_index = pd.Index(pd.to_datetime(trading_dates, errors="coerce").dropna().sort_values().unique())
    aligned: list[pd.Timestamp] = []
    for value in pd.to_datetime(news_dates, errors="coerce"):
        if pd.isna(value):
            aligned.append(pd.NaT)
            continue
        has_intraday_time = any([value.hour, value.minute, value.second])
        candidate = value.normalize()
        if not has_intraday_time:
            candidate = candidate + pd.Timedelta(days=1)
        elif value.hour > 15 or (value.hour == 15 and value.minute > 0):
            candidate = candidate + pd.Timedelta(days=1)
        elif value.hour < 9 or (value.hour == 9 and value.minute <= 30):
            candidate = candidate
        else:
            candidate = candidate + pd.Timedelta(days=1)

        position = trading_index.searchsorted(candidate, side="left")
        if position >= len(trading_index):
            aligned.append(trading_index[-1])
        else:
            aligned.append(pd.Timestamp(trading_index[position]))
    return pd.Series(aligned)


def _proxy_sentiment(market: pd.DataFrame) -> pd.DataFrame:
    data = market.copy()
    data["ticker"] = data.get("symbol", "")
    event_count = data["event_count"] if "event_count" in data.columns else pd.Series([0] * len(data), index=data.index)
    data["event_count"] = pd.to_numeric(event_count, errors="coerce").fillna(0)
    return pd.DataFrame(
        {
            "ticker": data["ticker"],
            "date": pd.to_datetime(data["date"]),
            "sentiment_score": np.tanh(data["event_count"] / 10.0),
            "daily_sentiment_score": np.tanh(data["event_count"] / 10.0),
            "news_count": 0,
            "sentiment_method": "event_count_proxy",
            "sentiment_fallback_used": True,
            "finbert_status": "skipped",
            "alignment_rule": "next_trading_day_no_intraday_timestamp",
        }
    )
