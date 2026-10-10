"""Frozen peer models with explicit news availability and label maturity.

The formal coursework runner uses these APIs. Legacy all-period corpus
builders remain diagnostic tools and must not supply formal result signals.
"""

from __future__ import annotations

import re
import numpy as np
import pandas as pd

from src.nlp.lexicon_sentiment import score_texts
from src.nlp.logistic_sentiment import fit_logistic_sentiment

TIMEZONE = "Asia/Shanghai"


def session_time(dates: pd.Series, hour: int, minute: int = 0) -> pd.Series:
    return pd.to_datetime(dates).dt.normalize().dt.tz_localize(TIMEZONE).dt.tz_convert(
        "UTC"
    ) + pd.Timedelta(hours=hour, minutes=minute)


def normalize_news(news: pd.DataFrame) -> pd.DataFrame:
    required = {"news_id", "symbol", "text", "available_at", "source"}
    if not required.issubset(news.columns):
        raise ValueError(
            f"News requires {sorted(required)}; publication date alone is insufficient"
        )
    frame = news.copy()
    if (
        frame[["news_id", "symbol", "text", "source", "available_at"]]
        .isna()
        .any()
        .any()
    ):
        raise ValueError(
            "News identity, text, source and availability must be populated"
        )
    if (
        not frame["available_at"]
        .astype(str)
        .str.contains(r"\d{2}:\d{2}", regex=True)
        .all()
    ):
        raise ValueError("available_at needs an explicit time and timezone")
    if (
        not frame["available_at"]
        .astype(str)
        .str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", regex=True)
        .all()
    ):
        raise ValueError("available_at needs a timezone offset")
    frame["available_at"] = pd.to_datetime(
        frame["available_at"], utc=True, errors="raise"
    )
    frame["symbol"] = (
        frame["symbol"].astype(str).str.extract(r"(\d{1,6})", expand=False).str.zfill(6)
    )
    if (
        frame["symbol"].isna().any()
        or frame[["news_id", "source"]]
        .astype(str)
        .apply(lambda column: column.str.strip().eq(""))
        .any()
        .any()
    ):
        raise ValueError("News stock code, identity and source cannot be blank")
    frame["text_key"] = (
        frame["text"]
        .astype(str)
        .map(lambda value: re.sub(r"[\W_]+", "", value.casefold()))
    )
    if frame["text_key"].eq("").any():
        raise ValueError("Empty news text cannot be scored")
    # Revisions of one provider ID use the latest timestamp. Distinct IDs with
    # repeated text are retained here: a future repetition must not erase the
    # earlier text from an already-frozen training window.
    return (
        frame.sort_values("available_at")
        .drop_duplicates(["symbol", "news_id"], keep="last")
        .reset_index(drop=True)
    )


def map_news_to_sessions(news: pd.DataFrame, dates: pd.Series) -> pd.DataFrame:
    frame = news.copy()
    sessions = (
        pd.to_datetime(dates).drop_duplicates().sort_values().reset_index(drop=True)
    )
    opens = session_time(sessions, 9, 30)
    # Strictly before the opening auction; after-close/weekend stories wait
    # for the next session. This mapping is the ONLY text lagging layer.
    positions = np.searchsorted(
        opens.astype("int64").to_numpy(),
        frame["available_at"].astype("int64").to_numpy(),
        side="right",
    )
    frame["decision_date"] = [
        sessions.iloc[i] if i < len(sessions) else pd.NaT for i in positions
    ]
    return frame


def mature_impact_labels(
    peer_news: pd.DataFrame,
    peer_market: pd.DataFrame,
    fit_end: pd.Timestamp,
    horizon: int = 3,
) -> pd.DataFrame:
    frames = []
    for symbol, news in peer_news.groupby("symbol"):
        market = peer_market[peer_market["symbol"] == symbol].sort_values("date").copy()
        # Truncate before constructing returns, so appending future data cannot
        # alter a frozen model's labels or vocabulary.
        market = market[session_time(market["date"], 15) <= fit_end]
        if market.empty:
            continue
        mapped = map_news_to_sessions(news, market["date"])
        market["future_return"] = market["close"].shift(-horizon) / market["close"] - 1
        market["label_end_at"] = session_time(market["date"], 15).shift(-horizon)
        labelled = mapped.merge(
            market[["date", "future_return", "label_end_at"]],
            left_on="decision_date",
            right_on="date",
            how="left",
            validate="many_to_one",
        )
        labelled = labelled.dropna(subset=["future_return", "label_end_at"])
        labelled = labelled[
            (labelled["available_at"] <= fit_end)
            & (labelled["label_end_at"] <= fit_end)
        ].copy()
        labelled["label"] = np.select(
            [labelled["future_return"] > 0.015, labelled["future_return"] < -0.015],
            [1, -1],
            default=0,
        )
        frames.append(labelled)
    if not frames:
        raise ValueError("No matured peer impact labels before the NLP cutoff")
    return pd.concat(frames, ignore_index=True)


def fit_frozen_peer_models(
    peer_news: pd.DataFrame,
    peer_market: pd.DataFrame,
    target_symbol: str,
    fit_end: pd.Timestamp,
    target_text_keys: set[str] | None = None,
) -> tuple[dict, dict, pd.DataFrame]:
    peers = (
        peer_news[
            (peer_news["symbol"] != target_symbol)
            & (peer_news["available_at"] <= fit_end)
        ]
        .drop_duplicates(["symbol", "text_key"], keep="last")
        .copy()
    )
    if target_text_keys:
        peers = peers[~peers["text_key"].isin(target_text_keys)]
    if peers.empty:
        raise ValueError("No independent peer news before NLP fit cutoff")
    peers["label"] = score_texts(peers["text"].tolist())
    impact = mature_impact_labels(peers, peer_market, fit_end)
    models = {
        "sentiment": fit_logistic_sentiment(peers),
        "impact": fit_logistic_sentiment(impact),
    }
    for model in models.values():
        model.training_text_keys_ = set(peers["text_key"])
    provenance = {
        "nlp_fit_end": fit_end.isoformat(),
        "sentiment_training_rows": len(peers),
        "impact_training_rows": len(impact),
        "peer_symbols": sorted(peers["symbol"].unique().tolist()),
        "label_end_at_max": impact["label_end_at"].max().isoformat(),
        "sentiment_labels": "lexicon weak labels",
        "impact_labels": "3-session peer close return, thresholds +/-1.5%",
    }
    return models, provenance, impact


def score_frozen_target(
    models: dict, target_news: pd.DataFrame, dates: pd.Series, fit_end: pd.Timestamp
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = map_news_to_sessions(
        target_news[target_news["available_at"] > fit_end], dates
    )
    scored = (
        scored.dropna(subset=["decision_date"])
        .sort_values("available_at")
        .drop_duplicates(["symbol", "text_key"], keep="first")
        .copy()
    )
    training_keys = set().union(
        *(model.training_text_keys_ for model in models.values())
    )
    scored = scored[~scored["text_key"].isin(training_keys)].copy()
    for kind, model in models.items():
        if scored.empty:
            scored[f"{kind}_score"] = pd.Series(dtype=float)
            continue
        probabilities = pd.DataFrame(
            model.predict_proba(scored["text"].tolist()),
            columns=model.named_steps["clf"].classes_,
        )
        score = probabilities.get(
            1, pd.Series(0.0, index=probabilities.index)
        ) - probabilities.get(-1, pd.Series(0.0, index=probabilities.index))
        scored[f"{kind}_score"] = score.to_numpy()
    daily = pd.DataFrame(
        {"date": pd.to_datetime(dates).drop_duplicates().sort_values()}
    )
    grouped = (
        scored.groupby("decision_date")
        .agg(
            sentiment_score=("sentiment_score", "mean"),
            impact_score=("impact_score", "mean"),
            news_count=("news_id", "size"),
        )
        .reset_index()
        .rename(columns={"decision_date": "date"})
    )
    daily = daily.merge(grouped, on="date", how="left", validate="one_to_one")
    daily["news_count"] = daily["news_count"].fillna(0).astype(int)
    daily["news_missing"] = daily["news_count"].eq(0)
    daily[["sentiment_score", "impact_score"]] = daily[
        ["sentiment_score", "impact_score"]
    ].fillna(0.0)
    daily["nlp_fit_end"] = fit_end.isoformat()
    return daily, scored
