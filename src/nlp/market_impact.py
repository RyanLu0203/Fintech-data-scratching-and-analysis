"""Peer-trained market-impact NLP signals.

This module adds the improved experiment without replacing the existing
peer-sentiment experiment.  The target stock is excluded from all peer
training labels; peer news receives pseudo labels from that peer stock's
future returns, and the fitted model is then used only to score target news.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from src.config.paths import PROJECT_ROOT, stock_reports_dir, stock_results_dir
from src.data_ingestion.ingestion import fetch_market_data
from src.evaluation.information_density import define_experiment_window, detect_information_density_split
from src.nlp.aggregate_sentiment import align_news_to_trading_dates, build_news_frame
from src.nlp.peer_sentiment import (
    DATA_END_DATE,
    DATA_START_DATE,
    MIN_MARKETWIDE_TRAINING_NEWS,
    MIN_SECTOR_PEER_STOCKS,
    MIN_SECTOR_TRAINING_NEWS,
    PeerCorpus,
    _clean_news_text,
    _company_from_market,
    _date_text,
    _latest_integrated_csv,
    _normalize_symbol,
    _safe_read_csv,
    build_peer_nlp_corpora,
)
from src.nlp.preprocess import preprocess_text

LOGGER = logging.getLogger(__name__)

MARKET_IMPACT_HORIZON_DAYS = 3
MARKET_IMPACT_POS_THRESHOLD = 0.015
MARKET_IMPACT_NEG_THRESHOLD = -0.015
MIN_IMPACT_MODEL_LABELS = 30
REPORTS_TABLE_DIR = PROJECT_ROOT / "reports" / "tables"


@dataclass
class ImpactModelResult:
    model: Pipeline | None
    classes: list[str]
    method: str
    status: str
    labelled_frame: pd.DataFrame


def generate_peer_market_impact_daily_signal(
    input_csv: Path,
    *,
    symbol: str,
    company_name: str = "",
    start_date: str = DATA_START_DATE,
    end_date: str = DATA_END_DATE,
    sources: str = "tencent",
    news_count: int = 100000,
    allow_fetch_missing_sector_peers: bool = False,
    include_marketwide_peer: bool = True,
    horizon_days: int = MARKET_IMPACT_HORIZON_DAYS,
    positive_threshold: float = MARKET_IMPACT_POS_THRESHOLD,
    negative_threshold: float = MARKET_IMPACT_NEG_THRESHOLD,
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Create target-stock daily market-impact signals from peer corpora."""

    def emit(stage: str, message: str) -> None:
        LOGGER.info("[%s] %s", stage, message)
        if status_callback is not None:
            status_callback(stage, message)

    symbol = _normalize_symbol(symbol)
    reports_dir = stock_reports_dir(symbol)
    results_dir = stock_results_dir(symbol)
    reports_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    emit("impact_corpus", f"{symbol}: building peer market-impact corpora; target stock is excluded.")
    corpora = build_peer_nlp_corpora(
        symbol,
        target_company_name=company_name,
        allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
        sources=sources,
        news_count=news_count,
        include_marketwide_peer=include_marketwide_peer,
        status_callback=status_callback,
    )
    sector_corpus = corpora["sector_peer"]
    market_corpus = corpora["marketwide_peer"]
    assert isinstance(sector_corpus, PeerCorpus)
    assert isinstance(market_corpus, PeerCorpus)
    target_meta = corpora["target_meta"]
    sector = str(target_meta.get("sector", "UNKNOWN") or "UNKNOWN")
    company = company_name or str(target_meta.get("company_name", "") or "") or _company_from_market(symbol, _safe_read_csv(input_csv))

    emit("impact_labels", f"{symbol}: labelling sector-peer news with {horizon_days}d future-return impact.")
    sector_model = _fit_impact_model(
        sector_corpus,
        corpus_type="sector_impact",
        horizon_days=horizon_days,
        positive_threshold=positive_threshold,
        negative_threshold=negative_threshold,
    )
    emit("impact_labels", f"{symbol}: sector-impact corpus labelled rows={len(sector_model.labelled_frame)} status={sector_model.status}.")

    if include_marketwide_peer:
        emit("impact_labels", f"{symbol}: labelling marketwide-peer news with {horizon_days}d future-return impact.")
        market_model = _fit_impact_model(
            market_corpus,
            corpus_type="marketwide_impact",
            horizon_days=horizon_days,
            positive_threshold=positive_threshold,
            negative_threshold=negative_threshold,
        )
        emit("impact_labels", f"{symbol}: marketwide-impact corpus labelled rows={len(market_model.labelled_frame)} status={market_model.status}.")
    else:
        market_model = ImpactModelResult(
            None,
            [],
            "disabled_sector_only_scope",
            "DISABLED",
            pd.DataFrame(columns=list(market_corpus.frame.columns) + ["future_return_1d", "future_return_3d", "future_return_5d", "impact_label"]),
        )
        emit("impact_labels", f"{symbol}: marketwide-impact corpus skipped because peer corpus scope is sector_only.")

    corpus_summary = pd.DataFrame(
        [
            _impact_corpus_summary_row(symbol, company, sector, "sector_impact", sector_corpus, sector_model, horizon_days, positive_threshold, negative_threshold),
            _impact_corpus_summary_row(symbol, company, sector, "marketwide_impact", market_corpus, market_model, horizon_days, positive_threshold, negative_threshold),
        ]
    )
    _update_table(REPORTS_TABLE_DIR / "market_impact_corpus_summary.csv", corpus_summary, ["target_symbol", "corpus_type"])

    emit("impact_target_data", f"{symbol}: loading held-out target news for market-impact scoring.")
    market = fetch_market_data(symbol, start_date, end_date, input_csv=input_csv)
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    market = market.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    trading_dates = pd.Series(pd.to_datetime(market["date"]).drop_duplicates().sort_values().tolist())
    target_news = build_news_frame(input_csv, symbol, company, start_date, end_date, sources, news_count)
    target_news = _clean_news_text(target_news)
    high_start, high_end, window, split = _target_high_density_window(symbol, company, market, target_news)
    target_eval_news = target_news.copy()
    if not target_eval_news.empty and pd.notna(high_start) and pd.notna(high_end):
        target_eval_news = target_eval_news[(target_eval_news["date"] >= high_start) & (target_eval_news["date"] <= high_end)].copy()

    emit("impact_scoring", f"{symbol}: scoring {len(target_eval_news)} target news rows with peer market-impact models.")
    scored = target_eval_news.copy()
    if scored.empty:
        scored = pd.DataFrame(columns=["date", "news_id", "text"])
    sector_scores, sector_probs = _score_impact(sector_model, scored.get("text", pd.Series(dtype=str)).fillna("").astype(str).tolist())
    market_scores, market_probs = _score_impact(market_model, scored.get("text", pd.Series(dtype=str)).fillna("").astype(str).tolist())
    scored["sector_impact_score"] = sector_scores
    scored["marketwide_impact_score"] = market_scores
    for prefix, probs in [("sector", sector_probs), ("marketwide", market_probs)]:
        for label in ["bullish_impact", "neutral_impact", "bearish_impact"]:
            scored[f"{prefix}_{label}_prob"] = probs.get(label, pd.Series([0.0] * len(scored), index=scored.index if len(scored) else None))
    if not scored.empty:
        # Assign by position, not pandas index label. Target/news frames may keep
        # filtered indices, and label alignment would otherwise create NaT rows.
        scored["tradable_date"] = align_news_to_trading_dates(scored["date"], trading_dates).to_numpy()
    else:
        scored["tradable_date"] = pd.Series(dtype="datetime64[ns]")

    daily_dates = trading_dates[(trading_dates >= high_start) & (trading_dates <= high_end)] if pd.notna(high_start) and pd.notna(high_end) else trading_dates.iloc[0:0]
    daily = pd.DataFrame({"date": daily_dates})
    daily["symbol"] = symbol
    daily["company_name"] = company
    daily["sector"] = sector
    if scored.empty:
        grouped = pd.DataFrame(columns=["date", "sector_impact_score", "marketwide_impact_score", "target_news_count"])
    else:
        grouped = (
            scored.groupby("tradable_date", as_index=False)
            .agg(
                sector_impact_score=("sector_impact_score", "mean"),
                marketwide_impact_score=("marketwide_impact_score", "mean"),
                target_news_count=("news_id", "count"),
            )
            .rename(columns={"tradable_date": "date"})
        )
    daily = daily.merge(grouped, on="date", how="left")
    daily["target_news_count"] = pd.to_numeric(daily.get("target_news_count", 0), errors="coerce").fillna(0).astype(int)
    daily["news_available"] = (daily["target_news_count"] > 0).astype(int)
    for column, model in [("sector_impact_score", sector_model), ("marketwide_impact_score", market_model)]:
        if column not in daily.columns:
            daily[column] = np.nan
        daily[column] = pd.to_numeric(daily[column], errors="coerce")
        daily[f"{column.replace('_score', '')}_missing_flag"] = ((daily["news_available"] == 0) | (model.status != "READY") | daily[column].isna()).astype(int)
        daily[column] = daily[column].fillna(0.0).clip(-1, 1)
    daily["impact_missing_flag"] = daily[["sector_impact_missing_flag", "marketwide_impact_missing_flag"]].max(axis=1)
    daily["sector_impact_news_count"] = daily["target_news_count"]
    daily["marketwide_impact_news_count"] = daily["target_news_count"]
    daily["sector_impact_method"] = sector_model.method
    daily["marketwide_impact_method"] = market_model.method
    daily["sector_impact_corpus_status"] = sector_model.status
    daily["marketwide_impact_corpus_status"] = market_model.status
    daily["sector_impact_training_news_count"] = int(len(sector_model.labelled_frame))
    daily["marketwide_impact_training_news_count"] = int(len(market_model.labelled_frame))
    daily["sector_peer_stock_count"] = int(sector_corpus.summary.get("number_of_peer_stocks", 0))
    daily["marketwide_peer_stock_count"] = int(market_corpus.summary.get("number_of_peer_stocks", 0))
    daily["sector_peer_sector_count"] = int(sector_corpus.summary.get("peer_sector_count", 0))
    daily["marketwide_peer_sector_count"] = int(market_corpus.summary.get("peer_sector_count", 0))
    daily["marketwide_distinct_from_sector"] = int(bool(market_corpus.summary.get("marketwide_distinct_from_sector", False)))
    daily["impact_horizon_days"] = int(horizon_days)
    daily["positive_threshold"] = float(positive_threshold)
    daily["negative_threshold"] = float(negative_threshold)
    daily["high_density_eval_start"] = _date_text(high_start)
    daily["high_density_eval_end"] = _date_text(high_end)
    daily["alignment_rule"] = "target_news_to_next_trading_day_no_intraday_timestamp"
    daily["peer_corpus_scope"] = "sector_plus_marketwide" if include_marketwide_peer else "sector_only"
    daily["marketwide_enabled"] = int(bool(include_marketwide_peer))

    signal_path = results_dir / "peer_market_impact_daily_signal.csv"
    item_path = results_dir / "peer_market_impact_item_signal.csv"
    window_path = reports_dir / "market_impact_experiment_window.csv"
    daily.to_csv(signal_path, index=False, encoding="utf-8-sig")
    scored.to_csv(item_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([window]).to_csv(window_path, index=False, encoding="utf-8-sig")
    emit("impact_saved", f"{symbol}: saved peer_market_impact_daily_signal.csv.")

    return {
        "daily_signal": daily,
        "item_signal": scored,
        "corpus_summary": corpus_summary,
        "density_split": pd.DataFrame([split]),
        "window": pd.DataFrame([window]),
        "peer_market_impact_daily_signal_csv": signal_path,
        "peer_market_impact_item_signal_csv": item_path,
        "market_impact_window_csv": window_path,
    }


def _fit_impact_model(
    corpus: PeerCorpus,
    *,
    corpus_type: str,
    horizon_days: int,
    positive_threshold: float,
    negative_threshold: float,
) -> ImpactModelResult:
    labelled = _label_peer_news_by_future_return(corpus.frame, horizon_days, positive_threshold, negative_threshold)
    labels = labelled.get("impact_label", pd.Series(dtype=str)).dropna().astype(str)
    text = labelled.get("text", pd.Series(dtype=str)).fillna("").astype(str)
    peer_count = int(corpus.summary.get("number_of_peer_stocks", 0) or 0)
    required_news = MIN_SECTOR_TRAINING_NEWS if corpus_type == "sector_impact" else MIN_MARKETWIDE_TRAINING_NEWS

    if str(corpus.summary.get("corpus_status", "")) != "READY":
        status = "INSUFFICIENT"
    elif peer_count < MIN_SECTOR_PEER_STOCKS and corpus_type == "sector_impact":
        status = "INSUFFICIENT"
    elif len(labelled) < required_news:
        status = "INSUFFICIENT"
    elif labels.nunique() < 2 or len(labelled) < MIN_IMPACT_MODEL_LABELS:
        status = "NOT_RELIABLE"
    else:
        status = "READY"

    if text.empty or labels.nunique() < 2:
        return ImpactModelResult(None, sorted(labels.unique().tolist()), "constant_zero_impact_no_two_class_model", status, labelled)

    model = Pipeline(
        [
            ("tfidf", TfidfVectorizer(max_features=12000, ngram_range=(1, 2), preprocessor=preprocess_text)),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced", multi_class="auto")),
        ]
    )
    model.fit(text.tolist(), labels.tolist())
    return ImpactModelResult(model, sorted(labels.unique().tolist()), "tfidf_logistic_market_impact", status, labelled)


def _label_peer_news_by_future_return(
    frame: pd.DataFrame,
    horizon_days: int,
    positive_threshold: float,
    negative_threshold: float,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(frame.columns) + ["future_return_1d", "future_return_3d", "future_return_5d", "impact_label"])
    labelled_frames: list[pd.DataFrame] = []
    data = frame.copy()
    data["peer_symbol"] = data.get("peer_symbol", "").astype(str).str.extract(r"(\d{6})", expand=False).fillna(data.get("peer_symbol", ""))
    for peer_symbol, news in data.groupby("peer_symbol", dropna=True):
        market_path = _latest_integrated_csv(str(peer_symbol))
        market = _market_from_integrated_csv(market_path)
        if market.empty:
            continue
        trading_dates = pd.Series(pd.to_datetime(market["date"]).drop_duplicates().sort_values().tolist())
        returns = _future_returns(market, [1, 3, 5])
        peer_news = news.copy()
        # Assign by position. Peer corpus frames are concatenated across stocks,
        # so their original indices are not 0..n within each group.
        peer_news["tradable_date"] = align_news_to_trading_dates(peer_news["date"], trading_dates).to_numpy()
        peer_news = peer_news.merge(returns, left_on="tradable_date", right_on="date", how="left", suffixes=("", "_market"))
        horizon_column = f"future_return_{int(horizon_days)}d"
        if horizon_column not in peer_news.columns:
            horizon_column = "future_return_3d"
        peer_news = peer_news.dropna(subset=[horizon_column, "text"]).copy()
        peer_news["impact_label"] = np.select(
            [
                pd.to_numeric(peer_news[horizon_column], errors="coerce") > positive_threshold,
                pd.to_numeric(peer_news[horizon_column], errors="coerce") < negative_threshold,
            ],
            ["bullish_impact", "bearish_impact"],
            default="neutral_impact",
        )
        labelled_frames.append(peer_news)
    return pd.concat(labelled_frames, ignore_index=True) if labelled_frames else pd.DataFrame(columns=list(frame.columns) + ["future_return_1d", "future_return_3d", "future_return_5d", "impact_label"])


def _score_impact(model_result: ImpactModelResult, texts: list[str]) -> tuple[list[float], dict[str, pd.Series]]:
    index = range(len(texts))
    empty_probs = {label: pd.Series([0.0] * len(texts), index=index) for label in ["bullish_impact", "neutral_impact", "bearish_impact"]}
    if not texts:
        return [], empty_probs
    if model_result.model is None or model_result.status != "READY":
        return [0.0] * len(texts), empty_probs
    probabilities = model_result.model.predict_proba(texts)
    classes = list(model_result.model.named_steps["clf"].classes_)
    prob_frame = pd.DataFrame(probabilities, columns=classes)
    probs = {}
    for label in ["bullish_impact", "neutral_impact", "bearish_impact"]:
        probs[label] = pd.to_numeric(prob_frame[label], errors="coerce").fillna(0.0) if label in prob_frame.columns else pd.Series([0.0] * len(texts))
    scores = (probs["bullish_impact"] - probs["bearish_impact"]).clip(-1, 1).astype(float).tolist()
    return scores, probs


def _market_from_integrated_csv(path: Path | None) -> pd.DataFrame:
    market = _safe_read_csv(path)
    if market.empty or "date" not in market.columns:
        return pd.DataFrame(columns=["date", "close"])
    market = market.copy()
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    market["close"] = pd.to_numeric(market.get("close"), errors="coerce")
    market = market.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date", keep="last")
    return market[["date", "close"]].reset_index(drop=True)


def _future_returns(market: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    frame = market[["date", "close"]].copy()
    close = pd.to_numeric(frame["close"], errors="coerce")
    for horizon in horizons:
        frame[f"future_return_{horizon}d"] = close.shift(-horizon) / close - 1
    return frame.drop(columns=["close"])


def _target_high_density_window(symbol: str, company: str, market: pd.DataFrame, target_news: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp, dict[str, object], dict[str, object]]:
    split_news = target_news[["date"]].copy() if not target_news.empty else pd.DataFrame(columns=["date"])
    if not split_news.empty:
        split_news["news_count"] = 1
    split = detect_information_density_split(symbol, market, split_news)
    window = define_experiment_window(symbol, company, split)
    high_start = pd.to_datetime(window.get("high_density_eval_start"), errors="coerce")
    high_end = pd.to_datetime(window.get("high_density_eval_end"), errors="coerce")
    if pd.isna(high_start) or pd.isna(high_end):
        dates = pd.to_datetime(market["date"], errors="coerce").dropna().sort_values()
        tail = dates.tail(min(len(dates), 30))
        high_start = tail.min() if not tail.empty else pd.NaT
        high_end = tail.max() if not tail.empty else pd.NaT
        window["window_status"] = "NOT_RELIABLE_NO_TARGET_NEWS_WINDOW"
        window["recommended_usage"] = "NOT_RELIABLE"
    return high_start, high_end, window, split


def _impact_corpus_summary_row(
    target_symbol: str,
    target_company: str,
    target_sector: str,
    corpus_type: str,
    corpus: PeerCorpus,
    model: ImpactModelResult,
    horizon_days: int,
    positive_threshold: float,
    negative_threshold: float,
) -> dict[str, object]:
    labels = model.labelled_frame.get("impact_label", pd.Series(dtype=str)).astype(str)
    counts = labels.value_counts()
    class_counts = [int(counts.get(label, 0)) for label in ["bullish_impact", "neutral_impact", "bearish_impact"]]
    non_zero = [count for count in class_counts if count > 0]
    label_balance = float(min(non_zero) / max(non_zero)) if non_zero and max(non_zero) > 0 else 0.0
    return {
        "target_symbol": target_symbol,
        "target_company_name": target_company,
        "target_sector": target_sector,
        "corpus_type": corpus_type,
        "included_symbols": corpus.summary.get("included_symbols", ""),
        "excluded_symbols": corpus.summary.get("excluded_symbols", target_symbol),
        "number_of_peer_stocks": int(corpus.summary.get("number_of_peer_stocks", 0) or 0),
        "total_news_count": int(corpus.summary.get("total_news_count", 0) or 0),
        "labeled_news_count": int(len(model.labelled_frame)),
        "bullish_count": int(counts.get("bullish_impact", 0)),
        "bearish_count": int(counts.get("bearish_impact", 0)),
        "neutral_count": int(counts.get("neutral_impact", 0)),
        "label_balance": label_balance,
        "impact_horizon_days": int(horizon_days),
        "positive_threshold": float(positive_threshold),
        "negative_threshold": float(negative_threshold),
        "impact_method": model.method,
        "corpus_status": model.status,
        "class_balance_warning": bool(label_balance < 0.05),
    }


def _update_table(path: Path, incoming: pd.DataFrame, keys: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    if not existing.empty and all(column in existing.columns for column in keys) and all(column in incoming.columns for column in keys):
        incoming_keys = set(tuple(str(row[column]) for column in keys) for _, row in incoming.iterrows())
        existing = existing[~existing.apply(lambda row: tuple(str(row[column]) for column in keys) in incoming_keys, axis=1)]
    pd.concat([existing, incoming], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")
