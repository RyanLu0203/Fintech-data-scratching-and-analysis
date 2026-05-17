"""Peer-trained NLP sentiment utilities for the official transfer experiment.

The target stock is never used in its own NLP training corpus.  Sector-peer
and marketwide-peer corpora are built from existing local stock outputs first;
callers can explicitly allow configured same-sector peer fetching when the
local corpus is insufficient.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT, stock_data_dir, stock_reports_dir, stock_results_dir
from src.data_ingestion.ingestion import IngestionConfig, fetch_market_data, run_ingestion
from src.evaluation.information_density import build_daily_news_density, define_experiment_window, detect_information_density_split
from src.nlp.aggregate_sentiment import align_news_to_trading_dates, build_news_frame
from src.nlp.finbert_sentiment import FinBERTSentiment
from src.nlp.lexicon_sentiment import score_texts
from src.nlp.logistic_sentiment import build_model

LOGGER = logging.getLogger(__name__)

DATA_START_DATE = "2024-01-01"
DATA_END_DATE = "2026-04-30"
MIN_SECTOR_PEER_STOCKS = 4
MIN_SECTOR_TRAINING_NEWS = 1000
MIN_MARKETWIDE_TRAINING_NEWS = 3000
MIN_MARKETWIDE_PEER_STOCKS = 12
MIN_MARKETWIDE_PEER_SECTORS = 4
MAX_MARKETWIDE_FETCH_PER_NON_TARGET_SECTOR = 2
MIN_HIGH_DENSITY_TRADING_DAYS = 30
MIN_TARGET_SENTIMENT_COVERAGE = 0.50

REPORTS_TABLE_DIR = PROJECT_ROOT / "reports" / "tables"
SECTOR_CONFIG_PATH = PROJECT_ROOT / "config" / "stock_sector_mapping.csv"


@dataclass
class PeerCorpus:
    frame: pd.DataFrame
    summary: dict[str, object]


def build_stock_sector_mapping(
    *,
    stock_root: Path | None = None,
    config_path: Path | None = None,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Create the stock-sector mapping table required by the peer experiment."""

    stock_root = stock_root or STOCK_OUTPUT_ROOT
    config_path = config_path or SECTOR_CONFIG_PATH
    output_path = output_path or REPORTS_TABLE_DIR / "stock_sector_mapping.csv"
    REPORTS_TABLE_DIR.mkdir(parents=True, exist_ok=True)

    configured = _safe_read_csv(config_path)
    if configured.empty:
        configured = pd.DataFrame(columns=["symbol", "company_name", "sector", "industry", "sector_source", "is_target_candidate"])
    configured["symbol"] = configured.get("symbol", pd.Series(dtype=str)).astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    configured = configured.dropna(subset=["symbol"]).drop_duplicates("symbol", keep="last")
    by_symbol = {str(row["symbol"]): row.to_dict() for _, row in configured.iterrows()}

    configured_symbols = sorted([symbol for symbol in configured["symbol"].dropna().astype(str).tolist() if re.fullmatch(r"\d{6}", symbol)])
    all_symbols = sorted(set(_local_symbols(stock_root)).union(configured_symbols))

    rows: list[dict[str, object]] = []
    for symbol in all_symbols:
        csv_path = _latest_integrated_csv(symbol)
        market = _safe_read_csv(csv_path)
        configured_row = by_symbol.get(symbol, {})
        company = str(configured_row.get("company_name", "") or _company_from_market(symbol, market) or symbol)
        news = _load_local_news(symbol, company, DATA_START_DATE, DATA_END_DATE, news_count=100000)
        sector = str(configured_row.get("sector", "") or "UNKNOWN").strip() or "UNKNOWN"
        industry = str(configured_row.get("industry", "") or "UNKNOWN").strip() or "UNKNOWN"
        source = str(configured_row.get("sector_source", "") or "local_missing_manual_required").strip()
        if sector.upper() == "UNKNOWN":
            source = "missing_manual_mapping"
        rows.append(
            {
                "symbol": symbol,
                "company_name": str(configured_row.get("company_name", "") or company or symbol),
                "sector": sector if sector else "UNKNOWN",
                "industry": industry if industry else "UNKNOWN",
                "sector_source": source,
                "is_target_candidate": int(pd.to_numeric(pd.Series([configured_row.get("is_target_candidate", 1)]), errors="coerce").fillna(1).iloc[0]),
                "local_data_available": bool(csv_path and csv_path.exists() and not market.empty),
                "news_count_2024_2026": int(len(news)),
                "usable_for_sector_corpus": bool(csv_path and csv_path.exists() and not market.empty and len(news) > 0 and sector.upper() != "UNKNOWN"),
            }
        )

    mapping = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
    mapping.to_csv(output_path, index=False, encoding="utf-8-sig")
    return mapping


def build_peer_nlp_corpora(
    target_symbol: str,
    *,
    target_company_name: str = "",
    stock_root: Path | None = None,
    allow_fetch_missing_sector_peers: bool = False,
    sources: str = "tencent",
    news_count: int = 5000,
    include_marketwide_peer: bool = True,
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, PeerCorpus | pd.DataFrame | dict[str, object]]:
    """Build sector-peer and marketwide-peer corpora for a held-out target."""

    symbol = _normalize_symbol(target_symbol)
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    mapping = build_stock_sector_mapping(stock_root=stock_root)
    target_row = mapping[mapping["symbol"].astype(str) == symbol]
    if target_row.empty:
        target_meta = {
            "symbol": symbol,
            "company_name": target_company_name or symbol,
            "sector": "UNKNOWN",
            "industry": "UNKNOWN",
            "sector_source": "not_in_mapping",
        }
    else:
        target_meta = target_row.iloc[0].to_dict()
    target_sector = str(target_meta.get("sector", "UNKNOWN") or "UNKNOWN")
    target_company = str(target_meta.get("company_name", "") or target_company_name or symbol)

    usable = mapping[mapping["usable_for_sector_corpus"].astype(bool)].copy()
    sector_peers = usable[(usable["sector"].astype(str) == target_sector) & (usable["symbol"].astype(str) != symbol)]
    market_peers = usable[usable["symbol"].astype(str) != symbol]
    if not include_marketwide_peer:
        market_peers = market_peers.iloc[0:0].copy()

    if allow_fetch_missing_sector_peers and len(sector_peers) < MIN_SECTOR_PEER_STOCKS:
        fetched_symbols = _fetch_missing_configured_sector_peers(
            mapping,
            target_symbol=symbol,
            target_sector=target_sector,
            sources=sources,
            news_count=news_count,
        )
        if fetched_symbols:
            mapping = build_stock_sector_mapping(stock_root=stock_root)
            usable = mapping[mapping["usable_for_sector_corpus"].astype(bool)].copy()
            sector_peers = usable[(usable["sector"].astype(str) == target_sector) & (usable["symbol"].astype(str) != symbol)]
            market_peers = usable[usable["symbol"].astype(str) != symbol]
            if not include_marketwide_peer:
                market_peers = market_peers.iloc[0:0].copy()
    else:
        fetched_symbols: list[str] = []

    marketwide_fetched_symbols: list[str] = []
    if include_marketwide_peer and allow_fetch_missing_sector_peers and not _marketwide_scope_ready(market_peers, target_sector):
        marketwide_fetched_symbols = _fetch_missing_configured_marketwide_peers(
            mapping,
            target_symbol=symbol,
            target_sector=target_sector,
            sources=sources,
            news_count=news_count,
            status_callback=status_callback,
        )
        if marketwide_fetched_symbols:
            mapping = build_stock_sector_mapping(stock_root=stock_root)
            usable = mapping[mapping["usable_for_sector_corpus"].astype(bool)].copy()
            sector_peers = usable[(usable["sector"].astype(str) == target_sector) & (usable["symbol"].astype(str) != symbol)]
            market_peers = usable[usable["symbol"].astype(str) != symbol]
            if not include_marketwide_peer:
                market_peers = market_peers.iloc[0:0].copy()

    sector_frame, sector_included = _collect_peer_news(
        sector_peers["symbol"].astype(str).tolist(),
        mapping,
        corpus_type="sector_peer",
        status_callback=status_callback,
    )
    if include_marketwide_peer:
        market_frame, market_included = _collect_peer_news(
            market_peers["symbol"].astype(str).tolist(),
            mapping,
            corpus_type="marketwide_peer",
            status_callback=status_callback,
        )
    else:
        market_frame, market_included = pd.DataFrame(columns=["date", "text", "peer_symbol"]), []
    excluded = [symbol]
    if target_sector.upper() == "UNKNOWN":
        sector_status = "INSUFFICIENT"
        sector_reason = "target_sector_unknown"
    elif len(sector_included) < MIN_SECTOR_PEER_STOCKS:
        sector_status = "INSUFFICIENT"
        sector_reason = f"sector_peer_count_below_{MIN_SECTOR_PEER_STOCKS}"
    elif len(sector_frame) < MIN_SECTOR_TRAINING_NEWS:
        sector_status = "INSUFFICIENT"
        sector_reason = f"sector_training_news_below_{MIN_SECTOR_TRAINING_NEWS}"
    else:
        sector_status = "READY"
        sector_reason = ""

    if not include_marketwide_peer:
        market_status = "DISABLED"
        market_reason = "marketwide_peer_disabled_by_dashboard_scope"
    elif not _marketwide_scope_ready(pd.DataFrame({"symbol": market_included}).merge(mapping[["symbol", "sector"]], on="symbol", how="left"), target_sector):
        market_status = "INSUFFICIENT"
        market_reason = f"marketwide_scope_requires_{MIN_MARKETWIDE_PEER_STOCKS}_peers_and_{MIN_MARKETWIDE_PEER_SECTORS}_sectors"
    elif len(market_frame) < MIN_MARKETWIDE_TRAINING_NEWS:
        market_status = "INSUFFICIENT"
        market_reason = f"marketwide_training_news_below_{MIN_MARKETWIDE_TRAINING_NEWS}"
    else:
        market_status = "READY"
        market_reason = ""

    sector_summary = _corpus_summary_row(
        symbol,
        target_company,
        target_sector,
        "sector_peer",
        sector_included,
        excluded,
        sector_frame,
        sector_status,
        sector_reason,
        fetched_symbols,
    )
    market_summary = _corpus_summary_row(
        symbol,
        target_company,
        target_sector,
        "marketwide_peer",
        market_included,
        excluded,
        market_frame,
        market_status,
        market_reason,
        fetched_symbols + marketwide_fetched_symbols,
    )
    _save_corpus_summary([sector_summary, market_summary])

    return {
        "sector_peer": PeerCorpus(sector_frame, sector_summary),
        "marketwide_peer": PeerCorpus(market_frame, market_summary),
        "mapping": mapping,
        "target_meta": target_meta,
    }


def generate_peer_nlp_daily_sentiment(
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
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Score held-out target news with sector-peer and marketwide-peer models."""

    def emit(stage: str, message: str) -> None:
        LOGGER.info("[%s] %s", stage, message)
        if status_callback is not None:
            status_callback(stage, message)

    symbol = _normalize_symbol(symbol)
    reports_dir = stock_reports_dir(symbol)
    results_dir = stock_results_dir(symbol)
    reports_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    emit("peer_nlp_training_corpus", f"{symbol}: building sector-peer and marketwide-peer training corpora; target is excluded.")
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

    emit(
        "peer_nlp_training_corpus",
        f"{symbol}: sector corpus ready with {sector_corpus.summary.get('number_of_peer_stocks', 0)} peer stocks and {sector_corpus.summary.get('total_news_count', 0)} news rows.",
    )
    emit(
        "peer_nlp_training_corpus",
        f"{symbol}: marketwide corpus ready with {market_corpus.summary.get('number_of_peer_stocks', 0)} peer stocks and {market_corpus.summary.get('total_news_count', 0)} news rows.",
    )

    emit("peer_nlp_target_data", f"{symbol}: loading held-out target market/news data for peer NLP evaluation.")
    market = fetch_market_data(symbol, start_date, end_date, input_csv=input_csv)
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    market = market.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    trading_dates = pd.Series(pd.to_datetime(market["date"]).drop_duplicates().sort_values().tolist())
    target_news = build_news_frame(input_csv, symbol, company, start_date, end_date, sources, news_count)
    target_news = _clean_news_text(target_news)
    emit("peer_nlp_target_data", f"{symbol}: loaded {len(target_news)} held-out target news rows for high-density scoring.")

    split_news = target_news[["date"]].copy() if not target_news.empty else pd.DataFrame(columns=["date"])
    if not split_news.empty:
        split_news["news_count"] = 1
    split = detect_information_density_split(symbol, market, split_news)
    window = define_experiment_window(symbol, company, split)
    high_start = pd.to_datetime(window.get("high_density_eval_start"), errors="coerce")
    high_end = pd.to_datetime(window.get("high_density_eval_end"), errors="coerce")
    if pd.isna(high_start) or pd.isna(high_end):
        high_dates = trading_dates.tail(min(len(trading_dates), MIN_HIGH_DENSITY_TRADING_DAYS))
        high_start = high_dates.min() if not high_dates.empty else pd.NaT
        high_end = high_dates.max() if not high_dates.empty else pd.NaT
        window["window_status"] = "NOT_RELIABLE_NO_TARGET_NEWS_WINDOW"
        window["recommended_usage"] = "NOT_RELIABLE"

    target_eval_news = target_news.copy()
    if not target_eval_news.empty and pd.notna(high_start) and pd.notna(high_end):
        target_eval_news = target_eval_news[(target_eval_news["date"] >= high_start) & (target_eval_news["date"] <= high_end)].copy()

    emit(
        "peer_nlp_model_training",
        f"{symbol}: fitting sector-peer NLP model on {sector_corpus.summary.get('total_news_count', 0)} peer news rows before target scoring.",
    )
    emit("peer_nlp_target_scoring", f"{symbol}: scoring {len(target_eval_news)} held-out target news rows with the sector-peer model.")
    sector_scores, sector_method, sector_status = _score_target_from_peer_corpus(sector_corpus, target_eval_news)
    emit("peer_nlp_target_scoring", f"{symbol}: sector-peer target scoring finished with method {sector_method}.")
    emit(
        "peer_nlp_model_training",
        f"{symbol}: fitting marketwide-peer NLP model on {market_corpus.summary.get('total_news_count', 0)} peer news rows before target scoring.",
    )
    emit("peer_nlp_target_scoring", f"{symbol}: scoring {len(target_eval_news)} held-out target news rows with the marketwide-peer model.")
    market_scores, market_method, market_status = _score_target_from_peer_corpus(market_corpus, target_eval_news)
    emit("peer_nlp_target_scoring", f"{symbol}: marketwide-peer target scoring finished with method {market_method}.")
    scored = target_eval_news.copy()
    if scored.empty:
        scored = pd.DataFrame(columns=["date", "news_id", "text"])
    scored["sector_item_score"] = sector_scores if len(sector_scores) == len(scored) else []
    scored["marketwide_item_score"] = market_scores if len(market_scores) == len(scored) else []
    if not scored.empty:
        # Use positional assignment because target_eval_news can preserve a
        # filtered index; label-based assignment would turn valid dates into NaT.
        scored["tradable_date"] = align_news_to_trading_dates(scored["date"], trading_dates).to_numpy()
    else:
        scored["tradable_date"] = pd.Series(dtype="datetime64[ns]")

    daily_dates = trading_dates[(trading_dates >= high_start) & (trading_dates <= high_end)] if pd.notna(high_start) and pd.notna(high_end) else trading_dates.iloc[0:0]
    daily = pd.DataFrame({"date": daily_dates})
    daily["symbol"] = symbol
    daily["company_name"] = company
    daily["sector"] = sector

    if scored.empty:
        grouped = pd.DataFrame(columns=["date", "sector_sentiment_score", "marketwide_sentiment_score", "target_news_count"])
    else:
        grouped = (
            scored.groupby("tradable_date", as_index=False)
            .agg(
                sector_sentiment_score=("sector_item_score", "mean"),
                marketwide_sentiment_score=("marketwide_item_score", "mean"),
                target_news_count=("news_id", "count"),
            )
            .rename(columns={"tradable_date": "date"})
        )
    daily = daily.merge(grouped, on="date", how="left")
    daily["target_news_count"] = pd.to_numeric(daily.get("target_news_count", 0), errors="coerce").fillna(0).astype(int)
    daily["target_news_available"] = (daily["target_news_count"] > 0).astype(int)

    sector_ready = str(sector_corpus.summary.get("corpus_status", "")) == "READY" and sector_status in {"ok", "logistic_peer", "lexicon_peer"}
    market_ready = str(market_corpus.summary.get("corpus_status", "")) == "READY" and market_status in {"ok", "logistic_peer", "lexicon_peer"}
    for column, ready in [("sector_sentiment_score", sector_ready), ("marketwide_sentiment_score", market_ready)]:
        if column not in daily.columns:
            daily[column] = np.nan
        daily[column] = pd.to_numeric(daily[column], errors="coerce")
        daily[f"{column.replace('_score', '')}_missing_flag"] = ((daily["target_news_available"] == 0) | (not ready) | daily[column].isna()).astype(int)
        daily[column] = daily[column].fillna(0.0).clip(-1, 1)

    daily["sector_news_count_used_for_training"] = int(sector_corpus.summary.get("total_news_count", 0))
    daily["marketwide_news_count_used_for_training"] = int(market_corpus.summary.get("total_news_count", 0))
    daily["sector_peer_stock_count"] = int(sector_corpus.summary.get("number_of_peer_stocks", 0))
    daily["marketwide_peer_stock_count"] = int(market_corpus.summary.get("number_of_peer_stocks", 0))
    daily["sector_peer_sector_count"] = int(sector_corpus.summary.get("peer_sector_count", 0))
    daily["marketwide_peer_sector_count"] = int(market_corpus.summary.get("peer_sector_count", 0))
    daily["marketwide_distinct_from_sector"] = int(bool(market_corpus.summary.get("marketwide_distinct_from_sector", False)))
    daily["peer_corpus_scope"] = "sector_plus_marketwide" if include_marketwide_peer else "sector_only"
    daily["marketwide_enabled"] = int(bool(include_marketwide_peer))
    daily["sector_sentiment_method"] = sector_method
    daily["marketwide_sentiment_method"] = market_method
    daily["sector_corpus_status"] = sector_corpus.summary.get("corpus_status", "INSUFFICIENT")
    daily["marketwide_corpus_status"] = market_corpus.summary.get("corpus_status", "INSUFFICIENT")
    daily["high_density_eval_start"] = _date_text(high_start)
    daily["high_density_eval_end"] = _date_text(high_end)
    daily["alignment_rule"] = "target_news_to_next_trading_day_no_intraday_timestamp"

    target_coverage = float(daily["target_news_available"].mean()) if not daily.empty else 0.0
    split["target_sentiment_coverage"] = target_coverage
    split["peer_official_experiment"] = "peer_sector_nlp_transfer"

    output_path = results_dir / "peer_nlp_daily_sentiment.csv"
    item_path = results_dir / "peer_nlp_item_sentiment.csv"
    window_path = reports_dir / "peer_nlp_experiment_window.csv"
    split_path = reports_dir / "peer_nlp_information_density_split.csv"
    canonical_split_path = reports_dir / "information_density_split.csv"
    daily_density_path = reports_dir / "daily_news_density.csv"
    daily_density = build_daily_news_density(market, split_news)
    if not daily_density.empty and split.get("density_cutoff_date"):
        cutoff = pd.to_datetime(split["density_cutoff_date"], errors="coerce")
        daily_density["is_high_density_window"] = daily_density["date"] >= cutoff
    daily.to_csv(output_path, index=False, encoding="utf-8-sig")
    scored.to_csv(item_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([window]).to_csv(window_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([split]).to_csv(split_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([split]).to_csv(canonical_split_path, index=False, encoding="utf-8-sig")
    daily_density.to_csv(daily_density_path, index=False, encoding="utf-8-sig")
    emit("peer_nlp_sentiment_saved", f"{symbol}: saved peer_nlp_daily_sentiment.csv and item-level peer sentiment outputs.")

    return {
        "daily_sentiment": daily,
        "item_sentiment": scored,
        "density_split": pd.DataFrame([split]),
        "window": pd.DataFrame([window]),
        "peer_nlp_daily_sentiment_csv": output_path,
        "peer_nlp_item_sentiment_csv": item_path,
        "peer_nlp_window_csv": window_path,
        "sector_corpus": sector_corpus,
        "marketwide_corpus": market_corpus,
        "target_sentiment_coverage": target_coverage,
    }


def _score_target_from_peer_corpus(corpus: PeerCorpus, target_news: pd.DataFrame) -> tuple[list[float], str, str]:
    if target_news.empty:
        return [], "no_target_news", "no_target_news"
    if str(corpus.summary.get("corpus_status", "")) != "READY":
        return [0.0] * len(target_news), f"insufficient_{corpus.summary.get('corpus_type', 'peer')}_corpus", "insufficient"

    train_texts = corpus.frame.get("text", pd.Series(dtype=str)).fillna("").astype(str)
    train_texts = train_texts[train_texts.str.strip().ne("")].tolist()
    predict_texts = target_news.get("text", pd.Series(dtype=str)).fillna("").astype(str).tolist()
    if not train_texts or not predict_texts:
        return [0.0] * len(predict_texts), "no_usable_text", "insufficient"

    labels, label_method = _pseudo_label_peer_texts(train_texts)
    if len(set(labels)) < 2:
        return [float(value) for value in score_texts(predict_texts)], f"lexicon_peer_fallback_after_{label_method}_one_class", "lexicon_peer"

    model = build_model()
    model.fit(train_texts, labels)
    scores = [float(value) for value in model.predict(predict_texts)]
    return scores, f"logistic_tfidf_peer_trained_{label_method}", "logistic_peer"


def _pseudo_label_peer_texts(texts: list[str]) -> tuple[list[int], str]:
    if os.getenv("ENABLE_FINBERT_PEER_LABELING", "").strip() == "1":
        sample = texts[: min(32, len(texts))]
        sample_result = FinBERTSentiment().score(sample)
        if sample_result.status == "ok":
            full_result = FinBERTSentiment().score(texts)
            if full_result.status == "ok" and len(set(full_result.scores)) > 1:
                return [int(value) for value in full_result.scores], "finbert_pseudolabel"
    return [int(value) for value in score_texts(texts)], "lexicon_pseudolabel"


def _collect_peer_news(
    symbols: list[str],
    mapping: pd.DataFrame,
    *,
    corpus_type: str,
    status_callback: Callable[[str, str], None] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    included: list[str] = []
    total = max(len(symbols), 1)
    for index, symbol in enumerate(symbols, start=1):
        company = _mapping_company(mapping, symbol)
        if status_callback is not None:
            status_callback("peer_nlp_peer_processing", f"{corpus_type}: processing peer {symbol} {company} [{index}/{total}].")
        market_path = _latest_integrated_csv(symbol)
        if market_path is None:
            if status_callback is not None:
                status_callback("peer_nlp_peer_skipped", f"{corpus_type}: skipped peer {symbol} {company}; no integrated CSV.")
            continue
        market = _safe_read_csv(market_path)
        news = _load_local_news(symbol, company, DATA_START_DATE, DATA_END_DATE, news_count=100000)
        news = _clean_news_text(news)
        if market.empty or news.empty:
            if status_callback is not None:
                status_callback("peer_nlp_peer_skipped", f"{corpus_type}: skipped peer {symbol} {company}; market/news empty.")
            continue
        high_news = _select_high_density_news(symbol, market, news)
        if high_news.empty:
            high_news = news
        high_news["peer_symbol"] = symbol
        high_news["peer_company_name"] = company
        high_news["peer_sector"] = _mapping_sector(mapping, symbol)
        frames.append(high_news)
        included.append(symbol)
        if status_callback is not None:
            status_callback("peer_nlp_peer_processed", f"{corpus_type}: included peer {symbol} {company} with {len(high_news)} high-density news rows.")
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "text", "peer_symbol"])
    return combined, included


def _fetch_missing_configured_sector_peers(
    mapping: pd.DataFrame,
    *,
    target_symbol: str,
    target_sector: str,
    sources: str,
    news_count: int,
) -> list[str]:
    if not target_sector or target_sector.upper() == "UNKNOWN":
        return []
    sector_rows = mapping[mapping["sector"].astype(str) == target_sector].copy()
    if sector_rows.empty:
        return []
    missing = sector_rows[
        (~sector_rows["local_data_available"].astype(bool))
        & (sector_rows["symbol"].astype(str) != target_symbol)
    ].copy()
    fetched: list[str] = []
    for _, row in missing.iterrows():
        symbol = _normalize_symbol(str(row.get("symbol", "")))
        company = str(row.get("company_name", "") or symbol)
        if not re.fullmatch(r"\d{6}", symbol):
            continue
        try:
            LOGGER.info("Fetching missing sector peer %s %s for sector %s.", symbol, company, target_sector)
            run_ingestion(
                IngestionConfig(
                    symbol=symbol,
                    company_name=company,
                    start_date=DATA_START_DATE,
                    end_date=DATA_END_DATE,
                    sources=sources,
                    news_count=news_count,
                    reuse_existing_csv=True,
                    require_news=False,
                    use_sqlite=False,
                )
            )
            fetched.append(symbol)
        except Exception as exc:
            LOGGER.warning("Failed to fetch configured sector peer %s %s: %s", symbol, company, exc)
    return fetched


def _marketwide_scope_ready(peers: pd.DataFrame, target_sector: str) -> bool:
    if peers.empty or not {"symbol", "sector"}.issubset(peers.columns):
        return False
    frame = peers.copy()
    frame["symbol"] = frame["symbol"].astype(str).map(_normalize_symbol)
    frame["sector"] = frame["sector"].astype(str)
    valid = frame[(frame["symbol"].str.fullmatch(r"\d{6}", na=False)) & (frame["sector"].str.upper() != "UNKNOWN")]
    peer_count = int(valid["symbol"].nunique())
    sector_count = int(valid["sector"].nunique())
    has_cross_sector = bool((valid["sector"] != str(target_sector)).any())
    return peer_count >= MIN_MARKETWIDE_PEER_STOCKS and sector_count >= MIN_MARKETWIDE_PEER_SECTORS and has_cross_sector


def _fetch_missing_configured_marketwide_peers(
    mapping: pd.DataFrame,
    *,
    target_symbol: str,
    target_sector: str,
    sources: str,
    news_count: int,
    status_callback: Callable[[str, str], None] | None = None,
) -> list[str]:
    configured = mapping.copy()
    configured["symbol"] = configured["symbol"].astype(str).map(_normalize_symbol)
    configured["sector"] = configured["sector"].astype(str)
    existing = configured[
        (configured["local_data_available"].astype(bool))
        & (configured["symbol"] != target_symbol)
        & (configured["sector"].str.upper() != "UNKNOWN")
    ].copy()
    if _marketwide_scope_ready(existing, target_sector):
        return []

    missing = configured[
        (~configured["local_data_available"].astype(bool))
        & (configured["symbol"] != target_symbol)
        & (configured["sector"].str.upper() != "UNKNOWN")
        & (configured["sector"] != str(target_sector))
    ].copy()
    if missing.empty:
        return []

    fetched: list[str] = []
    planned_by_sector: dict[str, int] = {}
    sectors = sorted(missing["sector"].dropna().astype(str).unique().tolist())
    for sector in sectors:
        sector_rows = missing[missing["sector"].astype(str) == sector].head(MAX_MARKETWIDE_FETCH_PER_NON_TARGET_SECTOR)
        for _, row in sector_rows.iterrows():
            combined = pd.concat(
                [
                    existing[["symbol", "sector"]],
                    missing[missing["symbol"].isin(fetched)][["symbol", "sector"]],
                ],
                ignore_index=True,
            )
            if _marketwide_scope_ready(combined, target_sector):
                return fetched
            if planned_by_sector.get(sector, 0) >= MAX_MARKETWIDE_FETCH_PER_NON_TARGET_SECTOR:
                continue
            symbol = _normalize_symbol(str(row.get("symbol", "")))
            company = str(row.get("company_name", "") or symbol)
            if not re.fullmatch(r"\d{6}", symbol):
                continue
            try:
                if status_callback is not None:
                    status_callback("peer_nlp_peer_processing", f"marketwide_peer: fetching cross-sector peer {symbol} {company} ({sector}).")
                LOGGER.info("Fetching missing marketwide peer %s %s for sector %s.", symbol, company, sector)
                run_ingestion(
                    IngestionConfig(
                        symbol=symbol,
                        company_name=company,
                        start_date=DATA_START_DATE,
                        end_date=DATA_END_DATE,
                        sources=sources,
                        news_count=news_count,
                        reuse_existing_csv=True,
                        require_news=False,
                        use_sqlite=False,
                    )
                )
                fetched.append(symbol)
                planned_by_sector[sector] = planned_by_sector.get(sector, 0) + 1
                if status_callback is not None:
                    status_callback("peer_nlp_peer_processed", f"marketwide_peer: fetched cross-sector peer {symbol} {company}.")
            except Exception as exc:
                if status_callback is not None:
                    status_callback("peer_nlp_peer_skipped", f"marketwide_peer: failed to fetch {symbol} {company}; {exc}")
                LOGGER.warning("Failed to fetch configured marketwide peer %s %s: %s", symbol, company, exc)
    return fetched


def _select_high_density_news(symbol: str, market: pd.DataFrame, news: pd.DataFrame) -> pd.DataFrame:
    if news.empty:
        return news
    split_news = news[["date"]].copy()
    split_news["news_count"] = 1
    split = detect_information_density_split(symbol, market, split_news)
    start = pd.to_datetime(split.get("high_density_start_date"), errors="coerce")
    end = pd.to_datetime(split.get("high_density_end_date"), errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return pd.DataFrame(columns=news.columns)
    return news[(news["date"] >= start) & (news["date"] <= end)].copy()


def _load_local_news(symbol: str, company: str, start_date: str, end_date: str, news_count: int = 100000) -> pd.DataFrame:
    csv_path = _latest_integrated_csv(symbol)
    if csv_path is None:
        return pd.DataFrame(columns=["news_id", "ticker", "date", "title", "content", "source", "text"])
    try:
        return build_news_frame(csv_path, symbol, company, start_date, end_date, "local", news_count)
    except Exception as exc:
        LOGGER.warning("Could not load local news for %s from %s: %s", symbol, csv_path, exc)
        return pd.DataFrame(columns=["news_id", "ticker", "date", "title", "content", "source", "text"])


def _clean_news_text(news: pd.DataFrame) -> pd.DataFrame:
    if news.empty:
        return news.copy()
    frame = news.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    title = frame["title"] if "title" in frame.columns else pd.Series([""] * len(frame), index=frame.index)
    content = frame["content"] if "content" in frame.columns else pd.Series([""] * len(frame), index=frame.index)
    frame["text"] = (title.fillna("").astype(str) + " " + content.fillna("").astype(str)).str.strip()
    frame = frame.dropna(subset=["date"])
    frame = frame[frame["text"].str.strip().ne("")]
    return frame.reset_index(drop=True)


def _corpus_summary_row(
    target_symbol: str,
    target_company: str,
    target_sector: str,
    corpus_type: str,
    included_symbols: list[str],
    excluded_symbols: list[str],
    frame: pd.DataFrame,
    status: str,
    reason: str,
    fetched_symbols: list[str],
) -> dict[str, object]:
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna() if not frame.empty and "date" in frame.columns else pd.Series(dtype="datetime64[ns]")
    peer_sectors = sorted(frame.get("peer_sector", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()) if not frame.empty and "peer_sector" in frame.columns else []
    is_marketwide = corpus_type == "marketwide_peer"
    return {
        "target_symbol": target_symbol,
        "target_company_name": target_company,
        "target_sector": target_sector,
        "corpus_type": corpus_type,
        "included_symbols": ",".join(included_symbols),
        "excluded_symbols": ",".join(excluded_symbols),
        "number_of_peer_stocks": int(len(included_symbols)),
        "peer_sector_count": int(len(peer_sectors)),
        "peer_sectors": ",".join(peer_sectors),
        "total_news_count": int(len(frame)),
        "date_start": _date_text(dates.min()) if not dates.empty else "",
        "date_end": _date_text(dates.max()) if not dates.empty else "",
        "high_density_only": True,
        "corpus_scope": "sector_plus_marketwide" if is_marketwide and included_symbols else "sector_only",
        "marketwide_distinct_from_sector": bool(is_marketwide and any(sector != str(target_sector) for sector in peer_sectors)),
        "corpus_status": status,
        "reason_if_not_ready": reason,
        "newly_fetched_symbols": ",".join(fetched_symbols),
    }


def _save_corpus_summary(rows: list[dict[str, object]]) -> None:
    path = REPORTS_TABLE_DIR / "peer_nlp_corpus_summary.csv"
    REPORTS_TABLE_DIR.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    incoming = pd.DataFrame(rows)
    if not existing.empty and {"target_symbol", "corpus_type"}.issubset(existing.columns):
        existing["target_symbol"] = existing["target_symbol"].astype(str)
        existing = existing[existing["target_symbol"].str.fullmatch(r"\d{6}", na=False)].copy()
        keys = set(zip(incoming["target_symbol"].astype(str), incoming["corpus_type"].astype(str)))
        existing = existing[~existing.apply(lambda row: (str(row["target_symbol"]), str(row["corpus_type"])) in keys, axis=1)]
    pd.concat([existing, incoming], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")


def _latest_integrated_csv(symbol: str) -> Path | None:
    data_dir = stock_data_dir(symbol)
    if not data_dir.exists():
        return None
    candidates = [path for path in data_dir.glob("*_finance_text_*.csv") if not path.name.endswith("_master.csv") and path.stat().st_size > 4]
    if candidates:
        return sorted(candidates, key=lambda path: path.stat().st_mtime)[-1]
    master = data_dir / f"{_normalize_symbol(symbol)}_finance_text_master.csv"
    return master if master.exists() and master.stat().st_size > 4 else None


def _local_symbols(stock_root: Path) -> list[str]:
    if not stock_root.exists():
        return []
    return sorted([path.name for path in stock_root.iterdir() if path.is_dir() and re.fullmatch(r"\d{6}", path.name)])


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def _company_from_market(symbol: str, market: pd.DataFrame) -> str:
    for column in ["company_name", "longName", "name"]:
        if column in market.columns and market[column].notna().any():
            value = str(market[column].dropna().iloc[0]).strip()
            if value and value != symbol and not value.isdigit():
                return value
    return symbol


def _mapping_company(mapping: pd.DataFrame, symbol: str) -> str:
    row = mapping[mapping["symbol"].astype(str) == _normalize_symbol(symbol)]
    return str(row["company_name"].iloc[0]) if not row.empty else _normalize_symbol(symbol)


def _mapping_sector(mapping: pd.DataFrame, symbol: str) -> str:
    row = mapping[mapping["symbol"].astype(str) == _normalize_symbol(symbol)]
    return str(row["sector"].iloc[0]) if not row.empty else "UNKNOWN"


def _normalize_symbol(symbol: str) -> str:
    extracted = pd.Series([str(symbol)]).str.extract(r"(\d{6})", expand=False).iloc[0]
    return str(extracted) if pd.notna(extracted) else str(symbol).strip()


def _date_text(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(timestamp) else str(timestamp.date())
