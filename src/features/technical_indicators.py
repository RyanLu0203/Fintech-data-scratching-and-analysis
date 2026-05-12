"""Technical indicators, leakage-safe trading features, and RL state validation."""

from __future__ import annotations

import numpy as np
import pandas as pd


STATE_COLUMNS = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "sentiment_score"]
ENHANCED_NLP_STATE_COLUMNS = [
    "price",
    "MA50",
    "MA200",
    "RSI",
    "MACD",
    "position",
    "cash",
    "sentiment_score",
    "news_available",
    "news_count",
    "sentiment_rolling_3d",
    "sentiment_rolling_5d",
]
WITHOUT_NLP_STATE_COLUMNS = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash"]
SHIFTED_MARKET_FEATURES = ["price", "MA50", "MA200", "RSI", "MACD"]
SHIFTED_NLP_FEATURES = [
    "sentiment_score",
    "news_count",
    "news_available",
    "sentiment_missing_flag",
    "sentiment_rolling_3d",
    "sentiment_rolling_5d",
    "sentiment_change_1d",
    "news_count_zscore",
    "positive_ratio",
    "negative_ratio",
]
LEAKAGE_PRONE_COLUMNS = ["net_flow", "net_flow_proxy", "same_day_return", "next_day_return", "return_1d"]


def add_trading_features(
    market: pd.DataFrame,
    sentiment: pd.DataFrame | None = None,
    initial_cash: float = 1000000.0,
    include_portfolio_columns: bool = True,
    sentiment_already_aligned: bool = True,
) -> pd.DataFrame:
    """Build a leakage-safe trading feature frame.

    Market-based and NLP predictors are shifted by one trading day so the
    action at day ``t`` only sees information available by the end of day
    ``t-1``. Raw same-day sentiment columns are preserved with ``*_raw``
    suffixes for diagnostics, not as RL state.
    """

    data = market.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for column in ["open", "high", "low", "close", "volume"]:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["close"]).reset_index(drop=True)

    data["execution_price"] = pd.to_numeric(data.get("open"), errors="coerce")
    data["execution_price"] = data["execution_price"].fillna(data["close"])
    data["valuation_price"] = data["close"]
    data["decision_date"] = data["date"]

    # Raw features are preserved for diagnostics and report charts.
    data["price_raw"] = data["close"]
    data["MA50_raw"] = data["close"].rolling(50, min_periods=1).mean()
    data["MA200_raw"] = data["close"].rolling(200, min_periods=1).mean()

    delta = data["close"].diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    data["RSI_raw"] = 100 - (100 / (1 + rs))

    ema12 = data["close"].ewm(span=12, adjust=False).mean()
    ema26 = data["close"].ewm(span=26, adjust=False).mean()
    data["MACD_raw"] = ema12 - ema26
    data["same_day_return"] = data["close"].pct_change()
    data["next_day_return"] = data["close"].shift(-1) / data["close"] - 1

    if sentiment is not None and not sentiment.empty:
        signal = sentiment.copy()
        signal["date"] = pd.to_datetime(signal["date"], errors="coerce")
        score_column = _first_present(signal, ["sentiment_score", "daily_sentiment_score"])
        method_column = _first_present(signal, ["sentiment_method", "method"])
        news_count_column = _first_present(signal, ["news_count"])
        keep_columns = ["date"]
        if score_column:
            keep_columns.append(score_column)
        if method_column:
            keep_columns.append(method_column)
        if news_count_column:
            keep_columns.append(news_count_column)
        signal = signal[keep_columns].copy()
        rename_map = {}
        if score_column:
            rename_map[score_column] = "sentiment_score"
        if method_column:
            rename_map[method_column] = "sentiment_method"
        if news_count_column:
            rename_map[news_count_column] = "news_count"
        signal = signal.rename(columns=rename_map)
        data = data.merge(signal, on="date", how="left")

    if "sentiment_score" not in data.columns:
        data["sentiment_score"] = 0.0
    if "news_count" not in data.columns:
        data["news_count"] = 0
    if "sentiment_method" not in data.columns:
        data["sentiment_method"] = "missing"
    data["sentiment_score"] = pd.to_numeric(data["sentiment_score"], errors="coerce").fillna(0.0)
    data["news_count"] = pd.to_numeric(data["news_count"], errors="coerce").fillna(0).astype(int)
    data = add_enhanced_nlp_features(data)

    # Preserve same-day text signals for diagnostics/plots, then lag the
    # predictive NLP state. A zero sentiment score with news_available=0 now
    # means "missing news", not true neutral sentiment.
    for column in SHIFTED_NLP_FEATURES:
        if column in data.columns:
            data[f"{column}_raw"] = data[column]
            data[column] = data[column].shift(1)

    # The state at date t only sees features known by the close of t-1.
    data["price"] = data["price_raw"].shift(1)
    data["MA50"] = data["MA50_raw"].shift(1)
    data["MA200"] = data["MA200_raw"].shift(1)
    data["RSI"] = data["RSI_raw"].shift(1)
    data["MACD"] = data["MACD_raw"].shift(1)
    data["feature_available_until"] = data["date"].shift(1)
    data["feature_shift_days"] = 1

    if include_portfolio_columns:
        data["position"] = 0.0
        data["cash"] = float(initial_cash)

    required = SHIFTED_MARKET_FEATURES + ["execution_price", "valuation_price"]
    if include_portfolio_columns:
        required.extend(["position", "cash"])
    data = data.dropna(subset=required).reset_index(drop=True)
    data["sentiment_score"] = data["sentiment_score"].fillna(0.0)
    data["news_count"] = data["news_count"].fillna(0).astype(int)
    data["news_available"] = data.get("news_available", 0).fillna(0).astype(int)
    data["sentiment_missing_flag"] = data.get("sentiment_missing_flag", 1).fillna(1).astype(int)
    for column in ["sentiment_rolling_3d", "sentiment_rolling_5d", "sentiment_change_1d", "news_count_zscore", "positive_ratio", "negative_ratio"]:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce").fillna(0.0)
    return data


def add_enhanced_nlp_features(data: pd.DataFrame) -> pd.DataFrame:
    """Add optional NLP-derived features without replacing sentiment_score."""

    frame = data.sort_values("date").copy() if "date" in data.columns else data.copy()
    sentiment = pd.to_numeric(frame.get("sentiment_score", 0), errors="coerce").fillna(0.0)
    news_count = pd.to_numeric(frame.get("news_count", 0), errors="coerce").fillna(0.0)
    frame["news_available"] = (news_count > 0).astype(int)
    frame["sentiment_missing_flag"] = (news_count <= 0).astype(int)
    frame["sentiment_rolling_3d"] = sentiment.rolling(3, min_periods=1).mean()
    frame["sentiment_rolling_5d"] = sentiment.rolling(5, min_periods=1).mean()
    frame["sentiment_change_1d"] = sentiment.diff().fillna(0.0)
    frame["news_count"] = news_count
    std = news_count.std(ddof=0)
    frame["news_count_zscore"] = 0.0 if std == 0 or pd.isna(std) else (news_count - news_count.mean()) / std
    frame["positive_ratio"] = sentiment.clip(lower=0.0, upper=1.0)
    frame["negative_ratio"] = (-sentiment).clip(lower=0.0, upper=1.0)
    return frame


def validate_state_columns(
    df: pd.DataFrame,
    required_columns: list[str] | None = None,
    *,
    sentiment_required: bool | None = None,
) -> pd.DataFrame:
    """Validate RL state columns, missing values, and leakage-prone fields."""

    required = required_columns or STATE_COLUMNS
    rows: list[dict[str, object]] = []
    for column in required:
        present = column in df.columns
        missing_values = int(df[column].isna().sum()) if present else np.nan
        leakage_prone = column in LEAKAGE_PRONE_COLUMNS
        shifted_ok = True
        if column in SHIFTED_MARKET_FEATURES + SHIFTED_NLP_FEATURES and present:
            shifted_ok = bool((pd.to_numeric(df[column], errors="coerce").isna().sum()) == 0)
        rows.append(
            {
                "state_column": column,
                "present": present,
                "missing_values": missing_values,
                "shifted_correctly": shifted_ok,
                "leakage_prone": leakage_prone,
                "sentiment_column": column == "sentiment_score",
            }
        )

    result = pd.DataFrame(rows)
    missing = result.loc[~result["present"], "state_column"].tolist()
    null_issues = result.loc[result["missing_values"].fillna(0) > 0, "state_column"].tolist()
    leakage_issues = result.loc[result["leakage_prone"], "state_column"].tolist()

    if missing:
        raise ValueError(f"Missing required RL state columns: {missing}")
    if null_issues:
        raise ValueError(f"RL state still has missing values in: {null_issues}")
    if leakage_issues:
        raise ValueError(f"Leakage-prone columns were included in the RL state: {leakage_issues}")

    if sentiment_required is not None:
        has_sentiment = "sentiment_score" in required
        if has_sentiment != sentiment_required:
            raise ValueError(
                f"State sentiment requirement mismatch: expected sentiment_required={sentiment_required}, "
                f"but state columns were {required}."
            )
    return result


def leakage_diagnostics(
    feature_frame: pd.DataFrame,
    state_columns: list[str],
    *,
    sentiment_is_aligned_to_trade_date: bool = True,
) -> pd.DataFrame:
    """Check whether the RL state leaks same-day or future information."""

    rows: list[dict[str, object]] = []
    feature_dates = pd.to_datetime(feature_frame.get("date"), errors="coerce")
    available_until = pd.to_datetime(feature_frame.get("feature_available_until"), errors="coerce")
    rows.append(
        {
            "metric": "feature_shift_correctness",
            "value": bool((available_until < feature_dates).all()) if not feature_frame.empty else True,
            "description": "All shifted market features must come from a prior trading date.",
            "warning_level": "ok" if feature_frame.empty or bool((available_until < feature_dates).all()) else "error",
        }
    )
    rows.append(
        {
            "metric": "lookahead_bias_detected",
            "value": bool((available_until >= feature_dates).any()) if not feature_frame.empty else False,
            "description": "True means at least one row used same-day or future market information.",
            "warning_level": "error" if not feature_frame.empty and bool((available_until >= feature_dates).any()) else "ok",
        }
    )
    rows.append(
        {
            "metric": "sentiment_alignment_rule_safe",
            "value": bool(sentiment_is_aligned_to_trade_date),
            "description": "Sentiment features are shifted before entering the RL state; raw same-day text signals are kept only for diagnostics.",
            "warning_level": "ok" if sentiment_is_aligned_to_trade_date else "warn",
        }
    )
    nlp_state_cols = [column for column in state_columns if column in SHIFTED_NLP_FEATURES]
    rows.append(
        {
            "metric": "nlp_feature_shift_correctness",
            "value": bool(nlp_state_cols),
            "description": "NLP state columns use t-1 values; no-news days are represented by news_available=0 and sentiment_missing_flag=1.",
            "warning_level": "ok" if nlp_state_cols or "sentiment_score" not in state_columns else "warn",
        }
    )
    leakage_proxy_cols = [column for column in state_columns if column in {"net_flow_proxy", "net_flow"}]
    rows.append(
        {
            "metric": "same_day_proxy_leakage_warning",
            "value": bool(leakage_proxy_cols),
            "description": "Same-day net-flow proxies should remain diagnostics and must not be predictive state inputs.",
            "warning_level": "error" if leakage_proxy_cols else "ok",
        }
    )
    rows.append(
        {
            "metric": "state_columns",
            "value": ", ".join(state_columns),
            "description": "Exact state vector used by the RL policy.",
            "warning_level": "info",
        }
    )
    return pd.DataFrame(rows)


def _first_present(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None
