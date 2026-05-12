"""Diagnostics for NLP sentiment and market money-flow signals."""

from __future__ import annotations

import numpy as np
import pandas as pd


def signal_diagnostics(
    market: pd.DataFrame,
    daily_sentiment: pd.DataFrame | None = None,
    daily_net_flow: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build project-level diagnostics for sentiment and money-flow signals."""

    data = market.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    data["same_day_return"] = data["close"].pct_change()
    data["next_day_return"] = data["close"].shift(-1) / data["close"] - 1

    rows: list[dict[str, object]] = []
    rows.append({"metric": "market_rows", "value": float(len(data)), "description": "Number of trading rows used.", "warning_level": "info"})
    rows.append(
        {
            "metric": "close_cumulative_return",
            "value": float(data["close"].iloc[-1] / data["close"].iloc[0] - 1) if len(data) > 1 else np.nan,
            "description": "Raw close-price return over the sample.",
            "warning_level": "info",
        }
    )
    rows.append(
        {
            "metric": "duplicate_dates",
            "value": int(data["date"].duplicated().sum()),
            "description": "Trading rows sharing the same date.",
            "warning_level": "error" if int(data["date"].duplicated().sum()) > 0 else "ok",
        }
    )

    if daily_sentiment is not None and not daily_sentiment.empty:
        sentiment = daily_sentiment.copy()
        sentiment["date"] = pd.to_datetime(sentiment["date"], errors="coerce")
        score_col = "sentiment_score" if "sentiment_score" in sentiment.columns else "daily_sentiment_score"
        sentiment[score_col] = pd.to_numeric(sentiment[score_col], errors="coerce")
        if "news_count" not in sentiment.columns:
            sentiment["news_count"] = 0
        merged = data.merge(sentiment[["date", score_col, "news_count", "sentiment_method", "alignment_rule"]], on="date", how="left")
        coverage = float((pd.to_numeric(merged["news_count"], errors="coerce").fillna(0) > 0).mean())
        rows.extend(
            [
                {
                    "metric": "sentiment_coverage",
                    "value": coverage,
                    "description": "Share of trading days with at least one aligned news item.",
                    "warning_level": "warn" if coverage < 0.2 else "ok",
                },
                {
                    "metric": "sentiment_news_count_total",
                    "value": float(pd.to_numeric(merged["news_count"], errors="coerce").fillna(0).sum()),
                    "description": "Total aligned news items used for daily sentiment.",
                    "warning_level": "info",
                },
                {
                    "metric": "average_sentiment",
                    "value": float(merged[score_col].mean()) if merged[score_col].notna().any() else np.nan,
                    "description": "Average daily sentiment score after alignment.",
                    "warning_level": "info",
                },
                {
                    "metric": "sentiment_next_day_return_corr",
                    "value": _corr(merged[score_col], merged["next_day_return"]),
                    "description": "Correlation between aligned sentiment and next trading day's return.",
                    "warning_level": "info",
                },
                {
                    "metric": "sentiment_method",
                    "value": ", ".join(sorted(set(merged["sentiment_method"].dropna().astype(str)))) if "sentiment_method" in merged.columns else "unknown",
                    "description": "Main sentiment generation method over the sample.",
                    "warning_level": "info",
                },
                {
                    "metric": "news_alignment_rule",
                    "value": ", ".join(sorted(set(merged["alignment_rule"].dropna().astype(str)))) if "alignment_rule" in merged.columns else "unknown",
                    "description": "Rule used to convert news timestamps/dates into tradable dates.",
                    "warning_level": "info",
                },
            ]
        )

    if daily_net_flow is not None and not daily_net_flow.empty:
        flow = daily_net_flow.copy()
        flow["date"] = pd.to_datetime(flow["date"], errors="coerce")
        flow["net_flow"] = pd.to_numeric(flow.get("net_flow"), errors="coerce")
        merged = data.merge(flow[["date", "net_flow", "money_flow_method"]], on="date", how="left")
        same_day_corr = _corr(merged["net_flow"], merged["same_day_return"])
        next_day_corr = _corr(merged["net_flow"], merged["next_day_return"])
        method_text = ", ".join(sorted(set(merged["money_flow_method"].dropna().astype(str)))) if "money_flow_method" in merged.columns else "unknown"
        same_day_warning = (
            "High same-day correlation is expected when net_flow_proxy is derived from same-day OHLCV movement; do not interpret it as predictive power."
            if "proxy" in method_text and pd.notna(same_day_corr) and abs(same_day_corr) > 0.3
            else ""
        )
        rows.extend(
            [
                {
                    "metric": "average_net_flow",
                    "value": float(merged["net_flow"].mean()) if merged["net_flow"].notna().any() else np.nan,
                    "description": "Average reported or estimated daily net flow.",
                    "warning_level": "info",
                },
                {
                    "metric": "net_flow_same_day_return_corr",
                    "value": same_day_corr,
                    "description": "Correlation between net flow and same-day return. " + same_day_warning,
                    "warning_level": "warn" if same_day_warning else "info",
                },
                {
                    "metric": "net_flow_next_day_return_corr",
                    "value": next_day_corr,
                    "description": "Correlation between today's net flow and next trading day's return.",
                    "warning_level": "info",
                },
                {
                    "metric": "net_flow_method",
                    "value": method_text,
                    "description": "Reported flow source or explanatory same-day proxy method.",
                    "warning_level": "warn" if "proxy" in method_text else "info",
                },
                {
                    "metric": "same_day_proxy_leakage_warning",
                    "value": same_day_warning or "none",
                    "description": "Warns when the same-day proxy is mechanically correlated with same-day return.",
                    "warning_level": "warn" if same_day_warning else "ok",
                },
            ]
        )

    return pd.DataFrame(rows)


def _corr(left: pd.Series, right: pd.Series) -> float:
    frame = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")}).dropna()
    if len(frame) < 3 or frame["left"].std() == 0 or frame["right"].std() == 0:
        return float("nan")
    return float(frame["left"].corr(frame["right"]))
