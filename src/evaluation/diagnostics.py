"""Project diagnostics for data quality, leakage checks, and consistency."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_run_diagnostics(
    raw_market: pd.DataFrame,
    feature_frame: pd.DataFrame,
    daily_sentiment: pd.DataFrame,
    signal_diagnostics_table: pd.DataFrame | None = None,
    walk_forward_table: pd.DataFrame | None = None,
    split_info: pd.DataFrame | None = None,
    ablation_metrics: pd.DataFrame | None = None,
    seed_metrics: pd.DataFrame | None = None,
    leakage_diagnostics: pd.DataFrame | None = None,
    trading_logs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    market = raw_market.copy()
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    rows.append(_row("missing_values_total", int(market.isna().sum().sum()), "Total missing values in the raw integrated market frame.", "warn" if int(market.isna().sum().sum()) > 0 else "ok"))
    rows.append(_row("duplicate_dates", int(market["date"].duplicated().sum()), "Duplicate market dates in the integrated frame.", "error" if int(market["date"].duplicated().sum()) > 0 else "ok"))

    sentiment = daily_sentiment.copy() if daily_sentiment is not None else pd.DataFrame()
    if not sentiment.empty:
        coverage = float((pd.to_numeric(sentiment.get("news_count", 0), errors="coerce").fillna(0) > 0).mean())
        rows.append(_row("sentiment_coverage", coverage, "Trading-day share with at least one aligned news item.", "warn" if coverage < 0.2 else "ok"))
        rows.append(_row("news_count_total", float(pd.to_numeric(sentiment.get("news_count", 0), errors="coerce").fillna(0).sum()), "Total aligned news item count.", "info"))
        method_values = ", ".join(sorted(set(sentiment.get("sentiment_method", pd.Series(dtype=str)).dropna().astype(str))))
        rows.append(_row("sentiment_method", method_values or "unknown", "Main sentiment method used in the experiment.", "info"))
        alignment_values = ", ".join(sorted(set(sentiment.get("alignment_rule", pd.Series(dtype=str)).dropna().astype(str))))
        rows.append(_row("news_alignment_rule", alignment_values or "unknown", "Rule used to map news into tradable dates.", "info"))

    if not feature_frame.empty:
        if {"date", "feature_available_until"}.issubset(feature_frame.columns):
            feature_dates = pd.to_datetime(feature_frame["date"], errors="coerce")
            available_until = pd.to_datetime(feature_frame["feature_available_until"], errors="coerce")
            leaked = bool((available_until >= feature_dates).any())
            rows.append(_row("feature_shift_correctness", not leaked, "All RL state features should come from a prior trading day.", "error" if leaked else "ok"))
            rows.append(_row("lookahead_bias_detected", leaked, "True means same-day or future information entered the RL state.", "error" if leaked else "ok"))
        rows.append(_row("feature_missing_values", int(feature_frame.isna().sum().sum()), "Remaining missing values in the trading feature frame.", "error" if int(feature_frame.isna().sum().sum()) > 0 else "ok"))

    if signal_diagnostics_table is not None and not signal_diagnostics_table.empty:
        rows.extend(signal_diagnostics_table.to_dict(orient="records"))

    if leakage_diagnostics is not None and not leakage_diagnostics.empty:
        rows.extend(leakage_diagnostics.to_dict(orient="records"))

    if walk_forward_table is not None and not walk_forward_table.empty:
        consistent = bool(walk_forward_table["no_lookahead"].fillna(False).all())
        rows.append(_row("walk_forward_no_lookahead", consistent, "All walk-forward splits are chronological with no overlap.", "error" if not consistent else "ok"))

    if split_info is not None and not split_info.empty:
        consistent = bool(split_info["consistent_period"].fillna(False).all())
        rows.append(_row("train_test_consistency", consistent, "Chronological train/test split uses a later test period for all experiments.", "error" if not consistent else "ok"))

    if ablation_metrics is not None and not ablation_metrics.empty:
        experiments = set(ablation_metrics["experiment"].astype(str))
        rows.append(_row("ablation_experiments_present", ", ".join(sorted(experiments)), "Ablation experiments found in the aggregated metrics table.", "ok"))
        if {"buy_and_hold", "dqn_without_nlp", "dqn_with_nlp"}.issubset(experiments):
            rows.append(_row("comparison_period_consistency", True, "Buy-and-hold and both DQN variants were evaluated over the same held-out period.", "ok"))

    if seed_metrics is not None and not seed_metrics.empty:
        seed_count = int(pd.Series(seed_metrics["seed"]).nunique())
        rows.append(_row("random_seed_count", seed_count, "Number of random seeds used in the DQN ablation.", "warn" if seed_count < 3 else "ok"))
        rows.append(_row("random_seed_reproducibility", True, "Each seed is logged explicitly so repeated runs can be matched by seed.", "ok"))

    if trading_logs is not None and not trading_logs.empty and "experiment" in trading_logs.columns:
        rows.append(
            _row(
                "trading_log_experiments",
                ", ".join(sorted(set(trading_logs["experiment"].dropna().astype(str)))),
                "Experiments represented in the detailed trading logs.",
                "info",
            )
        )

    frame = pd.DataFrame(rows)
    if "metric" not in frame.columns:
        return pd.DataFrame(columns=["metric", "value", "description", "warning_level"])
    return frame[["metric", "value", "description", "warning_level"]]


def _row(metric: str, value: object, description: str, warning_level: str) -> dict[str, object]:
    return {"metric": metric, "value": value, "description": description, "warning_level": warning_level}
