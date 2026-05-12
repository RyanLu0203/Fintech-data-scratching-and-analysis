"""Walk-forward validation utilities that preserve chronological order."""

from __future__ import annotations

import pandas as pd

from src.evaluation.metrics import max_drawdown, sharpe_ratio
from src.utils.dates import DateRange, walk_forward_windows


def split_frame_walk_forward(
    frame: pd.DataFrame,
    start_date: str,
    end_date: str,
    train_days: int = 252,
    test_days: int = 63,
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    full_range = DateRange(pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date())
    splits = []
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"]).dt.date
    for train_range, test_range in walk_forward_windows(full_range, train_days, test_days):
        train = data[(data["date"] >= train_range.start) & (data["date"] <= train_range.end)]
        test = data[(data["date"] >= test_range.start) & (data["date"] <= test_range.end)]
        splits.append((train, test))
    return splits


def walk_forward_summary(
    frame: pd.DataFrame,
    start_date: str,
    end_date: str,
    train_days: int = 252,
    test_days: int = 63,
) -> pd.DataFrame:
    """Return a human-readable split table for reports."""

    rows = []
    for index, (train, test) in enumerate(split_frame_walk_forward(frame, start_date, end_date, train_days, test_days), start=1):
        rows.append(
            {
                "split": index,
                "train_start": train["date"].min() if not train.empty else pd.NaT,
                "train_end": train["date"].max() if not train.empty else pd.NaT,
                "train_rows": int(len(train)),
                "test_start": test["date"].min() if not test.empty else pd.NaT,
                "test_end": test["date"].max() if not test.empty else pd.NaT,
                "test_rows": int(len(test)),
                "no_lookahead": bool(train.empty or test.empty or train["date"].max() < test["date"].min()),
            }
        )
    return pd.DataFrame(rows)


def summarize_walk_forward_results(
    portfolio_curves: pd.DataFrame,
    splits: pd.DataFrame,
    output_csv: str | None = None,
) -> pd.DataFrame:
    """Compute per-split strategy metrics from existing portfolio curves.

    This is intentionally labelled as split diagnostics unless the caller has
    generated curves from true rolling retraining. It prevents the report from
    overclaiming full walk-forward validation when only holdout curves exist.
    """

    if portfolio_curves.empty or splits.empty:
        result = pd.DataFrame(
            [
                {
                    "validation_type": "chronological_holdout_walk_forward_split_diagnostics",
                    "status": "skipped_missing_curves_or_splits",
                    "train_start": "",
                    "train_end": "",
                    "test_start": "",
                    "test_end": "",
                    "strategy": "",
                    "final_equity": pd.NA,
                    "cumulative_return": pd.NA,
                    "sharpe_ratio": pd.NA,
                    "max_drawdown": pd.NA,
                }
            ]
        )
        if output_csv:
            result.to_csv(output_csv, index=False, encoding="utf-8-sig")
        return result

    curves = portfolio_curves.copy()
    curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
    rows = []
    for _, split in splits.iterrows():
        test_start = pd.to_datetime(split.get("test_start"), errors="coerce")
        test_end = pd.to_datetime(split.get("test_end"), errors="coerce")
        if pd.isna(test_start) or pd.isna(test_end):
            continue
        window = curves[(curves["date"] >= test_start) & (curves["date"] <= test_end)].copy()
        for strategy, group in window.groupby("experiment"):
            values = pd.to_numeric(group.sort_values("date")["portfolio_value"], errors="coerce").dropna()
            if values.empty:
                continue
            rows.append(
                {
                    "validation_type": "chronological_holdout_walk_forward_split_diagnostics",
                    "status": "not_full_rolling_retraining",
                    "train_start": split.get("train_start", ""),
                    "train_end": split.get("train_end", ""),
                    "test_start": split.get("test_start", ""),
                    "test_end": split.get("test_end", ""),
                    "strategy": strategy,
                    "final_equity": float(values.iloc[-1]),
                    "cumulative_return": float(values.iloc[-1] / values.iloc[0] - 1) if values.iloc[0] else pd.NA,
                    "sharpe_ratio": sharpe_ratio(values),
                    "max_drawdown": max_drawdown(values),
                }
            )
    result = pd.DataFrame(rows)
    if output_csv:
        result.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return result


def lag_sentiment_for_decision(frame: pd.DataFrame, sentiment_column: str = "sentiment_score") -> pd.DataFrame:
    """Lag sentiment by one row so decisions only use previously available news."""

    data = frame.sort_values("date").copy()
    if sentiment_column in data.columns:
        data[sentiment_column] = data[sentiment_column].shift(1).fillna(0.0)
    return data
