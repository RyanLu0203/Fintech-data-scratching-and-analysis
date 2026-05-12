"""Diagnostics for whether NLP signals have trading relevance."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT

SIGNAL_COLUMNS = [
    "sector_sentiment_score",
    "marketwide_sentiment_score",
    "sector_impact_score",
    "marketwide_impact_score",
]


def compute_signal_validity(
    market: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    symbol: str,
    reports_dir: Path,
    horizons: tuple[int, ...] = (1, 3, 5),
    rolling_window: int = 30,
) -> dict[str, pd.DataFrame | Path]:
    """Compute IC, quantile returns, hit rates, and rolling IC diagnostics."""

    reports_dir.mkdir(parents=True, exist_ok=True)
    merged = _merge_market_signals(market, signals, horizons)
    rows: list[dict[str, object]] = []
    quantile_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []

    for signal in SIGNAL_COLUMNS:
        if signal not in merged.columns:
            continue
        x = pd.to_numeric(merged[signal], errors="coerce")
        for horizon in horizons:
            y = pd.to_numeric(merged[f"future_return_{horizon}d"], errors="coerce")
            valid = pd.DataFrame({"signal": x, "future_return": y}).dropna()
            corr = _corr(valid["signal"], valid["future_return"], method="pearson")
            ic = _corr(valid["signal"], valid["future_return"], method="spearman")
            rows.append(
                {
                    "symbol": symbol,
                    "signal": signal,
                    "horizon_days": horizon,
                    "corr_signal_future_return": corr,
                    "information_coefficient": ic,
                    "hit_rate": _hit_rate(valid["signal"], valid["future_return"]),
                    "valid_rows": int(len(valid)),
                }
            )
            quantile_rows.extend(_quantile_returns(symbol, signal, horizon, valid))
            rolling_rows.extend(_rolling_ic(symbol, signal, horizon, merged[["date"]].join(valid, how="left"), rolling_window))

    summary = pd.DataFrame(rows)
    quantiles = pd.DataFrame(quantile_rows)
    rolling = pd.DataFrame(rolling_rows)
    paths = {
        "summary": reports_dir / "signal_validity_summary.csv",
        "quantiles": reports_dir / "signal_quantile_future_returns.csv",
        "rolling_ic": reports_dir / "rolling_information_coefficient.csv",
    }
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    quantiles.to_csv(paths["quantiles"], index=False, encoding="utf-8-sig")
    rolling.to_csv(paths["rolling_ic"], index=False, encoding="utf-8-sig")
    _update_global_signal_summary(summary)
    return {"summary": summary, "quantiles": quantiles, "rolling_ic": rolling, **paths}


def _merge_market_signals(market: pd.DataFrame, signals: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    data = market.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["close"] = pd.to_numeric(data.get("close"), errors="coerce")
    data = data.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date", keep="last")
    for horizon in horizons:
        data[f"future_return_{horizon}d"] = data["close"].shift(-horizon) / data["close"] - 1
    signal = signals.copy() if signals is not None else pd.DataFrame()
    if not signal.empty and "date" in signal.columns:
        signal["date"] = pd.to_datetime(signal["date"], errors="coerce")
        keep = ["date"] + [column for column in SIGNAL_COLUMNS if column in signal.columns]
        signal = signal[keep].dropna(subset=["date"]).drop_duplicates("date", keep="last")
        data = data.merge(signal, on="date", how="left")
    for column in SIGNAL_COLUMNS:
        if column not in data.columns:
            data[column] = np.nan
    return data.reset_index(drop=True)


def _corr(x: pd.Series, y: pd.Series, method: str) -> float:
    if len(x) < 3 or x.nunique(dropna=True) <= 1 or y.nunique(dropna=True) <= 1:
        return float("nan")
    return float(x.corr(y, method=method))


def _hit_rate(signal: pd.Series, future_return: pd.Series) -> float:
    if signal.empty or signal.nunique(dropna=True) <= 1:
        return float("nan")
    high = signal >= signal.quantile(0.75)
    low = signal <= signal.quantile(0.25)
    hits = ((high & (future_return > 0)) | (low & (future_return < 0))).astype(int)
    mask = high | low
    return float(hits[mask].mean()) if mask.any() else float("nan")


def _quantile_returns(symbol: str, signal: str, horizon: int, valid: pd.DataFrame) -> list[dict[str, object]]:
    if len(valid) < 10 or valid["signal"].nunique(dropna=True) < 4:
        return []
    try:
        bins = pd.qcut(valid["signal"], q=4, labels=["Q1_low", "Q2", "Q3", "Q4_high"], duplicates="drop")
    except ValueError:
        return []
    frame = valid.assign(signal_quantile=bins)
    rows = []
    for quantile, group in frame.groupby("signal_quantile", observed=True):
        rows.append(
            {
                "symbol": symbol,
                "signal": signal,
                "horizon_days": horizon,
                "signal_quantile": str(quantile),
                "average_future_return": float(group["future_return"].mean()),
                "rows": int(len(group)),
            }
        )
    return rows


def _rolling_ic(symbol: str, signal: str, horizon: int, frame: pd.DataFrame, rolling_window: int) -> list[dict[str, object]]:
    data = frame.dropna(subset=["signal", "future_return"]).copy()
    if len(data) < rolling_window:
        return []
    rows = []
    for index in range(rolling_window - 1, len(data)):
        window = data.iloc[index - rolling_window + 1 : index + 1]
        rows.append(
            {
                "symbol": symbol,
                "signal": signal,
                "horizon_days": horizon,
                "date": str(pd.to_datetime(data["date"].iloc[index]).date()),
                "rolling_ic": _corr(window["signal"], window["future_return"], method="spearman"),
                "window_rows": int(len(window)),
            }
        )
    return rows


def _update_global_signal_summary(summary: pd.DataFrame) -> None:
    path = PROJECT_ROOT / "reports" / "tables" / "signal_validity_summary.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    if summary.empty:
        if not path.exists():
            summary.to_csv(path, index=False, encoding="utf-8-sig")
        return
    existing = pd.DataFrame()
    if path.exists() and path.stat().st_size > 4:
        try:
            existing = pd.read_csv(path)
        except Exception:
            existing = pd.DataFrame()
    if not existing.empty and "symbol" in existing.columns:
        symbol = str(summary["symbol"].iloc[0])
        existing = existing[existing["symbol"].astype(str) != symbol]
    pd.concat([existing, summary], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")

