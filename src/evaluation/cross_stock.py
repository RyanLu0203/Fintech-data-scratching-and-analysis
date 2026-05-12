"""Cross-stock comparison utilities built on the existing per-stock outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.paths import SYSTEM_OUTPUT_DIR, STOCK_OUTPUT_ROOT, normalize_symbol_for_path
from src.evaluation.metrics import max_drawdown, sharpe_ratio


def build_cross_stock_summary(
    stock_root: Path | None = None,
    output_dir: Path | None = None,
    selected_symbols: list[str] | None = None,
) -> dict[str, Path | pd.DataFrame]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    output_dir = output_dir or SYSTEM_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    allowed = {normalize_symbol_for_path(symbol) for symbol in (selected_symbols or []) if str(symbol).strip()}

    rows: list[dict[str, object]] = []
    diagnostics_rows: list[dict[str, object]] = []
    for stock_dir in sorted(path for path in stock_root.iterdir() if path.is_dir()):
        if allowed and stock_dir.name not in allowed:
            continue
        summary = summarize_stock_folder(stock_dir)
        if summary:
            rows.append(summary)
            diagnostics_rows.append(_diagnostics_from_summary(summary))

    summary_df = pd.DataFrame(rows)
    summary_df, diagnostics_df = _apply_common_window(summary_df, stock_root, diagnostics_rows)
    summary_path = output_dir / "cross_stock_summary.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    diagnostics_path = output_dir / "cross_stock_diagnostics.csv"
    diagnostics_df.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")

    discussion_path = output_dir / "cross_stock_discussion.md"
    discussion_path.write_text(_cross_stock_discussion(summary_df), encoding="utf-8")
    return {
        "summary": summary_df,
        "diagnostics": diagnostics_df,
        "summary_csv": summary_path,
        "diagnostics_csv": diagnostics_path,
        "discussion_markdown": discussion_path,
        "discussion_md": discussion_path,
    }


def summarize_stock_folder(stock_dir: Path) -> dict[str, object]:
    symbol = stock_dir.name
    metrics_path = stock_dir / "results" / "ablation_metrics.csv"
    sentiment_path = _latest_file(stock_dir / "reports", "*_daily_sentiment.csv")
    signal_diag_path = _latest_file(stock_dir / "reports", "*_signal_diagnostics.csv")
    data_path = _latest_file(stock_dir / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv")
    if not metrics_path.exists() or sentiment_path is None or signal_diag_path is None or data_path is None:
        return {}

    metrics = pd.read_csv(metrics_path)
    sentiment = pd.read_csv(sentiment_path)
    diagnostics = pd.read_csv(signal_diag_path)
    market = pd.read_csv(data_path)
    market["close"] = pd.to_numeric(market["close"], errors="coerce")
    market["date"] = pd.to_datetime(market["date"], errors="coerce")

    buy_hold = _row_for_experiment(metrics, "buy_and_hold")
    without_nlp = _row_for_experiment(metrics, "dqn_without_nlp")
    with_nlp = _row_for_experiment(metrics, "dqn_with_nlp")
    sentiment_coverage = _metric_value(diagnostics, "sentiment_coverage")
    sentiment_next_day_corr = _metric_value(diagnostics, "sentiment_next_day_return_corr")
    net_flow_next_day_corr = _metric_value(diagnostics, "net_flow_next_day_return_corr")

    return {
        "symbol": symbol,
        "local_data_start": str(market["date"].min().date()) if market["date"].notna().any() else "",
        "local_data_end": str(market["date"].max().date()) if market["date"].notna().any() else "",
        "data_path": str(data_path),
        "portfolio_curves_path": str(stock_dir / "results" / "portfolio_curves.csv"),
        "market_regime": classify_market_regime(market["close"]),
        "buy_and_hold_final_equity": buy_hold.get("final_equity"),
        "dqn_without_nlp_final_equity": without_nlp.get("final_equity"),
        "dqn_with_nlp_final_equity": with_nlp.get("final_equity"),
        "buy_and_hold_cumulative_return": buy_hold.get("cumulative_return"),
        "dqn_without_nlp_cumulative_return": without_nlp.get("cumulative_return"),
        "dqn_with_nlp_cumulative_return": with_nlp.get("cumulative_return"),
        "buy_and_hold_sharpe": buy_hold.get("sharpe_ratio"),
        "dqn_without_nlp_sharpe": without_nlp.get("sharpe_ratio"),
        "dqn_with_nlp_sharpe": with_nlp.get("sharpe_ratio"),
        "buy_and_hold_max_drawdown": buy_hold.get("max_drawdown"),
        "dqn_without_nlp_max_drawdown": without_nlp.get("max_drawdown"),
        "dqn_with_nlp_max_drawdown": with_nlp.get("max_drawdown"),
        "final_equity": with_nlp.get("final_equity"),
        "cumulative_return": with_nlp.get("cumulative_return"),
        "annualized_return": with_nlp.get("annualized_return"),
        "annualized_volatility": with_nlp.get("annualized_volatility"),
        "sharpe_ratio": with_nlp.get("sharpe_ratio"),
        "sortino_ratio": with_nlp.get("sortino_ratio"),
        "calmar_ratio": with_nlp.get("calmar_ratio"),
        "max_drawdown": with_nlp.get("max_drawdown"),
        "number_of_trades": with_nlp.get("number_of_trades"),
        "exposure_ratio": with_nlp.get("exposure_ratio"),
        "sentiment_coverage": sentiment_coverage,
        "sentiment_coverage_ratio": sentiment_coverage,
        "sentiment_next_day_return_corr": sentiment_next_day_corr,
        "net_flow_next_day_return_corr": net_flow_next_day_corr,
        "nlp_final_equity_effect": _delta(with_nlp.get("final_equity"), without_nlp.get("final_equity")),
        "nlp_sharpe_effect": _delta(with_nlp.get("sharpe_ratio"), without_nlp.get("sharpe_ratio")),
        "nlp_return_effect": _delta(with_nlp.get("cumulative_return"), without_nlp.get("cumulative_return")),
        "conclusion_label": classify_nlp_effect(with_nlp, without_nlp),
        "sentiment_method": ", ".join(sorted(set(sentiment.get("sentiment_method", pd.Series(dtype=str)).dropna().astype(str)))),
    }


def _apply_common_window(
    summary_df: pd.DataFrame,
    stock_root: Path,
    diagnostics_rows: list[dict[str, object]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if summary_df.empty or not {"local_data_start", "local_data_end"}.issubset(summary_df.columns):
        diagnostics = pd.DataFrame(diagnostics_rows)
        if not summary_df.empty:
            summary_df["comparability_status"] = "NOT_RELIABLE"
        return summary_df, diagnostics

    starts = pd.to_datetime(summary_df["local_data_start"], errors="coerce")
    ends = pd.to_datetime(summary_df["local_data_end"], errors="coerce")
    common_start = starts.max()
    common_end = ends.min()
    if pd.isna(common_start) or pd.isna(common_end) or common_start > common_end:
        summary_df["comparability_status"] = "NOT_RELIABLE"
        summary_df["metrics_recomputed_common_window"] = False
        summary_df["common_start_date"] = ""
        summary_df["common_end_date"] = ""
        diagnostics = pd.DataFrame(diagnostics_rows)
        return summary_df, diagnostics

    recomputed_rows = []
    for row in summary_df.to_dict(orient="records"):
        symbol = str(row["symbol"])
        curves_path = stock_root / symbol / "results" / "portfolio_curves.csv"
        curves = _safe_read_csv(curves_path)
        metrics = _common_window_metrics(curves, common_start, common_end)
        if metrics:
            row.update(metrics)
            row["metrics_recomputed_common_window"] = True
        else:
            row["metrics_recomputed_common_window"] = False
        recomputed_rows.append(row)

    updated = pd.DataFrame(recomputed_rows)
    overlap_len = _common_overlap_trading_days(updated, stock_root, common_start, common_end)
    all_recomputed = bool(updated["metrics_recomputed_common_window"].fillna(False).all())
    if overlap_len < 60:
        status = "NOT_RELIABLE"
    elif all_recomputed:
        status = "DIRECTLY_COMPARABLE"
    else:
        status = "COMPARABLE_WITH_WARNINGS"
    updated["comparability_status"] = status
    updated["common_start_date"] = common_start.strftime("%Y-%m-%d")
    updated["common_end_date"] = common_end.strftime("%Y-%m-%d")
    updated["common_overlap_trading_days"] = overlap_len

    diagnostics = pd.DataFrame(diagnostics_rows)
    if not diagnostics.empty:
        diagnostics["common_start_date"] = common_start.strftime("%Y-%m-%d")
        diagnostics["common_end_date"] = common_end.strftime("%Y-%m-%d")
        diagnostics["common_overlap_trading_days"] = overlap_len
        diagnostics["comparability_status"] = status
        diagnostics["metrics_recomputed"] = diagnostics["symbol"].map(
            dict(zip(updated["symbol"], updated["metrics_recomputed_common_window"]))
        )
    return updated, diagnostics


def _common_window_metrics(curves: pd.DataFrame, common_start: pd.Timestamp, common_end: pd.Timestamp) -> dict[str, object]:
    if curves.empty or not {"date", "portfolio_value", "experiment"}.issubset(curves.columns):
        return {}
    data = curves.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["portfolio_value"] = pd.to_numeric(data["portfolio_value"], errors="coerce")
    data = data[(data["date"] >= common_start) & (data["date"] <= common_end)].dropna(subset=["date", "portfolio_value"])
    if data.empty:
        return {}
    metrics: dict[str, object] = {}
    for experiment, group in data.groupby("experiment"):
        ordered = group.sort_values("date")
        by_date = ordered.groupby("date", as_index=False)["portfolio_value"].mean()
        values = by_date["portfolio_value"]
        if values.empty:
            continue
        prefix = f"{experiment}_common"
        metrics[f"{prefix}_final_equity"] = float(values.iloc[-1])
        metrics[f"{prefix}_cumulative_return"] = float(values.iloc[-1] / values.iloc[0] - 1) if values.iloc[0] else pd.NA
        metrics[f"{prefix}_sharpe"] = sharpe_ratio(values)
        metrics[f"{prefix}_max_drawdown"] = max_drawdown(values)
    return metrics


def _common_overlap_trading_days(summary_df: pd.DataFrame, stock_root: Path, common_start: pd.Timestamp, common_end: pd.Timestamp) -> int:
    counts = []
    for symbol in summary_df.get("symbol", pd.Series(dtype=str)).astype(str):
        data_path = _latest_file(stock_root / symbol / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv")
        data = _safe_read_csv(data_path)
        if data.empty or "date" not in data.columns:
            continue
        dates = pd.to_datetime(data["date"], errors="coerce")
        counts.append(int(((dates >= common_start) & (dates <= common_end)).sum()))
    return min(counts) if counts else 0


def _diagnostics_from_summary(summary: dict[str, object]) -> dict[str, object]:
    return {
        "symbol": summary.get("symbol", ""),
        "missing_files": "",
        "local_data_start": summary.get("local_data_start", ""),
        "local_data_end": summary.get("local_data_end", ""),
        "data_path": summary.get("data_path", ""),
        "portfolio_curves_path": summary.get("portfolio_curves_path", ""),
        "sentiment_coverage": summary.get("sentiment_coverage", pd.NA),
    }


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def classify_market_regime(close: pd.Series) -> str:
    values = pd.to_numeric(close, errors="coerce").dropna()
    if len(values) < 2:
        return "unknown"
    cumulative_return = values.iloc[-1] / values.iloc[0] - 1
    volatility = values.pct_change().dropna().std() * (252 ** 0.5)
    if cumulative_return > 0.15 and volatility < 0.35:
        return "bullish"
    if cumulative_return < -0.15 and volatility < 0.35:
        return "bearish"
    if volatility >= 0.35:
        return "volatile"
    return "sideways"


def classify_nlp_effect(with_nlp: dict[str, object], without_nlp: dict[str, object]) -> str:
    equity_effect = _delta(with_nlp.get("final_equity"), without_nlp.get("final_equity"))
    sharpe_effect = _delta(with_nlp.get("sharpe_ratio"), without_nlp.get("sharpe_ratio"))
    if pd.notna(equity_effect) and pd.notna(sharpe_effect) and equity_effect > 0 and sharpe_effect > 0:
        return "NLP improves"
    if pd.notna(equity_effect) and pd.notna(sharpe_effect) and equity_effect < 0 and sharpe_effect < 0:
        return "NLP hurts"
    if pd.notna(equity_effect) or pd.notna(sharpe_effect):
        return "mixed effect"
    return "inconclusive"


def _latest_file(directory: Path, pattern: str, exclude_suffix: str | None = None) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if exclude_suffix is None or not path.name.endswith(exclude_suffix)]
    if not files:
        return None
    return sorted(files, key=lambda path: path.stat().st_mtime)[-1]


def _row_for_experiment(metrics: pd.DataFrame, experiment: str) -> dict[str, object]:
    subset = metrics.loc[metrics["experiment"] == experiment]
    return subset.iloc[0].to_dict() if not subset.empty else {}


def _metric_value(frame: pd.DataFrame, metric: str) -> float:
    subset = frame.loc[frame["metric"] == metric, "value"]
    if subset.empty:
        return float("nan")
    return pd.to_numeric(subset.iloc[0], errors="coerce")


def _delta(left: object, right: object) -> float:
    left_num = pd.to_numeric(pd.Series([left]), errors="coerce").iloc[0]
    right_num = pd.to_numeric(pd.Series([right]), errors="coerce").iloc[0]
    if pd.isna(left_num) or pd.isna(right_num):
        return float("nan")
    return float(left_num - right_num)


def _cross_stock_discussion(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return "# Cross-Stock Discussion\n\nNo cross-stock summary could be generated."
    improves = int((summary_df["conclusion_label"] == "NLP improves").sum())
    hurts = int((summary_df["conclusion_label"] == "NLP hurts").sum())
    mixed = int((summary_df["conclusion_label"] == "mixed effect").sum())
    status = (
        ", ".join(sorted(set(summary_df.get("comparability_status", pd.Series(["NOT_RELIABLE"])).dropna().astype(str))))
        or "NOT_RELIABLE"
    )
    common_start = str(summary_df.get("common_start_date", pd.Series([""])).dropna().iloc[0]) if "common_start_date" in summary_df else ""
    common_end = str(summary_df.get("common_end_date", pd.Series([""])).dropna().iloc[0]) if "common_end_date" in summary_df else ""
    overlap = str(summary_df.get("common_overlap_trading_days", pd.Series([""])).dropna().iloc[0]) if "common_overlap_trading_days" in summary_df else ""
    return "\n".join(
        [
            "# Cross-Stock Discussion",
            "",
            f"- Comparability status: `{status}`",
            f"- Common overlap window: `{common_start}` to `{common_end}`",
            f"- Common overlap trading days: `{overlap}`",
            "",
            "NLP sentiment does not universally improve DQN performance. It may help some stocks and hurt others. Its effectiveness depends on market regime, sentiment coverage, signal quality, and correct time alignment.",
            "",
            f"- Stocks where NLP improves: `{improves}`",
            f"- Stocks where NLP hurts: `{hurts}`",
            f"- Stocks with mixed effect: `{mixed}`",
            "",
            "The cautious conclusion is that NLP signals should be treated as stock-specific and regime-dependent rather than automatically beneficial.",
        ]
    )
