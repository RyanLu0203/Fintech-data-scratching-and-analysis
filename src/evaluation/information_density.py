"""Coverage-controlled information-density analysis from cached outputs."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR
from src.evaluation.cross_stock import classify_market_regime, classify_nlp_effect
from src.evaluation.metrics import calmar_ratio, max_drawdown, sharpe_ratio, sortino_ratio

REPORTS_DIR = PROJECT_ROOT / "reports"
TABLES_DIR = REPORTS_DIR / "tables"


def detect_information_density_split(
    symbol: str,
    market_data: pd.DataFrame,
    news_data: pd.DataFrame | None = None,
    *,
    recent_share: float = 0.8,
    min_high_density_days: int = 30,
    min_high_density_coverage: float = 0.5,
) -> dict[str, object]:
    """Detect where the most recent ``recent_share`` of news begins.

    The split is based on cumulative news count, not calendar duration. Daily
    rows with ``news_count == 0`` remain explicit missing-news observations.
    """

    daily = build_daily_news_density(market_data, news_data)
    if daily.empty:
        return _empty_split(symbol, "NOT_SUITABLE_FOR_NLP_EVALUATION")

    total_news = int(pd.to_numeric(daily["daily_news_count"], errors="coerce").fillna(0).sum())
    total_trading_days = int(len(daily))
    if total_news <= 0 or total_trading_days <= 0:
        split = _empty_split(symbol, "NOT_SUITABLE_FOR_NLP_EVALUATION")
        split.update({"total_trading_days": total_trading_days, "total_news_count": total_news})
        return split

    descending = daily.sort_values("date", ascending=False).copy()
    descending["recent_cumulative_news_count"] = descending["daily_news_count"].cumsum()
    descending["recent_cumulative_news_share"] = descending["recent_cumulative_news_count"] / total_news
    cutoff_row = descending.loc[descending["recent_cumulative_news_share"] >= recent_share].head(1)
    if cutoff_row.empty:
        cutoff_date = daily["date"].max()
    else:
        cutoff_date = pd.to_datetime(cutoff_row["date"].iloc[0])

    daily["is_high_density_window"] = daily["date"] >= cutoff_date
    daily = _add_cumulative_columns(daily, total_news)
    high = daily[daily["is_high_density_window"]].copy()
    low = daily[~daily["is_high_density_window"]].copy()

    high_days = int(len(high))
    low_days = int(len(low))
    high_news = int(high["daily_news_count"].sum())
    low_news = int(low["daily_news_count"].sum())
    high_coverage = float(high["news_available"].mean()) if high_days else 0.0

    status = "OK"
    if high_days < min_high_density_days:
        status = "SHORT_HIGH_DENSITY_WINDOW"
    elif high_coverage < min_high_density_coverage:
        status = "LOW_COVERAGE"
    if high_days <= 0 or high_news <= 0:
        status = "NOT_SUITABLE_FOR_NLP_EVALUATION"

    return {
        "symbol": str(symbol),
        "total_news_count": total_news,
        "total_trading_days": total_trading_days,
        "density_cutoff_date": _date_text(cutoff_date),
        "low_density_start_date": _date_text(low["date"].min()) if not low.empty else "",
        "low_density_end_date": _date_text(low["date"].max()) if not low.empty else "",
        "high_density_start_date": _date_text(high["date"].min()) if not high.empty else "",
        "high_density_end_date": _date_text(high["date"].max()) if not high.empty else "",
        "low_density_news_count": low_news,
        "high_density_news_count": high_news,
        "low_density_trading_days": low_days,
        "high_density_trading_days": high_days,
        "low_density_avg_news_per_day": float(low_news / low_days) if low_days else 0.0,
        "high_density_avg_news_per_day": float(high_news / high_days) if high_days else 0.0,
        "high_density_coverage_ratio": high_coverage,
        "density_status": status,
    }


def build_daily_news_density(market_data: pd.DataFrame, news_data: pd.DataFrame | None = None) -> pd.DataFrame:
    market = market_data.copy()
    if market.empty or "date" not in market.columns:
        return pd.DataFrame()
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    market = market.dropna(subset=["date"]).sort_values("date")
    daily = market[["date"]].drop_duplicates().copy()

    if news_data is not None and not news_data.empty and "date" in news_data.columns:
        news = news_data.copy()
        news["date"] = pd.to_datetime(news["date"], errors="coerce")
        count_col = "news_count" if "news_count" in news.columns else "event_count" if "event_count" in news.columns else ""
        if count_col:
            news[count_col] = pd.to_numeric(news[count_col], errors="coerce").fillna(0)
            counts = news.groupby("date", as_index=False)[count_col].sum().rename(columns={count_col: "daily_news_count"})
            daily = daily.merge(counts, on="date", how="left")

    if "daily_news_count" not in daily.columns:
        count_col = "external_event_count" if "external_event_count" in market.columns else "news_count" if "news_count" in market.columns else "event_count" if "event_count" in market.columns else ""
        daily["daily_news_count"] = pd.to_numeric(market.get(count_col, 0), errors="coerce").fillna(0).values if count_col else 0

    daily["daily_news_count"] = pd.to_numeric(daily["daily_news_count"], errors="coerce").fillna(0).clip(lower=0)
    daily["news_available"] = (daily["daily_news_count"] > 0).astype(int)
    total_news = float(daily["daily_news_count"].sum())
    return _add_cumulative_columns(daily, total_news)


def define_experiment_window(symbol: str, company_name: str, split: dict[str, object]) -> dict[str, object]:
    status = str(split.get("density_status", "NOT_SUITABLE_FOR_NLP_EVALUATION"))
    high_days = int(pd.to_numeric(pd.Series([split.get("high_density_trading_days", 0)]), errors="coerce").fillna(0).iloc[0])
    high_coverage = float(pd.to_numeric(pd.Series([split.get("high_density_coverage_ratio", 0)]), errors="coerce").fillna(0).iloc[0])
    if status == "OK":
        usage = "MAIN_EXPERIMENT"
        window_status = "READY"
    elif status in {"SHORT_HIGH_DENSITY_WINDOW", "LOW_COVERAGE"}:
        usage = "CASE_STUDY_ONLY"
        window_status = status
    else:
        usage = "NOT_RELIABLE"
        window_status = status

    return {
        "symbol": symbol,
        "company_name": company_name,
        "market_learning_start": split.get("low_density_start_date", ""),
        "market_learning_end": split.get("low_density_end_date", ""),
        "high_density_eval_start": split.get("high_density_start_date", ""),
        "high_density_eval_end": split.get("high_density_end_date", ""),
        "high_density_days": high_days,
        "high_density_news_count": split.get("high_density_news_count", 0),
        "high_density_coverage_ratio": high_coverage,
        "window_status": window_status,
        "recommended_usage": usage,
    }


def generate_information_density_outputs(
    stock_root: Path | None = None,
    output_dir: Path | None = None,
    selected_symbols: list[str] | None = None,
) -> dict[str, Path | pd.DataFrame]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    output_dir = output_dir or SYSTEM_OUTPUT_DIR
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    allowed = {str(symbol).strip() for symbol in (selected_symbols or []) if str(symbol).strip()}

    split_rows: list[dict[str, object]] = []
    window_rows: list[dict[str, object]] = []
    diagnostics_rows: list[dict[str, object]] = []
    for stock_dir in _stock_dirs(stock_root):
        symbol = stock_dir.name
        if allowed and symbol not in allowed:
            continue
        market_path = _latest_file(stock_dir / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv") or _latest_file(stock_dir / "data", "*_finance_text_master.csv")
        sentiment_path = _latest_file(stock_dir / "reports", "*_daily_sentiment.csv")
        market = _safe_read_csv(market_path)
        sentiment = _safe_read_csv(sentiment_path)
        company = _company_name(symbol, market)
        if market.empty:
            diagnostics_rows.append({"symbol": symbol, "status": "missing_market_data", "missing_files": "market"})
            continue

        split = detect_information_density_split(symbol, market, sentiment)
        split["company_name"] = company
        daily = build_daily_news_density(market, sentiment)
        if not daily.empty and split.get("density_cutoff_date"):
            cutoff = pd.to_datetime(split["density_cutoff_date"], errors="coerce")
            daily["is_high_density_window"] = daily["date"] >= cutoff
        window = define_experiment_window(symbol, company, split)
        report_dir = stock_dir / "reports"
        result_dir = stock_dir / "results"
        report_dir.mkdir(parents=True, exist_ok=True)
        result_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame([split]).to_csv(report_dir / "information_density_split.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame([split]).to_csv(report_dir / "density_split_summary.csv", index=False, encoding="utf-8-sig")
        daily.to_csv(report_dir / "daily_news_density.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame([window]).to_csv(report_dir / "experiment_window_summary.csv", index=False, encoding="utf-8-sig")
        (report_dir / "information_density_report.md").write_text(_density_report(split, window), encoding="utf-8")

        _plot_stock_density(report_dir, symbol, daily, split)
        _write_high_density_cached_results(stock_dir, split, window)

        split_rows.append(split)
        window_rows.append(window)
        diagnostics_rows.append({"symbol": symbol, "status": split.get("density_status", ""), "missing_files": "" if sentiment_path else "daily_sentiment"})

    split_df = pd.DataFrame(split_rows)
    window_df = pd.DataFrame(window_rows)
    diagnostics_df = pd.DataFrame(diagnostics_rows)
    split_path = TABLES_DIR / "information_density_split.csv"
    daily_summary_path = TABLES_DIR / "experiment_window_summary.csv"
    diagnostics_path = TABLES_DIR / "information_density_diagnostics.csv"
    split_df.to_csv(split_path, index=False, encoding="utf-8-sig")
    window_df.to_csv(daily_summary_path, index=False, encoding="utf-8-sig")
    diagnostics_df.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")

    cross = build_cross_stock_high_density_summary(stock_root=stock_root, output_dir=output_dir, split_df=split_df)
    summary_md = REPORTS_DIR / "result_overview_key_findings.md"
    summary_md.write_text(_key_findings(split_df, window_df, cross.get("summary", pd.DataFrame())), encoding="utf-8")
    overview_md = REPORTS_DIR / "result_overview_summary.md"
    overview_md.write_text(_result_overview_summary(split_df, window_df, cross.get("summary", pd.DataFrame())), encoding="utf-8")
    missing_csv = REPORTS_DIR / "result_overview_missing_outputs.csv"
    _missing_outputs(stock_root, split_df).to_csv(missing_csv, index=False, encoding="utf-8-sig")
    return {
        "information_density_split": split_path,
        "experiment_window_summary": daily_summary_path,
        "information_density_diagnostics": diagnostics_path,
        "cross_stock_high_density_summary": cross["summary_csv"],
        "cross_stock_high_density_diagnostics": cross["diagnostics_csv"],
        "cross_stock_high_density_discussion": cross["discussion_md"],
        "result_overview_summary": overview_md,
        "result_overview_missing_outputs": missing_csv,
        "result_overview_key_findings": summary_md,
        "split": split_df,
        "windows": window_df,
    }


def build_cross_stock_high_density_summary(
    stock_root: Path | None = None,
    output_dir: Path | None = None,
    split_df: pd.DataFrame | None = None,
) -> dict[str, Path | pd.DataFrame]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    output_dir = output_dir or SYSTEM_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    if split_df is None or split_df.empty:
        frames = [_safe_read_csv(stock_dir / "reports" / "information_density_split.csv") for stock_dir in _stock_dirs(stock_root)]
        split_df = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()

    diagnostics_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    if split_df.empty:
        summary = pd.DataFrame()
        diagnostics = pd.DataFrame([{"status": "NOT_RELIABLE", "reason": "No information-density split files found."}])
    else:
        eligible = split_df[split_df["density_status"].isin(["OK", "LOW_COVERAGE", "SHORT_HIGH_DENSITY_WINDOW"])].copy()
        eligible["high_density_start"] = pd.to_datetime(eligible["high_density_start_date"], errors="coerce")
        eligible["high_density_end"] = pd.to_datetime(eligible["high_density_end_date"], errors="coerce")
        eligible = eligible.dropna(subset=["high_density_start", "high_density_end"])
        if eligible.empty:
            summary = pd.DataFrame()
            diagnostics = pd.DataFrame([{"status": "NOT_RELIABLE", "reason": "No usable high-density windows."}])
        else:
            common_start = eligible["high_density_start"].max()
            common_end = eligible["high_density_end"].min()
            common_days = _min_market_days(stock_root, eligible["symbol"].astype(str).tolist(), common_start, common_end)
            comparability = "DIRECTLY_COMPARABLE" if common_days >= 30 and len(eligible) >= 2 else "NOT_RELIABLE"
            for _, split in eligible.iterrows():
                symbol = str(split["symbol"])
                stock_dir = stock_root / symbol
                high_metrics = _metrics_from_high_density_outputs(stock_dir, common_start, common_end)
                market = _safe_read_csv(_latest_file(stock_dir / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv"))
                row = {
                    "symbol": symbol,
                    "company_name": split.get("company_name", ""),
                    "high_density_start": _date_text(split.get("high_density_start")),
                    "high_density_end": _date_text(split.get("high_density_end")),
                    "common_start": _date_text(common_start),
                    "common_end": _date_text(common_end),
                    "common_window_trading_days": common_days,
                    "high_density_news_count": split.get("high_density_news_count", np.nan),
                    "high_density_coverage_ratio": split.get("high_density_coverage_ratio", np.nan),
                    "market_regime": classify_market_regime(pd.to_numeric(market.get("close"), errors="coerce")) if not market.empty else "unknown",
                    "comparability_status": comparability,
                }
                row.update(high_metrics)
                without = {"final_equity": row.get("dqn_without_nlp_final_equity"), "sharpe_ratio": row.get("dqn_without_nlp_sharpe")}
                basic = {"final_equity": row.get("dqn_with_basic_nlp_final_equity"), "sharpe_ratio": row.get("dqn_with_basic_nlp_sharpe")}
                row["nlp_basic_final_equity_effect"] = _delta(row.get("dqn_with_basic_nlp_final_equity"), row.get("dqn_without_nlp_final_equity"))
                row["nlp_enhanced_final_equity_effect"] = _delta(row.get("dqn_with_enhanced_nlp_final_equity"), row.get("dqn_without_nlp_final_equity"))
                row["nlp_basic_sharpe_effect"] = _delta(row.get("dqn_with_basic_nlp_sharpe"), row.get("dqn_without_nlp_sharpe"))
                row["nlp_enhanced_sharpe_effect"] = _delta(row.get("dqn_with_enhanced_nlp_sharpe"), row.get("dqn_without_nlp_sharpe"))
                row["best_strategy"] = _best_strategy(row)
                row["nlp_effect_label"] = classify_nlp_effect(basic, without)
                summary_rows.append(row)
                diagnostics_rows.append(
                    {
                        "symbol": symbol,
                        "density_status": split.get("density_status", ""),
                        "common_start": _date_text(common_start),
                        "common_end": _date_text(common_end),
                        "common_window_trading_days": common_days,
                        "comparability_status": comparability,
                        "metrics_recomputed_from_cached_curves": bool(high_metrics),
                        "metrics_source": high_metrics.get("metrics_source", "") if high_metrics else "",
                    }
                )
            summary = pd.DataFrame(summary_rows)
            diagnostics = pd.DataFrame(diagnostics_rows)

    summary_path = output_dir / "cross_stock_high_density_summary.csv"
    diagnostics_path = output_dir / "cross_stock_high_density_diagnostics.csv"
    discussion_path = output_dir / "cross_stock_high_density_discussion.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    diagnostics.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")
    discussion_path.write_text(_cross_high_density_discussion(summary, diagnostics), encoding="utf-8")
    _plot_cross_density(output_dir, summary)
    return {"summary": summary, "diagnostics": diagnostics, "summary_csv": summary_path, "diagnostics_csv": diagnostics_path, "discussion_md": discussion_path}


def _write_high_density_cached_results(stock_dir: Path, split: dict[str, object], window: dict[str, object]) -> None:
    start = pd.to_datetime(window.get("high_density_eval_start"), errors="coerce")
    end = pd.to_datetime(window.get("high_density_eval_end"), errors="coerce")
    results_dir = stock_dir / "results"
    report_dir = stock_dir / "reports"
    trained_marker = results_dir / "high_density_training_rewards_all_seeds.csv"
    metrics_path = results_dir / "high_density_ablation_metrics.csv"
    curves_path = results_dir / "high_density_portfolio_curves.csv"
    logs_path = results_dir / "high_density_trading_logs.csv"
    if trained_marker.exists() and metrics_path.exists() and curves_path.exists():
        metrics = _safe_read_csv(metrics_path)
        (report_dir / "high_density_report_section.md").write_text(_high_density_report(stock_dir.name, split, window, metrics), encoding="utf-8")
        return

    curves = _safe_read_csv(results_dir / "portfolio_curves.csv")
    logs = _safe_read_csv(results_dir / "trading_logs.csv")
    if not curves.empty and "date" in curves.columns and pd.notna(start) and pd.notna(end):
        curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
        high_curves = curves[(curves["date"] >= start) & (curves["date"] <= end)].copy()
    else:
        high_curves = pd.DataFrame()
    high_curves.to_csv(curves_path, index=False, encoding="utf-8-sig")
    if not logs.empty and "date" in logs.columns and pd.notna(start) and pd.notna(end):
        logs["date"] = pd.to_datetime(logs["date"], errors="coerce")
        high_logs = logs[(logs["date"] >= start) & (logs["date"] <= end)].copy()
    else:
        high_logs = pd.DataFrame()
    high_logs.to_csv(logs_path, index=False, encoding="utf-8-sig")
    metrics = _metrics_from_curves_and_logs(high_curves, high_logs)
    metrics.to_csv(metrics_path, index=False, encoding="utf-8-sig")
    (report_dir / "high_density_report_section.md").write_text(_high_density_report(stock_dir.name, split, window, metrics), encoding="utf-8")


def _metrics_from_curves_and_logs(curves: pd.DataFrame, logs: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if not curves.empty and {"experiment", "portfolio_value", "date"}.issubset(curves.columns):
        for experiment, group in curves.groupby("experiment"):
            normalized = _normalize_experiment(str(experiment))
            by_date = group.sort_values("date").groupby("date", as_index=False)["portfolio_value"].mean()
            values = pd.to_numeric(by_date["portfolio_value"], errors="coerce").dropna()
            if values.empty:
                continue
            log_part = logs[logs.get("experiment", pd.Series(dtype=str)).astype(str) == experiment] if not logs.empty and "experiment" in logs.columns else pd.DataFrame()
            row = {
                "experiment": normalized,
                "source_experiment": experiment,
                "evaluation_window": "high_density",
                "final_equity": float(values.iloc[-1]),
                "cumulative_return": float(values.iloc[-1] / values.iloc[0] - 1) if values.iloc[0] else np.nan,
                "sharpe_ratio": sharpe_ratio(values),
                "sortino_ratio": sortino_ratio(values),
                "calmar_ratio": calmar_ratio(values),
                "max_drawdown": max_drawdown(values),
                "number_of_trades": _trade_count(log_part),
                "exposure_ratio": _exposure_ratio(log_part),
                "win_rate": _win_rate(log_part),
                "reward_mode": "portfolio_return",
                "training_status": "loaded_cached_policy_or_cached_curves",
            }
            rows.append(row)
    existing = {row["experiment"] for row in rows}
    for experiment in ["buy_and_hold", "dqn_without_nlp", "dqn_with_basic_nlp", "dqn_with_enhanced_nlp"]:
        if experiment not in existing:
            rows.append({"experiment": experiment, "evaluation_window": "high_density", "training_status": "not_available_or_not_run_by_default"})
    return pd.DataFrame(rows)


def _metrics_from_cached_curves(curves_path: Path, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, object]:
    curves = _safe_read_csv(curves_path)
    if curves.empty or not {"date", "experiment", "portfolio_value"}.issubset(curves.columns):
        return {}
    curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
    curves = curves[(curves["date"] >= start) & (curves["date"] <= end)].copy()
    metrics = _metrics_from_curves_and_logs(curves, pd.DataFrame())
    row: dict[str, object] = {}
    for _, item in metrics.iterrows():
        exp = str(item["experiment"])
        for metric, suffix in [
            ("final_equity", "final_equity"),
            ("cumulative_return", "cumulative_return"),
            ("sharpe_ratio", "sharpe"),
            ("max_drawdown", "max_drawdown"),
        ]:
            row[f"{exp}_{suffix}"] = item.get(metric)
    return row


def _metrics_from_high_density_outputs(stock_dir: Path, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, object]:
    """Prefer true high-density DQN outputs, then fall back to sliced full-period curves."""

    results_dir = stock_dir / "results"
    curves = _safe_read_csv(results_dir / "high_density_portfolio_curves.csv")
    logs = _safe_read_csv(results_dir / "high_density_trading_logs.csv")
    trained_marker = results_dir / "high_density_training_rewards_all_seeds.csv"
    if trained_marker.exists() and not curves.empty and {"date", "experiment", "portfolio_value"}.issubset(curves.columns):
        curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
        curves = curves[(curves["date"] >= start) & (curves["date"] <= end)].copy()
        if not logs.empty and "date" in logs.columns:
            logs["date"] = pd.to_datetime(logs["date"], errors="coerce")
            logs = logs[(logs["date"] >= start) & (logs["date"] <= end)].copy()
        metrics = _metrics_from_curves_and_logs(curves, logs)
        row = _flatten_metrics(metrics)
        if row:
            row["metrics_source"] = "high_density_trained_outputs"
            return row

    row = _metrics_from_cached_curves(results_dir / "portfolio_curves.csv", start, end)
    if row:
        row["metrics_source"] = "sliced_full_period_cached_curves"
    return row


def _flatten_metrics(metrics: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {}
    if metrics.empty:
        return row
    for _, item in metrics.iterrows():
        exp = str(item.get("experiment", ""))
        if not exp:
            continue
        for metric, suffix in [
            ("final_equity", "final_equity"),
            ("cumulative_return", "cumulative_return"),
            ("sharpe_ratio", "sharpe"),
            ("max_drawdown", "max_drawdown"),
        ]:
            row[f"{exp}_{suffix}"] = item.get(metric)
    return row


def _plot_stock_density(report_dir: Path, symbol: str, daily: pd.DataFrame, split: dict[str, object]) -> None:
    if daily.empty:
        return
    report_dir.mkdir(parents=True, exist_ok=True)
    cutoff = pd.to_datetime(split.get("density_cutoff_date"), errors="coerce")
    dates = pd.to_datetime(daily["date"], errors="coerce")
    _line_plot(dates, daily["daily_news_count"], report_dir / "information_density_daily_news_count.png", "Daily news count", cutoff)
    pct = pd.to_numeric(daily.get("cumulative_news_pct"), errors="coerce").fillna(0)
    _line_plot(dates, pct, report_dir / "information_density_cumulative_news_pct.png", "Cumulative news percentage", cutoff)
    _bar_plot(
        ["Low density", "High density"],
        [split.get("low_density_avg_news_per_day", 0), split.get("high_density_avg_news_per_day", 0)],
        report_dir / "information_density_low_high_bar.png",
        "Average news per trading day",
    )
    coverage = daily.assign(rolling_coverage=daily["news_available"].rolling(20, min_periods=1).mean())
    _line_plot(dates, coverage["rolling_coverage"], report_dir / "information_density_sentiment_coverage.png", "20-day sentiment coverage", cutoff)


def _plot_cross_density(output_dir: Path, summary: pd.DataFrame) -> None:
    fig_dir = output_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    if summary.empty:
        return
    if {"symbol", "high_density_start", "high_density_end"}.issubset(summary.columns):
        fig, ax = plt.subplots(figsize=(10, max(3, 0.35 * len(summary))))
        for idx, row in summary.reset_index(drop=True).iterrows():
            start = pd.to_datetime(row["high_density_start"], errors="coerce")
            end = pd.to_datetime(row["high_density_end"], errors="coerce")
            if pd.notna(start) and pd.notna(end):
                ax.hlines(idx, start, end, linewidth=5)
        if "common_start" in summary.columns and "common_end" in summary.columns:
            common_start = pd.to_datetime(summary["common_start"].dropna().astype(str).iloc[0], errors="coerce")
            common_end = pd.to_datetime(summary["common_end"].dropna().astype(str).iloc[0], errors="coerce")
            if pd.notna(common_start):
                ax.axvline(common_start, color="red", linestyle="--", linewidth=1)
            if pd.notna(common_end):
                ax.axvline(common_end, color="red", linestyle="--", linewidth=1)
        ax.set_yticks(range(len(summary)))
        ax.set_yticklabels(summary["symbol"].astype(str).tolist())
        ax.set_title("High-density windows and common overlap")
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(fig_dir / "cross_stock_high_density_timeline.png", dpi=180)
        plt.close(fig)
    if {"symbol", "high_density_news_count"}.issubset(summary.columns):
        _bar_plot(summary["symbol"].astype(str), summary["high_density_news_count"], fig_dir / "cross_stock_high_density_news_count.png", "High-density news count")
    if {"symbol", "high_density_coverage_ratio"}.issubset(summary.columns):
        _bar_plot(summary["symbol"].astype(str), summary["high_density_coverage_ratio"], fig_dir / "cross_stock_high_density_coverage.png", "High-density sentiment coverage")


def _line_plot(x: pd.Series, y: pd.Series, path: Path, title: str, cutoff: pd.Timestamp | None = None) -> None:
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(x, pd.to_numeric(y, errors="coerce").fillna(0), linewidth=1.7)
    if cutoff is not None and pd.notna(cutoff):
        ax.axvline(cutoff, color="red", linestyle="--", linewidth=1, label="80% cutoff")
        ax.legend()
    ax.set_title(title)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _bar_plot(x: object, y: object, path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(7, 3.6))
    ax.bar(list(x), pd.to_numeric(pd.Series(list(y)), errors="coerce").fillna(0))
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _add_cumulative_columns(daily: pd.DataFrame, total_news: float) -> pd.DataFrame:
    frame = daily.sort_values("date").copy()
    frame["daily_news_count"] = pd.to_numeric(frame["daily_news_count"], errors="coerce").fillna(0)
    frame["cumulative_news_count"] = frame["daily_news_count"].cumsum()
    frame["cumulative_news_pct"] = frame["cumulative_news_count"] / total_news if total_news else 0.0
    desc = frame.sort_values("date", ascending=False).copy()
    desc["recent_cumulative_news_count"] = desc["daily_news_count"].cumsum()
    desc["recent_cumulative_news_pct"] = desc["recent_cumulative_news_count"] / total_news if total_news else 0.0
    return frame.merge(desc[["date", "recent_cumulative_news_count", "recent_cumulative_news_pct"]], on="date", how="left")


def _density_report(split: dict[str, object], window: dict[str, object]) -> str:
    return "\n".join(
        [
            "# Information Density Report",
            "",
            f"- Symbol: `{split.get('symbol', '')}`",
            f"- Density status: `{split.get('density_status', '')}`",
            f"- 80% high-density cutoff date: `{split.get('density_cutoff_date', '')}`",
            f"- Low-density period: `{split.get('low_density_start_date', '')}` to `{split.get('low_density_end_date', '')}`",
            f"- High-density evaluation period: `{split.get('high_density_start_date', '')}` to `{split.get('high_density_end_date', '')}`",
            f"- Low-density average news/day: `{split.get('low_density_avg_news_per_day', 0):.3f}`",
            f"- High-density average news/day: `{split.get('high_density_avg_news_per_day', 0):.3f}`",
            f"- High-density coverage ratio: `{split.get('high_density_coverage_ratio', 0):.1%}`",
            f"- Recommended usage: `{window.get('recommended_usage', '')}`",
            "",
            "No-news days are treated as missing text information, not neutral sentiment. NLP state features are lagged before DQN decisions.",
        ]
    )


def _high_density_report(symbol: str, split: dict[str, object], window: dict[str, object], metrics: pd.DataFrame) -> str:
    label = "Inconclusive"
    if not metrics.empty and {"experiment", "final_equity", "sharpe_ratio"}.issubset(metrics.columns):
        rows = metrics.set_index("experiment")
        if {"dqn_without_nlp", "dqn_with_basic_nlp"}.issubset(rows.index):
            label = classify_nlp_effect(rows.loc["dqn_with_basic_nlp"].to_dict(), rows.loc["dqn_without_nlp"].to_dict())
    return "\n".join(
        [
            "# High-Density Experiment Section",
            "",
            f"- Symbol: `{symbol}`",
            f"- Market-learning period: `{window.get('market_learning_start', '')}` to `{window.get('market_learning_end', '')}`",
            f"- High-density evaluation period: `{window.get('high_density_eval_start', '')}` to `{window.get('high_density_eval_end', '')}`",
            f"- Density status: `{split.get('density_status', '')}`",
            f"- NLP effect label: `{label}`",
            "",
            "Metrics are recomputed from cached curves/logs inside the high-density window. The dashboard/notebook do not retrain by default.",
        ]
    )


def _cross_high_density_discussion(summary: pd.DataFrame, diagnostics: pd.DataFrame) -> str:
    if summary.empty:
        return "# Cross-Stock High-Density Discussion\n\nNo reliable high-density cross-stock comparison could be generated."
    status = ", ".join(sorted(set(summary.get("comparability_status", pd.Series(dtype=str)).dropna().astype(str)))) or "NOT_RELIABLE"
    common_start = summary.get("common_start", pd.Series([""])).dropna().astype(str).iloc[0] if "common_start" in summary else ""
    common_end = summary.get("common_end", pd.Series([""])).dropna().astype(str).iloc[0] if "common_end" in summary else ""
    improves = int((summary.get("nlp_effect_label", pd.Series(dtype=str)) == "NLP improves").sum())
    hurts = int((summary.get("nlp_effect_label", pd.Series(dtype=str)) == "NLP hurts").sum())
    return "\n".join(
        [
            "# Cross-Stock High-Density Discussion",
            "",
            f"- Comparability status: `{status}`",
            f"- Common high-density overlap: `{common_start}` to `{common_end}`",
            f"- Valid stocks: `{len(summary)}`",
            f"- NLP improves rows: `{improves}`",
            f"- NLP hurts rows: `{hurts}`",
            "",
            "Cross-stock robustness uses each stock's recent high-information-density window first, then compares only the common overlap. This avoids treating sparse historical no-news days as neutral sentiment.",
        ]
    )


def _key_findings(split: pd.DataFrame, windows: pd.DataFrame, cross: pd.DataFrame) -> str:
    if split.empty:
        return "# Result Overview Key Findings\n\nInformation-density outputs are missing."
    main = int((windows.get("recommended_usage", pd.Series(dtype=str)) == "MAIN_EXPERIMENT").sum()) if not windows.empty else 0
    case = int((windows.get("recommended_usage", pd.Series(dtype=str)) == "CASE_STUDY_ONLY").sum()) if not windows.empty else 0
    not_reliable = int((windows.get("recommended_usage", pd.Series(dtype=str)) == "NOT_RELIABLE").sum()) if not windows.empty else 0
    return "\n".join(
        [
            "# Result Overview Key Findings",
            "",
            f"- Stocks with MAIN_EXPERIMENT high-density windows: `{main}`",
            f"- Stocks limited to CASE_STUDY_ONLY: `{case}`",
            f"- Stocks marked NOT_RELIABLE: `{not_reliable}`",
            f"- Cross-stock high-density rows: `{len(cross) if isinstance(cross, pd.DataFrame) else 0}`",
            "",
            "Overall interpretation: NLP value should be judged mainly in high-density windows. Full-period outputs remain robustness checks, not the strongest NLP evidence.",
        ]
    )


def _result_overview_summary(split: pd.DataFrame, windows: pd.DataFrame, cross: pd.DataFrame) -> str:
    common = "missing"
    status = "missing"
    if isinstance(cross, pd.DataFrame) and not cross.empty:
        common = f"{cross.get('common_start', pd.Series([''])).astype(str).iloc[0]} to {cross.get('common_end', pd.Series([''])).astype(str).iloc[0]}"
        status = ", ".join(sorted(set(cross.get("comparability_status", pd.Series(dtype=str)).dropna().astype(str)))) or "unknown"
    return "\n".join(
        [
            "# Result Overview Summary",
            "",
            "## Coverage-Controlled Project Logic",
            "",
            "Long historical OHLCV data is used to learn market-based trading behavior. NLP value is evaluated mainly in each stock's recent high-information-density window, defined by the most recent 80% of news observations.",
            "",
            "No-news days are not treated as neutral sentiment. The RL state keeps explicit `news_available` and `sentiment_missing_flag` columns, and all NLP state features are lagged before trading decisions.",
            "",
            "## Current Density Summary",
            "",
            f"- Stocks analyzed: `{len(split) if isinstance(split, pd.DataFrame) else 0}`",
            f"- MAIN_EXPERIMENT stocks: `{int((windows.get('recommended_usage', pd.Series(dtype=str)) == 'MAIN_EXPERIMENT').sum()) if isinstance(windows, pd.DataFrame) and not windows.empty else 0}`",
            f"- Cross-stock high-density common window: `{common}`",
            f"- Cross-stock comparability: `{status}`",
            "",
            "## Interpretation",
            "",
            "NLP does not universally improve DQN performance. The high-density design makes this conclusion more honest by asking whether sentiment helps when textual information is actually available.",
        ]
    )


def _missing_outputs(stock_root: Path, split: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    expected_system = [
        SYSTEM_OUTPUT_DIR / "cross_stock_high_density_summary.csv",
        SYSTEM_OUTPUT_DIR / "cross_stock_high_density_diagnostics.csv",
        SYSTEM_OUTPUT_DIR / "cross_stock_high_density_discussion.md",
        TABLES_DIR / "information_density_split.csv",
        TABLES_DIR / "experiment_window_summary.csv",
    ]
    for path in expected_system:
        rows.append({"scope": "system", "symbol": "", "path": str(path.relative_to(PROJECT_ROOT)), "status": "Complete" if path.exists() and path.stat().st_size > 4 else "Missing"})
    symbols = split.get("symbol", pd.Series(dtype=str)).astype(str).tolist() if isinstance(split, pd.DataFrame) and not split.empty else []
    for symbol in symbols:
        for rel in [
            "reports/information_density_split.csv",
            "reports/daily_news_density.csv",
            "reports/experiment_window_summary.csv",
            "results/high_density_ablation_metrics.csv",
            "results/high_density_portfolio_curves.csv",
        ]:
            path = stock_root / symbol / rel
            rows.append({"scope": "stock", "symbol": symbol, "path": str(path.relative_to(PROJECT_ROOT)), "status": "Complete" if path.exists() and path.stat().st_size > 4 else "Missing"})
    return pd.DataFrame(rows)


def _empty_split(symbol: str, status: str) -> dict[str, object]:
    return {
        "symbol": str(symbol),
        "total_news_count": 0,
        "total_trading_days": 0,
        "density_cutoff_date": "",
        "low_density_start_date": "",
        "low_density_end_date": "",
        "high_density_start_date": "",
        "high_density_end_date": "",
        "low_density_news_count": 0,
        "high_density_news_count": 0,
        "low_density_trading_days": 0,
        "high_density_trading_days": 0,
        "low_density_avg_news_per_day": 0.0,
        "high_density_avg_news_per_day": 0.0,
        "high_density_coverage_ratio": 0.0,
        "density_status": status,
    }


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _latest_file(directory: Path, pattern: str, exclude_suffix: str | None = None) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if exclude_suffix is None or not path.name.endswith(exclude_suffix)]
    return sorted(files, key=lambda path: path.stat().st_mtime)[-1] if files else None


def _stock_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir() and path.name.isdigit())


def _company_name(symbol: str, market: pd.DataFrame) -> str:
    if not market.empty and "company_name" in market.columns:
        values = market["company_name"].dropna().astype(str).str.strip()
        values = values[values != ""]
        if not values.empty:
            return str(values.iloc[0])
    return symbol


def _date_text(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(timestamp) else str(timestamp.date())


def _normalize_experiment(experiment: str) -> str:
    return "dqn_with_basic_nlp" if experiment == "dqn_with_nlp" else experiment


def _trade_count(logs: pd.DataFrame) -> float:
    if logs.empty or "action" not in logs.columns:
        return np.nan
    actions = logs["action"].astype(str)
    return float(actions.isin(["Buy", "Sell"]).sum())


def _exposure_ratio(logs: pd.DataFrame) -> float:
    if logs.empty or "position" not in logs.columns:
        return np.nan
    return float((pd.to_numeric(logs["position"], errors="coerce").fillna(0) > 0).mean())


def _win_rate(logs: pd.DataFrame) -> float:
    if logs.empty or "reward" not in logs.columns:
        return np.nan
    rewards = pd.to_numeric(logs["reward"], errors="coerce").dropna()
    return float((rewards > 0).mean()) if not rewards.empty else np.nan


def _delta(left: object, right: object) -> float:
    left_num = pd.to_numeric(pd.Series([left]), errors="coerce").iloc[0]
    right_num = pd.to_numeric(pd.Series([right]), errors="coerce").iloc[0]
    if pd.isna(left_num) or pd.isna(right_num):
        return float("nan")
    return float(left_num - right_num)


def _best_strategy(row: dict[str, object]) -> str:
    candidates = {
        "buy_and_hold": row.get("buy_and_hold_final_equity"),
        "dqn_without_nlp": row.get("dqn_without_nlp_final_equity"),
        "dqn_with_basic_nlp": row.get("dqn_with_basic_nlp_final_equity"),
        "dqn_with_enhanced_nlp": row.get("dqn_with_enhanced_nlp_final_equity"),
    }
    numeric = {key: pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0] for key, value in candidates.items()}
    numeric = {key: value for key, value in numeric.items() if pd.notna(value)}
    return max(numeric, key=numeric.get) if numeric else "unknown"


def _min_market_days(stock_root: Path, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp) -> int:
    if pd.isna(start) or pd.isna(end) or start > end:
        return 0
    counts = []
    for symbol in symbols:
        market = _safe_read_csv(_latest_file(stock_root / symbol / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv"))
        if market.empty or "date" not in market.columns:
            continue
        dates = pd.to_datetime(market["date"], errors="coerce")
        counts.append(int(((dates >= start) & (dates <= end)).sum()))
    return min(counts) if counts else 0
