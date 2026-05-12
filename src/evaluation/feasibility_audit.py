"""Feasibility and data-quality audit for cross-stock robustness analysis.

This module is intentionally conservative: dry-run mode only inspects local
artifacts and never fetches data or trains DQN models. Full-run mode delegates to
the existing pipeline only when the caller explicitly allows fetching missing
data.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable
import re

import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR, normalize_symbol_for_path, stock_data_dir
from src.config.settings import settings
from src.evaluation.cross_stock import classify_market_regime
from src.features.technical_indicators import STATE_COLUMNS, add_trading_features


SENTIMENT_COVERAGE_WARNING = 0.20
VALID_STATE_WARNING = 0.70
COMMON_OVERLAP_WARNING_DAYS = 252
MISSING_RATE_WARNING = 0.30
BOUNDARY_TOLERANCE_DAYS = 10
REQUIRED_EXPERIMENTS = {"buy_and_hold", "dqn_without_nlp", "dqn_with_nlp"}


@dataclass
class AuditConfig:
    symbols: list[str] | None = None
    start_date: str | None = None
    end_date: str | None = None
    years: int = 5
    mode: str = "dry_run"
    use_existing_local_data: bool = True
    allow_fetch_missing_data: bool = False
    sources: str = "tencent"
    news_count: int = 5000
    episodes: int = 200
    output_dir: Path | None = None


def run_feasibility_audit(
    symbols: list[str] | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    years: int = 5,
    mode: str = "dry_run",
    use_existing_local_data: bool = True,
    allow_fetch_missing_data: bool = False,
    sources: str = "tencent",
    news_count: int = 5000,
    episodes: int = 200,
    output_dir: Path | None = None,
) -> dict[str, object]:
    """Run the audit and save report-ready CSV/Markdown artifacts."""

    config = AuditConfig(
        symbols=_parse_symbols(symbols),
        start_date=start_date,
        end_date=end_date,
        years=years,
        mode=mode,
        use_existing_local_data=use_existing_local_data,
        allow_fetch_missing_data=allow_fetch_missing_data,
        sources=sources,
        news_count=news_count,
        episodes=episodes,
        output_dir=output_dir,
    )
    if config.mode not in {"dry_run", "full_run"}:
        raise ValueError("mode must be 'dry_run' or 'full_run'.")

    audit_start, audit_end = _audit_dates(config)
    selected_symbols = _selected_symbols(config.symbols)
    aliases = _load_stock_aliases()

    stock_rows: list[dict[str, object]] = []
    diagnostic_rows: list[dict[str, object]] = []
    missing_rows: list[dict[str, object]] = []

    for symbol in selected_symbols:
        fetch_error = ""
        if config.mode == "full_run" and config.allow_fetch_missing_data:
            try:
                _run_existing_pipeline_if_needed(symbol, aliases.get(symbol, ""), audit_start, audit_end, config)
            except Exception as exc:
                fetch_error = str(exc)

        stock_audit = audit_stock(symbol, audit_start, audit_end, aliases.get(symbol, ""))
        row = stock_audit["row"]
        row["fetch_error"] = fetch_error
        needs_fetch = not bool(row.get("local_covers_requested_range")) or not bool(row.get("market_data_exists"))
        missing_analysis = not (
            bool(row.get("sentiment_data_exists"))
            and bool(row.get("ablation_metrics_usable"))
            and bool(row.get("portfolio_curves_usable"))
        )
        if bool(row.get("unfillable_start_gap_suspected")):
            warning_text = str(row.get("warnings", "") or "")
            addition = (
                "requested start appears to be before the first available trading date; "
                "adjust the date range or exclude this stock from strict cross-stock comparison"
            )
            row["warnings"] = f"{warning_text}; {addition}".strip("; ")
            row["pipeline_feasibility_status"] = "PARTIAL" if bool(row.get("market_data_exists")) else "FAILED"
        elif fetch_error:
            warning_text = str(row.get("warnings", "") or "")
            row["warnings"] = f"{warning_text}; fetch/recompute failed: {fetch_error}".strip("; ")
            if not bool(row.get("market_data_exists")):
                row["pipeline_feasibility_status"] = "FAILED"
        elif needs_fetch and config.allow_fetch_missing_data:
            row["pipeline_feasibility_status"] = "READY_AFTER_FETCH"
            warning_text = str(row.get("warnings", "") or "")
            addition = "missing or incomplete local data; full-run fetch/recompute required"
            row["warnings"] = f"{warning_text}; {addition}".strip("; ")
        row["would_fetch_missing_data"] = bool(needs_fetch and config.allow_fetch_missing_data)
        row["would_recompute_analysis_outputs"] = bool((needs_fetch or missing_analysis) and config.mode == "full_run")
        row["dry_run_expected_action"] = _expected_action(row, config)
        if needs_fetch and not config.allow_fetch_missing_data and row["pipeline_feasibility_status"] == "READY_AFTER_FETCH":
            row["pipeline_feasibility_status"] = "PARTIAL"
            warning_text = str(row.get("warnings", "") or "")
            addition = "requested five-year range is not fully covered locally and fetch is disabled"
            row["warnings"] = f"{warning_text}; {addition}".strip("; ")
        stock_rows.append(stock_audit["row"])
        diagnostic_rows.extend(stock_audit["diagnostics"])
        missing_rows.extend(stock_audit["missing_files"])

    audit_df = pd.DataFrame(stock_rows)
    diagnostics_df = pd.DataFrame(diagnostic_rows)
    missing_df = pd.DataFrame(missing_rows)
    summary_df = build_cross_stock_feasibility_summary(audit_df, audit_start, audit_end, config)

    output_root = config.output_dir or (SYSTEM_OUTPUT_DIR / "reports")
    output_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "feasibility_audit_csv": output_root / "feasibility_audit.csv",
        "cross_stock_feasibility_summary_csv": output_root / "cross_stock_feasibility_summary.csv",
        "data_quality_diagnostics_csv": output_root / "data_quality_diagnostics.csv",
        "missing_file_report_csv": output_root / "missing_file_report.csv",
        "feasibility_audit_md": output_root / "feasibility_audit.md",
    }
    audit_df.to_csv(paths["feasibility_audit_csv"], index=False, encoding="utf-8-sig")
    summary_df.to_csv(paths["cross_stock_feasibility_summary_csv"], index=False, encoding="utf-8-sig")
    diagnostics_df.to_csv(paths["data_quality_diagnostics_csv"], index=False, encoding="utf-8-sig")
    missing_df.to_csv(paths["missing_file_report_csv"], index=False, encoding="utf-8-sig")
    paths["feasibility_audit_md"].write_text(
        build_markdown_report(audit_df, summary_df, diagnostics_df, missing_df, audit_start, audit_end, config),
        encoding="utf-8",
    )

    return {
        "audit": audit_df,
        "summary": summary_df,
        "diagnostics": diagnostics_df,
        "missing_files": missing_df,
        "paths": paths,
        "selected_symbols": selected_symbols,
        "common_start_date": summary_df["common_start_date"].iloc[0] if not summary_df.empty else "",
        "common_end_date": summary_df["common_end_date"].iloc[0] if not summary_df.empty else "",
        "cross_stock_status": summary_df["cross_stock_feasibility_status"].iloc[0] if not summary_df.empty else "FAILED",
    }


def audit_stock(symbol: str, start_date: str, end_date: str, company_name: str = "") -> dict[str, object]:
    symbol = normalize_symbol_for_path(symbol)
    stock_dir = STOCK_OUTPUT_ROOT / symbol
    data_dir = stock_dir / "data"
    reports_dir = stock_dir / "reports"
    results_dir = stock_dir / "results"

    market_path = _best_market_file(symbol, start_date, end_date)
    sentiment_path = _latest_file(reports_dir, "*_daily_sentiment.csv")
    metrics_path = results_dir / "ablation_metrics.csv"
    curves_path = results_dir / "portfolio_curves.csv"
    logs_path = results_dir / "trading_logs.csv"

    market = _safe_read_csv(market_path)
    sentiment = _safe_read_csv(sentiment_path)
    metrics = _safe_read_csv(metrics_path)
    curves = _safe_read_csv(curves_path)
    logs = _safe_read_csv(logs_path)

    market = _normalize_dates(market)
    sentiment = _normalize_dates(sentiment)
    curves = _normalize_dates(curves)
    logs = _normalize_dates(logs)
    market_window_raw = _filter_date_range(market, start_date, end_date)
    market_window = _market_rows_only(market_window_raw)
    market_for_range = _market_rows_only(market)

    market_rows = int(len(market_window))
    trading_days = market_rows
    local_start = _date_min(market_for_range)
    local_end = _date_max(market_for_range)
    local_covers_requested = _covers_with_tolerance(local_start, local_end, start_date, end_date)
    available_start_gap_days = _start_gap_days(local_start, start_date)
    available_history_starts_after_request = available_start_gap_days > BOUNDARY_TOLERANCE_DAYS

    missing_rates = _market_missing_rates(market_window)
    sentiment_stats = _sentiment_stats(sentiment, market_window)
    state_stats = _state_stats(market_window, sentiment)
    ablation_stats = _ablation_stats(metrics, curves)
    portfolio_rows = int(len(curves))
    unfillable_start_gap = _unfillable_start_gap_suspected(symbol, start_date, local_start)

    file_checks = {
        "market_data": (market_path, not market_window.empty),
        "news_data": (market_path, sentiment_stats["news_rows"] > 0 or _external_news_rows(market_window) > 0),
        "sentiment_data": (sentiment_path, not sentiment.empty and "sentiment_score" in sentiment.columns),
        "ablation_metrics": (metrics_path, ablation_stats["ablation_metrics_usable"]),
        "portfolio_curves": (curves_path, ablation_stats["portfolio_curves_usable"]),
        "trading_logs": (logs_path, logs_path.exists() and not logs.empty),
    }

    warnings = _stock_warnings(missing_rates, sentiment_stats, state_stats, ablation_stats, trading_days)
    if available_history_starts_after_request:
        warnings.append(
            "available market history starts after requested start date; "
            "this may indicate a recent listing or incomplete local cache"
        )
    status = _stock_status(file_checks, warnings, local_covers_requested)

    row = {
        "symbol": symbol,
        "company_name": company_name,
        "pipeline_feasibility_status": status,
        "market_data_exists": file_checks["market_data"][1],
        "news_data_exists": file_checks["news_data"][1],
        "sentiment_data_exists": file_checks["sentiment_data"][1],
        "ablation_metrics_exists": metrics_path.exists() and not metrics.empty,
        "ablation_metrics_usable": ablation_stats["ablation_metrics_usable"],
        "portfolio_curves_exists": curves_path.exists() and not curves.empty,
        "portfolio_curves_usable": ablation_stats["portfolio_curves_usable"],
        "trading_logs_exists": logs_path.exists() and not logs.empty,
        "local_data_start": local_start,
        "local_data_end": local_end,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "local_covers_requested_range": local_covers_requested,
        "available_history_starts_after_requested_start": available_history_starts_after_request,
        "available_history_start_gap_days": available_start_gap_days,
        "unfillable_start_gap_suspected": unfillable_start_gap,
        "market_rows": market_rows,
        "raw_rows_in_requested_window": int(len(market_window_raw)),
        "news_rows": sentiment_stats["news_rows"],
        "sentiment_rows": sentiment_stats["sentiment_rows"],
        "portfolio_curve_rows": portfolio_rows,
        "trading_days": trading_days,
        "days_with_news": sentiment_stats["days_with_news"],
        "sentiment_coverage_ratio": sentiment_stats["sentiment_coverage_ratio"],
        "sentiment_score_missing_rate": sentiment_stats["sentiment_missing_rate"],
        "sentiment_zero_without_news_rate": sentiment_stats["zero_without_news_rate"],
        "market_missing_value_rate": missing_rates["market_missing_value_rate"],
        "technical_indicator_missing_rate": state_stats["technical_indicator_missing_rate"],
        "valid_rl_state_rows": state_stats["valid_rl_state_rows"],
        "valid_rl_state_row_pct": state_stats["valid_rl_state_row_pct"],
        "metrics_nan_rate": ablation_stats["metrics_nan_rate"],
        "portfolio_curves_flat_or_identical": ablation_stats["portfolio_curves_flat_or_identical"],
        "market_regime": classify_market_regime(market_window["close"]) if "close" in market_window.columns else "unknown",
        "warnings": "; ".join(warnings),
    }

    diagnostics = [
        _diag(symbol, "market_missing_value_rate", row["market_missing_value_rate"], _level(row["market_missing_value_rate"] > MISSING_RATE_WARNING), "Missing rate across open/high/low/close/volume."),
        _diag(symbol, "technical_indicator_missing_rate", row["technical_indicator_missing_rate"], _level(row["technical_indicator_missing_rate"] > MISSING_RATE_WARNING), "Missing rate after technical feature construction."),
        _diag(symbol, "sentiment_score_missing_rate", row["sentiment_score_missing_rate"], _level(row["sentiment_score_missing_rate"] > MISSING_RATE_WARNING), "Missing sentiment values before conservative zero fallback."),
        _diag(symbol, "sentiment_coverage_ratio", row["sentiment_coverage_ratio"], _level(row["sentiment_coverage_ratio"] < SENTIMENT_COVERAGE_WARNING), "Trading days with news_count > 0 divided by trading days."),
        _diag(symbol, "valid_rl_state_row_pct", row["valid_rl_state_row_pct"], _level(row["valid_rl_state_row_pct"] < VALID_STATE_WARNING), "Rows usable by the RL state vector after leakage-safe shifts."),
        _diag(symbol, "metrics_nan_rate", row["metrics_nan_rate"], _level(row["metrics_nan_rate"] > 0.5), "NaN rate in numeric ablation metric cells."),
        _diag(symbol, "portfolio_curves_flat_or_identical", row["portfolio_curves_flat_or_identical"], _level(bool(row["portfolio_curves_flat_or_identical"])), "True means curves are missing, flat, or all strategies are indistinguishable."),
    ]
    missing_files = [
        {
            "symbol": symbol,
            "file_type": file_type,
            "path": str(path) if path else "",
            "exists": bool(path and path.exists()),
            "usable": bool(usable),
        }
        for file_type, (path, usable) in file_checks.items()
    ]
    return {"row": row, "diagnostics": diagnostics, "missing_files": missing_files}


def build_cross_stock_feasibility_summary(audit_df: pd.DataFrame, start_date: str, end_date: str, config: AuditConfig) -> pd.DataFrame:
    if audit_df.empty:
        return pd.DataFrame(
            [
                {
                    "audit_mode": config.mode,
                    "selected_stock_count": 0,
                    "valid_stock_count": 0,
                    "cross_stock_feasibility_status": "FAILED",
                    "final_recommendation": "Not reliable without more data",
                }
            ]
        )

    date_rows = audit_df.dropna(subset=["local_data_start", "local_data_end"]).copy()
    if not date_rows.empty:
        common_start = max(pd.to_datetime(date_rows["local_data_start"], errors="coerce").dropna().max(), pd.Timestamp(start_date))
        common_end = min(pd.to_datetime(date_rows["local_data_end"], errors="coerce").dropna().min(), pd.Timestamp(end_date))
    else:
        common_start = pd.NaT
        common_end = pd.NaT

    valid_mask = audit_df["pipeline_feasibility_status"].isin(["READY_LOCAL", "READY_AFTER_FETCH"])
    market_mask = audit_df["market_data_exists"].fillna(False)
    valid_count = int(valid_mask.sum())
    overlap_days = _common_trading_days(audit_df.loc[market_mask, "symbol"].tolist(), common_start, common_end)
    valid_overlap_days = _common_trading_days(audit_df.loc[valid_mask, "symbol"].tolist(), common_start, common_end)
    expected_after_fetch_overlap_days = _business_days(start_date, end_date) if config.allow_fetch_missing_data and valid_count >= 2 else 0
    comparable_overlap_days = max(valid_overlap_days, expected_after_fetch_overlap_days)
    warning_count = int(audit_df["warnings"].fillna("").astype(str).ne("").sum())
    missing_ablation = int((~audit_df["ablation_metrics_usable"].fillna(False)).sum())
    weak_coverage = int((pd.to_numeric(audit_df["sentiment_coverage_ratio"], errors="coerce") < SENTIMENT_COVERAGE_WARNING).sum())
    weak_state = int((pd.to_numeric(audit_df["valid_rl_state_row_pct"], errors="coerce") < VALID_STATE_WARNING).sum())

    if valid_count < 2:
        status = "NOT_RELIABLE"
    elif comparable_overlap_days < COMMON_OVERLAP_WARNING_DAYS:
        status = "NOT_RELIABLE"
    elif missing_ablation or weak_coverage or weak_state or warning_count:
        status = "READY_WITH_WARNINGS"
    else:
        status = "READY_FOR_CROSS_STOCK_ANALYSIS"

    recommendation = {
        "READY_FOR_CROSS_STOCK_ANALYSIS": "Safe to run cross-stock analysis",
        "READY_WITH_WARNINGS": "Run with warnings",
        "NOT_RELIABLE": "Not reliable without more data",
        "FAILED": "Not reliable without more data",
    }.get(status, "Not reliable without more data")

    return pd.DataFrame(
        [
            {
                "audit_mode": config.mode,
                "use_existing_local_data": config.use_existing_local_data,
                "allow_fetch_missing_data": config.allow_fetch_missing_data,
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "common_start_date": _fmt_date(common_start),
                "common_end_date": _fmt_date(common_end),
                "common_overlap_trading_days": overlap_days,
                "valid_stock_common_overlap_trading_days": valid_overlap_days,
                "expected_after_fetch_overlap_trading_days": expected_after_fetch_overlap_days,
                "selected_stock_count": int(len(audit_df)),
                "valid_stock_count": valid_count,
                "partial_stock_count": int((audit_df["pipeline_feasibility_status"] == "PARTIAL").sum()),
                "failed_stock_count": int((audit_df["pipeline_feasibility_status"] == "FAILED").sum()),
                "stocks_with_low_sentiment_coverage": weak_coverage,
                "stocks_with_low_valid_state_rows": weak_state,
                "stocks_missing_usable_ablation": missing_ablation,
                "cross_stock_feasibility_status": status,
                "final_recommendation": recommendation,
            }
        ]
    )


def build_markdown_report(
    audit_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    diagnostics_df: pd.DataFrame,
    missing_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    config: AuditConfig,
) -> str:
    summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {}
    usable = audit_df.loc[audit_df["pipeline_feasibility_status"].isin(["READY_LOCAL", "READY_AFTER_FETCH"]), "symbol"].tolist() if not audit_df.empty else []
    missing = missing_df.loc[~missing_df["usable"], ["symbol", "file_type"]].copy() if not missing_df.empty else pd.DataFrame()
    missing_lines = []
    if not missing.empty:
        for symbol, group in missing.groupby("symbol"):
            missing_lines.append(f"- `{symbol}`: " + ", ".join(group["file_type"].astype(str).tolist()))
    else:
        missing_lines.append("- No missing required artifacts detected.")

    warnings = diagnostics_df.loc[diagnostics_df.get("warning_level", "") == "warn"] if not diagnostics_df.empty else pd.DataFrame()
    warning_lines = []
    if not warnings.empty:
        for _, row in warnings.iterrows():
            warning_lines.append(f"- `{row['symbol']}` `{row['metric']}` = `{row['value']}`: {row['description']}")
    else:
        warning_lines.append("- No threshold warnings detected.")

    return "\n".join(
        [
            "# Program Feasibility and Data Quality Audit",
            "",
            f"- Audit mode: `{config.mode}`",
            f"- Existing local data only: `{config.use_existing_local_data}`",
            f"- Allow fetch missing data: `{config.allow_fetch_missing_data}`",
            f"- Requested date range: `{start_date}` to `{end_date}`",
            f"- Cross-stock status: `{summary.get('cross_stock_feasibility_status', 'FAILED')}`",
            f"- Final recommendation: **{summary.get('final_recommendation', 'Not reliable without more data')}**",
            "",
            "## Usable Stocks",
            "",
            "- " + (", ".join(f"`{symbol}`" for symbol in usable) if usable else "No stock is currently fully usable."),
            "",
            "## Missing Or Unusable Artifacts",
            "",
            *missing_lines,
            "",
            "## Common Overlap",
            "",
            f"- Common start date: `{summary.get('common_start_date', '')}`",
            f"- Common end date: `{summary.get('common_end_date', '')}`",
            f"- Common overlap trading days: `{summary.get('common_overlap_trading_days', 0)}`",
            "",
            "## Warnings",
            "",
            *warning_lines,
            "",
            "## Interpretation",
            "",
            "This audit distinguishes missing sentiment from neutral sentiment: a zero sentiment score only counts as news coverage when `news_count > 0`. Dry-run mode does not fetch data or rerun DQN training. Full-run mode can use the existing pipeline to fetch and recompute missing artifacts only when explicitly enabled.",
            "",
            "Cross-stock analysis should not be treated as reliable when portfolio curves are missing, ablation metrics are mostly empty, RL state rows are too few, or sentiment coverage is dominated by no-news days.",
        ]
    )


def _run_existing_pipeline_if_needed(symbol: str, company_name: str, start_date: str, end_date: str, config: AuditConfig) -> None:
    precheck = audit_stock(symbol, start_date, end_date, company_name)["row"]
    if precheck["pipeline_feasibility_status"] == "READY_LOCAL":
        return
    from main import run_pipeline_for_symbol

    run_pipeline_for_symbol(
        symbol=symbol,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        sources=config.sources,
        news_count=config.news_count,
        run_ingestion_flag=True,
        run_nlp_flag=True,
        run_rl_flag=True,
        run_ablation_flag=True,
        episodes=config.episodes,
        use_sqlite=False,
        reuse_existing_csv=True,
        require_news=False,
        build_cross_stock_outputs=False,
    )


def _stock_warnings(
    missing_rates: dict[str, float],
    sentiment_stats: dict[str, object],
    state_stats: dict[str, object],
    ablation_stats: dict[str, object],
    trading_days: int,
) -> list[str]:
    warnings: list[str] = []
    if trading_days == 0:
        warnings.append("no usable market rows")
    if missing_rates["market_missing_value_rate"] > MISSING_RATE_WARNING:
        warnings.append("market missing value rate above 30%")
    if state_stats["technical_indicator_missing_rate"] > MISSING_RATE_WARNING:
        warnings.append("technical indicator missing value rate above 30%")
    if sentiment_stats["sentiment_coverage_ratio"] < SENTIMENT_COVERAGE_WARNING:
        warnings.append("sentiment coverage below 20%")
    if state_stats["valid_rl_state_row_pct"] < VALID_STATE_WARNING:
        warnings.append("valid RL state rows below 70%")
    if not ablation_stats["ablation_metrics_usable"]:
        warnings.append("ablation result missing or unusable")
    if not ablation_stats["portfolio_curves_usable"]:
        warnings.append("portfolio curves missing, flat, or unusable")
    if ablation_stats["metrics_nan_rate"] > 0.5:
        warnings.append("ablation metrics dominated by NaN")
    if sentiment_stats["zero_without_news_rate"] > 0.8 and sentiment_stats["sentiment_coverage_ratio"] < SENTIMENT_COVERAGE_WARNING:
        warnings.append("sentiment is mostly zero because no news is available")
    return warnings


def _unfillable_start_gap_suspected(symbol: str, requested_start: str, local_start: str) -> bool:
    if not local_start:
        return False
    if (pd.Timestamp(local_start) - pd.Timestamp(requested_start)).days <= BOUNDARY_TOLERANCE_DAYS:
        return False
    data_dir = stock_data_dir(symbol)
    for path in sorted(data_dir.glob(f"{normalize_symbol_for_path(symbol)}_diagnostic_*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        text = json.dumps(payload, ensure_ascii=False)
        if "No rows to write" not in text and "requested range still could not be materialized" not in text:
            continue
        diag_start = str(payload.get("start_date") or "")
        diag_end = str(payload.get("end_date") or "")
        if not diag_start or not diag_end:
            return True
        try:
            if pd.Timestamp(diag_start) <= pd.Timestamp(local_start) and pd.Timestamp(diag_end) < pd.Timestamp(local_start):
                return True
        except Exception:
            return True
    return False


def _start_gap_days(local_start: str, requested_start: str) -> int:
    if not local_start:
        return 0
    try:
        return max(0, int((pd.Timestamp(local_start) - pd.Timestamp(requested_start)).days))
    except Exception:
        return 0


def _stock_status(file_checks: dict[str, tuple[Path | None, bool]], warnings: list[str], local_covers_requested: bool) -> str:
    if not file_checks["market_data"][1]:
        return "FAILED"
    required_ok = all(file_checks[name][1] for name in ["market_data", "sentiment_data", "ablation_metrics", "portfolio_curves"])
    severe = any(text in warnings for text in ["valid RL state rows below 70%", "ablation metrics dominated by NaN", "portfolio curves missing, flat, or unusable"])
    if required_ok and local_covers_requested and not severe:
        return "READY_LOCAL"
    return "PARTIAL"


def _expected_action(row: dict[str, object], config: AuditConfig) -> str:
    if config.mode == "dry_run":
        if row.get("pipeline_feasibility_status") == "READY_LOCAL":
            return "load local artifacts only"
        if row.get("would_fetch_missing_data"):
            return "would fetch missing data and recompute outputs in full-run mode"
        return "inspect only; fetch disabled so incomplete stocks remain partial"
    if row.get("would_fetch_missing_data"):
        return "fetch missing data, run NLP, run DQN ablation, refresh reports"
    if row.get("would_recompute_analysis_outputs"):
        return "reuse local data and recompute missing analysis outputs"
    return "load local artifacts"


def _state_stats(market: pd.DataFrame, sentiment: pd.DataFrame) -> dict[str, object]:
    trading_days = max(len(market), 1)
    if market.empty:
        return {"technical_indicator_missing_rate": 1.0, "valid_rl_state_rows": 0, "valid_rl_state_row_pct": 0.0}
    try:
        features = add_trading_features(market, sentiment, sentiment_already_aligned=True)
    except Exception:
        return {"technical_indicator_missing_rate": 1.0, "valid_rl_state_rows": 0, "valid_rl_state_row_pct": 0.0}
    present_cols = [col for col in STATE_COLUMNS if col in features.columns]
    if not present_cols or features.empty:
        missing_rate = 1.0
    else:
        missing_rate = float(features[present_cols].isna().mean().mean())
    return {
        "technical_indicator_missing_rate": missing_rate,
        "valid_rl_state_rows": int(len(features)),
        "valid_rl_state_row_pct": float(len(features) / trading_days),
    }


def _sentiment_stats(sentiment: pd.DataFrame, market: pd.DataFrame) -> dict[str, object]:
    trading_days = max(len(market), 1)
    if sentiment.empty:
        return {
            "sentiment_rows": 0,
            "news_rows": 0,
            "days_with_news": 0,
            "sentiment_coverage_ratio": 0.0,
            "sentiment_missing_rate": 1.0,
            "zero_without_news_rate": 1.0,
        }
    score_col = "sentiment_score" if "sentiment_score" in sentiment.columns else "daily_sentiment_score" if "daily_sentiment_score" in sentiment.columns else None
    if score_col and score_col != "sentiment_score":
        sentiment = sentiment.rename(columns={score_col: "sentiment_score"})
    if "news_count" not in sentiment.columns:
        sentiment["news_count"] = 0
    sentiment["news_count"] = pd.to_numeric(sentiment["news_count"], errors="coerce").fillna(0)
    sentiment["sentiment_score"] = pd.to_numeric(sentiment.get("sentiment_score", np.nan), errors="coerce")
    if not market.empty and "date" in market.columns:
        merged = market[["date"]].merge(sentiment[["date", "sentiment_score", "news_count"]], on="date", how="left")
    else:
        merged = sentiment
    days_with_news = int((pd.to_numeric(merged.get("news_count", 0), errors="coerce").fillna(0) > 0).sum())
    zero_without_news = (
        (pd.to_numeric(merged.get("sentiment_score", np.nan), errors="coerce").fillna(0).eq(0))
        & (pd.to_numeric(merged.get("news_count", 0), errors="coerce").fillna(0).eq(0))
    )
    return {
        "sentiment_rows": int(len(sentiment)),
        "news_rows": int(pd.to_numeric(sentiment.get("news_count", 0), errors="coerce").fillna(0).sum()),
        "days_with_news": days_with_news,
        "sentiment_coverage_ratio": float(days_with_news / trading_days),
        "sentiment_missing_rate": float(pd.to_numeric(merged.get("sentiment_score", np.nan), errors="coerce").isna().mean()),
        "zero_without_news_rate": float(zero_without_news.mean()) if len(merged) else 1.0,
    }


def _ablation_stats(metrics: pd.DataFrame, curves: pd.DataFrame) -> dict[str, object]:
    usable_metrics = False
    metrics_nan_rate = 1.0
    if not metrics.empty and "experiment" in metrics.columns:
        experiments = set(metrics["experiment"].astype(str))
        numeric = metrics.select_dtypes(include=[np.number])
        metrics_nan_rate = float(numeric.isna().mean().mean()) if not numeric.empty else 1.0
        usable_metrics = REQUIRED_EXPERIMENTS.issubset(experiments) and metrics_nan_rate <= 0.5

    usable_curves = False
    flat_or_identical = True
    if not curves.empty and {"experiment", "portfolio_value"}.issubset(curves.columns):
        curves["portfolio_value"] = pd.to_numeric(curves["portfolio_value"], errors="coerce")
        curve_counts = curves.groupby("experiment")["portfolio_value"].nunique(dropna=True)
        usable_curves = REQUIRED_EXPERIMENTS.issubset(set(curves["experiment"].astype(str))) and bool((curve_counts > 1).any())
        terminal = curves.dropna(subset=["portfolio_value"]).groupby("experiment")["portfolio_value"].last()
        flat_or_identical = (not usable_curves) or (terminal.nunique(dropna=True) <= 1)
    return {
        "ablation_metrics_usable": bool(usable_metrics),
        "metrics_nan_rate": metrics_nan_rate,
        "portfolio_curves_usable": bool(usable_curves),
        "portfolio_curves_flat_or_identical": bool(flat_or_identical),
    }


def _market_missing_rates(market: pd.DataFrame) -> dict[str, float]:
    if market.empty:
        return {"market_missing_value_rate": 1.0}
    key_cols = [col for col in ["open", "high", "low", "close", "volume"] if col in market.columns]
    if not key_cols:
        return {"market_missing_value_rate": 1.0}
    frame = market[key_cols].apply(pd.to_numeric, errors="coerce")
    return {"market_missing_value_rate": float(frame.isna().mean().mean())}


def _market_rows_only(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    if "close" not in data.columns:
        return data.iloc[0:0].copy()
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    return data.dropna(subset=["close"]).copy().reset_index(drop=True)


def _covers_with_tolerance(local_start: str, local_end: str, requested_start: str, requested_end: str) -> bool:
    if not local_start or not local_end:
        return False
    first = pd.Timestamp(local_start)
    last = pd.Timestamp(local_end)
    start = pd.Timestamp(requested_start)
    end = pd.Timestamp(requested_end)
    start_gap_days = max(0, (first - start).days)
    end_gap_days = max(0, (end - last).days)
    return (first <= start or start_gap_days <= BOUNDARY_TOLERANCE_DAYS) and (
        last >= end or end_gap_days <= BOUNDARY_TOLERANCE_DAYS
    )


def _external_news_rows(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    if "external_event_count" in frame.columns:
        return int(pd.to_numeric(frame["external_event_count"], errors="coerce").fillna(0).sum())
    if "has_external_text" in frame.columns:
        return int(frame["has_external_text"].astype(str).str.lower().isin(["true", "1", "yes"]).sum())
    return int(pd.to_numeric(frame.get("event_count", 0), errors="coerce").fillna(0).sum())


def _best_market_file(symbol: str, start_date: str, end_date: str) -> Path | None:
    data_dir = stock_data_dir(symbol)
    if not data_dir.exists():
        return None
    exact = data_dir / f"{symbol}_finance_text_{start_date}_{end_date}.csv"
    candidates = []
    if exact.exists():
        candidates.append(exact)
    master = data_dir / f"{symbol}_finance_text_master.csv"
    if master.exists():
        candidates.append(master)
    candidates.extend(path for path in data_dir.glob(f"{symbol}_finance_text_*.csv") if not path.name.endswith("_master.csv"))
    best_path = None
    best_score = -1
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)
    for path in candidates:
        frame = _normalize_dates(_safe_read_csv(path))
        if frame.empty or "date" not in frame.columns:
            continue
        window_rows = len(_filter_date_range(frame, start_date, end_date))
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        covers_bonus = 100000 if (not dates.empty and dates.min() <= requested_start and dates.max() >= requested_end) else 0
        score = covers_bonus + window_rows
        if score > best_score:
            best_score = score
            best_path = path
    return best_path


def _common_trading_days(symbols: list[str], common_start: pd.Timestamp, common_end: pd.Timestamp) -> int:
    if not symbols or pd.isna(common_start) or pd.isna(common_end) or common_end < common_start:
        return 0
    sets = []
    for symbol in symbols:
        market = _market_rows_only(_normalize_dates(_safe_read_csv(_best_market_file(symbol, str(common_start.date()), str(common_end.date())))))
        market = _filter_date_range(market, str(common_start.date()), str(common_end.date()))
        if "date" in market.columns:
            sets.append(set(pd.to_datetime(market["date"], errors="coerce").dropna().dt.date))
    if not sets:
        return 0
    return len(set.intersection(*sets))


def _business_days(start_date: str, end_date: str) -> int:
    try:
        return int(len(pd.bdate_range(pd.Timestamp(start_date), pd.Timestamp(end_date))))
    except Exception:
        return 0


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (pd.errors.EmptyDataError, UnicodeDecodeError, OSError, ValueError):
        return pd.DataFrame()


def _normalize_dates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    return result.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def _filter_date_range(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame()
    mask = (frame["date"] >= pd.Timestamp(start_date)) & (frame["date"] <= pd.Timestamp(end_date))
    return frame.loc[mask].copy()


def _latest_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if path.is_file()]
    return sorted(files, key=lambda path: path.stat().st_mtime)[-1] if files else None


def _selected_symbols(symbols: list[str] | None) -> list[str]:
    if symbols:
        return [normalize_symbol_for_path(symbol) for symbol in symbols if str(symbol).strip()]
    if not STOCK_OUTPUT_ROOT.exists():
        return [normalize_symbol_for_path(settings.default_symbol)]
    detected = [path.name for path in sorted(STOCK_OUTPUT_ROOT.iterdir()) if path.is_dir() and re.fullmatch(r"\d{6}", path.name)]
    return detected or [normalize_symbol_for_path(settings.default_symbol)]


def _parse_symbols(symbols: list[str] | str | None) -> list[str] | None:
    if symbols is None:
        return None
    if isinstance(symbols, str):
        return [part.strip() for part in symbols.split(",") if part.strip()]
    return [str(symbol).strip() for symbol in symbols if str(symbol).strip()]


def _audit_dates(config: AuditConfig) -> tuple[str, str]:
    end = pd.Timestamp(config.end_date) if config.end_date else pd.Timestamp(date.today())
    start = pd.Timestamp(config.start_date) if config.start_date else end - pd.DateOffset(years=config.years)
    return str(start.date()), str(end.date())


def _load_stock_aliases() -> dict[str, str]:
    path = PROJECT_ROOT / "config" / "stock_aliases.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {normalize_symbol_for_path(key): str(value).strip() for key, value in payload.items() if str(value).strip()}


def _date_min(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return str(values.min().date()) if not values.empty else ""


def _date_max(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return str(values.max().date()) if not values.empty else ""


def _fmt_date(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(pd.Timestamp(value).date())


def _diag(symbol: str, metric: str, value: object, warning_level: str, description: str) -> dict[str, object]:
    return {"symbol": symbol, "metric": metric, "value": value, "warning_level": warning_level, "description": description}


def _level(flag: bool) -> str:
    return "warn" if flag else "ok"
