"""Cached model-performance and trading-behavior visualizations."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR

REPORTS_DIR = PROJECT_ROOT / "reports"


STRATEGY_LABELS = {
    "buy_and_hold": "Buy-and-Hold",
    "predict_then_trade": "Predict-then-Trade",
    "dqn_without_nlp": "DQN without NLP",
    "dqn_with_nlp": "DQN with Basic NLP",
    "dqn_with_basic_nlp": "DQN with Basic NLP",
    "dqn_with_enhanced_nlp": "DQN with Enhanced NLP",
}


def generate_trading_visualizations(
    stock_root: Path | None = None,
    system_dir: Path | None = None,
) -> dict[str, Path | pd.DataFrame]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    system_dir = system_dir or SYSTEM_OUTPUT_DIR
    rows: list[dict[str, object]] = []
    for stock_dir in _stock_dirs(stock_root):
        rows.extend(_generate_stock_visuals(stock_dir))
    summary = pd.DataFrame(rows)
    index_path = REPORTS_DIR / "tables" / "trading_visualization_index.csv"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(index_path, index=False, encoding="utf-8-sig")
    cross_path = _plot_cross_stock_visual_summary(system_dir)
    return {"trading_visualization_index": index_path, "cross_stock_visual_summary": cross_path, "index": summary}


def _generate_stock_visuals(stock_dir: Path) -> list[dict[str, object]]:
    symbol = stock_dir.name
    reports = stock_dir / "reports"
    results = stock_dir / "results"
    reports.mkdir(parents=True, exist_ok=True)
    market = _safe_read_csv(_latest_file(stock_dir / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv"))
    sentiment = _safe_read_csv(_latest_file(reports, "*_daily_sentiment.csv"))
    curves = _safe_read_csv(results / "high_density_portfolio_curves.csv")
    if curves.empty:
        curves = _safe_read_csv(results / "portfolio_curves.csv")
    logs = _safe_read_csv(results / "high_density_trading_logs.csv")
    if logs.empty:
        logs = _safe_read_csv(results / "trading_logs.csv")
    metrics = _safe_read_csv(results / "high_density_ablation_metrics.csv")
    if metrics.empty:
        metrics = _safe_read_csv(results / "ablation_metrics.csv")
    split = _safe_read_csv(reports / "information_density_split.csv")
    market, sentiment, curves, logs = _filter_to_eval_window(split, market, sentiment, curves, logs)

    rows = []
    specs = [
        ("price_with_trading_actions.png", lambda path: _plot_price_with_actions(market, logs, path)),
        ("portfolio_value_comparison.png", lambda path: _plot_portfolio_curves(curves, path)),
        ("action_distribution.png", lambda path: _plot_action_distribution(logs, path)),
        ("trade_outcome_win_rate.png", lambda path: _plot_trade_outcomes(logs, path, reports / "trade_outcome_win_rate.csv")),
        ("drawdown_curve_comparison.png", lambda path: _plot_drawdown(curves, path)),
        ("sentiment_action_overlay.png", lambda path: _plot_sentiment_action_overlay(market, sentiment, logs, path)),
        ("prediction_vs_actual_direction.png", lambda path: _plot_prediction_vs_actual(stock_dir, path)),
    ]
    for filename, builder in specs:
        path = reports / filename
        status = "Complete"
        try:
            made = builder(path)
            if not made:
                status = "Missing"
        except Exception as exc:
            status = f"Warning: {exc}"
        rows.append({"symbol": symbol, "visualization": filename, "path": str(path.relative_to(PROJECT_ROOT)), "status": status})
    _write_behavior_summary(symbol, logs, metrics, reports / "model_behavior_visual_summary.csv")
    return rows


def _plot_price_with_actions(market: pd.DataFrame, logs: pd.DataFrame, path: Path) -> bool:
    if market.empty or "close" not in market.columns:
        return False
    market = _with_dates(market)
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.plot(market["date"], pd.to_numeric(market["close"], errors="coerce"), color="#2f2f2f", linewidth=1.5, label="Close")
    strategy = _preferred_strategy(logs)
    action_rows = _representative_logs(logs, strategy)
    if not action_rows.empty:
        joined = action_rows.merge(market[["date", "close"]], on="date", how="left")
        buys = joined[joined["action"].astype(str) == "Buy"]
        sells = joined[joined["action"].astype(str) == "Sell"]
        holds = joined[joined["action"].astype(str) == "Hold"]
        if not holds.empty and len(holds) < 250:
            ax.scatter(holds["date"], holds["close"], color="lightgrey", s=12, label="Hold", alpha=0.45)
        ax.scatter(buys["date"], buys["close"], marker="^", color="green", s=60, label="Buy")
        ax.scatter(sells["date"], sells["close"], marker="v", color="red", s=60, label="Sell")
    ax.set_title(f"Price with trading actions - {STRATEGY_LABELS.get(strategy, strategy)}")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_portfolio_curves(curves: pd.DataFrame, path: Path) -> bool:
    if curves.empty or not {"date", "portfolio_value", "experiment"}.issubset(curves.columns):
        return False
    curves = _normalize_experiments(_with_dates(curves))
    fig, ax = plt.subplots(figsize=(11, 4.2))
    for exp, group in curves.groupby("experiment", sort=False):
        by_date = group.sort_values("date").groupby("date", as_index=False)["portfolio_value"].mean()
        ax.plot(by_date["date"], pd.to_numeric(by_date["portfolio_value"], errors="coerce"), label=STRATEGY_LABELS.get(exp, exp), linewidth=1.8)
    ax.set_title("Portfolio value comparison")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_action_distribution(logs: pd.DataFrame, path: Path) -> bool:
    if logs.empty or not {"experiment", "action"}.issubset(logs.columns):
        return False
    logs = _normalize_experiments(logs)
    counts = logs.groupby(["experiment", "action"]).size().unstack(fill_value=0)
    for action in ["Buy", "Sell", "Hold"]:
        if action not in counts.columns:
            counts[action] = 0
    counts = counts[["Buy", "Sell", "Hold"]]
    pct = counts.div(counts.sum(axis=1).replace(0, np.nan), axis=0)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    counts.plot(kind="bar", stacked=False, ax=axes[0], color=["green", "red", "grey"])
    pct.plot(kind="bar", stacked=True, ax=axes[1], color=["green", "red", "grey"])
    axes[0].set_title("Action counts")
    axes[1].set_title("Action percentages")
    for ax in axes:
        ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_trade_outcomes(logs: pd.DataFrame, path: Path, table_path: Path) -> bool:
    if logs.empty or not {"date", "action", "reward", "experiment"}.issubset(logs.columns):
        return False
    logs = _normalize_experiments(_with_dates(logs))
    trades = logs[logs["action"].astype(str).isin(["Buy", "Sell"])].copy()
    trades["reward"] = pd.to_numeric(trades["reward"], errors="coerce")
    if trades.empty:
        table_path.write_text("metric,value\nnumber_of_trades,0\n", encoding="utf-8")
        return False
    summary_rows = []
    for exp, group in trades.groupby("experiment"):
        rewards = group["reward"].dropna()
        wins = rewards[rewards > 0]
        losses = rewards[rewards < 0]
        summary_rows.append(
            {
                "experiment": exp,
                "win_rate": float((rewards > 0).mean()) if not rewards.empty else np.nan,
                "average_win": float(wins.mean()) if not wins.empty else np.nan,
                "average_loss": float(losses.mean()) if not losses.empty else np.nan,
                "profit_factor": float(wins.sum() / abs(losses.sum())) if not losses.empty and abs(losses.sum()) > 0 else np.nan,
                "number_of_trades": int(len(group)),
            }
        )
    pd.DataFrame(summary_rows).to_csv(table_path, index=False, encoding="utf-8-sig")
    fig, ax = plt.subplots(figsize=(11, 4))
    colors = ["green" if value > 0 else "red" for value in trades["reward"].fillna(0)]
    ax.bar(trades["date"], trades["reward"], color=colors, width=1.0)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Trade outcome / reward by date")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_drawdown(curves: pd.DataFrame, path: Path) -> bool:
    if curves.empty or not {"date", "portfolio_value", "experiment"}.issubset(curves.columns):
        return False
    curves = _normalize_experiments(_with_dates(curves))
    fig, ax = plt.subplots(figsize=(11, 4.2))
    for exp, group in curves.groupby("experiment", sort=False):
        by_date = group.sort_values("date").groupby("date", as_index=False)["portfolio_value"].mean()
        values = pd.to_numeric(by_date["portfolio_value"], errors="coerce")
        drawdown = values / values.cummax() - 1
        ax.plot(by_date["date"], drawdown, label=STRATEGY_LABELS.get(exp, exp), linewidth=1.8)
    ax.set_title("Drawdown curve comparison")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_sentiment_action_overlay(market: pd.DataFrame, sentiment: pd.DataFrame, logs: pd.DataFrame, path: Path) -> bool:
    if market.empty or sentiment.empty or "close" not in market.columns:
        return False
    market = _with_dates(market)
    sentiment = _with_dates(sentiment)
    score_col = "sentiment_score" if "sentiment_score" in sentiment.columns else "daily_sentiment_score" if "daily_sentiment_score" in sentiment.columns else ""
    if not score_col:
        return False
    fig, axes = plt.subplots(2, 1, figsize=(11, 6), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    axes[0].plot(market["date"], pd.to_numeric(market["close"], errors="coerce"), color="#2f2f2f", linewidth=1.5, label="Close")
    strategy = _preferred_strategy(logs)
    action_rows = _representative_logs(logs, strategy)
    if not action_rows.empty:
        joined = action_rows.merge(market[["date", "close"]], on="date", how="left")
        axes[0].scatter(joined.loc[joined["action"] == "Buy", "date"], joined.loc[joined["action"] == "Buy", "close"], marker="^", color="green", s=55, label="Buy")
        axes[0].scatter(joined.loc[joined["action"] == "Sell", "date"], joined.loc[joined["action"] == "Sell", "close"], marker="v", color="red", s=55, label="Sell")
    axes[0].legend(loc="best")
    axes[0].set_title("Sentiment and action overlay")
    axes[1].plot(sentiment["date"], pd.to_numeric(sentiment[score_col], errors="coerce"), color="#995a70", label="Sentiment score")
    if "news_count" in sentiment.columns:
        ax2 = axes[1].twinx()
        ax2.bar(sentiment["date"], pd.to_numeric(sentiment["news_count"], errors="coerce").fillna(0), color="lightgrey", alpha=0.45, label="News count")
        ax2.set_ylabel("News count")
    axes[1].axhline(0, color="black", linewidth=0.7)
    axes[1].set_ylabel("Sentiment")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_prediction_vs_actual(stock_dir: Path, path: Path) -> bool:
    prediction = _safe_read_csv(_latest_file(stock_dir / "results", "*prediction*.csv") or _latest_file(stock_dir / "reports", "*prediction*.csv"))
    if prediction.empty:
        return False
    date_col = "date" if "date" in prediction.columns else ""
    actual_col = _first_present(prediction, ["actual_direction", "actual_next_day_direction", "actual"])
    pred_col = _first_present(prediction, ["predicted_direction", "prediction", "predicted"])
    if not date_col or not actual_col or not pred_col:
        return False
    prediction = _with_dates(prediction)
    prediction["correct"] = prediction[actual_col].astype(str) == prediction[pred_col].astype(str)
    prediction["acc_5"] = prediction["correct"].rolling(5, min_periods=1).mean()
    prediction["acc_10"] = prediction["correct"].rolling(10, min_periods=1).mean()
    fig, axes = plt.subplots(2, 1, figsize=(11, 5), sharex=True)
    colors = ["green" if ok else "red" for ok in prediction["correct"]]
    axes[0].scatter(prediction["date"], prediction[pred_col], color=colors, s=24)
    axes[0].set_title("Predicted vs actual direction")
    axes[1].plot(prediction["date"], prediction["acc_5"], label="5-day accuracy")
    axes[1].plot(prediction["date"], prediction["acc_10"], label="10-day accuracy")
    axes[1].legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return True


def _plot_cross_stock_visual_summary(system_dir: Path) -> Path:
    summary = _safe_read_csv(system_dir / "cross_stock_high_density_summary.csv")
    path = system_dir / "figures" / "cross_stock_visual_summary.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    table_path = system_dir / "cross_stock_visual_summary.csv"
    if summary.empty:
        pd.DataFrame([{"warning": "cross_stock_high_density_summary.csv is missing"}]).to_csv(table_path, index=False)
        return path
    if "symbol" in summary.columns:
        summary["symbol"] = summary["symbol"].astype(str).str.extract(r"(\d+)", expand=False).fillna(summary["symbol"].astype(str)).str.zfill(6)
    cols = [
        "symbol",
        "high_density_start",
        "high_density_end",
        "high_density_coverage_ratio",
        "best_strategy",
        "nlp_effect_label",
        "nlp_basic_final_equity_effect",
        "nlp_basic_sharpe_effect",
        "dqn_with_basic_nlp_max_drawdown",
        "comparability_status",
    ]
    compact = summary[[col for col in cols if col in summary.columns]].copy()
    compact.to_csv(table_path, index=False, encoding="utf-8-sig")
    fig, axes = plt.subplots(2, 1, figsize=(12, 7))
    if "nlp_basic_final_equity_effect" in summary.columns:
        axes[0].bar(summary["symbol"].astype(str), pd.to_numeric(summary["nlp_basic_final_equity_effect"], errors="coerce"), color="#995a70")
        axes[0].axhline(0, color="black", linewidth=0.8)
        axes[0].set_title("NLP basic final equity effect by stock")
    if "high_density_coverage_ratio" in summary.columns:
        axes[1].bar(summary["symbol"].astype(str), pd.to_numeric(summary["high_density_coverage_ratio"], errors="coerce"), color="#846992")
        axes[1].set_title("High-density sentiment coverage by stock")
        axes[1].set_ylim(0, 1.05)
    for ax in axes:
        ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def _write_behavior_summary(symbol: str, logs: pd.DataFrame, metrics: pd.DataFrame, path: Path) -> None:
    rows = []
    if not logs.empty and {"experiment", "action"}.issubset(logs.columns):
        logs = _normalize_experiments(logs)
        for exp, group in logs.groupby("experiment"):
            total = max(len(group), 1)
            counts = group["action"].value_counts()
            hold_ratio = float(counts.get("Hold", 0) / total)
            trade_ratio = float((counts.get("Buy", 0) + counts.get("Sell", 0)) / total)
            warning = "ok"
            if hold_ratio > 0.9:
                warning = "Hold ratio > 90%; model may be too conservative"
            elif trade_ratio > 0.5:
                warning = "High Buy/Sell turnover; model may be overtrading"
            rows.append({"symbol": symbol, "experiment": exp, "hold_ratio": hold_ratio, "trade_ratio": trade_ratio, "warning": warning})
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _filter_to_eval_window(split: pd.DataFrame, *frames: pd.DataFrame) -> tuple[pd.DataFrame, ...]:
    if split.empty or "high_density_start_date" not in split.columns or "high_density_end_date" not in split.columns:
        return frames
    start = pd.to_datetime(split["high_density_start_date"].iloc[0], errors="coerce")
    end = pd.to_datetime(split["high_density_end_date"].iloc[0], errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return frames
    output = []
    for frame in frames:
        if frame.empty or "date" not in frame.columns:
            output.append(frame)
            continue
        temp = _with_dates(frame)
        output.append(temp[(temp["date"] >= start) & (temp["date"] <= end)].copy())
    return tuple(output)


def _representative_logs(logs: pd.DataFrame, strategy: str) -> pd.DataFrame:
    if logs.empty or "experiment" not in logs.columns:
        return pd.DataFrame()
    data = _normalize_experiments(_with_dates(logs))
    subset = data[data["experiment"] == strategy].copy()
    if subset.empty:
        return subset
    if "seed" in subset.columns:
        seed = subset["seed"].dropna().astype(str).iloc[0]
        subset = subset[subset["seed"].astype(str) == seed]
    return subset


def _preferred_strategy(logs: pd.DataFrame) -> str:
    if logs.empty or "experiment" not in logs.columns:
        return "dqn_with_basic_nlp"
    exps = list(dict.fromkeys(_normalize_experiments(logs)["experiment"].astype(str).tolist()))
    for candidate in ["dqn_with_enhanced_nlp", "dqn_with_basic_nlp", "dqn_without_nlp", "predict_then_trade"]:
        if candidate in exps:
            return candidate
    return exps[0] if exps else "dqn_with_basic_nlp"


def _normalize_experiments(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    if "experiment" in data.columns:
        data["experiment"] = data["experiment"].astype(str).replace({"dqn_with_nlp": "dqn_with_basic_nlp"})
    return data


def _with_dates(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    return data.dropna(subset=["date"]).sort_values("date")


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _latest_file(directory: Path, pattern: str, exclude_suffix: str | None = None) -> Path | None:
    if directory is None or not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if exclude_suffix is None or not path.name.endswith(exclude_suffix)]
    return sorted(files, key=lambda path: path.stat().st_mtime)[-1] if files else None


def _stock_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir() and path.name.isdigit())


def _first_present(frame: pd.DataFrame, candidates: list[str]) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    return ""
