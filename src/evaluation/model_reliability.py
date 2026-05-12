"""Model reliability and interpretability diagnostics from cached outputs."""

from __future__ import annotations

from math import log2
from pathlib import Path

import numpy as np
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT
from src.evaluation.metrics import max_drawdown, sharpe_ratio
from src.features.technical_indicators import add_enhanced_nlp_features

REPORT_TABLES = PROJECT_ROOT / "reports" / "tables"


def generate_model_reliability_outputs(stock_root: Path | None = None, output_dir: Path | None = None) -> dict[str, Path]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    output_dir = output_dir or REPORT_TABLES
    output_dir.mkdir(parents=True, exist_ok=True)

    stability = build_model_stability_summary(stock_root)
    actions = build_action_behavior_diagnostics(stock_root)
    enhanced = build_enhanced_nlp_ablation_metrics(stock_root)
    signals = build_enhanced_signal_diagnostics(stock_root)

    paths = {
        "model_stability_summary": output_dir / "model_stability_summary.csv",
        "action_behavior_diagnostics": output_dir / "action_behavior_diagnostics.csv",
        "enhanced_nlp_ablation_metrics": output_dir / "enhanced_nlp_ablation_metrics.csv",
        "signal_diagnostics_enhanced": output_dir / "signal_diagnostics_enhanced.csv",
        "model_optimization_summary": PROJECT_ROOT / "reports" / "model_optimization_summary.md",
    }
    stability.to_csv(paths["model_stability_summary"], index=False, encoding="utf-8-sig")
    actions.to_csv(paths["action_behavior_diagnostics"], index=False, encoding="utf-8-sig")
    enhanced.to_csv(paths["enhanced_nlp_ablation_metrics"], index=False, encoding="utf-8-sig")
    signals.to_csv(paths["signal_diagnostics_enhanced"], index=False, encoding="utf-8-sig")
    paths["model_optimization_summary"].write_text(
        build_model_optimization_markdown(stability, actions, enhanced, signals),
        encoding="utf-8",
    )
    return paths


def build_model_stability_summary(stock_root: Path) -> pd.DataFrame:
    rows = []
    for stock_dir in _stock_dirs(stock_root):
        path = stock_dir / "results" / "ablation_metrics_by_seed.csv"
        data = _safe_read_csv(path)
        if data.empty:
            continue
        for experiment, group in data.groupby("experiment"):
            if experiment == "buy_and_hold":
                continue
            row = {"symbol": stock_dir.name, "experiment": experiment, "seed_count": int(group["seed"].nunique())}
            for metric in ["final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown", "number_of_trades", "exposure_ratio"]:
                values = pd.to_numeric(group.get(metric), errors="coerce")
                row[f"{metric}_mean"] = float(values.mean()) if values.notna().any() else np.nan
                row[f"{metric}_std"] = float(values.std(ddof=0)) if values.notna().any() else np.nan
            mean_equity = row.get("final_equity_mean", np.nan)
            std_equity = row.get("final_equity_std", np.nan)
            row["high_variance_warning"] = bool(pd.notna(mean_equity) and pd.notna(std_equity) and abs(std_equity) > 0.1 * max(abs(mean_equity), 1.0))
            row["stability_label"] = "Warning" if row["high_variance_warning"] else "Stable"
            rows.append(row)
    return pd.DataFrame(rows)


def build_action_behavior_diagnostics(stock_root: Path) -> pd.DataFrame:
    rows = []
    for stock_dir in _stock_dirs(stock_root):
        logs = _safe_read_csv(stock_dir / "results" / "trading_logs.csv")
        if logs.empty or "experiment" not in logs.columns:
            continue
        if "seed" not in logs.columns:
            logs["seed"] = "unknown"
        for keys, group in logs.groupby(["experiment", "seed"], dropna=False):
            experiment, seed = keys
            actions = group.get("action", pd.Series(dtype=str)).astype(str)
            counts = actions.value_counts()
            total = max(int(len(group)), 1)
            buy_count = int(counts.get("Buy", 0))
            sell_count = int(counts.get("Sell", 0))
            hold_count = int(counts.get("Hold", 0))
            positions = pd.to_numeric(_series_or_default(group, "position", 0), errors="coerce").fillna(0)
            portfolio = pd.to_numeric(_series_or_default(group, "portfolio_value", 0), errors="coerce").fillna(0)
            costs = pd.to_numeric(_series_or_default(group, "transaction_cost", 0), errors="coerce").fillna(0)
            turnover = float(costs.sum() / max(float(portfolio.iloc[0]) if not portfolio.empty else 1.0, 1.0))
            entropy = _action_entropy([hold_count, buy_count, sell_count])
            average_holding = _average_holding_period(positions)
            hold_ratio = hold_count / total
            warnings = []
            if hold_ratio > 0.9:
                warnings.append("Hold ratio > 90%; model may be too conservative")
            if turnover > 0.2:
                warnings.append("Turnover is high; model may be overtrading")
            rows.append(
                {
                    "symbol": stock_dir.name,
                    "experiment": experiment,
                    "seed": seed,
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "hold_count": hold_count,
                    "hold_ratio": hold_ratio,
                    "number_of_trades": buy_count + sell_count,
                    "exposure_ratio": float((positions > 0).mean()) if total else np.nan,
                    "average_holding_period": average_holding,
                    "turnover": turnover,
                    "action_entropy": entropy,
                    "warning": "; ".join(warnings) if warnings else "ok",
                }
            )
    return pd.DataFrame(rows)


def build_enhanced_nlp_ablation_metrics(stock_root: Path) -> pd.DataFrame:
    rows = []
    for stock_dir in _stock_dirs(stock_root):
        metrics = _safe_read_csv(stock_dir / "results" / "ablation_metrics.csv")
        if metrics.empty:
            continue
        for _, row in metrics.iterrows():
            item = row.to_dict()
            item["symbol"] = stock_dir.name
            if item.get("experiment") == "dqn_with_nlp":
                item["experiment"] = "dqn_with_basic_nlp"
            item["enhanced_nlp_status"] = "existing_cached_result"
            rows.append(item)
        if "dqn_with_enhanced_nlp" not in set(metrics.get("experiment", pd.Series(dtype=str)).astype(str)):
            template = {"symbol": stock_dir.name, "experiment": "dqn_with_enhanced_nlp", "enhanced_nlp_status": "not_run_by_default"}
            for column in metrics.columns:
                template.setdefault(column, np.nan)
            rows.append(template)
    return pd.DataFrame(rows)


def build_enhanced_signal_diagnostics(stock_root: Path) -> pd.DataFrame:
    rows = []
    for stock_dir in _stock_dirs(stock_root):
        market_path = _latest_file(stock_dir / "data", "*_finance_text_*.csv", exclude_suffix="_master.csv")
        sentiment_path = _latest_file(stock_dir / "reports", "*_daily_sentiment.csv")
        market = _safe_read_csv(market_path)
        sentiment = _safe_read_csv(sentiment_path)
        if market.empty or sentiment.empty:
            continue
        market["date"] = pd.to_datetime(market["date"], errors="coerce")
        market["close"] = pd.to_numeric(market.get("close"), errors="coerce")
        market = market.dropna(subset=["date", "close"]).sort_values("date")
        market["next_day_return"] = market["close"].shift(-1) / market["close"] - 1
        sentiment["date"] = pd.to_datetime(sentiment["date"], errors="coerce")
        if "sentiment_score" not in sentiment.columns and "daily_sentiment_score" in sentiment.columns:
            sentiment["sentiment_score"] = sentiment["daily_sentiment_score"]
        enhanced = add_enhanced_nlp_features(sentiment)
        merged = market.merge(enhanced, on="date", how="left")
        for feature in [
            "sentiment_score",
            "sentiment_rolling_3d",
            "sentiment_rolling_5d",
            "news_count",
            "positive_ratio",
            "negative_ratio",
        ]:
            metric_feature = "sentiment" if feature == "sentiment_score" else feature
            rows.append(
                {
                    "symbol": stock_dir.name,
                    "metric": f"{metric_feature}_next_day_return_corr",
                    "value": _corr(merged.get(feature), merged.get("next_day_return")),
                    "feature": feature,
                    "warning": "low_or_no_signal" if merged.get(feature, pd.Series(dtype=float)).fillna(0).abs().sum() == 0 else "ok",
                }
            )
    return pd.DataFrame(rows)


def build_model_optimization_markdown(
    stability: pd.DataFrame,
    actions: pd.DataFrame,
    enhanced: pd.DataFrame,
    signals: pd.DataFrame,
) -> str:
    high_var = int(stability.get("high_variance_warning", pd.Series(dtype=bool)).fillna(False).sum()) if not stability.empty else 0
    action_warn = actions[actions.get("warning", pd.Series(dtype=str)).astype(str) != "ok"] if not actions.empty else pd.DataFrame()
    enhanced_status = ", ".join(sorted(set(enhanced.get("enhanced_nlp_status", pd.Series(dtype=str)).dropna().astype(str)))) if not enhanced.empty else "missing"
    return "\n".join(
        [
            "# Model Optimization and Reliability Summary",
            "",
            "This report is generated from cached outputs. It does not scrape data or retrain DQN by default.",
            "",
            "## DQN Stability",
            f"- High-variance experiment rows: `{high_var}`",
            "- If improvement is smaller than seed-level standard deviation, treat the NLP effect as inconclusive.",
            "",
            "## Action Behavior",
            f"- Action-warning rows: `{len(action_warn)}`",
            "- Hold ratio above 90% means the model may be too conservative.",
            "- High turnover means the model may be overtrading.",
            "",
            "## Enhanced NLP Ablation",
            f"- Enhanced NLP status values: `{enhanced_status}`",
            "- `dqn_with_enhanced_nlp` is not trained by default. Use an explicit configured run before claiming enhanced NLP performance.",
            "",
            "## Reward Modes",
            "- Default: `portfolio_return`.",
            "- Optional: `portfolio_return_minus_turnover_penalty`, `portfolio_return_minus_drawdown_penalty`.",
            "- The chosen reward mode is written to trading logs when DQN is run.",
            "",
            "## Enhanced Signal Diagnostics",
            "- Enhanced feature correlations are saved in `signal_diagnostics_enhanced.csv`.",
            "- Low correlations or low sentiment coverage should lower confidence in NLP-driven conclusions.",
        ]
    )


def _stock_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir() and path.name.isdigit())


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


def _corr(left: pd.Series | None, right: pd.Series | None) -> float:
    if left is None or right is None:
        return float("nan")
    frame = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")}).dropna()
    if len(frame) < 3 or frame["left"].std() == 0 or frame["right"].std() == 0:
        return float("nan")
    return float(frame["left"].corr(frame["right"]))


def _action_entropy(counts: list[int]) -> float:
    total = sum(counts)
    if total <= 0:
        return 0.0
    probs = [count / total for count in counts if count > 0]
    return float(-sum(prob * log2(prob) for prob in probs))


def _average_holding_period(positions: pd.Series) -> float:
    exposure = positions > 0
    periods = []
    current = 0
    for value in exposure.tolist():
        if value:
            current += 1
        elif current:
            periods.append(current)
            current = 0
    if current:
        periods.append(current)
    return float(np.mean(periods)) if periods else 0.0


def _series_or_default(frame: pd.DataFrame, column: str, default: object) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)
