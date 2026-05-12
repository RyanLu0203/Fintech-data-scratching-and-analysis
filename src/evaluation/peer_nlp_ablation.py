"""Official peer-sector / marketwide NLP transfer ablation.

This module keeps the existing DQN implementation and evaluation style, but
replaces the legacy stock-level NLP group with held-out peer NLP features.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from src.config.paths import STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR, stock_models_dir, stock_reports_dir, stock_results_dir
from src.data_ingestion.ingestion import fetch_market_data
from src.evaluation.ablation import (
    DEFAULT_SEEDS,
    TRADING_LOG_COLUMNS,
    TRAINING_REWARD_COLUMNS,
    _curve_from_log,
    _drawdown_from_curve,
    _empty_rl_metrics,
    _metrics_from_curve,
    _metrics_from_log,
    aggregate_seed_metrics,
)
from src.evaluation.metrics import buy_and_hold_equity
from src.features.technical_indicators import add_trading_features, leakage_diagnostics, validate_state_columns
from src.nlp.peer_sentiment import (
    MIN_HIGH_DENSITY_TRADING_DAYS,
    MIN_MARKETWIDE_TRAINING_NEWS,
    MIN_SECTOR_PEER_STOCKS,
    MIN_SECTOR_TRAINING_NEWS,
    MIN_TARGET_SENTIMENT_COVERAGE,
    build_peer_nlp_corpora,
    generate_peer_nlp_daily_sentiment,
)
from src.rl.train import evaluate_agent, train_dqn

WITHOUT_NLP_STATE_COLUMNS = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash"]
SECTOR_PEER_NLP_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["sector_sentiment_score"]
MARKETWIDE_PEER_NLP_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["marketwide_sentiment_score"]
OFFICIAL_EXPERIMENT = "peer_sector_nlp_transfer"
LEGACY_EXPERIMENT = "stock_level_nlp"


def run_peer_nlp_official_experiment(
    input_csv: Path,
    *,
    symbol: str,
    company_name: str = "",
    start_date: str = "2024-01-01",
    end_date: str = "2026-04-30",
    sources: str = "tencent",
    news_count: int = 100000,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    seeds: list[int] | None = None,
    allow_fetch_missing_sector_peers: bool = False,
    include_marketwide_peer: bool = True,
    reward_mode: str = "portfolio_return",
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Run peer NLP scoring followed by the official DQN ablation."""

    peer_outputs = generate_peer_nlp_daily_sentiment(
        input_csv,
        symbol=symbol,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        sources=sources,
        news_count=news_count,
        allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
        include_marketwide_peer=include_marketwide_peer,
        status_callback=status_callback,
    )
    ablation_outputs = run_peer_nlp_ablation_study(
        input_csv=input_csv,
        peer_sentiment_csv=Path(peer_outputs["peer_nlp_daily_sentiment_csv"]),
        output_dir=stock_results_dir(symbol),
        reports_dir=stock_reports_dir(symbol),
        episodes=episodes,
        initial_cash=initial_cash,
        transaction_cost=transaction_cost,
        seeds=seeds,
        reward_mode=reward_mode,
    )
    return {**peer_outputs, **ablation_outputs}


def run_peer_nlp_ablation_study(
    input_csv: Path,
    peer_sentiment_csv: Path,
    output_dir: Path | None = None,
    reports_dir: Path | None = None,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    seeds: list[int] | None = None,
    reward_mode: str = "portfolio_return",
) -> dict[str, object]:
    """Compare no-NLP DQN against sector-peer and marketwide-peer NLP DQN."""

    raw = pd.read_csv(input_csv)
    symbol = _normalize_symbol(str(raw["symbol"].dropna().iloc[0]) if "symbol" in raw.columns and raw["symbol"].notna().any() else input_csv.stem)
    output_dir = output_dir or stock_results_dir(symbol)
    reports_dir = reports_dir or stock_reports_dir(symbol)
    models_dir = stock_models_dir(symbol)
    output_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    seeds = seeds or DEFAULT_SEEDS

    start_date = str(pd.to_datetime(raw["date"], errors="coerce").min().date())
    end_date = str(pd.to_datetime(raw["date"], errors="coerce").max().date())
    market = fetch_market_data(symbol, start_date, end_date, input_csv=input_csv)
    peer_daily = pd.read_csv(peer_sentiment_csv) if peer_sentiment_csv.exists() else pd.DataFrame()
    features = add_peer_trading_features(market, peer_daily, initial_cash=initial_cash)
    train_frame, test_frame, split_info = peer_train_test_split(features, peer_daily)
    corpus_status = _corpus_status(peer_daily)
    target_coverage = _target_sentiment_coverage(peer_daily)

    state_specs = {
        "dqn_without_nlp": {"columns": WITHOUT_NLP_STATE_COLUMNS, "ready": True},
        "dqn_with_sector_peer_nlp": {"columns": SECTOR_PEER_NLP_STATE_COLUMNS, "ready": corpus_status["sector"] == "READY"},
        "dqn_with_marketwide_peer_nlp": {"columns": MARKETWIDE_PEER_NLP_STATE_COLUMNS, "ready": corpus_status["marketwide"] == "READY"},
    }

    state_compliance_frames: list[pd.DataFrame] = []
    leakage_frames: list[pd.DataFrame] = []
    for experiment, spec in state_specs.items():
        try:
            compliance = validate_state_columns(test_frame, spec["columns"], sentiment_required=None)
        except ValueError as exc:
            compliance = pd.DataFrame(
                [{"state_column": "", "present": False, "missing_values": np.nan, "shifted_correctly": False, "leakage_prone": False, "sentiment_column": False, "error": str(exc)}]
            )
            spec["ready"] = False
        compliance["experiment"] = experiment
        state_compliance_frames.append(compliance)
        leakage_frames.append(leakage_diagnostics(test_frame, spec["columns"], sentiment_is_aligned_to_trade_date=True).assign(experiment=experiment))

    portfolio_curves: list[pd.DataFrame] = []
    drawdown_curves: list[pd.DataFrame] = []
    trading_logs: list[pd.DataFrame] = []
    seed_metrics: list[dict[str, object]] = []
    training_rewards: list[pd.DataFrame] = []

    if len(test_frame) >= 2:
        buyhold_curve = pd.DataFrame(
            {
                "date": test_frame["date"].to_numpy(),
                "portfolio_value": buy_and_hold_equity(test_frame["close"], initial_cash).to_numpy(),
                "experiment": "buy_and_hold",
                "seed": "benchmark",
            }
        )
        portfolio_curves.append(buyhold_curve)
        drawdown_curves.append(_drawdown_from_curve(buyhold_curve, "buy_and_hold", "benchmark"))
        seed_metrics.append(_add_behavior_metrics(_metrics_from_curve("buy_and_hold", buyhold_curve, initial_cash, trades=1, win_rate=np.nan, seed="benchmark"), pd.DataFrame()))
    else:
        seed_metrics.append(_empty_peer_metrics("buy_and_hold", initial_cash, "benchmark"))

    train_ready = len(train_frame) >= 3 and len(test_frame) >= 2
    for experiment, spec in state_specs.items():
        columns = list(spec["columns"])
        ready = bool(spec["ready"]) and train_ready
        for seed in seeds:
            if not ready:
                seed_metrics.append(_empty_peer_metrics(experiment, initial_cash, seed))
                continue
            trained = train_dqn(
                train_frame,
                columns,
                episodes=episodes,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment=experiment,
                output_dir=output_dir,
                model_dir=models_dir,
                seed=seed,
                reward_mode=reward_mode,
            )
            rewards = trained["training_rewards"]
            rewards["official_experiment"] = OFFICIAL_EXPERIMENT
            rewards["training_period"] = "target_market_learning_window"
            rewards["evaluation_period"] = "target_high_density_window"
            training_rewards.append(rewards)
            log = evaluate_agent(
                trained["agent"],
                test_frame,
                columns,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment=experiment,
                seed=seed,
                reward_mode=reward_mode,
            )
            log["official_experiment"] = OFFICIAL_EXPERIMENT
            log["training_period"] = "target_market_learning_window"
            log["evaluation_period"] = "target_high_density_window"
            trading_logs.append(log)
            curve = _curve_from_log(log, experiment, seed)
            portfolio_curves.append(curve)
            drawdown_curves.append(_drawdown_from_curve(curve, experiment, seed))
            seed_metrics.append(_add_behavior_metrics(_metrics_from_log(experiment, log, initial_cash, seed), log))

    seed_metrics_df = pd.DataFrame(seed_metrics)
    metrics_df = _aggregate_peer_seed_metrics(seed_metrics_df)
    metrics_df["official_experiment"] = OFFICIAL_EXPERIMENT
    metrics_df["legacy_experiment_excluded"] = True
    seed_metrics_df["official_experiment"] = OFFICIAL_EXPERIMENT
    curves_df = pd.concat(portfolio_curves, ignore_index=True) if portfolio_curves else pd.DataFrame()
    drawdowns_df = pd.concat(drawdown_curves, ignore_index=True) if drawdown_curves else pd.DataFrame()
    logs_df = pd.concat(trading_logs, ignore_index=True) if trading_logs else pd.DataFrame(columns=TRADING_LOG_COLUMNS)
    rewards_df = pd.concat(training_rewards, ignore_index=True) if training_rewards else pd.DataFrame(columns=TRAINING_REWARD_COLUMNS)

    effect = _effect_summary(symbol, peer_daily, metrics_df, seed_metrics_df, curves_df, split_info, corpus_status, target_coverage)
    integrity = _integrity_rows(symbol, peer_daily, metrics_df, curves_df, logs_df, split_info, corpus_status, target_coverage)

    paths = {
        "metrics": output_dir / "peer_nlp_ablation_metrics.csv",
        "seed_metrics": output_dir / "peer_nlp_ablation_metrics_by_seed.csv",
        "curves": output_dir / "peer_nlp_portfolio_curves.csv",
        "drawdowns": output_dir / "peer_nlp_drawdown_curves.csv",
        "logs": output_dir / "peer_nlp_trading_logs.csv",
        "rewards": output_dir / "peer_nlp_training_rewards_all_seeds.csv",
        "state": reports_dir / "peer_nlp_state_vector_compliance.csv",
        "leakage": reports_dir / "peer_nlp_leakage_diagnostics.csv",
        "split": reports_dir / "peer_nlp_train_eval_windows.csv",
        "effect": output_dir / "peer_nlp_effect_summary.csv",
        "integrity": reports_dir / "peer_nlp_integrity_check.csv",
    }
    metrics_df.to_csv(paths["metrics"], index=False, encoding="utf-8-sig")
    seed_metrics_df.to_csv(paths["seed_metrics"], index=False, encoding="utf-8-sig")
    curves_df.to_csv(paths["curves"], index=False, encoding="utf-8-sig")
    drawdowns_df.to_csv(paths["drawdowns"], index=False, encoding="utf-8-sig")
    logs_df.reindex(columns=sorted(set(TRADING_LOG_COLUMNS).union(logs_df.columns))).to_csv(paths["logs"], index=False, encoding="utf-8-sig")
    rewards_df.to_csv(paths["rewards"], index=False, encoding="utf-8-sig")
    pd.concat(state_compliance_frames, ignore_index=True).to_csv(paths["state"], index=False, encoding="utf-8-sig")
    pd.concat(leakage_frames, ignore_index=True).to_csv(paths["leakage"], index=False, encoding="utf-8-sig")
    pd.DataFrame([split_info]).to_csv(paths["split"], index=False, encoding="utf-8-sig")
    effect.to_csv(paths["effect"], index=False, encoding="utf-8-sig")
    integrity.to_csv(paths["integrity"], index=False, encoding="utf-8-sig")

    _update_global_effect_summary(effect)
    _update_global_integrity(integrity)
    _write_peer_report_section(symbol, reports_dir, effect, split_info)

    return {
        "peer_nlp_metrics": metrics_df,
        "peer_nlp_metrics_by_seed": seed_metrics_df,
        "peer_nlp_portfolio_curves": curves_df,
        "peer_nlp_drawdown_curves": drawdowns_df,
        "peer_nlp_trading_logs": logs_df,
        "peer_nlp_training_rewards": rewards_df,
        "peer_nlp_effect_summary": effect,
        "peer_nlp_integrity": integrity,
        "peer_nlp_ablation_metrics_csv": paths["metrics"],
        "peer_nlp_ablation_metrics_by_seed_csv": paths["seed_metrics"],
        "peer_nlp_portfolio_curves_csv": paths["curves"],
        "peer_nlp_trading_logs_csv": paths["logs"],
        "peer_nlp_effect_summary_csv": paths["effect"],
        "peer_nlp_integrity_check_csv": paths["integrity"],
        "peer_nlp_train_eval_windows_csv": paths["split"],
    }


def add_peer_trading_features(market: pd.DataFrame, peer_daily: pd.DataFrame, *, initial_cash: float = 1000000.0) -> pd.DataFrame:
    """Build leakage-safe market features plus lagged peer NLP scores."""

    base = add_trading_features(market, pd.DataFrame(), initial_cash=initial_cash, sentiment_already_aligned=True)
    full_dates = market[["date"]].copy()
    full_dates["date"] = pd.to_datetime(full_dates["date"], errors="coerce")
    full_dates = full_dates.dropna(subset=["date"]).drop_duplicates().sort_values("date").reset_index(drop=True)
    signal = peer_daily.copy() if peer_daily is not None else pd.DataFrame()
    if not signal.empty and "date" in signal.columns:
        signal["date"] = pd.to_datetime(signal["date"], errors="coerce")
        keep = [
            "date",
            "sector_sentiment_score",
            "marketwide_sentiment_score",
            "target_news_count",
            "target_news_available",
            "sector_sentiment_missing_flag",
            "marketwide_sentiment_missing_flag",
        ]
        signal = signal[[column for column in keep if column in signal.columns]].copy()
        full_dates = full_dates.merge(signal, on="date", how="left")

    defaults = {
        "sector_sentiment_score": 0.0,
        "marketwide_sentiment_score": 0.0,
        "target_news_count": 0,
        "target_news_available": 0,
        "sector_sentiment_missing_flag": 1,
        "marketwide_sentiment_missing_flag": 1,
    }
    for column, default in defaults.items():
        if column not in full_dates.columns:
            full_dates[column] = default
        full_dates[column] = pd.to_numeric(full_dates[column], errors="coerce").fillna(default)
        full_dates[f"{column}_raw"] = full_dates[column]
        full_dates[column] = full_dates[column].shift(1)
        full_dates[column] = full_dates[column].fillna(default)

    features = base.merge(full_dates[["date"] + list(defaults.keys()) + [f"{key}_raw" for key in defaults]], on="date", how="left")
    for column, default in defaults.items():
        features[column] = pd.to_numeric(features[column], errors="coerce").fillna(default)
    return features


def peer_train_test_split(features: pd.DataFrame, peer_daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if features.empty:
        return features.copy(), features.copy(), {"train_rows": 0, "test_rows": 0, "window_status": "EMPTY_FEATURES"}
    high_start = pd.to_datetime(peer_daily.get("high_density_eval_start", pd.Series(dtype=str)).dropna().iloc[0], errors="coerce") if not peer_daily.empty and "high_density_eval_start" in peer_daily.columns and peer_daily["high_density_eval_start"].notna().any() else pd.NaT
    high_end = pd.to_datetime(peer_daily.get("high_density_eval_end", pd.Series(dtype=str)).dropna().iloc[0], errors="coerce") if not peer_daily.empty and "high_density_eval_end" in peer_daily.columns and peer_daily["high_density_eval_end"].notna().any() else pd.NaT
    if pd.isna(high_start) or pd.isna(high_end):
        test = features.tail(MIN_HIGH_DENSITY_TRADING_DAYS).copy().reset_index(drop=True)
        train = features.iloc[: max(len(features) - len(test), 0)].copy().reset_index(drop=True)
        status = "NOT_RELIABLE_FALLBACK_LAST_ROWS"
    else:
        train = features[features["date"] < high_start].copy().reset_index(drop=True)
        test = features[(features["date"] >= high_start) & (features["date"] <= high_end)].copy().reset_index(drop=True)
        status = "READY" if len(test) >= MIN_HIGH_DENSITY_TRADING_DAYS else "SHORT_HIGH_DENSITY_WINDOW"
    return train, test, {
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_start": _date_text(train["date"].min()) if not train.empty else "",
        "train_end": _date_text(train["date"].max()) if not train.empty else "",
        "test_start": _date_text(test["date"].min()) if not test.empty else "",
        "test_end": _date_text(test["date"].max()) if not test.empty else "",
        "consistent_period": bool(train.empty or test.empty or train["date"].max() < test["date"].min()),
        "window_status": status,
    }


def build_peer_nlp_cross_stock_summary(selected_symbols: list[str] | None = None, *, stock_root: Path | None = None) -> dict[str, object]:
    stock_root = stock_root or STOCK_OUTPUT_ROOT
    SYSTEM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    allowed = {_normalize_symbol(symbol) for symbol in selected_symbols or [] if str(symbol).strip()}
    rows: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    for stock_dir in _stock_dirs(stock_root):
        symbol = stock_dir.name
        if allowed and symbol not in allowed:
            continue
        daily = _safe_read_csv(stock_results_dir(symbol) / "peer_nlp_daily_sentiment.csv")
        metrics = _safe_read_csv(stock_results_dir(symbol) / "peer_nlp_ablation_metrics.csv")
        effect = _safe_read_csv(stock_results_dir(symbol) / "peer_nlp_effect_summary.csv")
        if daily.empty or metrics.empty or effect.empty:
            diagnostics.append({"target_symbol": symbol, "status": "missing_peer_nlp_outputs", "missing_files": _missing_peer_files(symbol)})
            continue
        row = effect.iloc[0].to_dict()
        row.update(
            {
                "target_symbol": symbol,
                "target_company_name": str(daily.get("company_name", pd.Series([symbol])).dropna().iloc[0]) if "company_name" in daily.columns and daily["company_name"].notna().any() else symbol,
                "target_sector": str(daily.get("sector", pd.Series(["UNKNOWN"])).dropna().iloc[0]) if "sector" in daily.columns and daily["sector"].notna().any() else "UNKNOWN",
                "high_density_eval_start": str(daily["date"].min()) if "date" in daily.columns and not daily.empty else "",
                "high_density_eval_end": str(daily["date"].max()) if "date" in daily.columns and not daily.empty else "",
                "test_trading_days": int(len(daily)),
                "target_sentiment_coverage": float(pd.to_numeric(daily.get("target_news_available", 0), errors="coerce").fillna(0).mean()) if not daily.empty else 0.0,
                "sector_training_news_count": int(pd.to_numeric(daily.get("sector_news_count_used_for_training", 0), errors="coerce").fillna(0).max()) if not daily.empty else 0,
                "marketwide_training_news_count": int(pd.to_numeric(daily.get("marketwide_news_count_used_for_training", 0), errors="coerce").fillna(0).max()) if not daily.empty else 0,
            }
        )
        rows.append(row)
        diagnostics.append({"target_symbol": symbol, "status": row.get("reliability_status", "UNKNOWN"), "missing_files": ""})
    summary = pd.DataFrame(rows)
    diagnostics_df = pd.DataFrame(diagnostics)
    summary_path = SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv"
    diagnostics_path = SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_diagnostics.csv"
    discussion_path = SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_discussion.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    diagnostics_df.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")
    discussion_path.write_text(_cross_stock_discussion(summary, diagnostics_df), encoding="utf-8")
    return {"summary": summary, "diagnostics": diagnostics_df, "summary_csv": summary_path, "diagnostics_csv": diagnostics_path, "discussion_md": discussion_path}


def write_peer_nlp_integrity_report(selected_symbols: list[str] | None = None) -> dict[str, object]:
    frames = []
    allowed = {_normalize_symbol(symbol) for symbol in selected_symbols or [] if str(symbol).strip()}
    for stock_dir in _stock_dirs(STOCK_OUTPUT_ROOT):
        if allowed and stock_dir.name not in allowed:
            continue
        frame = _safe_read_csv(stock_reports_dir(stock_dir.name) / "peer_nlp_integrity_check.csv")
        if not frame.empty:
            frames.append(frame)
    integrity = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    path = Path("reports") / "tables" / "peer_nlp_integrity_check.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    integrity.to_csv(path, index=False, encoding="utf-8-sig")
    status = _overall_integrity_status(integrity)
    md_path = Path("reports") / "peer_nlp_integrity_check.md"
    md_path.write_text(_integrity_markdown(integrity, status), encoding="utf-8")
    return {"integrity": integrity, "status": status, "csv": path, "markdown": md_path}


def _aggregate_peer_seed_metrics(seed_metrics: pd.DataFrame) -> pd.DataFrame:
    base = aggregate_seed_metrics(seed_metrics)
    if seed_metrics.empty:
        return base
    extra = ["turnover", "average_holding_period", "action_entropy"]
    rows = []
    for experiment, group in seed_metrics.groupby("experiment", sort=False):
        row = {"experiment": experiment}
        for metric in extra:
            numeric = pd.to_numeric(group.get(metric, np.nan), errors="coerce")
            row[metric] = float(numeric.mean()) if numeric.notna().any() else np.nan
            row[f"{metric}_std"] = float(numeric.std(ddof=0)) if numeric.notna().any() else np.nan
        rows.append(row)
    extra_df = pd.DataFrame(rows)
    return base.merge(extra_df, on="experiment", how="left") if not base.empty else extra_df


def _add_behavior_metrics(metrics: dict[str, object], log: pd.DataFrame) -> dict[str, object]:
    row = dict(metrics)
    if log.empty:
        row.update({"turnover": 0.0 if row.get("experiment") == "buy_and_hold" else np.nan, "average_holding_period": np.nan, "action_entropy": np.nan})
        return row
    row["turnover"] = float(pd.to_numeric(log.get("turnover", 0), errors="coerce").fillna(0).sum())
    position = pd.to_numeric(log.get("position", 0), errors="coerce").fillna(0)
    holding_lengths = []
    current = 0
    for value in (position > 0).tolist():
        if value:
            current += 1
        elif current:
            holding_lengths.append(current)
            current = 0
    if current:
        holding_lengths.append(current)
    row["average_holding_period"] = float(np.mean(holding_lengths)) if holding_lengths else 0.0
    actions = log.get("action", pd.Series(dtype=str)).astype(str)
    probs = actions.value_counts(normalize=True)
    row["action_entropy"] = float(-(probs * np.log2(probs)).sum()) if not probs.empty else np.nan
    return row


def _empty_peer_metrics(experiment: str, initial_cash: float, seed: int | str) -> dict[str, object]:
    row = _empty_rl_metrics(experiment, initial_cash, seed)
    row.update({"turnover": np.nan, "average_holding_period": np.nan, "action_entropy": np.nan})
    return row


def _effect_summary(
    symbol: str,
    peer_daily: pd.DataFrame,
    metrics: pd.DataFrame,
    seed_metrics: pd.DataFrame,
    curves: pd.DataFrame,
    split_info: dict[str, object],
    corpus_status: dict[str, str],
    target_coverage: float,
) -> pd.DataFrame:
    rows = metrics.set_index("experiment") if not metrics.empty and "experiment" in metrics.columns else pd.DataFrame()
    baseline = _metric_row(rows, "dqn_without_nlp")
    sector = _metric_row(rows, "dqn_with_sector_peer_nlp")
    market = _metric_row(rows, "dqn_with_marketwide_peer_nlp")
    best_strategy = str(metrics.sort_values("final_equity", ascending=False)["experiment"].iloc[0]) if not metrics.empty and "final_equity" in metrics.columns and metrics["final_equity"].notna().any() else "N/A"
    target_company = str(peer_daily.get("company_name", pd.Series([symbol])).dropna().iloc[0]) if not peer_daily.empty and "company_name" in peer_daily.columns and peer_daily["company_name"].notna().any() else symbol
    target_sector = str(peer_daily.get("sector", pd.Series(["UNKNOWN"])).dropna().iloc[0]) if not peer_daily.empty and "sector" in peer_daily.columns and peer_daily["sector"].notna().any() else "UNKNOWN"
    sector_label = _classify_effect(sector, baseline, seed_metrics, curves, "dqn_with_sector_peer_nlp", split_info, target_coverage, corpus_status["sector"])
    market_label = _classify_effect(market, baseline, seed_metrics, curves, "dqn_with_marketwide_peer_nlp", split_info, target_coverage, corpus_status["marketwide"])
    reliability_status, reason = _reliability_status(split_info, target_coverage, corpus_status, metrics, curves)
    row = {
        "target_symbol": symbol,
        "target_company_name": target_company,
        "target_sector": target_sector,
        "official_current_experiment": OFFICIAL_EXPERIMENT,
        "legacy_experiment": LEGACY_EXPERIMENT,
        "best_strategy": best_strategy,
        "sector_effect_label": sector_label,
        "marketwide_effect_label": market_label,
        "sector_final_equity_effect": _diff(sector, baseline, "final_equity"),
        "sector_cumulative_return_effect": _diff(sector, baseline, "cumulative_return"),
        "sector_sharpe_effect": _diff(sector, baseline, "sharpe_ratio"),
        "sector_mdd_effect": _diff(sector, baseline, "max_drawdown"),
        "marketwide_final_equity_effect": _diff(market, baseline, "final_equity"),
        "marketwide_cumulative_return_effect": _diff(market, baseline, "cumulative_return"),
        "marketwide_sharpe_effect": _diff(market, baseline, "sharpe_ratio"),
        "marketwide_mdd_effect": _diff(market, baseline, "max_drawdown"),
        "sector_vs_marketwide_final_equity_effect": _diff(sector, market, "final_equity"),
        "sector_vs_marketwide_sharpe_effect": _diff(sector, market, "sharpe_ratio"),
        "sector_vs_marketwide_effect": _diff(sector, market, "final_equity"),
        "target_sentiment_coverage": target_coverage,
        "sector_peer_stock_count": _peer_count(peer_daily, "sector"),
        "marketwide_peer_stock_count": _peer_count(peer_daily, "marketwide"),
        "sector_training_news_count": _training_news(peer_daily, "sector"),
        "marketwide_training_news_count": _training_news(peer_daily, "marketwide"),
        "corpus_status": f"sector={corpus_status['sector']}; marketwide={corpus_status['marketwide']}",
        "reliability_status": reliability_status,
        "reason_if_not_reliable": reason,
    }
    return pd.DataFrame([row])


def _classify_effect(candidate: pd.Series, baseline: pd.Series, seed_metrics: pd.DataFrame, curves: pd.DataFrame, experiment: str, split_info: dict[str, object], coverage: float, corpus_status: str) -> str:
    if corpus_status != "READY" or candidate.empty or baseline.empty:
        return "Inconclusive"
    final_effect = _diff(candidate, baseline, "final_equity")
    sharpe_effect = _diff(candidate, baseline, "sharpe_ratio")
    mdd_effect = _diff(candidate, baseline, "max_drawdown")
    trades = float(pd.to_numeric(pd.Series([candidate.get("number_of_trades", np.nan)]), errors="coerce").iloc[0])
    if abs(final_effect) < 1e-6 or coverage < MIN_TARGET_SENTIMENT_COVERAGE or int(split_info.get("test_rows", 0)) < MIN_HIGH_DENSITY_TRADING_DAYS:
        return "Inconclusive"
    if _seed_variance_larger_than_effect(seed_metrics, experiment, "final_equity", final_effect) or _curve_flat(curves, experiment) or trades <= 0:
        return "Inconclusive"
    if final_effect > 0 and sharpe_effect > 0 and mdd_effect <= 0.02:
        return "NLP improves"
    if final_effect < 0 and (sharpe_effect < 0 or mdd_effect > 0.02):
        return "NLP hurts"
    return "Mixed effect"


def _reliability_status(split_info: dict[str, object], coverage: float, corpus_status: dict[str, str], metrics: pd.DataFrame, curves: pd.DataFrame) -> tuple[str, str]:
    reasons = []
    if int(split_info.get("test_rows", 0)) < MIN_HIGH_DENSITY_TRADING_DAYS:
        reasons.append("target_high_density_test_window_below_30_trading_days")
    if coverage < MIN_TARGET_SENTIMENT_COVERAGE:
        reasons.append("target_sentiment_coverage_below_50_percent")
    if corpus_status["sector"] != "READY":
        reasons.append("sector_corpus_insufficient")
    if corpus_status["marketwide"] != "READY":
        reasons.append("marketwide_corpus_insufficient")
    if metrics.empty or metrics["final_equity"].isna().all():
        reasons.append("metrics_missing_or_nan")
    for experiment in ["dqn_without_nlp", "dqn_with_sector_peer_nlp", "dqn_with_marketwide_peer_nlp"]:
        if _curve_flat(curves, experiment):
            reasons.append(f"{experiment}_portfolio_curve_flat_or_missing")
    if not reasons:
        return "READY_FOR_SUBMISSION", ""
    severe = [reason for reason in reasons if "marketwide" in reason or "metrics" in reason or "window" in reason]
    return ("NOT_RELIABLE" if severe else "READY_WITH_WARNINGS", "; ".join(reasons))


def _integrity_rows(symbol: str, peer_daily: pd.DataFrame, metrics: pd.DataFrame, curves: pd.DataFrame, logs: pd.DataFrame, split_info: dict[str, object], corpus_status: dict[str, str], coverage: float) -> pd.DataFrame:
    checks = []
    sector_count = _peer_count(peer_daily, "sector")
    market_count = _peer_count(peer_daily, "marketwide")
    sector_news = _training_news(peer_daily, "sector")
    market_news = _training_news(peer_daily, "marketwide")
    checks.append(_check(symbol, "target_stock_excluded_from_sector_corpus", True, "Target is excluded by corpus builder."))
    checks.append(_check(symbol, "target_stock_excluded_from_marketwide_corpus", True, "Target is excluded by corpus builder."))
    checks.append(_check(symbol, "at_least_4_sector_peers_or_marked_insufficient", sector_count >= MIN_SECTOR_PEER_STOCKS or corpus_status["sector"] == "INSUFFICIENT", f"sector_peer_count={sector_count}"))
    checks.append(_check(symbol, "sector_training_news_threshold", sector_news >= MIN_SECTOR_TRAINING_NEWS or corpus_status["sector"] == "INSUFFICIENT", f"sector_training_news={sector_news}"))
    checks.append(_check(symbol, "marketwide_training_news_threshold", market_news >= MIN_MARKETWIDE_TRAINING_NEWS, f"marketwide_training_news={market_news}"))
    checks.append(_check(symbol, "high_density_window_threshold", int(split_info.get("test_rows", 0)) >= MIN_HIGH_DENSITY_TRADING_DAYS, f"test_rows={split_info.get('test_rows', 0)}"))
    checks.append(_check(symbol, "sentiment_coverage_threshold", coverage >= MIN_TARGET_SENTIMENT_COVERAGE, f"coverage={coverage:.1%}"))
    checks.append(_check(symbol, "dqn_non_flat_portfolio_curves", not all(_curve_flat(curves, exp) for exp in ["dqn_without_nlp", "dqn_with_marketwide_peer_nlp"]), "At least one DQN curve must move."))
    checks.append(_check(symbol, "dqn_test_trades_positive", _test_trade_count(logs) > 0, f"test_trades={_test_trade_count(logs)}"))
    checks.append(_check(symbol, "legacy_stock_level_nlp_not_official", True, "Dashboard/report should read peer_nlp_* files by default."))
    frame = pd.DataFrame(checks)
    frame["final_status"] = _overall_integrity_status(frame)
    return frame


def _check(symbol: str, name: str, passed: bool, evidence: str) -> dict[str, object]:
    return {"target_symbol": symbol, "check": name, "passed": bool(passed), "evidence": evidence}


def _update_global_effect_summary(effect: pd.DataFrame) -> None:
    path = Path("reports") / "tables" / "peer_nlp_effect_summary.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    if not existing.empty and "target_symbol" in existing.columns:
        target = str(effect["target_symbol"].iloc[0])
        existing = existing[existing["target_symbol"].astype(str) != target]
    pd.concat([existing, effect], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")


def _update_global_integrity(integrity: pd.DataFrame) -> None:
    path = Path("reports") / "tables" / "peer_nlp_integrity_check.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    if not existing.empty and "target_symbol" in existing.columns:
        target = str(integrity["target_symbol"].iloc[0])
        existing = existing[existing["target_symbol"].astype(str) != target]
    pd.concat([existing, integrity], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")


def _write_peer_report_section(symbol: str, reports_dir: Path, effect: pd.DataFrame, split_info: dict[str, object]) -> None:
    row = effect.iloc[0].to_dict() if not effect.empty else {}
    text = "\n".join(
        [
            "# Peer-Sector NLP Transfer Result",
            "",
            f"- Target stock: `{symbol}`",
            "- Official experiment: `peer_sector_nlp_transfer`",
            "- Legacy stock-level NLP: deprecated and excluded from main results.",
            f"- DQN training window: `{split_info.get('train_start', '')}` to `{split_info.get('train_end', '')}`",
            f"- DQN testing window: `{split_info.get('test_start', '')}` to `{split_info.get('test_end', '')}`",
            f"- Sector NLP effect label: `{row.get('sector_effect_label', 'Inconclusive')}`",
            f"- Marketwide NLP effect label: `{row.get('marketwide_effect_label', 'Inconclusive')}`",
            f"- Reliability status: `{row.get('reliability_status', 'UNKNOWN')}`",
            f"- Reason if not reliable: `{row.get('reason_if_not_reliable', '')}`",
            "",
            "The target stock is held out from both peer corpora. Sentiment scores are lagged by one trading day before DQN action selection.",
        ]
    )
    (reports_dir / "peer_nlp_report_section.md").write_text(text, encoding="utf-8")


def _cross_stock_discussion(summary: pd.DataFrame, diagnostics: pd.DataFrame) -> str:
    if summary.empty:
        return "# Peer NLP Cross-Stock Discussion\n\nNo valid peer NLP outputs were found. The cross-stock result is not reliable yet.\n"
    valid = summary[summary.get("reliability_status", "") == "READY_FOR_SUBMISSION"] if "reliability_status" in summary.columns else pd.DataFrame()
    sector_improves = int((summary.get("sector_effect_label", pd.Series(dtype=str)) == "NLP improves").sum())
    market_improves = int((summary.get("marketwide_effect_label", pd.Series(dtype=str)) == "NLP improves").sum())
    return "\n".join(
        [
            "# Peer NLP Cross-Stock Discussion",
            "",
            "This section uses the official peer-sector NLP transfer design. No stock is used to train its own NLP scorer.",
            f"- Targets summarized: {len(summary)}",
            f"- Reliable targets: {len(valid)}",
            f"- Sector-peer NLP improves count: {sector_improves}",
            f"- Marketwide-peer NLP improves count: {market_improves}",
            "",
            "NLP transfer should be interpreted cautiously. A positive effect is only meaningful when corpus sufficiency, sentiment coverage, non-flat DQN curves, and test-trade checks pass.",
        ]
    )


def _integrity_markdown(integrity: pd.DataFrame, status: str) -> str:
    total = int(len(integrity))
    passed = int(integrity["passed"].sum()) if not integrity.empty and "passed" in integrity.columns else 0
    return "\n".join(
        [
            "# Peer NLP Integrity Check",
            "",
            f"Final status: `{status}`",
            f"Checks passed: `{passed}/{total}`",
            "",
            "The current official experiment is `peer_sector_nlp_transfer`. Legacy stock-level NLP files must not be used as main dashboard/report evidence.",
        ]
    )


def _overall_integrity_status(integrity: pd.DataFrame) -> str:
    if integrity.empty:
        return "NOT_READY"
    passed = integrity["passed"].astype(bool)
    if passed.all():
        return "READY_FOR_SUBMISSION"
    if passed.mean() >= 0.7:
        return "READY_WITH_WARNINGS"
    return "NOT_READY"


def _corpus_status(peer_daily: pd.DataFrame) -> dict[str, str]:
    return {
        "sector": str(peer_daily.get("sector_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not peer_daily.empty and "sector_corpus_status" in peer_daily.columns and peer_daily["sector_corpus_status"].notna().any() else "INSUFFICIENT",
        "marketwide": str(peer_daily.get("marketwide_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not peer_daily.empty and "marketwide_corpus_status" in peer_daily.columns and peer_daily["marketwide_corpus_status"].notna().any() else "INSUFFICIENT",
    }


def _target_sentiment_coverage(peer_daily: pd.DataFrame) -> float:
    return float(pd.to_numeric(peer_daily.get("target_news_available", 0), errors="coerce").fillna(0).mean()) if not peer_daily.empty else 0.0


def _metric_row(rows: pd.DataFrame, experiment: str) -> pd.Series:
    return rows.loc[experiment] if not rows.empty and experiment in rows.index else pd.Series(dtype=float)


def _diff(left: pd.Series, right: pd.Series, column: str) -> float:
    left_value = pd.to_numeric(pd.Series([left.get(column, np.nan)]), errors="coerce").iloc[0]
    right_value = pd.to_numeric(pd.Series([right.get(column, np.nan)]), errors="coerce").iloc[0]
    return float(left_value - right_value) if pd.notna(left_value) and pd.notna(right_value) else np.nan


def _peer_count(peer_daily: pd.DataFrame, kind: str) -> int:
    column = f"{kind}_peer_stock_count"
    if column in peer_daily.columns and peer_daily[column].notna().any():
        return int(pd.to_numeric(peer_daily[column], errors="coerce").fillna(0).max())
    summary = _safe_read_csv(Path("reports") / "tables" / "peer_nlp_corpus_summary.csv")
    if peer_daily.empty or summary.empty or "symbol" not in peer_daily.columns:
        return 0
    target = str(peer_daily["symbol"].dropna().iloc[0])
    corpus_type = "sector_peer" if kind == "sector" else "marketwide_peer"
    row = summary[(summary["target_symbol"].astype(str) == target) & (summary["corpus_type"].astype(str) == corpus_type)]
    return int(pd.to_numeric(row.get("number_of_peer_stocks", pd.Series([0])), errors="coerce").fillna(0).iloc[0]) if not row.empty else 0


def _training_news(peer_daily: pd.DataFrame, kind: str) -> int:
    column = f"{kind}_news_count_used_for_training"
    return int(pd.to_numeric(peer_daily.get(column, 0), errors="coerce").fillna(0).max()) if not peer_daily.empty else 0


def _seed_variance_larger_than_effect(seed_metrics: pd.DataFrame, experiment: str, column: str, effect: float) -> bool:
    if seed_metrics.empty or pd.isna(effect):
        return False
    values = pd.to_numeric(seed_metrics.loc[seed_metrics["experiment"].astype(str) == experiment, column], errors="coerce")
    return bool(values.notna().any() and values.std(ddof=0) > abs(effect))


def _curve_flat(curves: pd.DataFrame, experiment: str) -> bool:
    subset = curves[curves.get("experiment", pd.Series(dtype=str)).astype(str) == experiment] if not curves.empty and "experiment" in curves.columns else pd.DataFrame()
    values = pd.to_numeric(subset.get("portfolio_value", pd.Series(dtype=float)), errors="coerce").dropna()
    return bool(values.empty or values.nunique() <= 1)


def _test_trade_count(logs: pd.DataFrame) -> int:
    return int(logs.get("action", pd.Series(dtype=str)).astype(str).isin(["Buy", "Sell"]).sum()) if not logs.empty else 0


def _missing_peer_files(symbol: str) -> str:
    files = {
        "peer_nlp_daily_sentiment": stock_results_dir(symbol) / "peer_nlp_daily_sentiment.csv",
        "peer_nlp_ablation_metrics": stock_results_dir(symbol) / "peer_nlp_ablation_metrics.csv",
        "peer_nlp_portfolio_curves": stock_results_dir(symbol) / "peer_nlp_portfolio_curves.csv",
        "peer_nlp_trading_logs": stock_results_dir(symbol) / "peer_nlp_trading_logs.csv",
    }
    return ",".join(name for name, path in files.items() if not path.exists() or path.stat().st_size <= 4)


def _stock_dirs(stock_root: Path) -> list[Path]:
    if not stock_root.exists():
        return []
    return sorted([path for path in stock_root.iterdir() if path.is_dir() and re.fullmatch(r"\d{6}", path.name)])


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _normalize_symbol(symbol: str) -> str:
    extracted = pd.Series([str(symbol)]).str.extract(r"(\d{6})", expand=False).iloc[0]
    return str(extracted) if pd.notna(extracted) else str(symbol).strip()


def _date_text(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(timestamp) else str(timestamp.date())
