"""Ablation experiments for NLP-driven RL trading."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.config.paths import stock_models_dir, stock_reports_dir, stock_results_dir
from src.data_ingestion.ingestion import fetch_market_data
from src.evaluation.cross_stock import classify_nlp_effect
from src.evaluation.metrics import (
    annualized_return,
    annualized_volatility,
    buy_and_hold_equity,
    calmar_ratio,
    drawdown_series,
    max_drawdown,
    portfolio_returns,
    profit_factor_from_rewards,
    sharpe_ratio,
    sortino_ratio,
    value_at_risk,
)
from src.features import technical_indicators as ti
from src.rl.train import evaluate_agent, train_dqn
from src.evaluation.information_density import detect_information_density_split, define_experiment_window


DEFAULT_SEEDS = [42, 123, 2024, 2025, 3407]
STATE_COLUMNS = getattr(ti, "STATE_COLUMNS", ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "sentiment_score"])
ENHANCED_NLP_STATE_COLUMNS = getattr(
    ti,
    "ENHANCED_NLP_STATE_COLUMNS",
    ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "sentiment_score", "sentiment_rolling_3d", "sentiment_rolling_5d", "news_count"],
)
WITHOUT_NLP_STATE_COLUMNS = getattr(ti, "WITHOUT_NLP_STATE_COLUMNS", ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash"])
add_trading_features = ti.add_trading_features
validate_state_columns = ti.validate_state_columns
leakage_diagnostics = ti.leakage_diagnostics

TRADING_LOG_COLUMNS = [
    "episode",
    "date",
    "action",
    "reward",
    "position",
    "cash",
    "portfolio_value",
    "price",
    "shares",
    "portfolio_value_t",
    "transaction_cost",
    "turnover",
    "drawdown",
    "reward_mode",
    "seed",
    "experiment",
    "experiment_name",
]
TRAINING_REWARD_COLUMNS = ["episode", "total_reward", "epsilon", "experiment", "seed", "loss", "reward_mode"]


def run_ablation_study(
    input_csv: Path,
    sentiment_csv: Path,
    output_dir: Path | None = None,
    reports_dir: Path | None = None,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    seeds: list[int] | None = None,
    reward_mode: str = "portfolio_return",
) -> dict[str, pd.DataFrame | Path | dict[str, object]]:
    """Run buy-and-hold, DQN without NLP, and DQN with NLP on one shared test set."""

    raw = pd.read_csv(input_csv)
    symbol = str(raw["symbol"].dropna().iloc[0]) if "symbol" in raw.columns and raw["symbol"].notna().any() else input_csv.stem.split("_")[0]
    output_dir = output_dir or stock_results_dir(symbol)
    reports_dir = reports_dir or stock_reports_dir(symbol)
    models_dir = stock_models_dir(symbol)
    output_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    seeds = seeds or DEFAULT_SEEDS

    start_date = str(pd.to_datetime(raw["date"]).min().date())
    end_date = str(pd.to_datetime(raw["date"]).max().date())
    market = fetch_market_data(symbol, start_date, end_date, input_csv=input_csv)
    sentiment = pd.read_csv(sentiment_csv) if sentiment_csv.exists() else pd.DataFrame()
    features = add_trading_features(market, sentiment, initial_cash=initial_cash, sentiment_already_aligned=True)
    train_frame, test_frame, split_info = chronological_train_test_split(features)

    with_state_compliance = validate_state_columns(test_frame, STATE_COLUMNS, sentiment_required=True)
    with_state_compliance["experiment"] = "dqn_with_nlp"
    without_state_compliance = validate_state_columns(test_frame, WITHOUT_NLP_STATE_COLUMNS, sentiment_required=False)
    without_state_compliance["experiment"] = "dqn_without_nlp"
    state_compliance = pd.concat([with_state_compliance, without_state_compliance], ignore_index=True)
    leakage_report = pd.concat(
        [
            leakage_diagnostics(test_frame, WITHOUT_NLP_STATE_COLUMNS, sentiment_is_aligned_to_trade_date=True).assign(experiment="dqn_without_nlp"),
            leakage_diagnostics(test_frame, STATE_COLUMNS, sentiment_is_aligned_to_trade_date=True).assign(experiment="dqn_with_nlp"),
        ],
        ignore_index=True,
    )

    portfolio_curves: list[pd.DataFrame] = []
    drawdown_curves: list[pd.DataFrame] = []
    trading_logs: list[pd.DataFrame] = []
    seed_metrics: list[dict[str, object]] = []
    training_rewards: list[pd.DataFrame] = []

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
    seed_metrics.append(_metrics_from_curve("buy_and_hold", buyhold_curve, initial_cash, trades=1, win_rate=np.nan, seed="benchmark"))

    if len(train_frame) >= 3 and len(test_frame) >= 2:
        for seed in seeds:
            without = train_dqn(
                train_frame,
                WITHOUT_NLP_STATE_COLUMNS,
                episodes=episodes,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment="dqn_without_nlp",
                output_dir=output_dir,
                model_dir=models_dir,
                seed=seed,
                reward_mode=reward_mode,
            )
            training_rewards.append(without["training_rewards"])
            without_log = evaluate_agent(
                without["agent"],
                test_frame,
                WITHOUT_NLP_STATE_COLUMNS,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment="dqn_without_nlp",
                seed=seed,
                reward_mode=reward_mode,
                state_scaler=without.get("state_scaler"),
            )
            trading_logs.append(without_log)
            without_curve = _curve_from_log(without_log, "dqn_without_nlp", seed)
            portfolio_curves.append(without_curve)
            drawdown_curves.append(_drawdown_from_curve(without_curve, "dqn_without_nlp", seed))
            seed_metrics.append(_metrics_from_log("dqn_without_nlp", without_log, initial_cash, seed))

            with_nlp = train_dqn(
                train_frame,
                STATE_COLUMNS,
                episodes=episodes,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment="dqn_with_nlp",
                output_dir=output_dir,
                model_dir=models_dir,
                seed=seed,
                reward_mode=reward_mode,
            )
            training_rewards.append(with_nlp["training_rewards"])
            with_log = evaluate_agent(
                with_nlp["agent"],
                test_frame,
                STATE_COLUMNS,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment="dqn_with_nlp",
                seed=seed,
                reward_mode=reward_mode,
                state_scaler=with_nlp.get("state_scaler"),
            )
            trading_logs.append(with_log)
            with_curve = _curve_from_log(with_log, "dqn_with_nlp", seed)
            portfolio_curves.append(with_curve)
            drawdown_curves.append(_drawdown_from_curve(with_curve, "dqn_with_nlp", seed))
            seed_metrics.append(_metrics_from_log("dqn_with_nlp", with_log, initial_cash, seed))
    else:
        for experiment in ["dqn_without_nlp", "dqn_with_nlp"]:
            for seed in seeds:
                seed_metrics.append(_empty_rl_metrics(experiment, initial_cash, seed))

    seed_metrics_df = pd.DataFrame(seed_metrics)
    metrics_df = aggregate_seed_metrics(seed_metrics_df)
    metrics_df["reward_mode"] = reward_mode
    seed_metrics_df["reward_mode"] = reward_mode
    curves_df = pd.concat(portfolio_curves, ignore_index=True) if portfolio_curves else pd.DataFrame()
    drawdowns_df = pd.concat(drawdown_curves, ignore_index=True) if drawdown_curves else pd.DataFrame()
    logs_df = pd.concat(trading_logs, ignore_index=True) if trading_logs else pd.DataFrame(columns=TRADING_LOG_COLUMNS)
    rewards_df = pd.concat(training_rewards, ignore_index=True) if training_rewards else pd.DataFrame(columns=TRAINING_REWARD_COLUMNS)
    logs_df = logs_df.reindex(columns=TRADING_LOG_COLUMNS)
    rewards_df = rewards_df.reindex(columns=TRAINING_REWARD_COLUMNS)

    metrics_path = output_dir / "ablation_metrics.csv"
    seed_metrics_path = output_dir / "ablation_metrics_by_seed.csv"
    curves_path = output_dir / "portfolio_curves.csv"
    drawdowns_path = output_dir / "drawdown_curves.csv"
    logs_path = output_dir / "trading_logs.csv"
    rewards_path = output_dir / "training_rewards_all_seeds.csv"
    compliance_path = reports_dir / f"{input_csv.stem}_state_vector_compliance.csv"
    leakage_path = reports_dir / f"{input_csv.stem}_leakage_diagnostics.csv"
    split_path = reports_dir / f"{input_csv.stem}_train_test_split.csv"

    metrics_df.to_csv(metrics_path, index=False, encoding="utf-8-sig")
    seed_metrics_df.to_csv(seed_metrics_path, index=False, encoding="utf-8-sig")
    curves_df.to_csv(curves_path, index=False, encoding="utf-8-sig")
    drawdowns_df.to_csv(drawdowns_path, index=False, encoding="utf-8-sig")
    logs_df.to_csv(logs_path, index=False, encoding="utf-8-sig")
    rewards_df.to_csv(rewards_path, index=False, encoding="utf-8-sig")
    state_compliance.to_csv(compliance_path, index=False, encoding="utf-8-sig")
    leakage_report.to_csv(leakage_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([split_info]).to_csv(split_path, index=False, encoding="utf-8-sig")

    stem_metrics_path = reports_dir / f"{input_csv.stem}_ablation_metrics.csv"
    metrics_df.to_csv(stem_metrics_path, index=False, encoding="utf-8-sig")

    return {
        "metrics": metrics_df,
        "metrics_by_seed": seed_metrics_df,
        "portfolio_curves": curves_df,
        "drawdown_curves": drawdowns_df,
        "trading_logs": logs_df,
        "training_rewards": rewards_df,
        "state_compliance": state_compliance,
        "leakage_diagnostics": leakage_report,
        "split_info": pd.DataFrame([split_info]),
        "ablation_metrics_csv": metrics_path,
        "ablation_metrics_by_seed_csv": seed_metrics_path,
        "portfolio_curves_csv": curves_path,
        "drawdown_curves_csv": drawdowns_path,
        "trading_logs_csv": logs_path,
        "training_rewards_csv": rewards_path,
        "state_compliance_csv": compliance_path,
        "leakage_diagnostics_csv": leakage_path,
        "split_info_csv": split_path,
        "report_ablation_metrics_csv": stem_metrics_path,
        "test_period": split_info,
    }


def run_coverage_controlled_ablation_study(
    input_csv: Path,
    sentiment_csv: Path,
    output_dir: Path | None = None,
    reports_dir: Path | None = None,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    seeds: list[int] | None = None,
    reward_mode: str = "portfolio_return",
) -> dict[str, pd.DataFrame | Path | dict[str, object]]:
    """Train on long-history/low-density rows and evaluate in the dense NLP window.

    This is opt-in and does not run from the dashboard/notebook by default. It
    implements the coverage-controlled design:

    raw news -> daily sentiment features -> lagged RL state -> DQN actions ->
    with/without/enhanced NLP ablation on the high-information-density window.
    """

    raw = pd.read_csv(input_csv)
    symbol = str(raw["symbol"].dropna().iloc[0]) if "symbol" in raw.columns and raw["symbol"].notna().any() else input_csv.stem.split("_")[0]
    output_dir = output_dir or stock_results_dir(symbol)
    reports_dir = reports_dir or stock_reports_dir(symbol)
    models_dir = stock_models_dir(symbol)
    output_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    seeds = seeds or DEFAULT_SEEDS

    start_date = str(pd.to_datetime(raw["date"]).min().date())
    end_date = str(pd.to_datetime(raw["date"]).max().date())
    market = fetch_market_data(symbol, start_date, end_date, input_csv=input_csv)
    sentiment = pd.read_csv(sentiment_csv) if sentiment_csv.exists() else pd.DataFrame()
    features = add_trading_features(market, sentiment, initial_cash=initial_cash, sentiment_already_aligned=True)
    company_name = str(raw.get("company_name", pd.Series([symbol])).dropna().iloc[0]) if "company_name" in raw.columns and raw["company_name"].notna().any() else symbol
    split = detect_information_density_split(symbol, market, sentiment)
    window = define_experiment_window(symbol, company_name, split)

    high_start = pd.to_datetime(window.get("high_density_eval_start"), errors="coerce")
    high_end = pd.to_datetime(window.get("high_density_eval_end"), errors="coerce")
    if pd.isna(high_start) or pd.isna(high_end):
        train_frame, test_frame, split_info = chronological_train_test_split(features)
        window_status = "fallback_chronological_split"
    else:
        train_frame = features[features["date"] < high_start].copy().reset_index(drop=True)
        test_frame = features[(features["date"] >= high_start) & (features["date"] <= high_end)].copy().reset_index(drop=True)
        if len(train_frame) < 3 or len(test_frame) < 2:
            fallback_train, fallback_test, fallback_info = chronological_train_test_split(features)
            train_frame = fallback_train
            test_frame = fallback_test
            window_status = f"fallback_chronological_split_due_to_{window.get('window_status', 'window_issue')}"
            split_info = fallback_info
        else:
            window_status = str(window.get("window_status", "READY"))
            split_info = {
                "train_rows": int(len(train_frame)),
                "test_rows": int(len(test_frame)),
                "train_start": str(train_frame["date"].min().date()),
                "train_end": str(train_frame["date"].max().date()),
                "test_start": str(test_frame["date"].min().date()),
                "test_end": str(test_frame["date"].max().date()),
                "consistent_period": bool(train_frame["date"].max() < test_frame["date"].min()),
                "density_status": split.get("density_status", ""),
                "window_status": window_status,
                "recommended_usage": window.get("recommended_usage", ""),
            }

    state_specs = {
        "dqn_without_nlp": WITHOUT_NLP_STATE_COLUMNS,
        "dqn_with_basic_nlp": STATE_COLUMNS,
        "dqn_with_enhanced_nlp": ENHANCED_NLP_STATE_COLUMNS,
    }
    state_compliance = []
    leakage_report = []
    for experiment, columns in state_specs.items():
        compliance = validate_state_columns(test_frame, columns, sentiment_required=("nlp" in experiment))
        compliance["experiment"] = experiment
        state_compliance.append(compliance)
        leakage_report.append(leakage_diagnostics(test_frame, columns, sentiment_is_aligned_to_trade_date=True).assign(experiment=experiment))

    portfolio_curves: list[pd.DataFrame] = []
    drawdown_curves: list[pd.DataFrame] = []
    trading_logs: list[pd.DataFrame] = []
    seed_metrics: list[dict[str, object]] = []
    training_rewards: list[pd.DataFrame] = []

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
    seed_metrics.append(_metrics_from_curve("buy_and_hold", buyhold_curve, initial_cash, trades=1, win_rate=np.nan, seed="benchmark"))

    if len(train_frame) >= 3 and len(test_frame) >= 2:
        for seed in seeds:
            for experiment, columns in state_specs.items():
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
                rewards["training_period"] = "market_learning_low_density_or_long_history"
                rewards["evaluation_period"] = "high_information_density"
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
                    state_scaler=trained.get("state_scaler"),
                )
                log["training_period"] = "market_learning_low_density_or_long_history"
                log["evaluation_period"] = "high_information_density"
                trading_logs.append(log)
                curve = _curve_from_log(log, experiment, seed)
                portfolio_curves.append(curve)
                drawdown_curves.append(_drawdown_from_curve(curve, experiment, seed))
                seed_metrics.append(_metrics_from_log(experiment, log, initial_cash, seed))
    else:
        for experiment in state_specs:
            for seed in seeds:
                seed_metrics.append(_empty_rl_metrics(experiment, initial_cash, seed))

    seed_metrics_df = pd.DataFrame(seed_metrics)
    metrics_df = aggregate_seed_metrics(seed_metrics_df)
    metrics_df["reward_mode"] = reward_mode
    metrics_df["evaluation_window"] = "high_density"
    metrics_df["training_window"] = "market_learning"
    seed_metrics_df["reward_mode"] = reward_mode
    curves_df = pd.concat(portfolio_curves, ignore_index=True) if portfolio_curves else pd.DataFrame()
    drawdowns_df = pd.concat(drawdown_curves, ignore_index=True) if drawdown_curves else pd.DataFrame()
    logs_df = pd.concat(trading_logs, ignore_index=True) if trading_logs else pd.DataFrame(columns=TRADING_LOG_COLUMNS)
    rewards_df = pd.concat(training_rewards, ignore_index=True) if training_rewards else pd.DataFrame(columns=TRAINING_REWARD_COLUMNS)

    split_df = pd.DataFrame([split])
    window_df = pd.DataFrame([window])
    split_df.to_csv(reports_dir / "information_density_split.csv", index=False, encoding="utf-8-sig")
    window_df.to_csv(reports_dir / "experiment_window_summary.csv", index=False, encoding="utf-8-sig")
    metrics_path = output_dir / "high_density_ablation_metrics.csv"
    seed_metrics_path = output_dir / "high_density_ablation_metrics_by_seed.csv"
    curves_path = output_dir / "high_density_portfolio_curves.csv"
    drawdowns_path = output_dir / "high_density_drawdown_curves.csv"
    logs_path = output_dir / "high_density_trading_logs.csv"
    rewards_path = output_dir / "high_density_training_rewards_all_seeds.csv"
    compliance_path = reports_dir / f"{input_csv.stem}_coverage_controlled_state_vector_compliance.csv"
    leakage_path = reports_dir / f"{input_csv.stem}_coverage_controlled_leakage_diagnostics.csv"
    split_path = reports_dir / f"{input_csv.stem}_coverage_controlled_train_eval_windows.csv"

    metrics_df.to_csv(metrics_path, index=False, encoding="utf-8-sig")
    seed_metrics_df.to_csv(seed_metrics_path, index=False, encoding="utf-8-sig")
    curves_df.to_csv(curves_path, index=False, encoding="utf-8-sig")
    drawdowns_df.to_csv(drawdowns_path, index=False, encoding="utf-8-sig")
    logs_df.to_csv(logs_path, index=False, encoding="utf-8-sig")
    rewards_df.to_csv(rewards_path, index=False, encoding="utf-8-sig")
    pd.concat(state_compliance, ignore_index=True).to_csv(compliance_path, index=False, encoding="utf-8-sig")
    pd.concat(leakage_report, ignore_index=True).to_csv(leakage_path, index=False, encoding="utf-8-sig")
    pd.DataFrame([split_info]).to_csv(split_path, index=False, encoding="utf-8-sig")
    (reports_dir / "high_density_report_section.md").write_text(
        _coverage_controlled_report(symbol, split, window, metrics_df, split_info),
        encoding="utf-8",
    )

    return {
        "metrics": metrics_df,
        "metrics_by_seed": seed_metrics_df,
        "portfolio_curves": curves_df,
        "drawdown_curves": drawdowns_df,
        "trading_logs": logs_df,
        "training_rewards": rewards_df,
        "density_split": split_df,
        "experiment_window_summary": window_df,
        "split_info": pd.DataFrame([split_info]),
        "high_density_ablation_metrics_csv": metrics_path,
        "high_density_ablation_metrics_by_seed_csv": seed_metrics_path,
        "high_density_portfolio_curves_csv": curves_path,
        "high_density_trading_logs_csv": logs_path,
        "coverage_controlled_leakage_diagnostics_csv": leakage_path,
        "window_status": window_status,
    }


def chronological_train_test_split(
    frame: pd.DataFrame,
    test_ratio: float = 0.2,
    min_test_rows: int = 20,
    min_train_rows: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if frame.empty:
        return frame.copy(), frame.copy(), {"train_rows": 0, "test_rows": 0, "consistent_period": False}
    total_rows = len(frame)
    target_test_rows = max(min_test_rows, int(total_rows * test_ratio))
    max_test_rows = max(1, total_rows - max(2, min_train_rows))
    test_rows = min(target_test_rows, max_test_rows)
    train_rows = total_rows - test_rows
    if train_rows < 2:
        train_rows = max(2, total_rows // 2)
        test_rows = total_rows - train_rows
    if test_rows < 1:
        test_rows = 1
        train_rows = total_rows - test_rows
    train = frame.iloc[:train_rows].copy().reset_index(drop=True)
    test = frame.iloc[train_rows:].copy().reset_index(drop=True)
    info = {
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_start": str(train["date"].min().date()) if not train.empty else "",
        "train_end": str(train["date"].max().date()) if not train.empty else "",
        "test_start": str(test["date"].min().date()) if not test.empty else "",
        "test_end": str(test["date"].max().date()) if not test.empty else "",
        "consistent_period": bool(train.empty or test.empty or train["date"].max() < test["date"].min()),
    }
    return train, test, info


def aggregate_seed_metrics(seed_metrics: pd.DataFrame) -> pd.DataFrame:
    if seed_metrics.empty:
        return seed_metrics
    metric_columns = [
        "final_equity",
        "final_portfolio_value",
        "cumulative_return",
        "annualized_return",
        "annualized_volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "max_drawdown",
        "win_rate",
        "number_of_trades",
        "exposure_ratio",
        "avg_reward",
        "profit_factor",
        "value_at_risk_95",
        "avg_daily_return",
        "best_day_return",
        "worst_day_return",
        "trade_frequency",
    ]
    rows: list[dict[str, object]] = []
    for experiment, group in seed_metrics.groupby("experiment", sort=False):
        row: dict[str, object] = {"experiment": experiment, "seed_count": int(group["seed"].nunique())}
        for metric in metric_columns:
            numeric = pd.to_numeric(group[metric], errors="coerce")
            row[metric] = float(numeric.mean()) if numeric.notna().any() else np.nan
            row[f"{metric}_std"] = float(numeric.std(ddof=0)) if numeric.notna().any() else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _curve_from_log(log: pd.DataFrame, experiment: str, seed: int | str) -> pd.DataFrame:
    if log.empty:
        return pd.DataFrame(columns=["date", "portfolio_value", "experiment", "seed"])
    return pd.DataFrame(
        {
            "date": pd.to_datetime(log["date"]),
            "portfolio_value": pd.to_numeric(log["portfolio_value"], errors="coerce"),
            "experiment": experiment,
            "seed": seed,
        }
    )


def _drawdown_from_curve(curve: pd.DataFrame, experiment: str, seed: int | str) -> pd.DataFrame:
    if curve.empty:
        return pd.DataFrame(columns=["date", "drawdown", "experiment", "seed"])
    values = pd.to_numeric(curve["portfolio_value"], errors="coerce")
    drawdowns = drawdown_series(values, as_positive=True)
    return pd.DataFrame(
        {
            "date": curve.loc[drawdowns.index, "date"].to_numpy(),
            "drawdown": drawdowns.to_numpy(),
            "experiment": experiment,
            "seed": seed,
        }
    )


def _metrics_from_log(experiment: str, log: pd.DataFrame, initial_cash: float, seed: int | str) -> dict[str, object]:
    if log.empty:
        return _empty_rl_metrics(experiment, initial_cash, seed)
    rewards = pd.to_numeric(log["reward"], errors="coerce")
    positions = pd.to_numeric(log["position"], errors="coerce")
    return _metrics_from_curve(
        experiment,
        _curve_from_log(log, experiment, seed),
        initial_cash,
        trades=int(log["action"].isin(["Buy", "Sell"]).sum()),
        win_rate=float((rewards > 0).mean()) if rewards.notna().any() else np.nan,
        seed=seed,
        exposure_ratio=float((positions > 0).mean()) if positions.notna().any() else np.nan,
        avg_reward=float(rewards.mean()) if rewards.notna().any() else np.nan,
        profit_factor=profit_factor_from_rewards(rewards),
    )


def _metrics_from_curve(
    experiment: str,
    curve: pd.DataFrame,
    initial_cash: float,
    trades: int,
    win_rate: float,
    *,
    seed: int | str,
    exposure_ratio: float | None = None,
    avg_reward: float | None = None,
    profit_factor: float | None = None,
) -> dict[str, object]:
    values = pd.to_numeric(curve["portfolio_value"], errors="coerce").dropna()
    final_value = float(values.iloc[-1]) if not values.empty else np.nan
    returns = portfolio_returns(values)
    exposure = 1.0 if exposure_ratio is None and experiment == "buy_and_hold" else exposure_ratio
    return {
        "experiment": experiment,
        "seed": seed,
        "final_portfolio_value": final_value,
        "final_equity": final_value,
        "cumulative_return": final_value / initial_cash - 1 if pd.notna(final_value) else np.nan,
        "annualized_return": annualized_return(values),
        "annualized_volatility": annualized_volatility(values),
        "sharpe_ratio": sharpe_ratio(values),
        "sortino_ratio": sortino_ratio(values),
        "calmar_ratio": calmar_ratio(values),
        "max_drawdown": max_drawdown(values),
        "value_at_risk_95": value_at_risk(values),
        "avg_daily_return": float(returns.mean()) if not returns.empty else np.nan,
        "best_day_return": float(returns.max()) if not returns.empty else np.nan,
        "worst_day_return": float(returns.min()) if not returns.empty else np.nan,
        "win_rate": win_rate,
        "number_of_trades": trades,
        "trade_frequency": trades / max(len(values), 1) if len(values) else np.nan,
        "exposure_ratio": exposure,
        "avg_reward": avg_reward,
        "profit_factor": profit_factor,
    }


def _empty_rl_metrics(experiment: str, initial_cash: float, seed: int | str) -> dict[str, object]:
    return {
        "experiment": experiment,
        "seed": seed,
        "final_portfolio_value": initial_cash if experiment == "buy_and_hold" else np.nan,
        "final_equity": initial_cash if experiment == "buy_and_hold" else np.nan,
        "cumulative_return": np.nan,
        "annualized_return": np.nan,
        "annualized_volatility": np.nan,
        "sharpe_ratio": np.nan,
        "sortino_ratio": np.nan,
        "calmar_ratio": np.nan,
        "max_drawdown": np.nan,
        "value_at_risk_95": np.nan,
        "avg_daily_return": np.nan,
        "best_day_return": np.nan,
        "worst_day_return": np.nan,
        "win_rate": np.nan,
        "number_of_trades": 0,
        "trade_frequency": np.nan,
        "exposure_ratio": np.nan,
        "avg_reward": np.nan,
        "profit_factor": np.nan,
    }


def _coverage_controlled_report(
    symbol: str,
    split: dict[str, object],
    window: dict[str, object],
    metrics: pd.DataFrame,
    split_info: dict[str, object],
) -> str:
    label = "Inconclusive"
    if not metrics.empty and {"experiment", "final_equity", "sharpe_ratio"}.issubset(metrics.columns):
        rows = metrics.set_index("experiment")
        if {"dqn_without_nlp", "dqn_with_basic_nlp"}.issubset(rows.index):
            label = classify_nlp_effect(
                rows.loc["dqn_with_basic_nlp"].to_dict(),
                rows.loc["dqn_without_nlp"].to_dict(),
            )
    return "\n".join(
        [
            "# Coverage-Controlled High-Density Experiment",
            "",
            f"- Symbol: `{symbol}`",
            f"- Density cutoff date: `{split.get('density_cutoff_date', '')}`",
            f"- Low-density / market-learning period: `{split_info.get('train_start', '')}` to `{split_info.get('train_end', '')}`",
            f"- High-density NLP evaluation period: `{split_info.get('test_start', '')}` to `{split_info.get('test_end', '')}`",
            f"- High-density coverage ratio: `{split.get('high_density_coverage_ratio', 0):.1%}`",
            f"- Recommended usage: `{window.get('recommended_usage', '')}`",
            f"- NLP effect label: `{label}`",
            "",
            "Experiments include buy-and-hold, DQN without NLP, DQN with basic NLP, and DQN with enhanced NLP. All state features are lagged before action selection.",
        ]
    )
