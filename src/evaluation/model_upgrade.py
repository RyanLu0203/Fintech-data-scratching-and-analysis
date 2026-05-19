"""Experimental DQN upgrade grid built on cached official experiment data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from src.config.paths import OUTPUTS_ROOT, stock_data_dir, stock_results_dir
from src.data_ingestion.ingestion import fetch_market_data
from src.evaluation.ablation import _metrics_from_log
from src.evaluation.market_impact_ablation import (
    EXPERIMENT_SIGNAL_COLUMNS,
    UNIFIED_DQN_STATE_COLUMNS,
    UNIFIED_NLP_STATE_COLUMN,
    _needs_nlp_aware_high_density_split,
    _high_density_internal_split,
    _with_unified_nlp_signal,
    add_market_impact_trading_features,
)
from src.evaluation.model_upgrade_diagnostics import generate_model_upgrade_diagnostic_reports
from src.evaluation.peer_nlp_ablation import _add_behavior_metrics, _safe_read_csv, peer_train_test_split
from src.rl.dqn_agent import DQNConfig
from src.rl.train import evaluate_agent, train_dqn

MODEL_UPGRADE_DIR = OUTPUTS_ROOT / "model_upgrade"
MODEL_VARIANTS = ["vanilla_dqn", "double_dqn", "dueling_dqn", "double_dueling_dqn"]
REWARD_VARIANTS = ["one_day_return", "three_day_return", "five_day_return", "risk_adjusted_return"]
STATE_FEATURE_MODES = ["normalized_plus"]
ACTION_MODES = ["target_position"]
NORMALIZED_PLUS_COLUMNS = [
    "daily_return",
    "price_ma50_gap",
    "price_ma200_gap",
    "ma50_ma200_gap",
    "RSI_normalized",
    "MACD_zscore",
    "position_ratio",
    "cash_ratio",
    "nlp_signal_score",
    "abs_nlp_signal",
    "nlp_signal_3day_mean",
    "nlp_signal_change",
]
ENV_DYNAMICS_COLUMNS = [
    "run_id",
    "run_mode",
    "period",
    "training_mode",
    "episode",
    "step",
    "date",
    "raw_action_id",
    "action_mode",
    "target_exposure",
    "current_stock_value",
    "target_stock_value",
    "trade_value",
    "current_exposure_before",
    "current_exposure_after",
    "mapped_action_label",
    "action",
    "price",
    "cash_before",
    "shares_before",
    "portfolio_before",
    "portfolio_after_trade",
    "portfolio_next",
    "cash_after",
    "shares_after",
    "portfolio_after",
    "position_ratio_after",
    "cash_ratio_after",
    "reward",
    "transaction_cost",
]
Q_VALUE_COLUMNS = [
    "run_id",
    "run_mode",
    "period",
    "episode",
    "step",
    "state_feature_mode",
    "action_mode",
    "raw_action_id",
    "mapped_action_label",
    "q_hold_or_action0",
    "q_action1",
    "q_action2",
    "q_action3",
    "q_action4",
    "selected_q_value",
    "q_value_margin",
]
RUN_METADATA_COLUMNS = ["run_mode", "episodes", "max_seeds", "timestamp", "action_mode", "model_variant", "reward_variant", "state_feature_mode"]


@dataclass(frozen=True)
class ModelUpgradeRunConfig:
    target_symbol: str = "002475"
    target_company: str = "立讯精密"
    start_date: str = "2024-01-01"
    end_date: str = "2026-04-30"
    max_seeds: int = 20
    quick_test: bool = False
    medium_test: bool = False
    episodes: int = 200
    initial_cash: float = 1_000_000.0
    transaction_cost: float = 0.001


def run_model_upgrade_grid(config: ModelUpgradeRunConfig) -> dict[str, Path]:
    MODEL_UPGRADE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().isoformat(timespec="seconds")
    run_mode = "quick_test" if config.quick_test else "medium_test" if config.medium_test else "full_grid"
    log_lines = [
        "# Model Upgrade Run Log",
        "",
        f"- target_symbol: {config.target_symbol}",
        f"- target_company: {config.target_company}",
        f"- window: {config.start_date} -> {config.end_date}",
        f"- run_mode: {run_mode}",
        f"- timestamp: {timestamp}",
        f"- official_baseline_action_mode: discrete_trade",
        f"- model_upgrade_default_action_mode: target_position",
        "- reward_timing_convention: post_trade_to_future_value; one-day reward uses (portfolio_next - portfolio_after_trade) / portfolio_after_trade, preserving the existing environment convention.",
        "- quick_test_interpretation: code-path and environment-dynamics validation only; final_equity, Sharpe ratio, and max_drawdown are not model performance evidence.",
        "",
    ]
    input_csv = _resolve_target_input_csv(config)
    results_dir = stock_results_dir(config.target_symbol)
    peer_daily_path = results_dir / "peer_nlp_daily_sentiment.csv"
    impact_daily_path = results_dir / "peer_market_impact_daily_signal.csv"
    missing = [str(path) for path in [input_csv, peer_daily_path, impact_daily_path] if not path.exists()]
    if missing:
        log_lines.append(f"Missing required cached files: {', '.join(missing)}")
        _write_empty_outputs(log_lines)
        raise FileNotFoundError("Run the official target experiment before model upgrade grid. Missing: " + ", ".join(missing))

    if config.quick_test:
        model_variants = ["vanilla_dqn", "double_dueling_dqn"]
        reward_variants = ["one_day_return"]
        state_modes = ["normalized_plus"]
        action_modes = ["target_position"]
        seeds = [1, 2]
        episodes = 5
    elif config.medium_test:
        model_variants = ["vanilla_dqn", "double_dueling_dqn"]
        reward_variants = ["one_day_return", "three_day_return"]
        state_modes = ["normalized_plus"]
        action_modes = ["target_position"]
        seeds = [1, 2, 3, 4, 5]
        episodes = 50
    else:
        model_variants = MODEL_VARIANTS
        reward_variants = REWARD_VARIANTS
        state_modes = STATE_FEATURE_MODES
        action_modes = ACTION_MODES
        seeds = list(range(1, int(config.max_seeds) + 1))
        episodes = int(config.episodes)
    effective_max_seeds = len(seeds)
    log_lines.append(f"- model_variants: {model_variants}")
    log_lines.append(f"- reward_variants: {reward_variants}")
    log_lines.append(f"- state_feature_modes: {state_modes}")
    log_lines.append(f"- action_modes: {action_modes}")
    log_lines.append(f"- seeds: {seeds}")
    log_lines.append(f"- episodes: {episodes}")
    log_lines.append("")

    market = fetch_market_data(config.target_symbol, config.start_date, config.end_date, input_csv=input_csv)
    peer_daily = _safe_read_csv(peer_daily_path)
    impact_daily = _safe_read_csv(impact_daily_path)
    features = add_market_impact_trading_features(market, peer_daily, impact_daily, initial_cash=config.initial_cash)
    split_source = peer_daily if not peer_daily.empty else impact_daily
    market_learning_frame, high_density_frame, split_info = peer_train_test_split(features, split_source)
    pretrain_frame = _with_unified_nlp_signal(market_learning_frame, None)
    fine_tune_frame = market_learning_frame.copy()
    test_frame = high_density_frame.copy()
    if _needs_nlp_aware_high_density_split(market_learning_frame, high_density_frame):
        fine_tune_frame, test_frame, split_info = _high_density_internal_split(high_density_frame, split_info)
    if len(fine_tune_frame) < 3 or len(test_frame) < 2:
        log_lines.append(f"Insufficient DQN rows: train={len(fine_tune_frame)}, test={len(test_frame)}")
        _write_empty_outputs(log_lines)
        raise RuntimeError("Insufficient cached rows for model upgrade grid.")

    seed_rows: list[dict[str, object]] = []
    action_rows: list[dict[str, object]] = []
    state_diag_rows: list[dict[str, object]] = []
    env_dynamics_rows: list[dict[str, object]] = []
    exploration_rows: list[dict[str, object]] = []
    portfolio_rows: list[dict[str, object]] = []
    q_value_rows: list[dict[str, object]] = []
    for model_variant in model_variants:
        for reward_variant in reward_variants:
            for state_mode in state_modes:
                prepared = _prepare_group_frames(fine_tune_frame, test_frame, state_mode, log_lines)
                state_columns = NORMALIZED_PLUS_COLUMNS if state_mode == "normalized_plus" else UNIFIED_DQN_STATE_COLUMNS
                for action_mode in action_modes:
                    metadata_base = _metadata(
                        run_mode=run_mode,
                        episodes=episodes,
                        max_seeds=effective_max_seeds,
                        timestamp=timestamp,
                        action_mode=action_mode,
                        model_variant=model_variant,
                        reward_variant=reward_variant,
                        state_feature_mode=state_mode,
                    )
                    metadata_base["target_company"] = config.target_company
                    for experiment, frames in prepared.items():
                        _append_state_diagnostics(
                            state_diag_rows,
                            target_symbol=config.target_symbol,
                            experiment=experiment,
                            train_frame=frames["train"],
                            test_frame=frames["test"],
                            state_columns=state_columns,
                            metadata=metadata_base,
                        )
                    for seed in seeds:
                        pretrained_state = None
                        pretrain_scaler = None
                        if len(pretrain_frame) >= 3:
                            pretrain_prepared = _prepare_single_frame_pair(pretrain_frame, pretrain_frame, None, state_mode, log_lines)
                            pretrain_data = pretrain_prepared["train"]
                            pretrain_columns = state_columns
                            pretrain_config = _dqn_config(pretrain_columns, seed, model_variant, reward_variant, state_mode, action_mode)
                            pretrained = train_dqn(
                                pretrain_data,
                                pretrain_columns,
                                episodes=episodes,
                                initial_cash=config.initial_cash,
                                transaction_cost=config.transaction_cost,
                                experiment="model_upgrade_shared_pretrain",
                                output_dir=None,
                                seed=seed,
                                dqn_config=pretrain_config,
                                reward_variant=reward_variant,
                                action_mode=action_mode,
                            )
                            pretrained_state = {key: value.detach().clone() for key, value in pretrained["agent"].online.state_dict().items()}
                            pretrain_scaler = pretrained.get("state_scaler")
                        for experiment, frames in prepared.items():
                            run_id = f"{config.target_symbol}_{model_variant}_{reward_variant}_{state_mode}_{action_mode}_{experiment}_seed{seed}"
                            dqn_config = _dqn_config(state_columns, seed, model_variant, reward_variant, state_mode, action_mode)
                            trained = train_dqn(
                                frames["train"],
                                state_columns,
                                episodes=episodes,
                                initial_cash=config.initial_cash,
                                transaction_cost=config.transaction_cost,
                                experiment=experiment,
                                output_dir=None,
                                seed=seed,
                                dqn_config=dqn_config,
                                reward_variant=reward_variant,
                                action_mode=action_mode,
                                initial_state_dict=pretrained_state,
                                initial_epsilon=0.30 if pretrained_state is not None else None,
                                state_scaler=pretrain_scaler,
                            )
                            log = evaluate_agent(
                                trained["agent"],
                                frames["test"],
                                state_columns,
                                initial_cash=config.initial_cash,
                                transaction_cost=config.transaction_cost,
                                experiment=experiment,
                                seed=seed,
                                reward_variant=reward_variant,
                                state_scaler=trained.get("state_scaler"),
                                action_mode=action_mode,
                                log_q_values=True,
                            )
                            metrics = _add_behavior_metrics(_metrics_from_log(experiment, log, config.initial_cash, seed), log)
                            counts = _action_counts(log)
                            average_exposure = _average_exposure(log)
                            unique_portfolio_values = _unique_portfolio_values(log)
                            row_metadata = {**metadata_base, "run_id": run_id, "target_symbol": config.target_symbol, "target_company": config.target_company, "experiment": experiment, "seed": seed}
                            seed_rows.append(
                                {
                                    **row_metadata,
                                    "final_equity": metrics.get("final_equity"),
                                    "cumulative_return": metrics.get("cumulative_return"),
                                    "annualized_return": metrics.get("annualized_return"),
                                    "annualized_volatility": metrics.get("annualized_volatility"),
                                    "sharpe_ratio": metrics.get("sharpe_ratio"),
                                    "max_drawdown": metrics.get("max_drawdown"),
                                    "number_of_trades": metrics.get("number_of_trades"),
                                    "win_rate": metrics.get("win_rate"),
                                    "exposure_ratio": metrics.get("exposure_ratio"),
                                    "average_exposure": average_exposure,
                                    "turnover": metrics.get("turnover"),
                                    "unique_portfolio_values": unique_portfolio_values,
                                    "hold_ratio": counts["hold_count"] / max(counts["total_actions"], 1),
                                    "action_hold_count": counts["hold_count"],
                                    "action_buy_count": counts["buy_count"],
                                    "action_sell_count": counts["sell_count"],
                                }
                            )
                            action_rows.extend(_action_distribution_rows(log, row_metadata))
                            env_dynamics_rows.extend(_env_dynamics_rows(log, row_metadata))
                            exploration_rows.extend(_exploration_rows(trained.get("exploration_diagnostics"), row_metadata))
                            portfolio_rows.extend(_portfolio_rows(log, row_metadata))
                            q_value_rows.extend(_q_value_rows(log, row_metadata))
                            log_lines.append(
                                f"- {run_id}: final_equity={metrics.get('final_equity')}, "
                                f"trades={metrics.get('number_of_trades')}, avg_exposure={average_exposure}, "
                                f"actions={counts}"
                            )

    seed_df = pd.DataFrame(seed_rows)
    action_df = pd.DataFrame(action_rows)
    state_diag_df = pd.DataFrame(state_diag_rows)
    env_order = ["target_symbol", "experiment", "seed"] + ENV_DYNAMICS_COLUMNS + [column for column in RUN_METADATA_COLUMNS if column not in ENV_DYNAMICS_COLUMNS]
    env_dynamics_df = pd.DataFrame(env_dynamics_rows)
    if not env_dynamics_df.empty:
        env_dynamics_df = env_dynamics_df[[column for column in env_order if column in env_dynamics_df.columns] + [column for column in env_dynamics_df.columns if column not in env_order]]
    exploration_df = pd.DataFrame(exploration_rows)
    portfolio_df = pd.DataFrame(portfolio_rows)
    q_value_df = pd.DataFrame(q_value_rows)
    _append_acceptance_status(log_lines, seed_df, action_df, portfolio_df, run_mode)
    paths = {
        "seed_metrics": MODEL_UPGRADE_DIR / "seed_level_metrics.csv",
        "summary": MODEL_UPGRADE_DIR / "model_upgrade_summary.csv",
        "actions": MODEL_UPGRADE_DIR / "action_distribution_diagnostics.csv",
        "env_dynamics": MODEL_UPGRADE_DIR / "env_dynamics_diagnostics.csv",
        "exploration": MODEL_UPGRADE_DIR / "exploration_diagnostics.csv",
        "portfolio_curves": MODEL_UPGRADE_DIR / "portfolio_curves.csv",
        "q_values": MODEL_UPGRADE_DIR / "q_value_diagnostics.csv",
        "reward_comparison": MODEL_UPGRADE_DIR / "reward_variant_comparison.csv",
        "state_features": MODEL_UPGRADE_DIR / "state_feature_diagnostics.csv",
        "log": MODEL_UPGRADE_DIR / "upgrade_run_log.md",
    }
    _write_run_id_output(paths["seed_metrics"], seed_df)
    _write_run_id_output(paths["actions"], action_df)
    _write_run_id_output(paths["env_dynamics"], env_dynamics_df)
    _write_run_id_output(paths["exploration"], exploration_df)
    _write_run_id_output(paths["portfolio_curves"], portfolio_df)
    _write_run_id_output(paths["q_values"], q_value_df)
    _write_run_id_output(paths["state_features"], state_diag_df)
    all_seed_df = _safe_read_csv(paths["seed_metrics"])
    _model_summary(all_seed_df).to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    _reward_comparison(all_seed_df).to_csv(paths["reward_comparison"], index=False, encoding="utf-8-sig")
    paths["log"].write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    report_paths = generate_model_upgrade_diagnostic_reports()
    paths.update({f"diagnostic_{name}": path for name, path in report_paths.items()})
    return paths


def _resolve_target_input_csv(config: ModelUpgradeRunConfig) -> Path:
    data_dir = stock_data_dir(config.target_symbol)
    exact = data_dir / f"{config.target_symbol}_finance_text_{config.start_date}_{config.end_date}.csv"
    if exact.exists():
        return exact
    master = data_dir / f"{config.target_symbol}_finance_text_master.csv"
    if master.exists():
        return master
    candidates = sorted(data_dir.glob(f"{config.target_symbol}_finance_text*.csv"))
    return candidates[0] if candidates else exact


def _prepare_group_frames(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    state_mode: str,
    log_lines: list[str],
) -> dict[str, dict[str, pd.DataFrame]]:
    prepared: dict[str, dict[str, pd.DataFrame]] = {}
    for experiment, signal_column in EXPERIMENT_SIGNAL_COLUMNS.items():
        prepared[experiment] = _prepare_single_frame_pair(train_frame, test_frame, signal_column, state_mode, log_lines)
    return prepared


def _prepare_single_frame_pair(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    signal_column: str | None,
    state_mode: str,
    log_lines: list[str],
) -> dict[str, pd.DataFrame]:
    train = _with_unified_nlp_signal(train_frame, signal_column)
    test = _with_unified_nlp_signal(test_frame, signal_column)
    if state_mode == "official_8d":
        return {"train": train, "test": test}
    return _add_normalized_plus_features(train, test, signal_column, log_lines)


def _add_normalized_plus_features(
    train: pd.DataFrame,
    test: pd.DataFrame,
    signal_column: str | None,
    log_lines: list[str],
) -> dict[str, pd.DataFrame]:
    train = train.copy()
    test = test.copy()
    macd = pd.to_numeric(train.get("MACD", 0.0), errors="coerce").replace([np.inf, -np.inf], np.nan)
    macd_mean = float(macd.mean()) if macd.notna().any() else 0.0
    macd_std = float(macd.std(ddof=0)) if macd.notna().sum() > 1 else 1.0
    if not np.isfinite(macd_std) or macd_std < 1e-9:
        macd_std = 1.0
    beta = _market_residual_beta(train, signal_column)
    for frame, period in [(train, "train"), (test, "test")]:
        price = _numeric(frame, "price")
        ma50 = _numeric(frame, "MA50").replace(0, np.nan)
        ma200 = _numeric(frame, "MA200").replace(0, np.nan)
        signal = _numeric(frame, UNIFIED_NLP_STATE_COLUMN)
        frame["daily_return"] = _numeric(frame, "close").pct_change().fillna(0.0)
        frame["price_ma50_gap"] = (price / ma50 - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        frame["price_ma200_gap"] = (price / ma200 - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        frame["ma50_ma200_gap"] = (ma50 / ma200 - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        frame["RSI_normalized"] = (_numeric(frame, "RSI") / 100.0).fillna(0.0)
        frame["MACD_zscore"] = ((_numeric(frame, "MACD") - macd_mean) / macd_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        frame["position_ratio"] = 0.0
        frame["cash_ratio"] = 1.0
        frame["abs_nlp_signal"] = signal.abs()
        frame["nlp_signal_3day_mean"] = signal.rolling(3, min_periods=1).mean()
        frame["nlp_signal_change"] = signal.diff().fillna(0.0)
        if signal_column and _paired_signal_columns(signal_column):
            sector_col, market_col = _paired_signal_columns(signal_column)
            if sector_col in frame.columns and market_col in frame.columns:
                if beta is not None:
                    frame["marketwide_residual_signal"] = _numeric(frame, market_col) - beta * _numeric(frame, sector_col)
            else:
                log_lines.append(f"Skipped relative/residual signals for {signal_column} during {period}; paired columns missing.")
    return {"train": train, "test": test}


def _paired_signal_columns(signal_column: str | None) -> tuple[str, str] | None:
    if signal_column in {"sector_sentiment_score", "marketwide_sentiment_score"}:
        return ("sector_sentiment_score", "marketwide_sentiment_score")
    if signal_column in {"sector_impact_score", "marketwide_impact_score"}:
        return ("sector_impact_score", "marketwide_impact_score")
    return None


def _market_residual_beta(train: pd.DataFrame, signal_column: str | None) -> float | None:
    pair = _paired_signal_columns(signal_column)
    if not pair:
        return None
    sector_col, market_col = pair
    if sector_col not in train.columns or market_col not in train.columns:
        return None
    sector = _numeric(train, sector_col)
    market = _numeric(train, market_col)
    denom = float((sector * sector).sum())
    if abs(denom) < 1e-12:
        return None
    return float((market * sector).sum() / denom)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame.get(column, pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)


def _dqn_config(state_columns: Iterable[str], seed: int, model_variant: str, reward_variant: str, state_mode: str, action_mode: str) -> DQNConfig:
    return DQNConfig(
        state_dim=len(list(state_columns)),
        seed=seed,
        model_variant=model_variant,
        reward_variant=reward_variant,
        state_feature_mode=state_mode,
        action_mode=action_mode,
    )


def _append_state_diagnostics(
    rows: list[dict[str, object]],
    *,
    target_symbol: str,
    experiment: str,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    state_columns: list[str],
    metadata: dict[str, object],
) -> None:
    for period, frame in [("train", train_frame), ("test", test_frame)]:
        for feature in state_columns:
            values = pd.to_numeric(frame.get(feature, pd.Series(dtype=float)), errors="coerce")
            rows.append(
                {
                    **metadata,
                    "run_id": f"{target_symbol}_{metadata['model_variant']}_{metadata['reward_variant']}_{metadata['state_feature_mode']}_{metadata['action_mode']}_{experiment}_{period}",
                    "target_symbol": target_symbol,
                    "experiment": experiment,
                    "period": period,
                    "feature_name": feature,
                    "mean": float(values.mean()) if values.notna().any() else np.nan,
                    "std": float(values.std(ddof=0)) if values.notna().sum() > 1 else np.nan,
                    "min": float(values.min()) if values.notna().any() else np.nan,
                    "max": float(values.max()) if values.notna().any() else np.nan,
                    "missing_count": int(values.isna().sum()),
                    "nonzero_count": int((values.fillna(0).abs() > 1e-12).sum()),
                }
            )


def _action_counts(log: pd.DataFrame) -> dict[str, int]:
    actions = log.get("action", pd.Series(dtype=str)).astype(str)
    return {
        "hold_count": int((actions == "Hold").sum()),
        "buy_count": int((actions == "Buy").sum()),
        "sell_count": int((actions == "Sell").sum()),
        "total_actions": int(len(actions)),
    }


def _metadata(
    *,
    run_mode: str,
    episodes: int,
    max_seeds: int,
    timestamp: str,
    action_mode: str,
    model_variant: str,
    reward_variant: str,
    state_feature_mode: str,
) -> dict[str, object]:
    return {
        "run_mode": run_mode,
        "episodes": int(episodes),
        "max_seeds": int(max_seeds),
        "timestamp": timestamp,
        "action_mode": action_mode,
        "model_variant": model_variant,
        "reward_variant": reward_variant,
        "state_feature_mode": state_feature_mode,
    }


def _average_exposure(log: pd.DataFrame) -> float:
    values = pd.to_numeric(log.get("current_exposure_after", log.get("position_ratio_after", pd.Series(dtype=float))), errors="coerce")
    return float(values.dropna().mean()) if values.notna().any() else 0.0


def _unique_portfolio_values(log: pd.DataFrame) -> int:
    values = pd.to_numeric(log.get("portfolio_value", log.get("portfolio_after", pd.Series(dtype=float))), errors="coerce").dropna()
    if values.empty:
        return 0
    return int(values.round(6).nunique())


def _action_distribution_rows(log: pd.DataFrame, metadata: dict[str, object]) -> list[dict[str, object]]:
    if log.empty:
        return []
    frame = log.copy()
    for column in ["raw_action_id", "target_exposure", "current_exposure_before", "current_exposure_after"]:
        if column not in frame.columns:
            frame[column] = np.nan
    if "mapped_action_label" not in frame.columns:
        frame["mapped_action_label"] = frame.get("action", "Hold")
    rows: list[dict[str, object]] = []
    total = max(len(frame), 1)
    grouped = frame.groupby(["raw_action_id", "mapped_action_label"], dropna=False)
    for (raw_action_id, mapped_action_label), part in grouped:
        label = str(mapped_action_label)
        rows.append(
            {
                **metadata,
                "period": "test",
                "raw_action_id": raw_action_id,
                "target_exposure": float(pd.to_numeric(part["target_exposure"], errors="coerce").mean()),
                "current_exposure_before": float(pd.to_numeric(part["current_exposure_before"], errors="coerce").mean()),
                "current_exposure_after": float(pd.to_numeric(part["current_exposure_after"], errors="coerce").mean()),
                "mapped_action_label": label,
                "hold_count": int((part["mapped_action_label"].astype(str) == "Hold").sum()),
                "buy_count": int((part["mapped_action_label"].astype(str) == "Buy").sum()),
                "sell_count": int((part["mapped_action_label"].astype(str) == "Sell").sum()),
                "total_actions": int(len(part)),
                "hold_ratio": int((part["mapped_action_label"].astype(str) == "Hold").sum()) / total,
                "buy_ratio": int((part["mapped_action_label"].astype(str) == "Buy").sum()) / total,
                "sell_ratio": int((part["mapped_action_label"].astype(str) == "Sell").sum()) / total,
            }
        )
    return rows


def _env_dynamics_rows(log: pd.DataFrame, metadata: dict[str, object]) -> list[dict[str, object]]:
    if log.empty:
        return []
    rows: list[dict[str, object]] = []
    for _, item in log.iterrows():
        row = {**metadata, "run_id": metadata["run_id"], "target_symbol": metadata["target_symbol"], "experiment": metadata["experiment"], "seed": metadata["seed"]}
        row.update(
            {
                "period": "test",
                "training_mode": bool(item.get("training_mode", False)),
                "episode": int(item.get("episode", 0) or 0),
                "step": int(item.get("step", 0) or 0),
                "date": item.get("date"),
                "raw_action_id": item.get("raw_action_id"),
                "target_exposure": item.get("target_exposure"),
                "current_stock_value": item.get("current_stock_value"),
                "target_stock_value": item.get("target_stock_value"),
                "trade_value": item.get("trade_value"),
                "current_exposure_before": item.get("current_exposure_before"),
                "current_exposure_after": item.get("current_exposure_after"),
                "mapped_action_label": item.get("mapped_action_label", item.get("action")),
                "action": item.get("action"),
                "price": item.get("price"),
                "cash_before": item.get("cash_before"),
                "shares_before": item.get("shares_before"),
                "portfolio_before": item.get("portfolio_before"),
                "portfolio_after_trade": item.get("portfolio_after_trade", item.get("portfolio_after")),
                "portfolio_next": item.get("portfolio_next", item.get("portfolio_value")),
                "cash_after": item.get("cash_after"),
                "shares_after": item.get("shares_after"),
                "portfolio_after": item.get("portfolio_after"),
                "position_ratio_after": item.get("position_ratio_after"),
                "cash_ratio_after": item.get("cash_ratio_after"),
                "reward": item.get("reward"),
                "transaction_cost": item.get("transaction_cost"),
            }
        )
        rows.append(row)
    return rows


def _q_value_rows(log: pd.DataFrame, metadata: dict[str, object]) -> list[dict[str, object]]:
    if log.empty or "selected_q_value" not in log.columns:
        return []
    rows: list[dict[str, object]] = []
    for _, item in log.iterrows():
        row = {
            **metadata,
            "period": "test",
            "episode": int(item.get("episode", 0) or 0),
            "step": int(item.get("step", 0) or 0),
            "raw_action_id": item.get("raw_action_id"),
            "mapped_action_label": item.get("mapped_action_label", item.get("action")),
            "q_hold_or_action0": item.get("q_hold_or_action0"),
            "q_action1": item.get("q_action1"),
            "q_action2": item.get("q_action2"),
            "q_action3": item.get("q_action3"),
            "q_action4": item.get("q_action4"),
            "selected_q_value": item.get("selected_q_value"),
            "q_value_margin": item.get("q_value_margin"),
        }
        rows.append(row)
    return rows


def _exploration_rows(exploration: object, metadata: dict[str, object]) -> list[dict[str, object]]:
    if not isinstance(exploration, pd.DataFrame) or exploration.empty:
        return []
    rows: list[dict[str, object]] = []
    for _, item in exploration.iterrows():
        row = {**metadata, "run_id": metadata["run_id"], "target_symbol": metadata["target_symbol"], "experiment": metadata["experiment"], "seed": metadata["seed"]}
        for column in ["episode", "epsilon", "random_action_count", "greedy_action_count", "buy_count", "sell_count", "hold_count", "average_exposure"]:
            row[column] = item.get(column)
        rows.append(row)
    return rows


def _portfolio_rows(log: pd.DataFrame, metadata: dict[str, object]) -> list[dict[str, object]]:
    if log.empty:
        return []
    rows: list[dict[str, object]] = []
    for _, item in log.iterrows():
        rows.append(
            {
                **metadata,
                "date": item.get("date"),
                "portfolio_value": item.get("portfolio_value", item.get("portfolio_after")),
                "current_exposure_after": item.get("current_exposure_after", item.get("position_ratio_after")),
                "raw_action_id": item.get("raw_action_id"),
                "mapped_action_label": item.get("mapped_action_label", item.get("action")),
            }
        )
    return rows


def _append_acceptance_status(
    log_lines: list[str],
    seed_df: pd.DataFrame,
    action_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    run_mode: str,
) -> None:
    log_lines.append("")
    log_lines.append("## Acceptance Status")
    if seed_df.empty:
        log_lines.append("- No seed metrics were produced.")
        return
    tp_seed = seed_df[seed_df["action_mode"].astype(str) == "target_position"].copy() if "action_mode" in seed_df.columns else pd.DataFrame()
    has_buy = bool((pd.to_numeric(tp_seed.get("action_buy_count", pd.Series(dtype=float)), errors="coerce") > 0).any())
    has_sell = bool((pd.to_numeric(tp_seed.get("action_sell_count", pd.Series(dtype=float)), errors="coerce") > 0).any())
    log_lines.append(f"- target_position_buy_count_positive: {has_buy}")
    log_lines.append(f"- target_position_sell_count_positive: {has_sell}")
    if run_mode == "medium_test":
        average_exposure_ok = bool((pd.to_numeric(tp_seed.get("average_exposure", pd.Series(dtype=float)), errors="coerce") > 0.10).any())
        hold_ratio_ok = bool((pd.to_numeric(tp_seed.get("hold_ratio", pd.Series(dtype=float)), errors="coerce") < 0.90).any())
        unique_values_ok = bool((pd.to_numeric(tp_seed.get("unique_portfolio_values", pd.Series(dtype=float)), errors="coerce") > 10).any())
        log_lines.append(f"- average_exposure_above_0_10: {average_exposure_ok}")
        log_lines.append(f"- hold_ratio_below_0_90: {hold_ratio_ok}")
        log_lines.append(f"- unique_portfolio_values_above_10: {unique_values_ok}")
        if not all([has_buy, has_sell, average_exposure_ok, hold_ratio_ok, unique_values_ok]):
            log_lines.append("- Environment runs, but policy remains hold-heavy.")
    elif run_mode == "quick_test" and not all([has_buy, has_sell]):
        log_lines.append("- Environment runs, but policy remains hold-heavy.")


def _model_summary(seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return pd.DataFrame()
    group_cols = ["target_symbol", "experiment", "run_mode", "episodes", "max_seeds", "timestamp", "action_mode", "model_variant", "reward_variant", "state_feature_mode"]
    if "target_company" in seed_df.columns:
        group_cols.insert(1, "target_company")
    grouped = seed_df.groupby(group_cols, dropna=False)
    return grouped.agg(
        seed_count=("seed", "nunique"),
        mean_final_equity=("final_equity", "mean"),
        std_final_equity=("final_equity", "std"),
        mean_cumulative_return=("cumulative_return", "mean"),
        std_cumulative_return=("cumulative_return", "std"),
        mean_sharpe_ratio=("sharpe_ratio", "mean"),
        std_sharpe_ratio=("sharpe_ratio", "std"),
        mean_max_drawdown=("max_drawdown", "mean"),
        mean_number_of_trades=("number_of_trades", "mean"),
        mean_exposure_ratio=("exposure_ratio", "mean"),
        mean_average_exposure=("average_exposure", "mean"),
        mean_turnover=("turnover", "mean"),
        mean_unique_portfolio_values=("unique_portfolio_values", "mean"),
    ).reset_index()


def _reward_comparison(seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return pd.DataFrame()
    group_cols = ["target_symbol", "experiment", "run_mode", "episodes", "max_seeds", "timestamp", "action_mode", "model_variant", "state_feature_mode", "reward_variant"]
    if "target_company" in seed_df.columns:
        group_cols.insert(1, "target_company")
    grouped = seed_df.groupby(group_cols, dropna=False)
    return grouped.agg(
        seed_count=("seed", "nunique"),
        mean_final_equity=("final_equity", "mean"),
        mean_sharpe_ratio=("sharpe_ratio", "mean"),
        mean_max_drawdown=("max_drawdown", "mean"),
        mean_number_of_trades=("number_of_trades", "mean"),
        mean_exposure_ratio=("exposure_ratio", "mean"),
    ).reset_index()


def _write_empty_outputs(log_lines: list[str]) -> None:
    MODEL_UPGRADE_DIR.mkdir(parents=True, exist_ok=True)
    (MODEL_UPGRADE_DIR / "upgrade_run_log.md").write_text("\n".join(log_lines) + "\n", encoding="utf-8")


def _write_run_id_output(path: Path, frame: pd.DataFrame) -> None:
    if path.exists():
        try:
            previous = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            previous = pd.DataFrame()
    else:
        previous = pd.DataFrame()
    if not previous.empty and not frame.empty and "run_id" in previous.columns and "run_id" in frame.columns:
        new_run_ids = set(frame["run_id"].dropna().astype(str).tolist())
        previous = previous[~previous["run_id"].astype(str).isin(new_run_ids)].copy()
    output = pd.concat([previous, frame], ignore_index=True, sort=False) if not previous.empty else frame
    output.to_csv(path, index=False, encoding="utf-8-sig")
