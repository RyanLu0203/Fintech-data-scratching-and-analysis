"""Experimental DQN upgrade grid built on cached official experiment data."""

from __future__ import annotations

from dataclasses import dataclass
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
from src.evaluation.peer_nlp_ablation import _add_behavior_metrics, _safe_read_csv, peer_train_test_split
from src.rl.dqn_agent import DQNConfig
from src.rl.train import evaluate_agent, train_dqn

MODEL_UPGRADE_DIR = OUTPUTS_ROOT / "model_upgrade"
MODEL_VARIANTS = ["vanilla_dqn", "double_dqn", "dueling_dqn", "double_dueling_dqn"]
REWARD_VARIANTS = ["one_day_return", "three_day_return", "five_day_return", "risk_adjusted_return"]
STATE_FEATURE_MODES = ["official_8d", "normalized_plus"]
NORMALIZED_PLUS_COLUMNS = [
    "daily_return",
    "price_ma50_gap",
    "price_ma200_gap",
    "ma50_ma200_gap",
    "RSI_normalized",
    "MACD_normalized",
    "position_ratio",
    "cash_ratio",
    "nlp_signal_score",
    "abs_nlp_signal",
    "nlp_signal_3day_mean",
    "nlp_signal_5day_mean",
    "nlp_signal_change",
    "relative_signal",
    "marketwide_residual_signal",
]


@dataclass(frozen=True)
class ModelUpgradeRunConfig:
    target_symbol: str = "002475"
    target_company: str = "立讯精密"
    start_date: str = "2024-01-01"
    end_date: str = "2026-04-30"
    max_seeds: int = 20
    quick_test: bool = False
    episodes: int = 200
    initial_cash: float = 1_000_000.0
    transaction_cost: float = 0.001


def run_model_upgrade_grid(config: ModelUpgradeRunConfig) -> dict[str, Path]:
    MODEL_UPGRADE_DIR.mkdir(parents=True, exist_ok=True)
    log_lines = [
        "# Model Upgrade Run Log",
        "",
        f"- target_symbol: {config.target_symbol}",
        f"- target_company: {config.target_company}",
        f"- window: {config.start_date} -> {config.end_date}",
        f"- quick_test: {config.quick_test}",
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

    model_variants = ["vanilla_dqn", "double_dueling_dqn"] if config.quick_test else MODEL_VARIANTS
    reward_variants = ["one_day_return"] if config.quick_test else REWARD_VARIANTS
    state_modes = ["official_8d"] if config.quick_test else STATE_FEATURE_MODES
    seeds = [1, 2] if config.quick_test else list(range(1, int(config.max_seeds) + 1))
    episodes = 5 if config.quick_test else int(config.episodes)
    log_lines.append(f"- model_variants: {model_variants}")
    log_lines.append(f"- reward_variants: {reward_variants}")
    log_lines.append(f"- state_feature_modes: {state_modes}")
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
    for model_variant in model_variants:
        for reward_variant in reward_variants:
            for state_mode in state_modes:
                prepared = _prepare_group_frames(fine_tune_frame, test_frame, state_mode, log_lines)
                state_columns = NORMALIZED_PLUS_COLUMNS if state_mode == "normalized_plus" else UNIFIED_DQN_STATE_COLUMNS
                for experiment, frames in prepared.items():
                    _append_state_diagnostics(
                        state_diag_rows,
                        target_symbol=config.target_symbol,
                        model_variant=model_variant,
                        reward_variant=reward_variant,
                        state_feature_mode=state_mode,
                        experiment=experiment,
                        train_frame=frames["train"],
                        test_frame=frames["test"],
                        state_columns=state_columns,
                    )
                for seed in seeds:
                    pretrained_state = None
                    pretrain_scaler = None
                    if len(pretrain_frame) >= 3:
                        pretrain_prepared = _prepare_single_frame_pair(pretrain_frame, pretrain_frame, None, state_mode, log_lines)
                        pretrain_data = pretrain_prepared["train"]
                        pretrain_columns = state_columns
                        pretrain_config = _dqn_config(pretrain_columns, seed, model_variant, reward_variant, state_mode)
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
                        )
                        pretrained_state = {key: value.detach().clone() for key, value in pretrained["agent"].online.state_dict().items()}
                        pretrain_scaler = pretrained.get("state_scaler")
                    for experiment, frames in prepared.items():
                        run_id = f"{config.target_symbol}_{model_variant}_{reward_variant}_{state_mode}_seed{seed}"
                        dqn_config = _dqn_config(state_columns, seed, model_variant, reward_variant, state_mode)
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
                        )
                        metrics = _add_behavior_metrics(_metrics_from_log(experiment, log, config.initial_cash, seed), log)
                        counts = _action_counts(log)
                        seed_rows.append(
                            {
                                "run_id": run_id,
                                "target_symbol": config.target_symbol,
                                "experiment": experiment,
                                "model_variant": model_variant,
                                "reward_variant": reward_variant,
                                "state_feature_mode": state_mode,
                                "seed": seed,
                                "final_equity": metrics.get("final_equity"),
                                "cumulative_return": metrics.get("cumulative_return"),
                                "annualized_return": metrics.get("annualized_return"),
                                "annualized_volatility": metrics.get("annualized_volatility"),
                                "sharpe_ratio": metrics.get("sharpe_ratio"),
                                "max_drawdown": metrics.get("max_drawdown"),
                                "number_of_trades": metrics.get("number_of_trades"),
                                "win_rate": metrics.get("win_rate"),
                                "exposure_ratio": metrics.get("exposure_ratio"),
                                "turnover": metrics.get("turnover"),
                                "action_hold_count": counts["hold_count"],
                                "action_buy_count": counts["buy_count"],
                                "action_sell_count": counts["sell_count"],
                            }
                        )
                        total = max(counts["total_actions"], 1)
                        action_rows.append(
                            {
                                "run_id": run_id,
                                "target_symbol": config.target_symbol,
                                "experiment": experiment,
                                "model_variant": model_variant,
                                "reward_variant": reward_variant,
                                "state_feature_mode": state_mode,
                                "seed": seed,
                                "period": "test",
                                **counts,
                                "hold_ratio": counts["hold_count"] / total,
                                "buy_ratio": counts["buy_count"] / total,
                                "sell_ratio": counts["sell_count"] / total,
                            }
                        )
                        log_lines.append(
                            f"- {run_id}: final_equity={metrics.get('final_equity')}, "
                            f"trades={metrics.get('number_of_trades')}, exposure={metrics.get('exposure_ratio')}"
                        )

    seed_df = pd.DataFrame(seed_rows)
    action_df = pd.DataFrame(action_rows)
    state_diag_df = pd.DataFrame(state_diag_rows)
    summary_df = _model_summary(seed_df)
    reward_df = _reward_comparison(seed_df)
    paths = {
        "seed_metrics": MODEL_UPGRADE_DIR / "seed_level_metrics.csv",
        "summary": MODEL_UPGRADE_DIR / "model_upgrade_summary.csv",
        "actions": MODEL_UPGRADE_DIR / "action_distribution_diagnostics.csv",
        "reward_comparison": MODEL_UPGRADE_DIR / "reward_variant_comparison.csv",
        "state_features": MODEL_UPGRADE_DIR / "state_feature_diagnostics.csv",
        "log": MODEL_UPGRADE_DIR / "upgrade_run_log.md",
    }
    seed_df.to_csv(paths["seed_metrics"], index=False, encoding="utf-8-sig")
    summary_df.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    action_df.to_csv(paths["actions"], index=False, encoding="utf-8-sig")
    reward_df.to_csv(paths["reward_comparison"], index=False, encoding="utf-8-sig")
    state_diag_df.to_csv(paths["state_features"], index=False, encoding="utf-8-sig")
    paths["log"].write_text("\n".join(log_lines) + "\n", encoding="utf-8")
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
        frame["MACD_normalized"] = ((_numeric(frame, "MACD") - macd_mean) / macd_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        frame["position_ratio"] = 0.0
        frame["cash_ratio"] = 1.0
        frame["abs_nlp_signal"] = signal.abs()
        frame["nlp_signal_3day_mean"] = signal.rolling(3, min_periods=1).mean()
        frame["nlp_signal_5day_mean"] = signal.rolling(5, min_periods=1).mean()
        frame["nlp_signal_change"] = signal.diff().fillna(0.0)
        frame["relative_signal"] = 0.0
        frame["marketwide_residual_signal"] = 0.0
        if signal_column and _paired_signal_columns(signal_column):
            sector_col, market_col = _paired_signal_columns(signal_column)
            if sector_col in frame.columns and market_col in frame.columns:
                frame["relative_signal"] = _numeric(frame, sector_col) - _numeric(frame, market_col)
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


def _dqn_config(state_columns: Iterable[str], seed: int, model_variant: str, reward_variant: str, state_mode: str) -> DQNConfig:
    return DQNConfig(
        state_dim=len(list(state_columns)),
        seed=seed,
        model_variant=model_variant,
        reward_variant=reward_variant,
        state_feature_mode=state_mode,
    )


def _append_state_diagnostics(
    rows: list[dict[str, object]],
    *,
    target_symbol: str,
    model_variant: str,
    reward_variant: str,
    state_feature_mode: str,
    experiment: str,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    state_columns: list[str],
) -> None:
    for period, frame in [("train", train_frame), ("test", test_frame)]:
        for feature in state_columns:
            values = pd.to_numeric(frame.get(feature, pd.Series(dtype=float)), errors="coerce")
            rows.append(
                {
                    "run_id": f"{target_symbol}_{model_variant}_{reward_variant}_{state_feature_mode}_{experiment}_{period}",
                    "model_variant": model_variant,
                    "reward_variant": reward_variant,
                    "state_feature_mode": state_feature_mode,
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


def _model_summary(seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return pd.DataFrame()
    grouped = seed_df.groupby(["target_symbol", "experiment", "model_variant", "reward_variant", "state_feature_mode"], dropna=False)
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
        mean_turnover=("turnover", "mean"),
    ).reset_index()


def _reward_comparison(seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return pd.DataFrame()
    grouped = seed_df.groupby(["target_symbol", "experiment", "model_variant", "state_feature_mode", "reward_variant"], dropna=False)
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
    pd.DataFrame().to_csv(MODEL_UPGRADE_DIR / "seed_level_metrics.csv", index=False)
    pd.DataFrame().to_csv(MODEL_UPGRADE_DIR / "model_upgrade_summary.csv", index=False)
    pd.DataFrame().to_csv(MODEL_UPGRADE_DIR / "action_distribution_diagnostics.csv", index=False)
    pd.DataFrame().to_csv(MODEL_UPGRADE_DIR / "reward_variant_comparison.csv", index=False)
    pd.DataFrame().to_csv(MODEL_UPGRADE_DIR / "state_feature_diagnostics.csv", index=False)
    (MODEL_UPGRADE_DIR / "upgrade_run_log.md").write_text("\n".join(log_lines) + "\n", encoding="utf-8")
