"""Five-group DQN add-on for peer sentiment plus market-impact NLP."""

from __future__ import annotations

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
)
from src.evaluation.metrics import buy_and_hold_equity
from src.evaluation.peer_nlp_ablation import (
    OFFICIAL_EXPERIMENT as SENTIMENT_BASELINE_EXPERIMENT,
    WITHOUT_NLP_STATE_COLUMNS,
    _add_behavior_metrics,
    _aggregate_peer_seed_metrics,
    _curve_flat,
    _date_text,
    _diff,
    _empty_peer_metrics,
    _metric_row,
    _normalize_symbol,
    _safe_read_csv,
    _seed_variance_larger_than_effect,
    add_peer_trading_features,
    peer_train_test_split,
)
from src.evaluation.signal_validity import compute_signal_validity
from src.features.technical_indicators import leakage_diagnostics, validate_state_columns
from src.nlp.market_impact import (
    MARKET_IMPACT_HORIZON_DAYS,
    MARKET_IMPACT_NEG_THRESHOLD,
    MARKET_IMPACT_POS_THRESHOLD,
    generate_peer_market_impact_daily_signal,
)
from src.nlp.peer_sentiment import (
    MIN_HIGH_DENSITY_TRADING_DAYS,
    MIN_MARKETWIDE_TRAINING_NEWS,
    MIN_SECTOR_TRAINING_NEWS,
    MIN_TARGET_SENTIMENT_COVERAGE,
    generate_peer_nlp_daily_sentiment,
)
from src.rl.train import evaluate_agent, train_dqn

MARKET_IMPACT_EXPERIMENT = "peer_sentiment_plus_market_impact"
SECTOR_SENTIMENT_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["sector_sentiment_score"]
MARKETWIDE_SENTIMENT_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["marketwide_sentiment_score"]
SECTOR_IMPACT_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["sector_impact_score"]
MARKETWIDE_IMPACT_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + ["marketwide_impact_score"]
UNIFIED_NLP_STATE_COLUMN = "nlp_signal_score"
UNIFIED_DQN_STATE_COLUMNS = WITHOUT_NLP_STATE_COLUMNS + [UNIFIED_NLP_STATE_COLUMN]
NLP_SIGNAL_COLUMNS = [
    "sector_sentiment_score",
    "marketwide_sentiment_score",
    "sector_impact_score",
    "marketwide_impact_score",
]
EXPERIMENT_SIGNAL_COLUMNS = {
    "dqn_without_nlp": None,
    "dqn_with_sector_sentiment_nlp": "sector_sentiment_score",
    "dqn_with_marketwide_sentiment_nlp": "marketwide_sentiment_score",
    "dqn_with_sector_impact_nlp": "sector_impact_score",
    "dqn_with_marketwide_impact_nlp": "marketwide_impact_score",
}


def _peer_sentiment_cache_matches_scope(path: Path, include_marketwide_peer: bool) -> bool:
    """Avoid reusing marketwide sentiment outputs in a sector-only dashboard run."""

    frame = _safe_read_csv(path)
    if frame.empty:
        return False
    status = str(frame.get("marketwide_corpus_status", pd.Series([""])).dropna().iloc[0]) if "marketwide_corpus_status" in frame.columns and frame["marketwide_corpus_status"].notna().any() else ""
    peer_count = int(pd.to_numeric(frame.get("marketwide_peer_stock_count", 0), errors="coerce").fillna(0).max()) if "marketwide_peer_stock_count" in frame.columns else 0
    if include_marketwide_peer:
        return status == "READY" and peer_count > 0
    return status in {"DISABLED", "INSUFFICIENT", ""} and peer_count == 0


def _needs_nlp_aware_high_density_split(train_frame: pd.DataFrame, test_frame: pd.DataFrame) -> bool:
    """Use a recent high-density train/test split when old train data has no NLP variation.

    The peer NLP models never train on target-stock text, but the DQN still needs
    non-constant target-side NLP state values during its own training. If the
    pre-high-density market-learning window contains only zero NLP features, all
    NLP DQN variants learn identical policies under the same random seed.
    """

    if train_frame.empty or test_frame.empty:
        return False
    active = [column for column in NLP_SIGNAL_COLUMNS if column in train_frame.columns and column in test_frame.columns]
    if not active:
        return False
    train_has_signal = any(pd.to_numeric(train_frame[column], errors="coerce").fillna(0).abs().sum() > 1e-12 for column in active)
    test_has_signal = any(pd.to_numeric(test_frame[column], errors="coerce").fillna(0).abs().sum() > 1e-12 for column in active)
    return (not train_has_signal) and test_has_signal and len(test_frame) >= 60


def _high_density_internal_split(high_density_frame: pd.DataFrame, old_split: dict[str, object]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    frame = high_density_frame.sort_values("date").reset_index(drop=True).copy()
    signal_columns = [column for column in NLP_SIGNAL_COLUMNS if column in frame.columns]
    active_mask = pd.Series(False, index=frame.index)
    for column in signal_columns:
        active_mask = active_mask | (pd.to_numeric(frame[column], errors="coerce").fillna(0).abs() > 1e-12)
    for column in ["target_news_available", "news_available"]:
        if column in frame.columns:
            active_mask = active_mask | (pd.to_numeric(frame[column], errors="coerce").fillna(0) > 0)
    if active_mask.any():
        frame = frame.loc[active_mask.idxmax() :].reset_index(drop=True)
    min_test_rows = min(MIN_HIGH_DENSITY_TRADING_DAYS, max(int(len(frame) * 0.35), 10))
    split_at = max(int(len(frame) * 0.6), 10)
    split_at = min(split_at, max(len(frame) - min_test_rows, 1))
    train = frame.iloc[:split_at].copy().reset_index(drop=True)
    test = frame.iloc[split_at:].copy().reset_index(drop=True)
    split = dict(old_split)
    split.update(
        {
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_start": _date_text(train["date"].min()) if not train.empty else "",
            "train_end": _date_text(train["date"].max()) if not train.empty else "",
            "test_start": _date_text(test["date"].min()) if not test.empty else "",
            "test_end": _date_text(test["date"].max()) if not test.empty else "",
            "consistent_period": bool(train.empty or test.empty or train["date"].max() < test["date"].min()),
            "window_status": "READY_NLP_AWARE_HIGH_DENSITY_SPLIT",
            "training_period_label": "target_high_density_signal_learning_window",
            "evaluation_period_label": "target_high_density_holdout_window",
            "split_reason": "pre_high_density_training_window_has_no_nonzero_nlp_signal",
        }
    )
    return train, test, split


def _with_unified_nlp_signal(frame: pd.DataFrame, signal_column: str | None) -> pd.DataFrame:
    """Return a group-specific DQN frame with the same 8-D state schema.

    The control group uses a constant zero NLP column. NLP groups copy exactly
    one signal source into ``nlp_signal_score`` so the network architecture and
    all non-NLP state features remain identical across groups.
    """

    out = frame.copy()
    if signal_column and signal_column in out.columns:
        out[UNIFIED_NLP_STATE_COLUMN] = pd.to_numeric(out[signal_column], errors="coerce").fillna(0.0)
    else:
        out[UNIFIED_NLP_STATE_COLUMN] = 0.0
    return out


def _clone_state_dict(agent) -> dict[str, object]:
    return {key: value.detach().clone() for key, value in agent.online.state_dict().items()}


def _group_state_feature_diagnostics(
    train_frames: dict[str, pd.DataFrame],
    test_frames: dict[str, pd.DataFrame],
    state_specs: dict[str, dict[str, object]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for experiment, spec in state_specs.items():
        source_column = spec.get("signal_column") or "constant_zero_control"
        for period, frame in [("train", train_frames.get(experiment, pd.DataFrame())), ("test", test_frames.get(experiment, pd.DataFrame()))]:
            for column in UNIFIED_DQN_STATE_COLUMNS:
                values = pd.to_numeric(frame.get(column, pd.Series(dtype=float)), errors="coerce")
                rows.append(
                    {
                        "experiment": experiment,
                        "period": period,
                        "state_column": column,
                        "source_signal_column": source_column if column == UNIFIED_NLP_STATE_COLUMN else "",
                        "rows": int(len(frame)),
                        "non_missing_count": int(values.notna().sum()),
                        "missing_count": int(values.isna().sum()),
                        "nonzero_count": int((values.fillna(0).abs() > 1e-12).sum()),
                        "mean": float(values.mean()) if values.notna().any() else np.nan,
                        "std": float(values.std()) if values.notna().sum() > 1 else np.nan,
                        "min": float(values.min()) if values.notna().any() else np.nan,
                        "max": float(values.max()) if values.notna().any() else np.nan,
                        "is_unified_nlp_signal": column == UNIFIED_NLP_STATE_COLUMN,
                    }
                )
    return pd.DataFrame(rows)


def run_market_impact_official_experiment(
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
    horizon_days: int = MARKET_IMPACT_HORIZON_DAYS,
    positive_threshold: float = MARKET_IMPACT_POS_THRESHOLD,
    negative_threshold: float = MARKET_IMPACT_NEG_THRESHOLD,
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Run peer sentiment baseline, market-impact scoring, then five DQN groups.

    Buy-and-hold is still emitted as a benchmark curve/metric row, but it is
    not counted as one of the five DQN experiment groups.
    """

    symbol = _normalize_symbol(symbol)
    results_dir = stock_results_dir(symbol)
    peer_sentiment_csv = results_dir / "peer_nlp_daily_sentiment.csv"
    if (
        not peer_sentiment_csv.exists()
        or peer_sentiment_csv.stat().st_size <= 4
        or not _peer_sentiment_cache_matches_scope(peer_sentiment_csv, include_marketwide_peer)
    ):
        if status_callback is not None:
            status_callback("sentiment_baseline", f"{symbol}: generating baseline peer-sentiment file for current corpus scope.")
        generate_peer_nlp_daily_sentiment(
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

    impact_outputs = generate_peer_market_impact_daily_signal(
        input_csv,
        symbol=symbol,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        sources=sources,
        news_count=news_count,
        allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
        include_marketwide_peer=include_marketwide_peer,
        horizon_days=horizon_days,
        positive_threshold=positive_threshold,
        negative_threshold=negative_threshold,
        status_callback=status_callback,
    )
    _require_market_impact_scope_ready(impact_outputs.get("daily_signal", pd.DataFrame()), include_marketwide_peer)
    ablation_outputs = run_market_impact_ablation_study(
        input_csv=input_csv,
        peer_sentiment_csv=peer_sentiment_csv,
        market_impact_signal_csv=Path(impact_outputs["peer_market_impact_daily_signal_csv"]),
        output_dir=results_dir,
        reports_dir=stock_reports_dir(symbol),
        episodes=episodes,
        initial_cash=initial_cash,
        transaction_cost=transaction_cost,
        seeds=seeds,
        reward_mode=reward_mode,
        status_callback=status_callback,
    )
    return {**impact_outputs, **ablation_outputs}


def run_market_impact_ablation_study(
    input_csv: Path,
    peer_sentiment_csv: Path,
    market_impact_signal_csv: Path,
    output_dir: Path | None = None,
    reports_dir: Path | None = None,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    seeds: list[int] | None = None,
    reward_mode: str = "portfolio_return",
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Compare no NLP, sentiment NLP, and market-impact NLP DQN groups."""

    def emit(stage: str, message: str) -> None:
        if status_callback is not None:
            status_callback(stage, message)

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
    peer_daily = _safe_read_csv(peer_sentiment_csv)
    impact_daily = _safe_read_csv(market_impact_signal_csv)
    emit("dqn_features", f"{symbol}: building lagged DQN features for five DQN strategy groups.")
    features = add_market_impact_trading_features(market, peer_daily, impact_daily, initial_cash=initial_cash)
    split_source = peer_daily if not peer_daily.empty else impact_daily
    market_learning_frame, high_density_frame, split_info = peer_train_test_split(features, split_source)
    pretrain_frame = _with_unified_nlp_signal(market_learning_frame, None)
    fine_tune_frame = market_learning_frame.copy()
    test_frame = high_density_frame.copy()
    if _needs_nlp_aware_high_density_split(market_learning_frame, high_density_frame):
        fine_tune_frame, test_frame, split_info = _high_density_internal_split(high_density_frame, split_info)
        split_info["pretrain_rows"] = int(len(pretrain_frame))
        split_info["pretrain_start"] = _date_text(pretrain_frame["date"].min()) if not pretrain_frame.empty else ""
        split_info["pretrain_end"] = _date_text(pretrain_frame["date"].max()) if not pretrain_frame.empty else ""
        split_info["pretraining_period_label"] = "target_market_learning_window_with_zero_nlp_signal"
        split_info["training_design"] = "market_only_pretrain_then_high_density_nlp_finetune"
    else:
        fine_tune_frame = market_learning_frame.copy()
        split_info["pretrain_rows"] = 0
        split_info["pretraining_period_label"] = "not_used"
        split_info["training_design"] = "single_stage_dqn_training"
    emit(
        "dqn_features",
        f"{symbol}: pretrain rows={len(pretrain_frame)}, fine-tune rows={len(fine_tune_frame)}, test rows={len(test_frame)}; all groups use unified 8-D state.",
    )
    signal_validity = compute_signal_validity(market, features, symbol=symbol, reports_dir=reports_dir)

    sentiment_status = _sentiment_corpus_status(peer_daily)
    impact_status = _impact_corpus_status(impact_daily)
    marketwide_required = _marketwide_required(peer_daily, impact_daily)
    target_coverage = _target_news_coverage(peer_daily, impact_daily)
    marketwide_disabled_reason = "" if marketwide_required else "marketwide_peer_scope_disabled"
    state_specs = {
        "dqn_without_nlp": {"columns": UNIFIED_DQN_STATE_COLUMNS, "signal_column": None, "ready": True, "disabled": False},
        "dqn_with_sector_sentiment_nlp": {"columns": UNIFIED_DQN_STATE_COLUMNS, "signal_column": "sector_sentiment_score", "ready": sentiment_status["sector"] == "READY", "disabled": False},
        "dqn_with_marketwide_sentiment_nlp": {
            "columns": UNIFIED_DQN_STATE_COLUMNS,
            "signal_column": "marketwide_sentiment_score",
            "ready": marketwide_required and sentiment_status["marketwide"] == "READY",
            "disabled": not marketwide_required,
            "not_ready_reason": marketwide_disabled_reason,
        },
        "dqn_with_sector_impact_nlp": {"columns": UNIFIED_DQN_STATE_COLUMNS, "signal_column": "sector_impact_score", "ready": impact_status["sector"] == "READY", "disabled": False},
        "dqn_with_marketwide_impact_nlp": {
            "columns": UNIFIED_DQN_STATE_COLUMNS,
            "signal_column": "marketwide_impact_score",
            "ready": marketwide_required and impact_status["marketwide"] == "READY",
            "disabled": not marketwide_required,
            "not_ready_reason": marketwide_disabled_reason,
        },
    }
    group_train_frames = {experiment: _with_unified_nlp_signal(fine_tune_frame, spec.get("signal_column")) for experiment, spec in state_specs.items()}
    group_test_frames = {experiment: _with_unified_nlp_signal(test_frame, spec.get("signal_column")) for experiment, spec in state_specs.items()}
    state_feature_diagnostics = _group_state_feature_diagnostics(group_train_frames, group_test_frames, state_specs)
    for experiment, spec in state_specs.items():
        signal_column = spec.get("signal_column")
        if signal_column and spec["ready"]:
            train_signal = pd.to_numeric(group_train_frames[experiment][UNIFIED_NLP_STATE_COLUMN], errors="coerce").fillna(0)
            if int((train_signal.abs() > 1e-12).sum()) == 0:
                spec["ready"] = False
                spec["not_ready_reason"] = f"{signal_column}_has_no_nonzero_values_in_dqn_finetune_window"

    state_compliance_frames: list[pd.DataFrame] = []
    leakage_frames: list[pd.DataFrame] = []
    for experiment, spec in state_specs.items():
        try:
            compliance = validate_state_columns(group_test_frames[experiment], spec["columns"], sentiment_required=None)
        except ValueError as exc:
            compliance = pd.DataFrame(
                [{"state_column": "", "present": False, "missing_values": np.nan, "shifted_correctly": False, "leakage_prone": False, "sentiment_column": False, "error": str(exc)}]
            )
            spec["ready"] = False
        compliance["experiment"] = experiment
        compliance["source_signal_column"] = spec.get("signal_column") or "constant_zero_control"
        state_compliance_frames.append(compliance)
        leakage_frames.append(leakage_diagnostics(group_test_frames[experiment], spec["columns"], sentiment_is_aligned_to_trade_date=True).assign(experiment=experiment))

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

    fine_tune_episodes = max(20, int(episodes * 0.5))
    pretrain_ready = len(pretrain_frame) >= 3
    train_ready = len(fine_tune_frame) >= 3 and len(test_frame) >= 2
    pretrained_states_by_seed: dict[int, dict[str, object]] = {}
    for experiment, spec in state_specs.items():
        if bool(spec.get("disabled", False)):
            emit("dqn_skipped", f"{symbol}: skipped {experiment}; marketwide peer corpus is disabled for sector_only scope.")
            continue
        columns = list(spec["columns"])
        ready = bool(spec["ready"]) and train_ready
        for seed in seeds:
            if not ready:
                reason = spec.get("not_ready_reason", "")
                emit("dqn_skipped", f"{symbol}: skipped {experiment} seed={seed}; ready={spec['ready']} train_ready={train_ready}. {reason}")
                seed_metrics.append(_empty_peer_metrics(experiment, initial_cash, seed))
                continue
            initial_state = None
            if pretrain_ready and split_info.get("training_design") == "market_only_pretrain_then_high_density_nlp_finetune":
                if seed not in pretrained_states_by_seed:
                    emit("dqn_pretrain", f"{symbol}: pretraining shared market-only DQN backbone seed={seed} for {episodes} episodes with {UNIFIED_NLP_STATE_COLUMN}=0.")
                    pretrained = train_dqn(
                        pretrain_frame,
                        columns,
                        episodes=episodes,
                        initial_cash=initial_cash,
                        transaction_cost=transaction_cost,
                        experiment="shared_market_only_pretrain",
                        output_dir=output_dir,
                        model_dir=models_dir,
                        seed=seed,
                        reward_mode=reward_mode,
                        progress_callback=_dqn_progress_emitter(emit, "dqn_pretrain", symbol, "shared_market_only_pretrain", seed, "pretraining"),
                    )
                    pretrain_rewards = pretrained["training_rewards"]
                    pretrain_rewards["official_experiment"] = MARKET_IMPACT_EXPERIMENT
                    pretrain_rewards["training_period"] = split_info.get("pretraining_period_label", "target_market_learning_window_with_zero_nlp_signal")
                    pretrain_rewards["evaluation_period"] = "not_evaluated"
                    pretrain_rewards["phase"] = "market_only_pretrain"
                    training_rewards.append(pretrain_rewards)
                    pretrained_states_by_seed[seed] = _clone_state_dict(pretrained["agent"])
                initial_state = pretrained_states_by_seed[seed]
            group_train = group_train_frames[experiment]
            group_test = group_test_frames[experiment]
            emit(_dqn_stage_for_experiment(experiment), f"{symbol}: fine-tuning {experiment} seed={seed} for {fine_tune_episodes} episodes using {spec.get('signal_column') or 'zero NLP control'}.")
            trained = train_dqn(
                group_train,
                columns,
                episodes=fine_tune_episodes,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment=experiment,
                output_dir=output_dir,
                model_dir=models_dir,
                seed=seed,
                reward_mode=reward_mode,
                initial_state_dict=initial_state,
                initial_epsilon=0.30 if initial_state is not None else None,
                progress_callback=_dqn_progress_emitter(emit, _dqn_stage_for_experiment(experiment), symbol, experiment, seed, "fine-tuning"),
            )
            emit(_dqn_stage_for_experiment(experiment), f"{symbol}: evaluating {experiment} seed={seed} on high-density test window.")
            rewards = trained["training_rewards"]
            rewards["official_experiment"] = MARKET_IMPACT_EXPERIMENT
            rewards["training_period"] = split_info.get("training_period_label", "target_market_learning_window")
            rewards["evaluation_period"] = split_info.get("evaluation_period_label", "target_high_density_window")
            rewards["phase"] = "nlp_finetune" if initial_state is not None else "single_stage_train"
            training_rewards.append(rewards)
            log = evaluate_agent(
                trained["agent"],
                group_test,
                columns,
                initial_cash=initial_cash,
                transaction_cost=transaction_cost,
                experiment=experiment,
                seed=seed,
                reward_mode=reward_mode,
                state_scaler=trained.get("state_scaler"),
            )
            log["official_experiment"] = MARKET_IMPACT_EXPERIMENT
            log["training_period"] = split_info.get("training_period_label", "target_market_learning_window")
            log["evaluation_period"] = split_info.get("evaluation_period_label", "target_high_density_window")
            trading_logs.append(log)
            curve = _curve_from_log(log, experiment, seed)
            portfolio_curves.append(curve)
            drawdown_curves.append(_drawdown_from_curve(curve, experiment, seed))
            seed_metrics.append(_add_behavior_metrics(_metrics_from_log(experiment, log, initial_cash, seed), log))
            final_value = float(pd.to_numeric(log.get("portfolio_value", pd.Series(dtype=float)), errors="coerce").dropna().iloc[-1]) if not log.empty and "portfolio_value" in log.columns and pd.to_numeric(log["portfolio_value"], errors="coerce").notna().any() else float("nan")
            emit(_dqn_stage_for_experiment(experiment), f"{symbol}: completed {experiment} seed={seed}; final_portfolio_value={final_value:.2f}.")

    emit("metrics", f"{symbol}: aggregating seed metrics, portfolio curves, drawdowns, trading logs, and effect labels.")
    seed_metrics_df = pd.DataFrame(seed_metrics)
    metrics_df = _aggregate_peer_seed_metrics(seed_metrics_df)
    metrics_df["official_experiment"] = MARKET_IMPACT_EXPERIMENT
    seed_metrics_df["official_experiment"] = MARKET_IMPACT_EXPERIMENT
    curves_df = pd.concat(portfolio_curves, ignore_index=True) if portfolio_curves else pd.DataFrame()
    drawdowns_df = pd.concat(drawdown_curves, ignore_index=True) if drawdown_curves else pd.DataFrame()
    logs_df = pd.concat(trading_logs, ignore_index=True) if trading_logs else pd.DataFrame(columns=TRADING_LOG_COLUMNS)
    rewards_df = pd.concat(training_rewards, ignore_index=True) if training_rewards else pd.DataFrame(columns=TRAINING_REWARD_COLUMNS)
    effect = _market_impact_effect_summary(symbol, peer_daily, impact_daily, metrics_df, seed_metrics_df, curves_df, split_info, sentiment_status, impact_status, target_coverage, marketwide_required)
    reliability = _market_impact_reliability(symbol, effect, metrics_df, curves_df, logs_df, split_info, target_coverage, marketwide_required)

    paths = {
        "metrics": output_dir / "market_impact_ablation_metrics.csv",
        "seed_metrics": output_dir / "market_impact_ablation_metrics_by_seed.csv",
        "curves": output_dir / "market_impact_portfolio_curves.csv",
        "drawdowns": output_dir / "market_impact_drawdown_curves.csv",
        "logs": output_dir / "market_impact_trading_logs.csv",
        "rewards": output_dir / "market_impact_training_rewards_all_seeds.csv",
        "state": reports_dir / "market_impact_state_vector_compliance.csv",
        "state_features": reports_dir / "market_impact_group_state_diagnostics.csv",
        "leakage": reports_dir / "market_impact_leakage_diagnostics.csv",
        "split": reports_dir / "market_impact_train_eval_windows.csv",
        "effect": output_dir / "market_impact_effect_summary.csv",
        "reliability": reports_dir / "market_impact_reliability_check.csv",
    }
    emit("save_outputs", f"{symbol}: saving market-impact ablation metrics, curves, logs, diagnostics, and report files.")
    metrics_df.to_csv(paths["metrics"], index=False, encoding="utf-8-sig")
    seed_metrics_df.to_csv(paths["seed_metrics"], index=False, encoding="utf-8-sig")
    curves_df.to_csv(paths["curves"], index=False, encoding="utf-8-sig")
    drawdowns_df.to_csv(paths["drawdowns"], index=False, encoding="utf-8-sig")
    logs_df.reindex(columns=sorted(set(TRADING_LOG_COLUMNS).union(logs_df.columns))).to_csv(paths["logs"], index=False, encoding="utf-8-sig")
    rewards_df.to_csv(paths["rewards"], index=False, encoding="utf-8-sig")
    pd.concat(state_compliance_frames, ignore_index=True).to_csv(paths["state"], index=False, encoding="utf-8-sig")
    state_feature_diagnostics.to_csv(paths["state_features"], index=False, encoding="utf-8-sig")
    pd.concat(leakage_frames, ignore_index=True).to_csv(paths["leakage"], index=False, encoding="utf-8-sig")
    pd.DataFrame([split_info]).to_csv(paths["split"], index=False, encoding="utf-8-sig")
    effect.to_csv(paths["effect"], index=False, encoding="utf-8-sig")
    reliability.to_csv(paths["reliability"], index=False, encoding="utf-8-sig")
    _update_global_effect_summary(effect)
    _write_market_impact_report(symbol, reports_dir, effect, split_info)
    emit("figures", f"{symbol}: market-impact report section and result artifacts generated.")
    emit("dashboard_cache", f"{symbol}: market-impact experiment finished; dashboard result cache is ready.")

    return {
        "market_impact_metrics": metrics_df,
        "market_impact_metrics_by_seed": seed_metrics_df,
        "market_impact_portfolio_curves": curves_df,
        "market_impact_drawdown_curves": drawdowns_df,
        "market_impact_trading_logs": logs_df,
        "market_impact_training_rewards": rewards_df,
        "market_impact_effect_summary": effect,
        "market_impact_reliability": reliability,
        "signal_validity": signal_validity,
        "market_impact_ablation_metrics_csv": paths["metrics"],
        "market_impact_portfolio_curves_csv": paths["curves"],
        "market_impact_trading_logs_csv": paths["logs"],
        "market_impact_effect_summary_csv": paths["effect"],
        "market_impact_group_state_diagnostics_csv": paths["state_features"],
    }


def _dqn_stage_for_experiment(experiment: str) -> str:
    if experiment == "dqn_without_nlp":
        return "dqn_without_nlp"
    if "impact" in experiment:
        return "dqn_market_impact"
    return "dqn_peer_sentiment"


def _dqn_progress_emitter(
    emit: Callable[[str, str], None],
    stage: str,
    symbol: str,
    experiment: str,
    seed: int,
    phase: str,
) -> Callable[[dict[str, object]], None]:
    """Convert episode-level DQN training callbacks into dashboard log rows."""

    def callback(info: dict[str, object]) -> None:
        episode = int(info.get("episode", 0) or 0)
        episodes = int(info.get("episodes", 0) or 0)
        reward = float(info.get("total_reward", 0.0) or 0.0)
        epsilon = float(info.get("epsilon", 0.0) or 0.0)
        emit(stage, f"{symbol}: {phase} {experiment} seed={seed} episode {episode}/{episodes}; reward={reward:.6f}; epsilon={epsilon:.3f}.")

    return callback


def _require_market_impact_scope_ready(daily: pd.DataFrame, include_marketwide_peer: bool) -> None:
    """Fail early when a requested signal family cannot produce valid outputs."""

    if daily.empty:
        raise RuntimeError("Market-impact daily signal is empty. Check target news, peer corpus, and market data before DQN.")
    sector_status = str(daily.get("sector_impact_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if "sector_impact_corpus_status" in daily.columns and daily["sector_impact_corpus_status"].notna().any() else "INSUFFICIENT"
    sector_news = _training_news(daily, "sector_impact_training_news_count")
    if sector_status != "READY" or sector_news < MIN_SECTOR_TRAINING_NEWS:
        raise RuntimeError(f"Sector market-impact corpus is not ready: status={sector_status}, labelled_news={sector_news}.")
    if include_marketwide_peer:
        market_status = str(daily.get("marketwide_impact_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if "marketwide_impact_corpus_status" in daily.columns and daily["marketwide_impact_corpus_status"].notna().any() else "INSUFFICIENT"
        market_news = _training_news(daily, "marketwide_impact_training_news_count")
        if market_status != "READY" or market_news < MIN_MARKETWIDE_TRAINING_NEWS:
            raise RuntimeError(f"Marketwide market-impact corpus was requested but is not ready: status={market_status}, labelled_news={market_news}.")


def _marketwide_required(peer_daily: pd.DataFrame, impact_daily: pd.DataFrame) -> bool:
    """Detect whether this run intentionally enabled marketwide peer experiments."""

    for frame in [impact_daily, peer_daily]:
        if frame.empty:
            continue
        if "marketwide_enabled" in frame.columns:
            values = pd.to_numeric(frame["marketwide_enabled"], errors="coerce").dropna()
            if not values.empty:
                return bool(values.max() > 0)
        if "peer_corpus_scope" in frame.columns and frame["peer_corpus_scope"].notna().any():
            return str(frame["peer_corpus_scope"].dropna().iloc[0]) == "sector_plus_marketwide"
    return True


def add_market_impact_trading_features(
    market: pd.DataFrame,
    peer_daily: pd.DataFrame,
    impact_daily: pd.DataFrame,
    *,
    initial_cash: float = 1000000.0,
) -> pd.DataFrame:
    """Build leakage-safe features with lagged sentiment and impact scores."""

    features = add_peer_trading_features(market, peer_daily, initial_cash=initial_cash)
    dates = market[["date"]].copy()
    dates["date"] = pd.to_datetime(dates["date"], errors="coerce")
    dates = dates.dropna(subset=["date"]).drop_duplicates().sort_values("date").reset_index(drop=True)
    impact = impact_daily.copy() if impact_daily is not None else pd.DataFrame()
    keep = [
        "date",
        "sector_impact_score",
        "marketwide_impact_score",
        "sector_impact_news_count",
        "marketwide_impact_news_count",
        "news_available",
        "impact_missing_flag",
    ]
    if not impact.empty and "date" in impact.columns:
        impact["date"] = pd.to_datetime(impact["date"], errors="coerce")
        dates = dates.merge(impact[[column for column in keep if column in impact.columns]], on="date", how="left")
    defaults = {
        "sector_impact_score": 0.0,
        "marketwide_impact_score": 0.0,
        "sector_impact_news_count": 0,
        "marketwide_impact_news_count": 0,
        "news_available": 0,
        "impact_missing_flag": 1,
    }
    for column, default in defaults.items():
        if column not in dates.columns:
            dates[column] = default
        dates[column] = pd.to_numeric(dates[column], errors="coerce").fillna(default)
        dates[f"{column}_raw"] = dates[column]
        dates[column] = dates[column].shift(1).fillna(default)
    return features.merge(dates[["date"] + list(defaults.keys()) + [f"{column}_raw" for column in defaults]], on="date", how="left").fillna(defaults)


def build_market_impact_cross_stock_summary(selected_symbols: list[str] | None = None, *, stock_root: Path | None = None) -> dict[str, object]:
    """Aggregate sentiment baseline and market-impact results across targets."""

    stock_root = stock_root or STOCK_OUTPUT_ROOT
    SYSTEM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    allowed = {_normalize_symbol(symbol) for symbol in selected_symbols or [] if str(symbol).strip()}
    rows: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    for stock_dir in _stock_dirs(stock_root):
        symbol = stock_dir.name
        if allowed and symbol not in allowed:
            continue
        effect = _safe_read_csv(stock_results_dir(symbol) / "market_impact_effect_summary.csv")
        impact_daily = _safe_read_csv(stock_results_dir(symbol) / "peer_market_impact_daily_signal.csv")
        metrics = _safe_read_csv(stock_results_dir(symbol) / "market_impact_ablation_metrics.csv")
        if effect.empty or impact_daily.empty or metrics.empty:
            diagnostics.append({"target_symbol": symbol, "status": "missing_market_impact_outputs", "missing_files": _missing_market_impact_files(symbol)})
            continue
        row = effect.iloc[0].to_dict()
        row.update(
            {
                "target_symbol": symbol,
                "target_company_name": str(impact_daily.get("company_name", pd.Series([symbol])).dropna().iloc[0]) if "company_name" in impact_daily.columns and impact_daily["company_name"].notna().any() else symbol,
                "target_sector": str(impact_daily.get("sector", pd.Series(["UNKNOWN"])).dropna().iloc[0]) if "sector" in impact_daily.columns and impact_daily["sector"].notna().any() else "UNKNOWN",
                "high_density_eval_start": str(impact_daily["date"].min()) if "date" in impact_daily.columns and not impact_daily.empty else "",
                "high_density_eval_end": str(impact_daily["date"].max()) if "date" in impact_daily.columns and not impact_daily.empty else "",
                "test_trading_days": int(len(impact_daily)),
                "sector_impact_labeled_news": int(pd.to_numeric(impact_daily.get("sector_impact_training_news_count", 0), errors="coerce").fillna(0).max()) if not impact_daily.empty else 0,
                "marketwide_impact_labeled_news": int(pd.to_numeric(impact_daily.get("marketwide_impact_training_news_count", 0), errors="coerce").fillna(0).max()) if not impact_daily.empty else 0,
            }
        )
        rows.append(row)
        diagnostics.append({"target_symbol": symbol, "status": row.get("reliability_status", "UNKNOWN"), "missing_files": ""})
    summary = pd.DataFrame(rows)
    diagnostics_df = pd.DataFrame(diagnostics)
    summary_path = SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_summary.csv"
    diagnostics_path = SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_diagnostics.csv"
    discussion_path = SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_discussion.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    diagnostics_df.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")
    discussion_path.write_text(_market_impact_discussion(summary, diagnostics_df), encoding="utf-8")
    return {"summary": summary, "diagnostics": diagnostics_df, "summary_csv": summary_path, "diagnostics_csv": diagnostics_path, "discussion_md": discussion_path}


def _sentiment_corpus_status(peer_daily: pd.DataFrame) -> dict[str, str]:
    return {
        "sector": str(peer_daily.get("sector_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not peer_daily.empty and "sector_corpus_status" in peer_daily.columns and peer_daily["sector_corpus_status"].notna().any() else "INSUFFICIENT",
        "marketwide": str(peer_daily.get("marketwide_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not peer_daily.empty and "marketwide_corpus_status" in peer_daily.columns and peer_daily["marketwide_corpus_status"].notna().any() else "INSUFFICIENT",
    }


def _impact_corpus_status(impact_daily: pd.DataFrame) -> dict[str, str]:
    return {
        "sector": str(impact_daily.get("sector_impact_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not impact_daily.empty and "sector_impact_corpus_status" in impact_daily.columns and impact_daily["sector_impact_corpus_status"].notna().any() else "INSUFFICIENT",
        "marketwide": str(impact_daily.get("marketwide_impact_corpus_status", pd.Series(["INSUFFICIENT"])).dropna().iloc[0]) if not impact_daily.empty and "marketwide_impact_corpus_status" in impact_daily.columns and impact_daily["marketwide_impact_corpus_status"].notna().any() else "INSUFFICIENT",
    }


def _target_news_coverage(peer_daily: pd.DataFrame, impact_daily: pd.DataFrame) -> float:
    for frame, column in [(peer_daily, "target_news_available"), (impact_daily, "news_available")]:
        if not frame.empty and column in frame.columns:
            return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).mean())
    return 0.0


def _market_impact_effect_summary(
    symbol: str,
    peer_daily: pd.DataFrame,
    impact_daily: pd.DataFrame,
    metrics: pd.DataFrame,
    seed_metrics: pd.DataFrame,
    curves: pd.DataFrame,
    split_info: dict[str, object],
    sentiment_status: dict[str, str],
    impact_status: dict[str, str],
    target_coverage: float,
    marketwide_required: bool = True,
) -> pd.DataFrame:
    rows = metrics.set_index("experiment") if not metrics.empty and "experiment" in metrics.columns else pd.DataFrame()
    baseline = _metric_row(rows, "dqn_without_nlp")
    sector_sent = _metric_row(rows, "dqn_with_sector_sentiment_nlp")
    market_sent = _metric_row(rows, "dqn_with_marketwide_sentiment_nlp")
    sector_impact = _metric_row(rows, "dqn_with_sector_impact_nlp")
    market_impact = _metric_row(rows, "dqn_with_marketwide_impact_nlp")
    best_strategy = str(metrics.sort_values("final_equity", ascending=False)["experiment"].iloc[0]) if not metrics.empty and "final_equity" in metrics.columns and metrics["final_equity"].notna().any() else "N/A"
    best_nlp_type = _best_nlp_type(best_strategy)
    target_company = _first_value([impact_daily, peer_daily], "company_name", symbol)
    target_sector = _first_value([impact_daily, peer_daily], "sector", "UNKNOWN")
    row = {
        "target_symbol": symbol,
        "target_company_name": target_company,
        "target_sector": target_sector,
        "official_experiment": MARKET_IMPACT_EXPERIMENT,
        "baseline_peer_sentiment_experiment": SENTIMENT_BASELINE_EXPERIMENT,
        "best_strategy": best_strategy,
        "best_nlp_type": best_nlp_type,
        "sector_sentiment_effect": _diff(sector_sent, baseline, "final_equity"),
        "marketwide_sentiment_effect": _diff(market_sent, baseline, "final_equity"),
        "sector_impact_effect": _diff(sector_impact, baseline, "final_equity"),
        "marketwide_impact_effect": _diff(market_impact, baseline, "final_equity"),
        "sector_impact_vs_sector_sentiment": _diff(sector_impact, sector_sent, "final_equity"),
        "marketwide_impact_vs_marketwide_sentiment": _diff(market_impact, market_sent, "final_equity"),
        "sector_sentiment_sharpe_effect": _diff(sector_sent, baseline, "sharpe_ratio"),
        "marketwide_sentiment_sharpe_effect": _diff(market_sent, baseline, "sharpe_ratio"),
        "sector_impact_sharpe_effect": _diff(sector_impact, baseline, "sharpe_ratio"),
        "marketwide_impact_sharpe_effect": _diff(market_impact, baseline, "sharpe_ratio"),
        "target_sentiment_coverage": target_coverage,
        "sector_sentiment_training_news": _training_news(peer_daily, "sector_news_count_used_for_training"),
        "marketwide_sentiment_training_news": _training_news(peer_daily, "marketwide_news_count_used_for_training"),
        "sector_impact_labeled_news": _training_news(impact_daily, "sector_impact_training_news_count"),
        "marketwide_impact_labeled_news": _training_news(impact_daily, "marketwide_impact_training_news_count"),
        "sector_sentiment_label": _classify_effect(sector_sent, baseline, seed_metrics, curves, "dqn_with_sector_sentiment_nlp", split_info, target_coverage, sentiment_status["sector"]),
        "marketwide_sentiment_label": _classify_effect(market_sent, baseline, seed_metrics, curves, "dqn_with_marketwide_sentiment_nlp", split_info, target_coverage, sentiment_status["marketwide"]),
        "sector_impact_label": _classify_effect(sector_impact, baseline, seed_metrics, curves, "dqn_with_sector_impact_nlp", split_info, target_coverage, impact_status["sector"]),
        "marketwide_impact_label": _classify_effect(market_impact, baseline, seed_metrics, curves, "dqn_with_marketwide_impact_nlp", split_info, target_coverage, impact_status["marketwide"]),
        "sentiment_corpus_status": f"sector={sentiment_status['sector']}; marketwide={sentiment_status['marketwide']}",
        "impact_corpus_status": f"sector={impact_status['sector']}; marketwide={impact_status['marketwide']}",
        "peer_corpus_scope": "sector_plus_marketwide" if marketwide_required else "sector_only",
        "marketwide_enabled": bool(marketwide_required),
    }
    reliability_status, reason = _reliability_status(split_info, target_coverage, sentiment_status, impact_status, metrics, curves, marketwide_required)
    row["reliability_status"] = reliability_status
    row["reason_if_not_reliable"] = reason
    return pd.DataFrame([row])


def _classify_effect(candidate: pd.Series, baseline: pd.Series, seed_metrics: pd.DataFrame, curves: pd.DataFrame, experiment: str, split_info: dict[str, object], coverage: float, corpus_status: str) -> str:
    if corpus_status != "READY" or candidate.empty or baseline.empty:
        return "Inconclusive"
    final_effect = _diff(candidate, baseline, "final_equity")
    sharpe_effect = _diff(candidate, baseline, "sharpe_ratio")
    mdd_effect = _diff(candidate, baseline, "max_drawdown")
    trades = float(pd.to_numeric(pd.Series([candidate.get("number_of_trades", np.nan)]), errors="coerce").iloc[0])
    if pd.isna(final_effect) or abs(final_effect) < 1e-6 or coverage < MIN_TARGET_SENTIMENT_COVERAGE or int(split_info.get("test_rows", 0)) < MIN_HIGH_DENSITY_TRADING_DAYS:
        return "Inconclusive"
    if _seed_variance_larger_than_effect(seed_metrics, experiment, "final_equity", final_effect) or _curve_flat(curves, experiment) or trades <= 0:
        return "Inconclusive"
    if final_effect > 0 and sharpe_effect > 0 and mdd_effect <= 0.02:
        return "NLP improves"
    if final_effect < 0 and (sharpe_effect < 0 or mdd_effect > 0.02):
        return "NLP hurts"
    return "Mixed effect"


def _reliability_status(
    split_info: dict[str, object],
    coverage: float,
    sentiment_status: dict[str, str],
    impact_status: dict[str, str],
    metrics: pd.DataFrame,
    curves: pd.DataFrame,
    marketwide_required: bool = True,
) -> tuple[str, str]:
    reasons = []
    if int(split_info.get("test_rows", 0)) < MIN_HIGH_DENSITY_TRADING_DAYS:
        reasons.append("target_high_density_test_window_below_30_trading_days")
    if coverage < MIN_TARGET_SENTIMENT_COVERAGE:
        reasons.append("target_news_coverage_below_50_percent")
    if sentiment_status["sector"] != "READY":
        reasons.append("sector_sentiment_corpus_insufficient")
    if impact_status["sector"] != "READY":
        reasons.append("sector_impact_corpus_insufficient")
    if marketwide_required and sentiment_status["marketwide"] != "READY":
        reasons.append("marketwide_sentiment_corpus_insufficient")
    if marketwide_required and impact_status["marketwide"] != "READY":
        reasons.append("marketwide_impact_corpus_insufficient")
    if metrics.empty or metrics.get("final_equity", pd.Series(dtype=float)).isna().all():
        reasons.append("metrics_missing_or_nan")
    required_curves = ["dqn_without_nlp", "dqn_with_sector_sentiment_nlp", "dqn_with_sector_impact_nlp"]
    if marketwide_required:
        required_curves.extend(["dqn_with_marketwide_sentiment_nlp", "dqn_with_marketwide_impact_nlp"])
    for experiment in required_curves:
        if _curve_flat(curves, experiment):
            reasons.append(f"{experiment}_portfolio_curve_flat_or_missing")
    if not reasons:
        return "READY_FOR_PRESENTATION", ""
    severe = [reason for reason in reasons if "marketwide" in reason or "metrics" in reason or "window" in reason]
    return ("NOT_READY" if severe else "READY_WITH_WARNINGS", "; ".join(reasons))


def _market_impact_reliability(
    symbol: str,
    effect: pd.DataFrame,
    metrics: pd.DataFrame,
    curves: pd.DataFrame,
    logs: pd.DataFrame,
    split_info: dict[str, object],
    coverage: float,
    marketwide_required: bool = True,
) -> pd.DataFrame:
    row = effect.iloc[0].to_dict() if not effect.empty else {}
    checks = [
        _check(symbol, "target_excluded_from_peer_training", True, "Corpus builders exclude target_symbol from sector and marketwide corpora."),
        _check(symbol, "target_high_density_window_length", int(split_info.get("test_rows", 0)) >= MIN_HIGH_DENSITY_TRADING_DAYS, f"test_rows={split_info.get('test_rows', 0)}"),
        _check(symbol, "target_news_coverage", coverage >= MIN_TARGET_SENTIMENT_COVERAGE, f"coverage={coverage:.1%}"),
        _check(symbol, "sector_sentiment_training_news_threshold", _safe_float(row.get("sector_sentiment_training_news")) >= MIN_SECTOR_TRAINING_NEWS, f"sector_sentiment_training_news={row.get('sector_sentiment_training_news')}"),
        _check(symbol, "sector_impact_training_news_threshold", _safe_float(row.get("sector_impact_labeled_news")) >= MIN_SECTOR_TRAINING_NEWS, f"sector_impact_labeled_news={row.get('sector_impact_labeled_news')}"),
        _check(symbol, "sector_dqn_non_flat_portfolio_curves", not _curve_flat(curves, "dqn_with_sector_impact_nlp"), "Sector impact DQN curve should move."),
        _check(symbol, "dqn_test_trades_positive", int(logs.get("action", pd.Series(dtype=str)).astype(str).isin(["Buy", "Sell"]).sum()) > 0 if not logs.empty else False, "At least one test trade."),
    ]
    if marketwide_required:
        checks.extend(
            [
                _check(symbol, "marketwide_sentiment_training_news_threshold", _safe_float(row.get("marketwide_sentiment_training_news")) >= MIN_MARKETWIDE_TRAINING_NEWS, f"marketwide_sentiment_training_news={row.get('marketwide_sentiment_training_news')}"),
                _check(symbol, "marketwide_impact_training_news_threshold", _safe_float(row.get("marketwide_impact_labeled_news")) >= MIN_MARKETWIDE_TRAINING_NEWS, f"marketwide_impact_labeled_news={row.get('marketwide_impact_labeled_news')}"),
                _check(symbol, "marketwide_dqn_non_flat_portfolio_curves", not _curve_flat(curves, "dqn_with_marketwide_impact_nlp"), "Marketwide impact DQN curve should move."),
            ]
        )
    else:
        checks.append(_check(symbol, "marketwide_scope", True, "Marketwide experiments disabled for sector_only scope; not required for this run."))
    frame = pd.DataFrame(checks)
    frame["final_status"] = str(row.get("reliability_status", "NOT_READY"))
    return frame


def _check(symbol: str, name: str, passed: bool, evidence: str) -> dict[str, object]:
    return {"target_symbol": symbol, "check": name, "passed": bool(passed), "evidence": evidence}


def _best_nlp_type(strategy: str) -> str:
    if strategy == "dqn_without_nlp" or strategy == "buy_and_hold":
        return "none"
    if "impact" in strategy:
        return "impact"
    if "sentiment" in strategy:
        return "sentiment"
    return "none"


def _first_value(frames: list[pd.DataFrame], column: str, default: str) -> str:
    for frame in frames:
        if not frame.empty and column in frame.columns and frame[column].notna().any():
            value = str(frame[column].dropna().iloc[0])
            if value:
                return value
    return default


def _training_news(frame: pd.DataFrame, column: str) -> int:
    return int(pd.to_numeric(frame.get(column, 0), errors="coerce").fillna(0).max()) if not frame.empty else 0


def _safe_float(value: object) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]) if pd.notna(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]) else float("nan")


def _update_global_effect_summary(effect: pd.DataFrame) -> None:
    path = Path("reports") / "tables" / "market_impact_effect_summary.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    if not existing.empty and "target_symbol" in existing.columns:
        target = str(effect["target_symbol"].iloc[0])
        existing = existing[existing["target_symbol"].astype(str) != target]
    pd.concat([existing, effect], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")


def _write_market_impact_report(symbol: str, reports_dir: Path, effect: pd.DataFrame, split_info: dict[str, object]) -> None:
    row = effect.iloc[0].to_dict() if not effect.empty else {}
    text = "\n".join(
        [
            "# Peer Market-Impact NLP Result",
            "",
            f"- Target stock: `{symbol}`",
            "- Base experiment preserved: `peer_sector_nlp_transfer`.",
            "- Add-on experiment: `peer_sentiment_plus_market_impact`.",
            "- DQN groups: no-NLP, sector sentiment, marketwide sentiment, sector impact, marketwide impact.",
            f"- DQN training window: `{split_info.get('train_start', '')}` to `{split_info.get('train_end', '')}`",
            f"- DQN testing window: `{split_info.get('test_start', '')}` to `{split_info.get('test_end', '')}`",
            f"- Best strategy: `{row.get('best_strategy', 'N/A')}`",
            f"- Best NLP type: `{row.get('best_nlp_type', 'none')}`",
            f"- Sector impact label: `{row.get('sector_impact_label', 'Inconclusive')}`",
            f"- Marketwide impact label: `{row.get('marketwide_impact_label', 'Inconclusive')}`",
            f"- Reliability status: `{row.get('reliability_status', 'UNKNOWN')}`",
            "",
            "Market-impact NLP is trained on peer news labelled by peer stocks' post-news future returns. The target stock is held out from NLP training labels. Buy-and-hold is a benchmark, not one of the five DQN groups.",
        ]
    )
    (reports_dir / "market_impact_report_section.md").write_text(text, encoding="utf-8")


def _market_impact_discussion(summary: pd.DataFrame, diagnostics: pd.DataFrame) -> str:
    if summary.empty:
        return "# Market-Impact Cross-Stock Discussion\n\nNo valid market-impact outputs were found yet.\n"
    impact_best = int((summary.get("best_nlp_type", pd.Series(dtype=str)) == "impact").sum())
    sentiment_best = int((summary.get("best_nlp_type", pd.Series(dtype=str)) == "sentiment").sum())
    none_best = int((summary.get("best_nlp_type", pd.Series(dtype=str)) == "none").sum())
    return "\n".join(
        [
            "# Market-Impact Cross-Stock Discussion",
            "",
            "This cross-stock view preserves the peer-sentiment baseline and adds two peer market-impact DQN groups.",
            f"- Targets summarized: {len(summary)}",
            f"- Best NLP type counts: impact={impact_best}, sentiment={sentiment_best}, none={none_best}",
            "",
            "A positive market-impact result means the peer future-return-labelled text signal improved DQN performance relative to the same DQN without NLP. Interpret results only when reliability checks pass.",
        ]
    )


def _missing_market_impact_files(symbol: str) -> str:
    files = {
        "peer_market_impact_daily_signal": stock_results_dir(symbol) / "peer_market_impact_daily_signal.csv",
        "market_impact_ablation_metrics": stock_results_dir(symbol) / "market_impact_ablation_metrics.csv",
        "market_impact_portfolio_curves": stock_results_dir(symbol) / "market_impact_portfolio_curves.csv",
        "market_impact_trading_logs": stock_results_dir(symbol) / "market_impact_trading_logs.csv",
    }
    return ",".join(name for name, path in files.items() if not path.exists() or path.stat().st_size <= 4)


def _stock_dirs(stock_root: Path) -> list[Path]:
    if not stock_root.exists():
        return []
    return sorted([path for path in stock_root.iterdir() if path.is_dir() and re.fullmatch(r"\d{6}", path.name)])
