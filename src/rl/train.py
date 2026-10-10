"""DQN training and evaluation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from dataclasses import replace

import numpy as np
import pandas as pd
import torch

from src.rl.dqn_agent import DQNAgent, DQNConfig, seed_everything
from src.rl.replay_buffer import Experience
from src.rl.trading_env import FinancialTradingEnv


def build_state_scaler(data: pd.DataFrame, state_columns: list[str], initial_cash: float) -> dict[str, dict[str, float]]:
    """Build stable per-column scaling for DQN states.

    Raw portfolio state mixes price-like features around tens with cash around
    one million and position measured in shares.  Feeding those directly into
    the Q-network makes the cash dimension dominate the policy and can collapse
    greedy evaluation into always holding.  The scaler is built on the training
    window and reused unchanged during evaluation.
    """

    scaler: dict[str, dict[str, float]] = {}
    price_reference = pd.to_numeric(
        data.get("price", data.get("close", data.get("execution_price", pd.Series([1.0])))),
        errors="coerce",
    ).replace(0, np.nan)
    median_price = float(price_reference.dropna().median()) if price_reference.notna().any() else 1.0
    max_position = max(float(initial_cash) / max(median_price, 1e-6), 1.0)
    for index, column in enumerate(state_columns):
        if column == "cash":
            scaler[column] = {"mode": 1.0, "center": 0.0, "scale": max(float(initial_cash), 1.0), "index": float(index)}
            continue
        if column == "position":
            scaler[column] = {"mode": 1.0, "center": 0.0, "scale": max_position, "index": float(index)}
            continue
        values = pd.to_numeric(data.get(column, pd.Series(dtype=float)), errors="coerce").replace([np.inf, -np.inf], np.nan)
        center = float(values.mean()) if values.notna().any() else 0.0
        scale = float(values.std(ddof=0)) if values.notna().sum() > 1 else 1.0
        if not np.isfinite(scale) or scale < 1e-6:
            scale = max(abs(center), 1.0)
        scaler[column] = {"mode": 0.0, "center": center, "scale": scale, "index": float(index)}
    return scaler


def normalize_state(state: np.ndarray, state_columns: list[str], scaler: dict[str, dict[str, float]] | None) -> np.ndarray:
    if not scaler:
        return state.astype(float)
    normalized = state.astype(float).copy()
    for index, column in enumerate(state_columns):
        spec = scaler.get(column, {"center": 0.0, "scale": 1.0})
        scale = float(spec.get("scale", 1.0)) or 1.0
        normalized[index] = (normalized[index] - float(spec.get("center", 0.0))) / scale
    return np.nan_to_num(normalized, nan=0.0, posinf=0.0, neginf=0.0)


def train_dqn(
    data: pd.DataFrame,
    state_columns: list[str],
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    experiment: str = "dqn",
    output_dir: Path | None = None,
    model_dir: Path | None = None,
    seed: int = 42,
    dqn_config: DQNConfig | None = None,
    reward_mode: str = "portfolio_return",
    reward_variant: str | None = None,
    risk_lambda: float | None = None,
    hold_penalty_enabled: bool | None = None,
    hold_penalty: float | None = None,
    hold_penalty_after_days: int | None = None,
    flat_penalty_enabled: bool | None = None,
    flat_penalty: float | None = None,
    flat_penalty_after_days: int | None = None,
    flat_exposure_threshold: float | None = None,
    trade_exploration_bonus_enabled: bool | None = None,
    trade_exploration_bonus: float | None = None,
    trade_exploration_bonus_episode_ratio: float | None = None,
    action_mode: str | None = None,
    initial_state_dict: dict[str, torch.Tensor] | None = None,
    initial_epsilon: float | None = None,
    state_scaler: dict[str, dict[str, float]] | None = None,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Train a DQN agent and return reward curve plus final trading log."""

    if len(data) < 3:
        raise ValueError("At least 3 market rows are required for DQN training.")
    seed_everything(seed)
    config = dqn_config or DQNConfig(state_dim=len(state_columns), seed=seed)
    config = replace(config, state_dim=len(state_columns), seed=seed)
    decay_steps = max(1, int(max(1, episodes) * max(0.0, min(1.0, config.epsilon_decay_ratio)) * max(1, len(data) - 1)))
    if config.epsilon_start > 0 and config.epsilon_end > 0:
        scheduled_decay = (config.epsilon_end / config.epsilon_start) ** (1.0 / decay_steps)
        config.epsilon_decay = float(min(1.0, max(0.0, scheduled_decay)))
        config.epsilon = float(config.epsilon_start)
        config.epsilon_min = float(config.epsilon_end)
    active_reward_variant = reward_variant or config.reward_variant
    active_risk_lambda = config.risk_lambda if risk_lambda is None else float(risk_lambda)
    active_hold_penalty_enabled = config.hold_penalty_enabled if hold_penalty_enabled is None else bool(hold_penalty_enabled)
    active_hold_penalty = config.hold_penalty if hold_penalty is None else float(hold_penalty)
    active_hold_penalty_after_days = config.hold_penalty_after_days if hold_penalty_after_days is None else int(hold_penalty_after_days)
    active_flat_penalty_enabled = config.flat_penalty_enabled if flat_penalty_enabled is None else bool(flat_penalty_enabled)
    active_flat_penalty = config.flat_penalty if flat_penalty is None else float(flat_penalty)
    active_flat_penalty_after_days = config.flat_penalty_after_days if flat_penalty_after_days is None else int(flat_penalty_after_days)
    active_flat_exposure_threshold = config.flat_exposure_threshold if flat_exposure_threshold is None else float(flat_exposure_threshold)
    active_trade_bonus_enabled = config.trade_exploration_bonus_enabled if trade_exploration_bonus_enabled is None else bool(trade_exploration_bonus_enabled)
    active_trade_bonus = config.trade_exploration_bonus if trade_exploration_bonus is None else float(trade_exploration_bonus)
    active_trade_bonus_ratio = config.trade_exploration_bonus_episode_ratio if trade_exploration_bonus_episode_ratio is None else float(trade_exploration_bonus_episode_ratio)
    active_action_mode = action_mode or config.action_mode
    agent = DQNAgent(config)
    if initial_state_dict is not None:
        agent.online.load_state_dict(initial_state_dict)
        agent.target.load_state_dict(initial_state_dict)
    if initial_epsilon is not None:
        agent.config.epsilon = float(initial_epsilon)
    scaler = state_scaler or build_state_scaler(data, state_columns, initial_cash)
    reward_rows = []
    exploration_rows = []
    final_log = pd.DataFrame()

    for episode in range(1, episodes + 1):
        env = FinancialTradingEnv(
            data,
            state_columns,
            initial_cash,
            transaction_cost,
            seed=seed,
            reward_mode=reward_mode,
            reward_variant=active_reward_variant,
            risk_lambda=active_risk_lambda,
            hold_penalty_enabled=active_hold_penalty_enabled,
            hold_penalty=active_hold_penalty,
            hold_penalty_after_days=active_hold_penalty_after_days,
            flat_penalty_enabled=active_flat_penalty_enabled,
            flat_penalty=active_flat_penalty,
            flat_penalty_after_days=active_flat_penalty_after_days,
            flat_exposure_threshold=active_flat_exposure_threshold,
            trade_exploration_bonus_enabled=active_trade_bonus_enabled,
            trade_exploration_bonus=active_trade_bonus,
            trade_exploration_bonus_episode_ratio=active_trade_bonus_ratio,
            action_mode=active_action_mode,
            training_mode=True,
            current_episode=episode,
            total_episodes=episodes,
        )
        state = normalize_state(env.reset(), state_columns, scaler)
        done = False
        total_reward = 0.0
        last_loss = 0.0
        random_action_count = 0
        greedy_action_count = 0
        action_counts = {"Buy": 0, "Sell": 0, "Hold": 0}
        exposure_values: list[float] = []
        while not done:
            action = agent.act(state, explore=True)
            if agent.last_action_was_random:
                random_action_count += 1
            else:
                greedy_action_count += 1
            next_state_raw, reward, done, _info = env.step(action)
            action_label = str(_info.get("mapped_action_label", _info.get("action", "Hold")))
            action_counts[action_label] = action_counts.get(action_label, 0) + 1
            exposure_values.append(float(_info.get("current_exposure_after", 0.0) or 0.0))
            next_state = normalize_state(next_state_raw, state_columns, scaler)
            agent.remember(Experience(state, action, reward, next_state, done))
            last_loss = agent.train_step()
            state = next_state
            total_reward += reward
        average_exposure = float(np.mean(exposure_values)) if exposure_values else 0.0
        reward_rows.append(
            {
                "episode": episode,
                "total_reward": total_reward,
                "epsilon": agent.config.epsilon,
                "experiment": experiment,
                "seed": seed,
                "loss": last_loss,
                "reward_mode": reward_mode,
                "reward_variant": active_reward_variant,
                "model_variant": agent.config.model_variant,
                "loss_type": agent.config.loss_type,
                "grad_clip_norm": agent.config.grad_clip_norm,
                "state_feature_mode": agent.config.state_feature_mode,
                "action_mode": active_action_mode,
                "random_action_count": random_action_count,
                "greedy_action_count": greedy_action_count,
                "buy_count": action_counts.get("Buy", 0),
                "sell_count": action_counts.get("Sell", 0),
                "hold_count": action_counts.get("Hold", 0),
                "average_exposure": average_exposure,
                "state_normalized": True,
            }
        )
        exploration_rows.append(
            {
                "episode": episode,
                "epsilon": agent.config.epsilon,
                "random_action_count": random_action_count,
                "greedy_action_count": greedy_action_count,
                "buy_count": action_counts.get("Buy", 0),
                "sell_count": action_counts.get("Sell", 0),
                "hold_count": action_counts.get("Hold", 0),
                "average_exposure": average_exposure,
                "experiment": experiment,
                "seed": seed,
                "model_variant": agent.config.model_variant,
                "reward_variant": active_reward_variant,
                "state_feature_mode": agent.config.state_feature_mode,
                "action_mode": active_action_mode,
            }
        )
        if episode == episodes:
            final_log = env.trading_log(episode=episode, experiment=experiment, seed=seed)
        if progress_callback is not None:
            interval = max(1, int(episodes / 10))
            if episode == 1 or episode == episodes or episode % interval == 0:
                progress_callback(
                    {
                        "episode": episode,
                        "episodes": episodes,
                        "total_reward": float(total_reward),
                        "epsilon": float(agent.config.epsilon),
                        "loss": float(last_loss),
                        "experiment": experiment,
                        "seed": seed,
                        "reward_mode": reward_mode,
                        "reward_variant": active_reward_variant,
                        "model_variant": agent.config.model_variant,
                    }
                )

    rewards = pd.DataFrame(reward_rows)
    exploration = pd.DataFrame(exploration_rows)
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        rewards.to_csv(output_dir / f"{experiment}_training_rewards.csv", index=False)
        exploration.to_csv(output_dir / f"{experiment}_exploration_diagnostics.csv", index=False)
        final_log.to_csv(output_dir / f"{experiment}_trading_log.csv", index=False)
        model_output_dir = model_dir or output_dir
        model_output_dir.mkdir(parents=True, exist_ok=True)
        torch.save(agent.online.state_dict(), model_output_dir / f"{experiment}_dqn_model.pt")
    return {"agent": agent, "training_rewards": rewards, "exploration_diagnostics": exploration, "trading_log": final_log, "state_scaler": scaler}


def evaluate_agent(
    agent: DQNAgent,
    data: pd.DataFrame,
    state_columns: list[str],
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    experiment: str = "dqn_eval",
    seed: int = 42,
    reward_mode: str = "portfolio_return",
    state_scaler: dict[str, dict[str, float]] | None = None,
    reward_variant: str | None = None,
    risk_lambda: float | None = None,
    hold_penalty_enabled: bool | None = None,
    hold_penalty: float | None = None,
    hold_penalty_after_days: int | None = None,
    flat_penalty_enabled: bool | None = None,
    flat_penalty: float | None = None,
    flat_penalty_after_days: int | None = None,
    flat_exposure_threshold: float | None = None,
    trade_exploration_bonus_enabled: bool | None = None,
    trade_exploration_bonus: float | None = None,
    trade_exploration_bonus_episode_ratio: float | None = None,
    action_mode: str | None = None,
    log_q_values: bool = False,
) -> pd.DataFrame:
    seed_everything(seed)
    active_reward_variant = reward_variant or agent.config.reward_variant
    env = FinancialTradingEnv(
        data,
        state_columns,
        initial_cash,
        transaction_cost,
        seed=seed,
        reward_mode=reward_mode,
        reward_variant=active_reward_variant,
        risk_lambda=agent.config.risk_lambda if risk_lambda is None else float(risk_lambda),
        hold_penalty_enabled=agent.config.hold_penalty_enabled if hold_penalty_enabled is None else bool(hold_penalty_enabled),
        hold_penalty=agent.config.hold_penalty if hold_penalty is None else float(hold_penalty),
        hold_penalty_after_days=agent.config.hold_penalty_after_days if hold_penalty_after_days is None else int(hold_penalty_after_days),
        flat_penalty_enabled=agent.config.flat_penalty_enabled if flat_penalty_enabled is None else bool(flat_penalty_enabled),
        flat_penalty=agent.config.flat_penalty if flat_penalty is None else float(flat_penalty),
        flat_penalty_after_days=agent.config.flat_penalty_after_days if flat_penalty_after_days is None else int(flat_penalty_after_days),
        flat_exposure_threshold=agent.config.flat_exposure_threshold if flat_exposure_threshold is None else float(flat_exposure_threshold),
        trade_exploration_bonus_enabled=agent.config.trade_exploration_bonus_enabled if trade_exploration_bonus_enabled is None else bool(trade_exploration_bonus_enabled),
        trade_exploration_bonus=agent.config.trade_exploration_bonus if trade_exploration_bonus is None else float(trade_exploration_bonus),
        trade_exploration_bonus_episode_ratio=agent.config.trade_exploration_bonus_episode_ratio if trade_exploration_bonus_episode_ratio is None else float(trade_exploration_bonus_episode_ratio),
        action_mode=action_mode or agent.config.action_mode,
        training_mode=False,
    )
    scaler = state_scaler or build_state_scaler(data, state_columns, initial_cash)
    state = normalize_state(env.reset(), state_columns, scaler)
    done = False
    q_rows: list[dict[str, float | int]] = []
    step_index = 0
    while not done:
        if log_q_values:
            with torch.no_grad():
                q_values_array = agent.online(torch.tensor(state, dtype=torch.float32).unsqueeze(0)).squeeze(0).detach().cpu().numpy()
            action = int(np.argmax(q_values_array))
            selected_q = float(q_values_array[action])
            if len(q_values_array) > 1:
                q_margin = float(selected_q - np.partition(q_values_array, -2)[-2])
            else:
                q_margin = 0.0
        else:
            q_values_array = np.array([], dtype=float)
            selected_q = np.nan
            q_margin = np.nan
            action = agent.act(state, explore=False)
        next_state_raw, _reward, done, _info = env.step(action)
        if log_q_values:
            q_row: dict[str, float | int] = {
                "step": int(step_index),
                "raw_action_id": int(action),
                "selected_q_value": selected_q,
                "q_value_margin": q_margin,
            }
            for column, value in zip(state_columns, state):
                q_row[f"state_{column}"] = float(value)
            for index in range(5):
                column = "q_hold_or_action0" if index == 0 else f"q_action{index}"
                q_row[column] = float(q_values_array[index]) if index < len(q_values_array) else np.nan
            q_rows.append(q_row)
        state = normalize_state(next_state_raw, state_columns, scaler)
        step_index += 1
    log = env.trading_log(episode=0, experiment=experiment, seed=seed)
    if log_q_values and q_rows and not log.empty:
        q_frame = pd.DataFrame(q_rows)
        log = log.merge(q_frame, on=["step", "raw_action_id"], how="left")
    return log
