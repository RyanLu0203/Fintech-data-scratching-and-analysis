"""DQN training and evaluation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

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
    initial_state_dict: dict[str, torch.Tensor] | None = None,
    initial_epsilon: float | None = None,
    state_scaler: dict[str, dict[str, float]] | None = None,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Train a DQN agent and return reward curve plus final trading log."""

    if len(data) < 3:
        raise ValueError("At least 3 market rows are required for DQN training.")
    seed_everything(seed)
    agent = DQNAgent(dqn_config or DQNConfig(state_dim=len(state_columns), seed=seed))
    if initial_state_dict is not None:
        agent.online.load_state_dict(initial_state_dict)
        agent.target.load_state_dict(initial_state_dict)
    if initial_epsilon is not None:
        agent.config.epsilon = float(initial_epsilon)
    scaler = state_scaler or build_state_scaler(data, state_columns, initial_cash)
    reward_rows = []
    final_log = pd.DataFrame()

    for episode in range(1, episodes + 1):
        env = FinancialTradingEnv(data, state_columns, initial_cash, transaction_cost, seed=seed, reward_mode=reward_mode)
        state = normalize_state(env.reset(), state_columns, scaler)
        done = False
        total_reward = 0.0
        last_loss = 0.0
        while not done:
            action = agent.act(state, explore=True)
            next_state_raw, reward, done, _info = env.step(action)
            next_state = normalize_state(next_state_raw, state_columns, scaler)
            agent.remember(Experience(state, action, reward, next_state, done))
            last_loss = agent.train_step()
            state = next_state
            total_reward += reward
        reward_rows.append(
            {
                "episode": episode,
                "total_reward": total_reward,
                "epsilon": agent.config.epsilon,
                "experiment": experiment,
                "seed": seed,
                "loss": last_loss,
                "reward_mode": reward_mode,
                "state_normalized": True,
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
                    }
                )

    rewards = pd.DataFrame(reward_rows)
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        rewards.to_csv(output_dir / f"{experiment}_training_rewards.csv", index=False)
        final_log.to_csv(output_dir / f"{experiment}_trading_log.csv", index=False)
        model_output_dir = model_dir or output_dir
        model_output_dir.mkdir(parents=True, exist_ok=True)
        torch.save(agent.online.state_dict(), model_output_dir / f"{experiment}_dqn_model.pt")
    return {"agent": agent, "training_rewards": rewards, "trading_log": final_log, "state_scaler": scaler}


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
) -> pd.DataFrame:
    seed_everything(seed)
    env = FinancialTradingEnv(data, state_columns, initial_cash, transaction_cost, seed=seed, reward_mode=reward_mode)
    scaler = state_scaler or build_state_scaler(data, state_columns, initial_cash)
    state = normalize_state(env.reset(), state_columns, scaler)
    done = False
    while not done:
        action = agent.act(state, explore=False)
        next_state_raw, _reward, done, _info = env.step(action)
        state = normalize_state(next_state_raw, state_columns, scaler)
    return env.trading_log(episode=0, experiment=experiment, seed=seed)
