"""DQN training and evaluation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd
import torch

from src.rl.dqn_agent import DQNAgent, DQNConfig, seed_everything
from src.rl.replay_buffer import Experience
from src.rl.trading_env import FinancialTradingEnv


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
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, pd.DataFrame | DQNAgent]:
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
    reward_rows = []
    final_log = pd.DataFrame()

    for episode in range(1, episodes + 1):
        env = FinancialTradingEnv(data, state_columns, initial_cash, transaction_cost, seed=seed, reward_mode=reward_mode)
        state = env.reset()
        done = False
        total_reward = 0.0
        last_loss = 0.0
        while not done:
            action = agent.act(state, explore=True)
            next_state, reward, done, _info = env.step(action)
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
    return {"agent": agent, "training_rewards": rewards, "trading_log": final_log}


def evaluate_agent(
    agent: DQNAgent,
    data: pd.DataFrame,
    state_columns: list[str],
    initial_cash: float = 1000000.0,
    transaction_cost: float = 0.001,
    experiment: str = "dqn_eval",
    seed: int = 42,
    reward_mode: str = "portfolio_return",
) -> pd.DataFrame:
    seed_everything(seed)
    env = FinancialTradingEnv(data, state_columns, initial_cash, transaction_cost, seed=seed, reward_mode=reward_mode)
    state = env.reset()
    done = False
    while not done:
        action = agent.act(state, explore=False)
        state, _reward, done, _info = env.step(action)
    return env.trading_log(episode=0, experiment=experiment, seed=seed)
