from __future__ import annotations

import numpy as np
import pandas as pd
import torch

from src.rl.dqn_agent import DQNAgent, DQNConfig
from src.rl.replay_buffer import Experience
from src.rl.trading_env import FinancialTradingEnv


def test_dqn_variants_forward_and_train_step() -> None:
    for variant in ["vanilla_dqn", "double_dqn", "dueling_dqn", "double_dueling_dqn"]:
        config = DQNConfig(state_dim=4, model_variant=variant, batch_size=2, replay_capacity=10, loss_type="huber", grad_clip_norm=1.0)
        agent = DQNAgent(config)
        q_values = agent.online(torch.zeros((3, 4), dtype=torch.float32))
        assert tuple(q_values.shape) == (3, 3)
        for _ in range(3):
            agent.remember(Experience(np.zeros(4), 0, 0.1, np.ones(4), False))
            agent.remember(Experience(np.ones(4), 1, -0.1, np.zeros(4), True))
        loss = agent.train_step()
        assert np.isfinite(loss)


def test_target_position_dqn_uses_five_action_outputs() -> None:
    config = DQNConfig(state_dim=4, action_mode="target_position")
    agent = DQNAgent(config)
    q_values = agent.online(torch.zeros((2, 4), dtype=torch.float32))
    assert config.action_dim == 5
    assert tuple(q_values.shape) == (2, 5)


def test_dqn_config_rejects_unknown_variant() -> None:
    try:
        DQNConfig(state_dim=4, model_variant="not_a_dqn")
    except ValueError as exc:
        assert "model_variant" in str(exc)
    else:
        raise AssertionError("unknown model_variant should fail")


def test_reward_variant_and_flat_penalty_are_logged() -> None:
    data = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=6),
            "price": [10, 10, 10, 10, 10, 10],
            "close": [10, 10, 10, 10, 10, 10],
            "MA50": [10, 10, 10, 10, 10, 10],
            "MA200": [10, 10, 10, 10, 10, 10],
            "RSI": [50, 50, 50, 50, 50, 50],
            "MACD": [0, 0, 0, 0, 0, 0],
            "position": [0, 0, 0, 0, 0, 0],
            "cash": [1000, 1000, 1000, 1000, 1000, 1000],
            "nlp_signal_score": [0, 0, 0, 0, 0, 0],
        }
    )
    env = FinancialTradingEnv(
        data,
        ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "nlp_signal_score"],
        initial_cash=1000,
        reward_variant="three_day_return",
        training_mode=True,
        flat_penalty_enabled=True,
        flat_penalty=0.01,
        flat_penalty_after_days=1,
        flat_exposure_threshold=0.05,
    )
    env.reset()
    _state, first_reward, done, _info = env.step(0)
    assert not done
    _state, second_reward, _done, _info = env.step(0)
    log = env.trading_log()
    assert log["reward_variant"].iloc[0] == "three_day_return"
    assert log["flat_penalty_applied"].iloc[1] == 0.01
    assert second_reward < first_reward
