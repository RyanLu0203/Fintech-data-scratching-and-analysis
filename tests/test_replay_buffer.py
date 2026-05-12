"""Unit tests for the DQN replay buffer."""

from __future__ import annotations

import numpy as np
import torch

from src.rl.dqn_agent import DQNAgent, DQNConfig
from src.rl.replay_buffer import Experience, ReplayBuffer


def test_replay_buffer_sample() -> None:
    buffer = ReplayBuffer(capacity=10)
    state = np.zeros(8)
    buffer.push(Experience(state, 0, 1.0, state, False))
    assert len(buffer.sample(1)) == 1


def test_dqn_target_network_updates() -> None:
    agent = DQNAgent(DQNConfig(state_dim=8, batch_size=2, target_update_steps=1, replay_capacity=10))
    state = np.zeros(8)
    next_state = np.ones(8)
    for _ in range(3):
        agent.remember(Experience(state, 1, 0.5, next_state, False))
    loss = agent.train_step()
    assert isinstance(loss, float)
    for online_param, target_param in zip(agent.online.parameters(), agent.target.parameters()):
        assert torch.equal(online_param, target_param)
