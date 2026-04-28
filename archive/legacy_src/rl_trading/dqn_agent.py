"""From-scratch DQN agent with experience replay and target network."""

from __future__ import annotations

import random
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn, optim

from src.rl_trading.replay_buffer import Experience, ReplayBuffer


ACTION_BUY = 0
ACTION_SELL = 1
ACTION_HOLD = 2


class QNetwork(nn.Module):
    def __init__(self, state_dim: int, action_dim: int = 3) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, action_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


@dataclass
class DQNConfig:
    state_dim: int = 8
    action_dim: int = 3
    gamma: float = 0.99
    lr: float = 1e-3
    epsilon: float = 1.0
    epsilon_min: float = 0.05
    epsilon_decay: float = 0.995
    batch_size: int = 64
    target_update_steps: int = 200


class DQNAgent:
    def __init__(self, config: DQNConfig) -> None:
        self.config = config
        self.online = QNetwork(config.state_dim, config.action_dim)
        self.target = QNetwork(config.state_dim, config.action_dim)
        self.target.load_state_dict(self.online.state_dict())
        self.optimizer = optim.Adam(self.online.parameters(), lr=config.lr)
        self.replay = ReplayBuffer()
        self.steps = 0

    def act(self, state: np.ndarray) -> int:
        if random.random() < self.config.epsilon:
            return random.randrange(self.config.action_dim)
        with torch.no_grad():
            q_values = self.online(torch.tensor(state, dtype=torch.float32).unsqueeze(0))
        return int(torch.argmax(q_values, dim=1).item())

    def remember(self, experience: Experience) -> None:
        self.replay.push(experience)

    def train_step(self) -> float:
        if len(self.replay) < self.config.batch_size:
            return 0.0
        batch = self.replay.sample(self.config.batch_size)
        states = torch.tensor(np.array([e.state for e in batch]), dtype=torch.float32)
        actions = torch.tensor([e.action for e in batch], dtype=torch.long)
        rewards = torch.tensor([e.reward for e in batch], dtype=torch.float32)
        next_states = torch.tensor(np.array([e.next_state for e in batch]), dtype=torch.float32)
        dones = torch.tensor([e.done for e in batch], dtype=torch.float32)

        q_values = self.online(states).gather(1, actions.unsqueeze(1)).squeeze(1)
        with torch.no_grad():
            next_q = self.target(next_states).max(dim=1).values
            target_q = rewards + self.config.gamma * next_q * (1 - dones)
        loss = nn.functional.mse_loss(q_values, target_q)
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

        self.steps += 1
        self.config.epsilon = max(self.config.epsilon_min, self.config.epsilon * self.config.epsilon_decay)
        if self.steps % self.config.target_update_steps == 0:
            self.target.load_state_dict(self.online.state_dict())
        return float(loss.item())

