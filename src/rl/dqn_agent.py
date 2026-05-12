"""PyTorch DQN implementation from scratch."""

from __future__ import annotations

import random
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn, optim

from src.rl.replay_buffer import Experience, ReplayBuffer


def seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


class QNetwork(nn.Module):
    def __init__(self, state_dim: int, action_dim: int = 3, hidden_layer_sizes: tuple[int, ...] = (64, 64)) -> None:
        super().__init__()
        layers: list[nn.Module] = []
        previous = state_dim
        for width in hidden_layer_sizes:
            layers.extend([nn.Linear(previous, int(width)), nn.ReLU()])
            previous = int(width)
        layers.append(nn.Linear(previous, action_dim))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


@dataclass
class DQNConfig:
    state_dim: int
    action_dim: int = 3
    seed: int = 42
    gamma: float = 0.99
    lr: float = 1e-3
    epsilon: float = 1.0
    epsilon_min: float = 0.05
    epsilon_decay: float = 0.995
    batch_size: int = 64
    replay_capacity: int = 10000
    target_update_steps: int = 200
    hidden_layer_sizes: tuple[int, ...] = (64, 64)


class DQNAgent:
    def __init__(self, config: DQNConfig) -> None:
        self.config = config
        seed_everything(config.seed)
        self.online = QNetwork(config.state_dim, config.action_dim, config.hidden_layer_sizes)
        self.target = QNetwork(config.state_dim, config.action_dim, config.hidden_layer_sizes)
        self.target.load_state_dict(self.online.state_dict())
        self.optimizer = optim.Adam(self.online.parameters(), lr=config.lr)
        self.replay = ReplayBuffer(config.replay_capacity)
        self.steps = 0

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        if explore and random.random() < self.config.epsilon:
            return random.randrange(self.config.action_dim)
        with torch.no_grad():
            q_values = self.online(torch.tensor(state, dtype=torch.float32).unsqueeze(0))
        return int(torch.argmax(q_values, dim=1).item())

    def remember(self, experience: Experience) -> None:
        self.replay.push(experience)

    def train_step(self) -> float:
        if len(self.replay) < max(2, min(self.config.batch_size, 8)):
            return 0.0
        batch = self.replay.sample(self.config.batch_size)
        states = torch.tensor(np.array([item.state for item in batch]), dtype=torch.float32)
        actions = torch.tensor([item.action for item in batch], dtype=torch.long)
        rewards = torch.tensor([item.reward for item in batch], dtype=torch.float32)
        next_states = torch.tensor(np.array([item.next_state for item in batch]), dtype=torch.float32)
        dones = torch.tensor([item.done for item in batch], dtype=torch.float32)

        q_values = self.online(states).gather(1, actions.unsqueeze(1)).squeeze(1)
        with torch.no_grad():
            target_q = rewards + self.config.gamma * self.target(next_states).max(dim=1).values * (1 - dones)
        loss = nn.functional.mse_loss(q_values, target_q)

        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
        self.steps += 1
        self.config.epsilon = max(self.config.epsilon_min, self.config.epsilon * self.config.epsilon_decay)
        if self.steps % self.config.target_update_steps == 0:
            self.target.load_state_dict(self.online.state_dict())
        return float(loss.item())
