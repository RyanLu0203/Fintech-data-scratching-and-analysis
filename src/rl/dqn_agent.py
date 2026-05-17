"""PyTorch DQN implementation from scratch."""

from __future__ import annotations

import random
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn, optim

from src.rl.replay_buffer import Experience, ReplayBuffer

MODEL_VARIANTS = {"vanilla_dqn", "double_dqn", "dueling_dqn", "double_dueling_dqn"}
LOSS_TYPES = {"mse", "huber"}


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


class DuelingQNetwork(nn.Module):
    def __init__(self, state_dim: int, action_dim: int = 3, hidden_layer_sizes: tuple[int, ...] = (64, 64)) -> None:
        super().__init__()
        layers: list[nn.Module] = []
        previous = state_dim
        for width in hidden_layer_sizes:
            layers.extend([nn.Linear(previous, int(width)), nn.ReLU()])
            previous = int(width)
        self.feature = nn.Sequential(*layers) if layers else nn.Identity()
        self.value_stream = nn.Sequential(nn.Linear(previous, previous), nn.ReLU(), nn.Linear(previous, 1))
        self.advantage_stream = nn.Sequential(nn.Linear(previous, previous), nn.ReLU(), nn.Linear(previous, action_dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        features = self.feature(x)
        value = self.value_stream(features)
        advantage = self.advantage_stream(features)
        return value + advantage - advantage.mean(dim=1, keepdim=True)


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
    model_variant: str = "vanilla_dqn"
    loss_type: str = "huber"
    grad_clip_norm: float | None = 1.0
    epsilon_start: float = 1.0
    epsilon_end: float = 0.05
    epsilon_decay_ratio: float = 0.70
    reward_variant: str = "one_day_return"
    risk_lambda: float = 0.1
    state_feature_mode: str = "official_8d"
    hold_penalty_enabled: bool = True
    hold_penalty: float = 0.00005
    hold_penalty_after_days: int = 10

    def __post_init__(self) -> None:
        if self.model_variant not in MODEL_VARIANTS:
            raise ValueError(f"model_variant must be one of {sorted(MODEL_VARIANTS)}")
        if self.loss_type not in LOSS_TYPES:
            raise ValueError(f"loss_type must be one of {sorted(LOSS_TYPES)}")
        self.epsilon = float(self.epsilon)
        self.epsilon_min = float(self.epsilon_min)
        self.epsilon_start = float(self.epsilon_start)
        self.epsilon_end = float(self.epsilon_end)
        self.epsilon_decay_ratio = float(self.epsilon_decay_ratio)


class DQNAgent:
    def __init__(self, config: DQNConfig) -> None:
        self.config = config
        seed_everything(config.seed)
        self.online = self._build_network()
        self.target = self._build_network()
        self.target.load_state_dict(self.online.state_dict())
        self.optimizer = optim.Adam(self.online.parameters(), lr=config.lr)
        self.replay = ReplayBuffer(config.replay_capacity)
        self.steps = 0

    def _build_network(self) -> nn.Module:
        if "dueling" in self.config.model_variant:
            return DuelingQNetwork(self.config.state_dim, self.config.action_dim, self.config.hidden_layer_sizes)
        return QNetwork(self.config.state_dim, self.config.action_dim, self.config.hidden_layer_sizes)

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
            if "double" in self.config.model_variant:
                next_actions = self.online(next_states).argmax(dim=1)
                next_values = self.target(next_states).gather(1, next_actions.unsqueeze(1)).squeeze(1)
            else:
                next_values = self.target(next_states).max(dim=1).values
            target_q = rewards + self.config.gamma * next_values * (1 - dones)
        if self.config.loss_type == "huber":
            loss = nn.functional.smooth_l1_loss(q_values, target_q)
        else:
            loss = nn.functional.mse_loss(q_values, target_q)

        self.optimizer.zero_grad()
        loss.backward()
        if self.config.grad_clip_norm is not None:
            nn.utils.clip_grad_norm_(self.online.parameters(), max_norm=float(self.config.grad_clip_norm))
        self.optimizer.step()
        self.steps += 1
        self.config.epsilon = max(self.config.epsilon_min, self.config.epsilon * self.config.epsilon_decay)
        if self.steps % self.config.target_update_steps == 0:
            self.target.load_state_dict(self.online.state_dict())
        return float(loss.item())
