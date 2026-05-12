"""Training loop for the DQN trading agent."""

from __future__ import annotations

from typing import List

from src.rl_trading.dqn_agent import DQNAgent
from src.rl_trading.environment import TradingEnvironment
from src.rl_trading.replay_buffer import Experience


def train_agent(env: TradingEnvironment, agent: DQNAgent, episodes: int = 5) -> List[float]:
    episode_rewards: List[float] = []
    for _ in range(episodes):
        state = env.reset()
        done = False
        total_reward = 0.0
        while not done:
            action = agent.act(state)
            next_state, reward, done, _info = env.step(action)
            agent.remember(Experience(state, action, reward, next_state, done))
            agent.train_step()
            state = next_state
            total_reward += reward
        episode_rewards.append(total_reward)
    return episode_rewards

