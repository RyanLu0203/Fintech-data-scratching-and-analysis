"""Trading environment with Buy/Sell/Hold actions."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.rl_trading.dqn_agent import ACTION_BUY, ACTION_HOLD, ACTION_SELL
from src.rl_trading.features import STATE_COLUMNS


class TradingEnvironment:
    def __init__(self, data: pd.DataFrame, initial_cash: float = 100000.0) -> None:
        self.data = data.reset_index(drop=True)
        self.initial_cash = initial_cash
        self.reset()

    def reset(self) -> np.ndarray:
        self.index = 0
        self.cash = self.initial_cash
        self.position = 0.0
        self.prev_value = self.initial_cash
        return self._state()

    def step(self, action: int) -> tuple[np.ndarray, float, bool, dict]:
        price = float(self.data.loc[self.index, "price"])
        if action == ACTION_BUY and self.cash >= price:
            shares = self.cash // price
            self.position += shares
            self.cash -= shares * price
        elif action == ACTION_SELL and self.position > 0:
            self.cash += self.position * price
            self.position = 0.0
        elif action == ACTION_HOLD:
            pass

        value = self.cash + self.position * price
        reward = value - self.prev_value
        self.prev_value = value
        self.index += 1
        done = self.index >= len(self.data) - 1
        return self._state(), reward, done, {"portfolio_value": value}

    def _state(self) -> np.ndarray:
        row = self.data.loc[self.index].copy()
        row["position"] = self.position
        row["cash"] = self.cash
        return row[STATE_COLUMNS].fillna(0).astype(float).to_numpy()

