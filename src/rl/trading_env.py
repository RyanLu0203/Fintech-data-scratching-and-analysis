"""Chronological financial trading environment with leakage-safe rewards."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.technical_indicators import STATE_COLUMNS

ACTION_HOLD = 0
ACTION_BUY = 1
ACTION_SELL = 2
ACTION_NAMES = {ACTION_HOLD: "Hold", ACTION_BUY: "Buy", ACTION_SELL: "Sell"}


class FinancialTradingEnv:
    """A compact trading environment for DQN training and evaluation.

    The state at row ``t`` is built from features shifted to information known
    by the end of ``t-1``. Actions are executed at day ``t`` using the row's
    execution price, and the reward is the portfolio return from ``t`` to
    ``t+1`` after transaction costs.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        state_columns: list[str] | None = None,
        initial_cash: float = 1000000.0,
        transaction_cost: float = 0.001,
        seed: int | None = None,
        reward_mode: str = "portfolio_return",
        turnover_penalty: float = 0.001,
        drawdown_penalty: float = 0.1,
    ) -> None:
        self.data = data.sort_values("date").reset_index(drop=True).copy()
        self.state_columns = state_columns or STATE_COLUMNS
        self.initial_cash = float(initial_cash)
        self.transaction_cost = float(transaction_cost)
        self.seed = seed
        self.reward_mode = reward_mode
        self.turnover_penalty = float(turnover_penalty)
        self.drawdown_penalty = float(drawdown_penalty)
        self.reset()

    def reset(self) -> np.ndarray:
        self.index = 0
        self.cash = self.initial_cash
        self.shares = 0.0
        self.peak_value = self.initial_cash
        self.logs: list[dict[str, float | str | int]] = []
        return self._state()

    def step(self, action: int) -> tuple[np.ndarray, float, bool, dict]:
        if self.index >= len(self.data) - 1:
            raise IndexError("Environment is already done; call reset() before stepping again.")

        row = self.data.loc[self.index]
        next_row = self.data.loc[self.index + 1]
        trade_price = self._execution_price(row)
        next_value_price = self._valuation_price(next_row)

        executed_action = ACTION_HOLD
        transaction_cost_amount = 0.0

        if action == ACTION_BUY and self.cash > trade_price:
            shares_to_buy = np.floor(self.cash / (trade_price * (1 + self.transaction_cost)))
            if shares_to_buy > 0:
                gross_cost = shares_to_buy * trade_price
                transaction_cost_amount = gross_cost * self.transaction_cost
                self.cash -= gross_cost + transaction_cost_amount
                self.shares += shares_to_buy
                executed_action = ACTION_BUY
        elif action == ACTION_SELL and self.shares > 0:
            gross_proceeds = self.shares * trade_price
            transaction_cost_amount = gross_proceeds * self.transaction_cost
            self.cash += gross_proceeds - transaction_cost_amount
            self.shares = 0.0
            executed_action = ACTION_SELL

        portfolio_value_t = self.cash + self.shares * trade_price
        portfolio_value_t1 = self.cash + self.shares * next_value_price
        base_reward = 0.0 if portfolio_value_t == 0 else (portfolio_value_t1 - portfolio_value_t) / portfolio_value_t
        turnover = 0.0 if portfolio_value_t == 0 else transaction_cost_amount / portfolio_value_t
        self.peak_value = max(self.peak_value, portfolio_value_t1)
        drawdown = 0.0 if self.peak_value == 0 else max(0.0, (self.peak_value - portfolio_value_t1) / self.peak_value)
        reward = self._reward(base_reward, turnover, drawdown)

        self.logs.append(
            {
                "date": row["date"],
                "action": ACTION_NAMES[executed_action],
                "cash": float(self.cash),
                "shares": float(self.shares),
                "position": float(self.shares),
                "price": float(trade_price),
                "reward": float(reward),
                "portfolio_value": float(portfolio_value_t1),
                "portfolio_value_t": float(portfolio_value_t),
                "transaction_cost": float(transaction_cost_amount),
                "turnover": float(turnover),
                "drawdown": float(drawdown),
                "reward_mode": self.reward_mode,
                "seed": self.seed if self.seed is not None else np.nan,
            }
        )

        self.index += 1
        done = self.index >= len(self.data) - 1
        next_state = self._state()
        info = {
            "portfolio_value_t": float(portfolio_value_t),
            "portfolio_value_t1": float(portfolio_value_t1),
            "action": ACTION_NAMES[executed_action],
        }
        return next_state, float(reward), bool(done), info

    def _reward(self, base_reward: float, turnover: float, drawdown: float) -> float:
        if self.reward_mode == "portfolio_return":
            return base_reward
        if self.reward_mode == "portfolio_return_minus_turnover_penalty":
            return base_reward - self.turnover_penalty * turnover
        if self.reward_mode == "portfolio_return_minus_drawdown_penalty":
            return base_reward - self.drawdown_penalty * drawdown
        raise ValueError(
            "reward_mode must be one of: portfolio_return, "
            "portfolio_return_minus_turnover_penalty, portfolio_return_minus_drawdown_penalty"
        )

    def _state(self) -> np.ndarray:
        row = self.data.loc[min(self.index, len(self.data) - 1)].copy()
        row["position"] = self.shares
        row["cash"] = self.cash
        return pd.to_numeric(row[self.state_columns], errors="coerce").fillna(0.0).astype(float).to_numpy()

    def _execution_price(self, row: pd.Series) -> float:
        for column in ["execution_price", "open", "close", "price_raw", "price"]:
            if column in row and pd.notna(row[column]):
                return float(row[column])
        raise ValueError("No valid execution price was found in the market row.")

    def _valuation_price(self, row: pd.Series) -> float:
        for column in ["valuation_price", "close", "execution_price", "price_raw", "price"]:
            if column in row and pd.notna(row[column]):
                return float(row[column])
        raise ValueError("No valid valuation price was found in the next market row.")

    def trading_log(self, episode: int = 0, experiment: str = "", seed: int | None = None) -> pd.DataFrame:
        log = pd.DataFrame(self.logs)
        if log.empty:
            return log
        log.insert(0, "episode", episode)
        log["experiment"] = experiment
        log["experiment_name"] = experiment
        log["seed"] = seed if seed is not None else log["seed"]
        return log
