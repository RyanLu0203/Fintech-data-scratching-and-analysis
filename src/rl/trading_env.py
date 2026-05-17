"""Chronological financial trading environment with leakage-safe rewards."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.technical_indicators import STATE_COLUMNS

ACTION_HOLD = 0
ACTION_BUY = 1
ACTION_SELL = 2
ACTION_NAMES = {ACTION_HOLD: "Hold", ACTION_BUY: "Buy", ACTION_SELL: "Sell"}
REWARD_VARIANTS = {"one_day_return", "three_day_return", "five_day_return", "risk_adjusted_return"}


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
        reward_variant: str = "one_day_return",
        risk_lambda: float = 0.1,
        hold_penalty_enabled: bool = True,
        hold_penalty: float = 0.00005,
        hold_penalty_after_days: int = 10,
    ) -> None:
        self.data = data.sort_values("date").reset_index(drop=True).copy()
        self.state_columns = state_columns or STATE_COLUMNS
        self.initial_cash = float(initial_cash)
        self.transaction_cost = float(transaction_cost)
        self.seed = seed
        self.reward_mode = reward_mode
        self.turnover_penalty = float(turnover_penalty)
        self.drawdown_penalty = float(drawdown_penalty)
        if reward_variant not in REWARD_VARIANTS:
            raise ValueError(f"reward_variant must be one of {sorted(REWARD_VARIANTS)}")
        self.reward_variant = reward_variant
        self.risk_lambda = float(risk_lambda)
        self.hold_penalty_enabled = bool(hold_penalty_enabled)
        self.hold_penalty = float(hold_penalty)
        self.hold_penalty_after_days = int(hold_penalty_after_days)
        self.reset()

    def reset(self) -> np.ndarray:
        self.index = 0
        self.cash = self.initial_cash
        self.shares = 0.0
        self.peak_value = self.initial_cash
        self.consecutive_hold_days = 0
        self.logs: list[dict[str, float | str | int]] = []
        return self._state()

    def step(self, action: int) -> tuple[np.ndarray, float, bool, dict]:
        if self.index >= len(self.data) - 1:
            raise IndexError("Environment is already done; call reset() before stepping again.")

        row = self.data.loc[self.index]
        trade_price = self._execution_price(row)

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

        if executed_action == ACTION_HOLD:
            self.consecutive_hold_days += 1
        else:
            self.consecutive_hold_days = 0

        portfolio_value_t = self.cash + self.shares * trade_price
        portfolio_value_t1 = self._future_portfolio_value(1)
        base_reward = self._base_reward(portfolio_value_t)
        turnover = 0.0 if portfolio_value_t == 0 else transaction_cost_amount / portfolio_value_t
        self.peak_value = max(self.peak_value, portfolio_value_t1)
        drawdown = 0.0 if self.peak_value == 0 else max(0.0, (self.peak_value - portfolio_value_t1) / self.peak_value)
        reward = self._reward(base_reward, turnover, drawdown)
        hold_penalty_applied = 0.0
        if self.hold_penalty_enabled and executed_action == ACTION_HOLD and self.consecutive_hold_days > self.hold_penalty_after_days:
            hold_penalty_applied = self.hold_penalty
            reward -= hold_penalty_applied

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
                "reward_variant": self.reward_variant,
                "consecutive_hold_days": int(self.consecutive_hold_days),
                "hold_penalty_applied": float(hold_penalty_applied),
                "exposure_ratio_step": float(1.0 if self.shares > 0 else 0.0),
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
            "reward_variant": self.reward_variant,
            "hold_penalty_applied": float(hold_penalty_applied),
        }
        return next_state, float(reward), bool(done), info

    def _base_reward(self, portfolio_value_t: float) -> float:
        if portfolio_value_t == 0:
            return 0.0
        if self.reward_variant == "one_day_return":
            return (self._future_portfolio_value(1) - portfolio_value_t) / portfolio_value_t
        if self.reward_variant == "three_day_return":
            return (self._future_portfolio_value(3) - portfolio_value_t) / portfolio_value_t
        if self.reward_variant == "five_day_return":
            return (self._future_portfolio_value(5) - portfolio_value_t) / portfolio_value_t
        if self.reward_variant == "risk_adjusted_return":
            base_return = (self._future_portfolio_value(1) - portfolio_value_t) / portfolio_value_t
            downside_penalty = self.risk_lambda * max(0.0, -base_return) ** 2
            return base_return - downside_penalty
        raise ValueError(f"Unsupported reward_variant: {self.reward_variant}")

    def _future_portfolio_value(self, horizon_days: int) -> float:
        target_index = min(self.index + max(int(horizon_days), 1), len(self.data) - 1)
        value_price = self._valuation_price(self.data.loc[target_index])
        return float(self.cash + self.shares * value_price)

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
        valuation_price = self._valuation_price(row)
        portfolio_value = max(float(self.cash + self.shares * valuation_price), 1e-9)
        row["position_ratio"] = float((self.shares * valuation_price) / portfolio_value)
        row["cash_ratio"] = float(self.cash / portfolio_value)
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
