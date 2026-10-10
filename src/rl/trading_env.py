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
ACTION_MODES = {"discrete_trade", "target_position"}
TARGET_POSITION_ACTIONS = (0.0, 0.25, 0.50, 0.75, 1.00)
MIN_TRADE_VALUE = 1e-8


class FinancialTradingEnv:
    """A compact trading environment for DQN training and evaluation.

    The state at row ``t`` is built from features shifted to information known
    by the end of ``t-1``. Actions are executed at day ``t`` using the row's
    opening price. The ledger marks holdings at the same day's close. The
    one-day reward is close-to-close NAV return, including fees and overnight
    gains on the previous position. Initial NAV is cash before the first trade.
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
        flat_penalty_enabled: bool = True,
        flat_penalty: float = 0.00005,
        flat_penalty_after_days: int = 10,
        flat_exposure_threshold: float = 0.05,
        trade_exploration_bonus_enabled: bool = True,
        trade_exploration_bonus: float = 0.00002,
        trade_exploration_bonus_episode_ratio: float = 0.30,
        action_mode: str = "discrete_trade",
        training_mode: bool = False,
        current_episode: int = 0,
        total_episodes: int = 0,
        cache_market_rows: bool = False,
    ) -> None:
        self.data = data.sort_values("date").reset_index(drop=True).copy()
        self.state_columns = state_columns or STATE_COLUMNS
        self.initial_cash = float(initial_cash)
        self.transaction_cost = float(transaction_cost)
        if self.data.empty or self.data["date"].duplicated().any():
            raise ValueError("Trading data must contain unique, non-empty sessions.")
        if not np.isfinite(self.initial_cash) or self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive and finite")
        if not np.isfinite(self.transaction_cost) or not 0 <= self.transaction_cost < 1:
            raise ValueError("transaction_cost must be in [0, 1)")
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
        self.flat_penalty_enabled = bool(flat_penalty_enabled)
        self.flat_penalty = float(flat_penalty)
        self.flat_penalty_after_days = int(flat_penalty_after_days)
        self.flat_exposure_threshold = float(flat_exposure_threshold)
        self.trade_exploration_bonus_enabled = bool(trade_exploration_bonus_enabled)
        self.trade_exploration_bonus = float(trade_exploration_bonus)
        self.trade_exploration_bonus_episode_ratio = float(trade_exploration_bonus_episode_ratio)
        if action_mode not in ACTION_MODES:
            raise ValueError(f"action_mode must be one of {sorted(ACTION_MODES)}")
        self.action_mode = action_mode
        self.training_mode = bool(training_mode)
        self.current_episode = int(current_episode)
        self.total_episodes = int(total_episodes)
        # Opt in only for frozen training inputs. The default keeps the public
        # mutable DataFrame behavior used by diagnostics and boundary tests.
        self._rows = self.data.to_dict("records") if cache_market_rows else None
        self._fixed_states = None
        if cache_market_rows:
            self._fixed_states = self.data.reindex(columns=self.state_columns).apply(
                pd.to_numeric, errors="coerce"
            ).fillna(0.0).to_numpy(dtype=float)
        self.reset()

    def _row(self, index: int):
        return self._rows[index] if self._rows is not None else self.data.loc[index]

    def reset(self) -> np.ndarray:
        self.index = 0
        self.cash = self.initial_cash
        self.shares = 0.0
        self.peak_value = self.initial_cash
        self.previous_close_value = self.initial_cash
        self.consecutive_hold_days = 0
        self.consecutive_flat_days = 0
        self.logs: list[dict[str, float | str | int]] = []
        return self._state()

    def step(self, action: int) -> tuple[np.ndarray, float, bool, dict]:
        if self.index >= len(self.data):
            raise IndexError("Environment is already done; call reset() before stepping again.")

        row = self._row(self.index)
        trade_price = self._execution_price(row)
        cash_before = float(self.cash)
        shares_before = float(self.shares)
        portfolio_before = float(cash_before + shares_before * trade_price)
        current_exposure_before = self._exposure_ratio(portfolio_before, shares_before, trade_price)
        raw_action_id = int(action)
        action_count = len(TARGET_POSITION_ACTIONS) if self.action_mode == "target_position" else 3
        if raw_action_id != action or not 0 <= raw_action_id < action_count:
            raise ValueError("Action is outside the configured action space")
        target_exposure = self._target_exposure(raw_action_id, current_exposure_before)
        current_stock_value = float(shares_before * trade_price)
        target_stock_value = float(target_exposure * portfolio_before)
        trade_value = float(target_stock_value - current_stock_value)
        mapped_action_label = self._mapped_action_label(target_exposure, current_exposure_before, trade_value)
        transaction_cost_amount = self._execute_action(raw_action_id, trade_price, portfolio_before, current_exposure_before, target_exposure, trade_value, mapped_action_label)
        executed_trade_value = abs(float(self.shares - shares_before)) * trade_price
        executed_action = "Buy" if self.shares > shares_before else "Sell" if self.shares < shares_before else "Hold"

        if executed_action == "Hold":
            self.consecutive_hold_days += 1
        else:
            self.consecutive_hold_days = 0

        portfolio_after_trade = self.cash + self.shares * trade_price
        valuation_price = self._valuation_price(row)
        portfolio_next = float(self.cash + self.shares * valuation_price)
        current_exposure_after = self._exposure_ratio(portfolio_after_trade, self.shares, trade_price)
        if current_exposure_after < self.flat_exposure_threshold:
            self.consecutive_flat_days += 1
        else:
            self.consecutive_flat_days = 0
        previous_close_value = self.previous_close_value
        base_reward = self._base_reward(previous_close_value)
        turnover = executed_trade_value / previous_close_value
        self.peak_value = max(self.peak_value, portfolio_next)
        drawdown = 0.0 if self.peak_value == 0 else max(0.0, (self.peak_value - portfolio_next) / self.peak_value)
        reward = self._reward(base_reward, turnover, drawdown)
        flat_penalty_applied = 0.0
        if self.training_mode and self.flat_penalty_enabled and current_exposure_after < self.flat_exposure_threshold and self.consecutive_flat_days > self.flat_penalty_after_days:
            flat_penalty_applied = self.flat_penalty
            reward -= flat_penalty_applied
        trade_exploration_bonus_applied = 0.0
        if self._trade_bonus_active() and executed_action in {"Buy", "Sell"}:
            trade_exploration_bonus_applied = self.trade_exploration_bonus
            reward += trade_exploration_bonus_applied

        self.logs.append(
            {
                "date": row["date"],
                "step": int(self.index),
                "raw_action_id": raw_action_id,
                "action_mode": self.action_mode,
                "target_exposure": float(target_exposure),
                "current_stock_value": float(current_stock_value),
                "target_stock_value": float(target_stock_value),
                "trade_value": float(trade_value),
                "current_exposure_before": float(current_exposure_before),
                "current_exposure_after": float(current_exposure_after),
                "mapped_action_label": mapped_action_label,
                "action": executed_action,
                "executed_action": executed_action,
                "executed_trade_value": executed_trade_value,
                "previous_close_value": previous_close_value,
                "valuation_price": valuation_price,
                "cash": float(self.cash),
                "shares": float(self.shares),
                "position": float(self.shares),
                "price": float(trade_price),
                "cash_before": float(cash_before),
                "shares_before": float(shares_before),
                "portfolio_before": float(portfolio_before),
                "cash_after": float(self.cash),
                "shares_after": float(self.shares),
                "portfolio_after": float(portfolio_after_trade),
                "portfolio_after_trade": float(portfolio_after_trade),
                "portfolio_next": float(portfolio_next),
                "position_ratio_after": float(current_exposure_after),
                "cash_ratio_after": float(0.0 if portfolio_after_trade == 0 else self.cash / portfolio_after_trade),
                "reward": float(reward),
                "portfolio_value": float(portfolio_next),
                "portfolio_value_t": float(portfolio_after_trade),
                "reward_timing_convention": "previous_close_to_current_close_v2",
                "transaction_cost": float(transaction_cost_amount),
                "turnover": float(turnover),
                "drawdown": float(drawdown),
                "reward_mode": self.reward_mode,
                "reward_variant": self.reward_variant,
                "consecutive_hold_days": int(self.consecutive_hold_days),
                "consecutive_flat_days": int(self.consecutive_flat_days),
                "hold_penalty_applied": 0.0,
                "flat_penalty_applied": float(flat_penalty_applied),
                "trade_exploration_bonus_applied": float(trade_exploration_bonus_applied),
                "exposure_ratio_step": float(current_exposure_after),
                "training_mode": bool(self.training_mode),
                "episode": int(self.current_episode),
                "seed": self.seed if self.seed is not None else np.nan,
            }
        )

        self.previous_close_value = portfolio_next
        self.index += 1
        done = self.index >= len(self.data)
        next_state = self._state()
        info = {
            "portfolio_before": float(portfolio_before),
            "portfolio_after_trade": float(portfolio_after_trade),
            "portfolio_next": float(portfolio_next),
            "portfolio_value_t": float(portfolio_after_trade),
            "portfolio_value_t1": float(portfolio_next),
            "action": executed_action,
            "executed_action": executed_action,
            "raw_action_id": raw_action_id,
            "action_mode": self.action_mode,
            "target_exposure": float(target_exposure),
            "trade_value": float(trade_value),
            "current_exposure_before": float(current_exposure_before),
            "current_exposure_after": float(current_exposure_after),
            "mapped_action_label": mapped_action_label,
            "reward_variant": self.reward_variant,
            "flat_penalty_applied": float(flat_penalty_applied),
            "trade_exploration_bonus_applied": float(trade_exploration_bonus_applied),
        }
        return next_state, float(reward), bool(done), info

    def _execute_action(
        self,
        action: int,
        trade_price: float,
        portfolio_before: float,
        current_exposure_before: float,
        target_exposure: float,
        trade_value: float,
        mapped_action_label: str,
    ) -> float:
        if self.action_mode == "target_position":
            return self._execute_target_position(trade_price, portfolio_before, target_exposure, trade_value, mapped_action_label)
        return self._execute_discrete_trade(action, trade_price)

    def _execute_discrete_trade(self, action: int, trade_price: float) -> float:
        transaction_cost_amount = 0.0
        if action == ACTION_BUY and self.cash > trade_price:
            shares_to_buy = np.floor(self.cash / (trade_price * (1 + self.transaction_cost)))
            if shares_to_buy > 0:
                gross_cost = shares_to_buy * trade_price
                transaction_cost_amount = gross_cost * self.transaction_cost
                self.cash -= gross_cost + transaction_cost_amount
                self.shares += shares_to_buy
        elif action == ACTION_SELL and self.shares > 0:
            gross_proceeds = self.shares * trade_price
            transaction_cost_amount = gross_proceeds * self.transaction_cost
            self.cash += gross_proceeds - transaction_cost_amount
            self.shares = 0.0
        self.cash = max(float(self.cash), 0.0)
        self.shares = max(float(self.shares), 0.0)
        return float(transaction_cost_amount)

    def _execute_target_position(
        self,
        trade_price: float,
        portfolio_before: float,
        target_exposure: float,
        trade_value: float,
        mapped_action_label: str,
    ) -> float:
        if portfolio_before <= 0 or trade_price <= 0:
            return 0.0
        if mapped_action_label == "Hold" or abs(trade_value) <= MIN_TRADE_VALUE:
            return 0.0
        transaction_cost_amount = 0.0
        current_stock_value = self.shares * trade_price
        if trade_value > 0 and self.cash > 0:
            buy_value = min(float(trade_value), self.cash / (1 + self.transaction_cost))
            if buy_value > MIN_TRADE_VALUE:
                transaction_cost_amount = buy_value * self.transaction_cost
                self.cash -= buy_value + transaction_cost_amount
                self.shares += buy_value / trade_price
        elif trade_value < 0 and self.shares > 0:
            sell_value = min(abs(float(trade_value)), current_stock_value)
            if sell_value > MIN_TRADE_VALUE:
                transaction_cost_amount = sell_value * self.transaction_cost
                self.cash += sell_value - transaction_cost_amount
                self.shares -= sell_value / trade_price
        self.cash = max(float(self.cash), 0.0)
        self.shares = max(float(self.shares), 0.0)
        return float(transaction_cost_amount)

    def _target_exposure(self, action: int, current_exposure: float) -> float:
        if self.action_mode == "target_position":
            if action < 0 or action >= len(TARGET_POSITION_ACTIONS):
                return float(current_exposure)
            return float(TARGET_POSITION_ACTIONS[action])
        if action == ACTION_BUY:
            return 1.0
        if action == ACTION_SELL:
            return 0.0
        return float(current_exposure)

    def _mapped_action_label(self, target_exposure: float, current_exposure_before: float, trade_value: float | None = None) -> str:
        if trade_value is not None and abs(float(trade_value)) <= MIN_TRADE_VALUE:
            return "Hold"
        if self.action_mode == "target_position":
            if target_exposure > current_exposure_before + 0.01:
                return "Buy"
            if target_exposure < current_exposure_before - 0.01:
                return "Sell"
            return "Hold"
        if target_exposure > current_exposure_before + 0.01:
            return "Buy"
        if target_exposure < current_exposure_before - 0.01:
            return "Sell"
        return "Hold"

    def _exposure_ratio(self, portfolio_value: float, shares: float, price: float) -> float:
        if portfolio_value <= 0:
            return 0.0
        return float(max(0.0, min(1.0, shares * price / portfolio_value)))

    def _trade_bonus_active(self) -> bool:
        if not (self.training_mode and self.trade_exploration_bonus_enabled):
            return False
        if self.total_episodes <= 0:
            return False
        return (self.current_episode / self.total_episodes) <= self.trade_exploration_bonus_episode_ratio

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
        # Multi-day variants remain counterfactual research targets; they are
        # not n-step Bellman returns. The formal runner uses one_day_return.
        target_index = min(self.index + max(int(horizon_days), 1) - 1, len(self.data) - 1)
        value_price = self._valuation_price(self._row(target_index))
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
        if self._fixed_states is not None:
            index = min(self.index, len(self.data) - 1)
            values = self._fixed_states[index].copy()
            row = self._row(index)
            price = self._valuation_price(self._row(min(self.index - 1, len(self.data) - 1))) if self.index > 0 else float(row.get("price", self._execution_price(row)))
            nav = max(float(self.cash + self.shares * price), 1e-9)
            dynamic = {"position": self.shares, "cash": self.cash,
                       "position_ratio": self.shares * price / nav, "cash_ratio": self.cash / nav}
            for column, value in dynamic.items():
                if column in self.state_columns:
                    values[self.state_columns.index(column)] = value
            return values
        row = self.data.loc[min(self.index, len(self.data) - 1)].copy()
        row["position"] = self.shares
        row["cash"] = self.cash
        if self.index > 0:
            valuation_price = self._valuation_price(self.data.loc[min(self.index - 1, len(self.data) - 1)])
        else:
            valuation_price = float(row.get("price", self._execution_price(row)))
        portfolio_value = max(float(self.cash + self.shares * valuation_price), 1e-9)
        row["position_ratio"] = float((self.shares * valuation_price) / portfolio_value)
        row["cash_ratio"] = float(self.cash / portfolio_value)
        return pd.to_numeric(row[self.state_columns], errors="coerce").fillna(0.0).astype(float).to_numpy()

    def _execution_price(self, row: pd.Series) -> float:
        for column in ["execution_price", "open", "close", "price_raw", "price"]:
            if column in row and pd.notna(row[column]):
                price = float(row[column])
                if not np.isfinite(price) or price <= 0:
                    raise ValueError("Execution price must be positive and finite")
                return price
        raise ValueError("No valid execution price was found in the market row.")

    def _valuation_price(self, row: pd.Series) -> float:
        for column in ["valuation_price", "close", "execution_price", "price_raw", "price"]:
            if column in row and pd.notna(row[column]):
                price = float(row[column])
                if not np.isfinite(price) or price <= 0:
                    raise ValueError("Valuation price must be positive and finite")
                return price
        raise ValueError("No valid valuation price was found in the next market row.")

    def trading_log(self, episode: int = 0, experiment: str = "", seed: int | None = None) -> pd.DataFrame:
        log = pd.DataFrame(self.logs)
        if log.empty:
            return log
        if "episode" in log.columns:
            log["episode"] = episode
        else:
            log.insert(0, "episode", episode)
        log["experiment"] = experiment
        log["experiment_name"] = experiment
        log["seed"] = seed if seed is not None else log["seed"]
        return log
