from __future__ import annotations

import numpy as np
import pandas as pd

from src.rl.trading_env import FinancialTradingEnv


STATE_COLUMNS = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "nlp_signal_score"]


def _market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=6),
            "price": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "close": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "MA50": [10.0, 10.5, 11.0, 11.5, 12.0, 12.5],
            "MA200": [10.0, 10.2, 10.4, 10.6, 10.8, 11.0],
            "RSI": [50.0, 52.0, 54.0, 56.0, 58.0, 60.0],
            "MACD": [0.0, 0.1, 0.2, 0.1, 0.0, -0.1],
            "position": [0.0] * 6,
            "cash": [1_000_000.0] * 6,
            "nlp_signal_score": [0.0] * 6,
        }
    )


def test_discrete_trade_updates_cash_shares_portfolio_and_state() -> None:
    initial_cash = 1_000_000
    env = FinancialTradingEnv(_market_frame(), STATE_COLUMNS, initial_cash=initial_cash)
    env.reset()

    next_state, _reward, _done, _info = env.step(1)
    buy_log = env.trading_log().iloc[-1]
    assert buy_log["shares_after"] > 0
    assert buy_log["cash_after"] < initial_cash
    assert np.isclose(buy_log["portfolio_after"], buy_log["cash_after"] + buy_log["shares_after"] * buy_log["price"])
    assert next_state[STATE_COLUMNS.index("position")] == buy_log["shares_after"]
    assert next_state[STATE_COLUMNS.index("cash")] == buy_log["cash_after"]

    shares_after_buy = float(buy_log["shares_after"])
    env.step(0)
    hold_log = env.trading_log().iloc[-1]
    assert hold_log["shares_after"] == shares_after_buy

    cash_before_sell = float(hold_log["cash_after"])
    next_state, _reward, _done, _info = env.step(2)
    sell_log = env.trading_log().iloc[-1]
    assert sell_log["shares_after"] <= shares_after_buy
    assert sell_log["cash_after"] > cash_before_sell
    assert next_state[STATE_COLUMNS.index("position")] == sell_log["shares_after"]
    assert next_state[STATE_COLUMNS.index("cash")] == sell_log["cash_after"]


def test_target_position_maps_buy_sell_hold_labels() -> None:
    env = FinancialTradingEnv(_market_frame(), STATE_COLUMNS, initial_cash=1_000_000, action_mode="target_position")
    env.reset()
    _state, _reward, _done, info = env.step(4)
    assert info["target_exposure"] == 1.0
    assert info["mapped_action_label"] == "Buy"

    _state, _reward, _done, info = env.step(4)
    assert info["mapped_action_label"] == "Hold"

    _state, _reward, _done, info = env.step(0)
    assert info["target_exposure"] == 0.0
    assert info["mapped_action_label"] == "Sell"


def test_target_position_uses_trade_value_formula_without_integer_rounding() -> None:
    env = FinancialTradingEnv(_market_frame(), STATE_COLUMNS, initial_cash=1_000_000, transaction_cost=0.001, action_mode="target_position")
    env.reset()
    _state, _reward, _done, _info = env.step(1)
    log = env.trading_log().iloc[-1]
    expected_buy_value = 0.25 * 1_000_000
    expected_cost = expected_buy_value * 0.001
    assert np.isclose(log["trade_value"], expected_buy_value)
    assert np.isclose(log["shares_after"], expected_buy_value / log["price"])
    assert np.isclose(log["cash_after"], 1_000_000 - expected_buy_value - expected_cost)
    assert np.isclose(log["portfolio_after_trade"], log["cash_after"] + log["shares_after"] * log["price"])
    assert "portfolio_next" in log.index


def test_flat_penalty_is_exposure_based_and_training_only() -> None:
    data = _market_frame()
    training_env = FinancialTradingEnv(
        data,
        STATE_COLUMNS,
        initial_cash=1_000_000,
        training_mode=True,
        flat_penalty_enabled=True,
        flat_penalty=0.01,
        flat_penalty_after_days=0,
        flat_exposure_threshold=0.05,
    )
    training_env.reset()
    _state, train_reward, _done, _info = training_env.step(0)
    train_log = training_env.trading_log().iloc[-1]
    assert train_log["flat_penalty_applied"] == 0.01

    eval_env = FinancialTradingEnv(
        data,
        STATE_COLUMNS,
        initial_cash=1_000_000,
        training_mode=False,
        flat_penalty_enabled=True,
        flat_penalty=0.01,
        flat_penalty_after_days=0,
        flat_exposure_threshold=0.05,
    )
    eval_env.reset()
    _state, eval_reward, _done, _info = eval_env.step(0)
    eval_log = eval_env.trading_log().iloc[-1]
    assert eval_log["flat_penalty_applied"] == 0.0
    assert eval_reward > train_reward


def test_trade_exploration_bonus_is_training_only() -> None:
    data = _market_frame()
    train_env = FinancialTradingEnv(
        data,
        STATE_COLUMNS,
        initial_cash=1_000_000,
        training_mode=True,
        current_episode=1,
        total_episodes=10,
        trade_exploration_bonus_enabled=True,
        trade_exploration_bonus=0.02,
        trade_exploration_bonus_episode_ratio=0.30,
    )
    train_env.reset()
    _state, train_reward, _done, _info = train_env.step(1)
    assert train_env.trading_log().iloc[-1]["trade_exploration_bonus_applied"] == 0.02

    eval_env = FinancialTradingEnv(
        data,
        STATE_COLUMNS,
        initial_cash=1_000_000,
        training_mode=False,
        trade_exploration_bonus_enabled=True,
        trade_exploration_bonus=0.02,
    )
    eval_env.reset()
    _state, eval_reward, _done, _info = eval_env.step(1)
    assert eval_env.trading_log().iloc[-1]["trade_exploration_bonus_applied"] == 0.0
    assert train_reward > eval_reward
