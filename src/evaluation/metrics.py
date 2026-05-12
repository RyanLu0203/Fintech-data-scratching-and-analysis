"""Portfolio evaluation metrics for strategy comparison."""

from __future__ import annotations

import numpy as np
import pandas as pd


def sharpe_ratio(values: pd.Series, periods_per_year: int = 252) -> float:
    returns = portfolio_returns(values)
    if returns.empty or returns.std() == 0:
        return float("nan")
    return float(np.sqrt(periods_per_year) * returns.mean() / returns.std())


def max_drawdown(values: pd.Series) -> float:
    drawdowns = drawdown_series(values, as_positive=False)
    if drawdowns.empty:
        return float("nan")
    return float(abs(drawdowns.min()))


def drawdown_series(values: pd.Series, as_positive: bool = False) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce").dropna()
    if values.empty:
        return pd.Series(dtype=float)
    drawdown = values / values.cummax() - 1
    return drawdown.abs() if as_positive else drawdown


def portfolio_returns(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").dropna().pct_change().dropna()


def annualized_return(values: pd.Series, periods_per_year: int = 252) -> float:
    values = pd.to_numeric(values, errors="coerce").dropna()
    if len(values) < 2 or values.iloc[0] == 0:
        return float("nan")
    total_return = values.iloc[-1] / values.iloc[0] - 1
    years = max((len(values) - 1) / periods_per_year, 1 / periods_per_year)
    return float((1 + total_return) ** (1 / years) - 1)


def annualized_volatility(values: pd.Series, periods_per_year: int = 252) -> float:
    returns = portfolio_returns(values)
    if returns.empty:
        return float("nan")
    return float(returns.std() * np.sqrt(periods_per_year))


def downside_deviation(values: pd.Series, periods_per_year: int = 252) -> float:
    returns = portfolio_returns(values)
    downside = returns[returns < 0]
    if downside.empty:
        return float("nan")
    return float(downside.std() * np.sqrt(periods_per_year))


def sortino_ratio(values: pd.Series, periods_per_year: int = 252) -> float:
    returns = portfolio_returns(values)
    downside = downside_deviation(values, periods_per_year)
    if returns.empty or pd.isna(downside) or downside == 0:
        return float("nan")
    return float((returns.mean() * periods_per_year) / downside)


def calmar_ratio(values: pd.Series, periods_per_year: int = 252) -> float:
    mdd = max_drawdown(pd.to_numeric(values, errors="coerce").dropna())
    if pd.isna(mdd) or mdd == 0:
        return float("nan")
    return float(annualized_return(values, periods_per_year) / mdd)


def value_at_risk(values: pd.Series, confidence: float = 0.95) -> float:
    returns = portfolio_returns(values)
    if returns.empty:
        return float("nan")
    return float(np.quantile(returns, 1 - confidence))


def profit_factor_from_rewards(rewards: pd.Series) -> float:
    rewards = pd.to_numeric(rewards, errors="coerce").dropna()
    if rewards.empty:
        return float("nan")
    gains = rewards[rewards > 0].sum()
    losses = abs(rewards[rewards < 0].sum())
    if losses == 0:
        return float("nan")
    return float(gains / losses)


def buy_and_hold_equity(close: pd.Series, initial_cash: float = 1000000.0) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce").dropna()
    if close.empty or close.iloc[0] == 0:
        return pd.Series(dtype=float)
    shares = initial_cash / close.iloc[0]
    return close * shares
