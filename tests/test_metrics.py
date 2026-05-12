"""Unit tests for evaluation metrics."""

from __future__ import annotations

import pandas as pd

from src.evaluation.metrics import max_drawdown, sharpe_ratio


def test_max_drawdown_positive_when_series_falls() -> None:
    values = pd.Series([100, 110, 90, 120])
    assert max_drawdown(values) > 0


def test_sharpe_ratio_returns_float() -> None:
    values = pd.Series([100, 101, 102, 101, 103])
    assert isinstance(sharpe_ratio(values), float)
