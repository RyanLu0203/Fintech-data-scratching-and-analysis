from __future__ import annotations

import inspect

import numpy as np
import pandas as pd

from src.rl.train import build_state_scaler, evaluate_agent, normalize_state


def test_state_scaler_keeps_cash_and_position_from_dominating_dqn_inputs() -> None:
    columns = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "nlp_signal_score"]
    data = pd.DataFrame(
        {
            "price": [10.0, 11.0, 12.0, 13.0],
            "MA50": [10.0, 10.5, 11.0, 11.5],
            "MA200": [9.5, 9.8, 10.1, 10.4],
            "RSI": [45.0, 50.0, 55.0, 60.0],
            "MACD": [-0.2, -0.1, 0.1, 0.2],
            "position": [0.0, 1000.0, 2000.0, 3000.0],
            "cash": [1_000_000.0, 990_000.0, 980_000.0, 970_000.0],
            "nlp_signal_score": [0.0, 0.1, -0.1, 0.2],
        }
    )

    scaler = build_state_scaler(data, columns, initial_cash=1_000_000.0)
    normalized = normalize_state(data.iloc[0][columns].to_numpy(dtype=float), columns, scaler)

    assert normalized[columns.index("cash")] == 1.0
    assert normalized[columns.index("position")] == 0.0
    assert np.isfinite(normalized).all()
    assert abs(normalized[columns.index("cash")]) < 5


def test_evaluate_agent_accepts_training_state_scaler() -> None:
    assert "state_scaler" in inspect.signature(evaluate_agent).parameters
