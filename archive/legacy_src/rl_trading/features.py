"""Build state vectors for the DQN agent."""

from __future__ import annotations

import pandas as pd


STATE_COLUMNS = ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "sentiment_score"]


def add_technical_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["price"] = data["close"]
    data["MA50"] = data["close"].rolling(50, min_periods=1).mean()
    data["MA200"] = data["close"].rolling(200, min_periods=1).mean()
    delta = data["close"].diff().fillna(0)
    gain = delta.clip(lower=0).rolling(14, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=1).mean()
    rs = gain / loss.replace(0, 1e-9)
    data["RSI"] = 100 - (100 / (1 + rs))
    ema12 = data["close"].ewm(span=12, adjust=False).mean()
    ema26 = data["close"].ewm(span=26, adjust=False).mean()
    data["MACD"] = ema12 - ema26
    return data

