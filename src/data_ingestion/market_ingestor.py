"""Fetch OHLCV market data using yfinance or existing A-share scrapers."""

from __future__ import annotations

from typing import Any

import pandas as pd
import yfinance as yf


class MarketIngestor:
    def fetch_yfinance(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False, progress=False)
        data = data.reset_index()
        data["symbol"] = symbol
        return data

    def normalize_ohlcv(self, frame: pd.DataFrame) -> pd.DataFrame:
        rename_map: dict[str, str] = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adjclose",
            "Volume": "volume",
        }
        return frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns})

