"""Money-flow style features derived from market data.

The integrated scraper does not always return a structured institutional
net-inflow field.  When a real net-flow column exists we use it directly;
otherwise we estimate signed money flow from OHLCV and traded value.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


TRUE_NET_FLOW_COLUMNS = [
    "net_inflow",
    "net_inflow_amount",
    "main_net_inflow",
    "main_net_inflow_amount",
    "fund_net_inflow",
    "capital_net_inflow",
    "主力净流入",
    "资金净流入",
]

AMOUNT_COLUMNS = [
    "amount",
    "turnover_amount",
    "成交额",
    "traded_value",
]


def compute_daily_net_flow(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a daily net inflow/outflow table.

    Output columns:
    - ``date``
    - ``net_flow``
    - ``net_flow_cny_million``
    - ``money_flow_method``
    - ``traded_value``
    - ``net_flow_direction``

    Method priority:
    1. Use a real net-flow column if one exists.
    2. Use Chaikin-style close location value times traded value.
    3. Fall back to signed close-to-open value when high/low are unavailable.
    """

    if frame.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "net_flow",
                "net_flow_cny_million",
                "money_flow_method",
                "traded_value",
                "net_flow_direction",
            ]
        )

    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    true_col = _first_existing_column(data, TRUE_NET_FLOW_COLUMNS)
    if true_col:
        net_flow = pd.to_numeric(data[true_col], errors="coerce").fillna(0.0)
        traded_value = _traded_value(data)
        method = f"reported:{true_col}"
    else:
        traded_value, used_amount = _traded_value(data)
        net_flow = _estimate_signed_flow(data, traded_value)
        amount_col = _first_existing_column(data, AMOUNT_COLUMNS)
        if used_amount:
            method = "ohlcv_amount_proxy"
        elif amount_col:
            method = "ohlcv_price_volume_proxy_fallback_from_empty_amount"
        else:
            method = "ohlcv_price_volume_proxy"

    out = pd.DataFrame(
        {
            "date": data["date"],
            "net_flow": net_flow,
            "net_flow_cny_million": net_flow / 1_000_000.0,
            "money_flow_method": method,
            "traded_value": traded_value,
        }
    )
    out["net_flow_direction"] = np.select(
        [out["net_flow"] > 0, out["net_flow"] < 0],
        ["inflow", "outflow"],
        default="flat",
    )
    return out


def _estimate_signed_flow(data: pd.DataFrame, traded_value: pd.Series) -> pd.Series:
    open_ = pd.to_numeric(data.get("open"), errors="coerce")
    high = pd.to_numeric(data.get("high"), errors="coerce")
    low = pd.to_numeric(data.get("low"), errors="coerce")
    close = pd.to_numeric(data.get("close"), errors="coerce")

    price_range = high - low
    close_location_value = ((close - low) - (high - close)) / price_range.replace(0, np.nan)
    fallback_direction = np.sign((close - open_).fillna(close.diff()))
    multiplier = close_location_value.replace([np.inf, -np.inf], np.nan).fillna(fallback_direction).fillna(0.0)
    return traded_value.fillna(0.0) * multiplier.clip(-1.0, 1.0)


def _traded_value(data: pd.DataFrame) -> tuple[pd.Series, bool]:
    amount_col = _first_existing_column(data, AMOUNT_COLUMNS)
    if amount_col:
        amount = pd.to_numeric(data[amount_col], errors="coerce")
        if amount.notna().any() and amount.fillna(0.0).abs().sum() > 0:
            return amount.fillna(0.0), True

    close = pd.to_numeric(data.get("close"), errors="coerce").fillna(0.0)
    volume = pd.to_numeric(data.get("volume"), errors="coerce").fillna(0.0)
    return close * volume * _infer_volume_factor(data), False


def _infer_volume_factor(data: pd.DataFrame) -> float:
    symbol_text = " ".join(data.get("symbol", pd.Series(dtype=str)).dropna().astype(str).head(5))
    exchange_text = " ".join(data.get("exchange", pd.Series(dtype=str)).dropna().astype(str).head(5))
    source_text = " ".join(data.get("data_source", pd.Series(dtype=str)).dropna().astype(str).head(5))
    combined = f"{symbol_text} {exchange_text} {source_text}".lower()
    if any(token in combined for token in [".sz", ".ss", "szse", "sse", "tencent", "eastmoney"]):
        return 100.0
    return 1.0


def _first_existing_column(data: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in data.columns:
            return column
    return None
