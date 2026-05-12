"""Centralized project settings loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUTS_ROOT = PROJECT_ROOT / "outputs"


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", f"sqlite:///{OUTPUTS_ROOT / 'database' / 'trading_platform.db'}")
    news_api_key: str = os.getenv("NEWS_API_KEY", "")
    default_symbol: str = os.getenv("DEFAULT_SYMBOL", "002475")
    default_start_date: str = os.getenv("DEFAULT_START_DATE", "2024-01-01")
    default_end_date: str = os.getenv("DEFAULT_END_DATE", "2026-04-30")
    default_initial_cash: float = float(os.getenv("INITIAL_CASH", "1000000"))
    market_impact_horizon_days: int = int(os.getenv("MARKET_IMPACT_HORIZON_DAYS", "3"))
    market_impact_pos_threshold: float = float(os.getenv("MARKET_IMPACT_POS_THRESHOLD", "0.015"))
    market_impact_neg_threshold: float = float(os.getenv("MARKET_IMPACT_NEG_THRESHOLD", "-0.015"))
    model_dir: Path = OUTPUTS_ROOT / "models"
    report_dir: Path = OUTPUTS_ROOT / "reports"


settings = Settings()
