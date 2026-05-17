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
    model_variant: str = os.getenv("MODEL_VARIANT", "vanilla_dqn")
    loss_type: str = os.getenv("LOSS_TYPE", "huber")
    grad_clip_norm: float = float(os.getenv("GRAD_CLIP_NORM", "1.0"))
    epsilon_decay_ratio: float = float(os.getenv("EPSILON_DECAY_RATIO", "0.70"))
    reward_variant: str = os.getenv("REWARD_VARIANT", "one_day_return")
    risk_lambda: float = float(os.getenv("RISK_LAMBDA", "0.1"))
    state_feature_mode: str = os.getenv("STATE_FEATURE_MODE", "official_8d")
    hold_penalty_enabled: bool = os.getenv("HOLD_PENALTY_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    hold_penalty: float = float(os.getenv("HOLD_PENALTY", "0.00005"))
    hold_penalty_after_days: int = int(os.getenv("HOLD_PENALTY_AFTER_DAYS", "10"))
    model_dir: Path = OUTPUTS_ROOT / "models"
    report_dir: Path = OUTPUTS_ROOT / "reports"


settings = Settings()
