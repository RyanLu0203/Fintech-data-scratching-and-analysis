#!/usr/bin/env python3
"""Generate cached trading-behavior visualizations without scraping/training."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.trading_visualizations import generate_trading_visualizations


if __name__ == "__main__":
    outputs = generate_trading_visualizations()
    for name, value in outputs.items():
        if hasattr(value, "shape"):
            print(f"{name}: dataframe {value.shape}")
        else:
            print(f"{name}: {value}")
