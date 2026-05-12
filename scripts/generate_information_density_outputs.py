#!/usr/bin/env python3
"""Generate coverage-controlled information-density outputs from cached data."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.information_density import generate_information_density_outputs


if __name__ == "__main__":
    outputs = generate_information_density_outputs()
    for name, path in outputs.items():
        if hasattr(path, "shape"):
            print(f"{name}: dataframe {path.shape}")
        else:
            print(f"{name}: {path}")
