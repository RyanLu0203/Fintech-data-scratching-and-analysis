#!/usr/bin/env python3
"""Generate cached-output model reliability and interpretability summaries."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.model_reliability import generate_model_reliability_outputs


def main() -> int:
    paths = generate_model_reliability_outputs()
    for name, path in sorted(paths.items()):
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
