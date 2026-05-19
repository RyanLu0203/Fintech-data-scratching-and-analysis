"""Generate model-upgrade diagnostic markdown reports."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

from src.evaluation.model_upgrade_diagnostics import generate_model_upgrade_diagnostic_reports


def main() -> None:
    paths = generate_model_upgrade_diagnostic_reports()
    print("Model-upgrade diagnostic reports generated.")
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
