"""Adapter around the completed finance_text_scraper.py program."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRAPER = PROJECT_ROOT / "program" / "finance_text_scraper.py"


def run_existing_scraper(
    symbol: str,
    start_date: str,
    end_date: str,
    output_csv: Path,
    source: str = "auto",
    news_count: int = 500,
) -> Path:
    cmd = [
        sys.executable,
        str(SCRAPER),
        symbol,
        "-o",
        str(output_csv),
        "--source",
        source,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--news-count",
        str(news_count),
    ]
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))
    return output_csv

