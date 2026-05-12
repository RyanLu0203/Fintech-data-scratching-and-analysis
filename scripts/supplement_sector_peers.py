"""Fetch missing configured sector peers for the official peer NLP experiment."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.sector_peer_bootstrap import ensure_sector_peer_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Supplement local sector-peer data for peer NLP cross analysis.")
    parser.add_argument("--symbols", default="", help="Comma-separated target symbols. Empty means configured target candidates.")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2026-04-30")
    parser.add_argument("--sources", default="tencent")
    parser.add_argument("--news-count", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true", help="Check readiness without fetching.")
    parser.add_argument(
        "--minimum-only",
        action="store_true",
        help="Fetch only enough configured peers to satisfy the minimum peer count. Default fetches every configured missing peer.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    symbols = [item.strip() for item in args.symbols.replace("\n", ",").split(",") if item.strip()]
    outputs = ensure_sector_peer_data(
        target_symbols=symbols or None,
        start_date=args.start_date,
        end_date=args.end_date,
        sources=args.sources,
        news_count=args.news_count,
        allow_fetch=not args.dry_run,
        fetch_all_configured_peers=not args.minimum_only,
    )
    readiness = outputs["readiness"]
    fetch_log = outputs["fetch_log"]
    print(readiness.to_string(index=False) if not readiness.empty else "No sector readiness rows.")
    if not fetch_log.empty:
        print("\nFetch log:")
        print(fetch_log.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
