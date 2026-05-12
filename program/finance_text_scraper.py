#!/usr/bin/env python3
"""Backward-compatible entry point for the finance text scraper."""

from __future__ import annotations

from finance_scraper.scraper import main


if __name__ == "__main__":
    raise SystemExit(main())

