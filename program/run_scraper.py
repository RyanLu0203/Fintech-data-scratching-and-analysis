#!/usr/bin/env python3
"""Backward-compatible entry point for the diagnostic scraper runner."""

from __future__ import annotations

from finance_scraper.runner import main


if __name__ == "__main__":
    raise SystemExit(main())

