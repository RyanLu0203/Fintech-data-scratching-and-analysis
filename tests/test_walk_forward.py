"""Unit tests for chronological walk-forward splitting."""

from __future__ import annotations

import datetime as dt

from src.utils.dates import DateRange, walk_forward_windows


def test_walk_forward_has_no_overlap() -> None:
    windows = list(walk_forward_windows(DateRange(dt.date(2024, 1, 1), dt.date(2024, 12, 31)), 60, 20, 20))
    assert windows
    for train, test in windows:
        assert train.end < test.start

