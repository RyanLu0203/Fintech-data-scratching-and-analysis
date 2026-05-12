"""Date parsing and walk-forward split helpers."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True)
class DateRange:
    start: dt.date
    end: dt.date


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def walk_forward_windows(
    full_range: DateRange,
    train_days: int = 252,
    test_days: int = 63,
    step_days: int = 63,
) -> Iterator[tuple[DateRange, DateRange]]:
    """Yield chronological train/test windows with no future leakage."""
    cursor = full_range.start
    while True:
        train_end = cursor + dt.timedelta(days=train_days - 1)
        test_start = train_end + dt.timedelta(days=1)
        test_end = test_start + dt.timedelta(days=test_days - 1)
        if test_end > full_range.end:
            break
        yield DateRange(cursor, train_end), DateRange(test_start, test_end)
        cursor += dt.timedelta(days=step_days)

