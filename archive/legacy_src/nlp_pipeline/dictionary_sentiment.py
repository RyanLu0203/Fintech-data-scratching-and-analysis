"""Dictionary-based sentiment scorer for transparent baseline signals."""

from __future__ import annotations


POSITIVE_WORDS = {"beat", "growth", "profit", "upgrade", "bullish", "上涨", "增长", "利好", "盈利"}
NEGATIVE_WORDS = {"miss", "loss", "downgrade", "bearish", "下跌", "亏损", "风险", "利空"}


def dictionary_score(text: str) -> float:
    tokens = set((text or "").lower().split())
    positive = len(tokens & POSITIVE_WORDS)
    negative = len(tokens & NEGATIVE_WORDS)
    total = positive + negative
    return 0.0 if total == 0 else (positive - negative) / total

