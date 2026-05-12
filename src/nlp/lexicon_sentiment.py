"""Lexicon-based financial sentiment baseline."""

from __future__ import annotations

from src.nlp.preprocess import tokenize

POSITIVE_TERMS = {
    "beat",
    "bullish",
    "growth",
    "improve",
    "profit",
    "raise",
    "strong",
    "surge",
    "upgrade",
    "上涨",
    "增长",
    "提升",
    "利好",
    "盈利",
    "净利",
    "预增",
    "回购",
    "分红",
    "流入",
}

NEGATIVE_TERMS = {
    "bearish",
    "decline",
    "downgrade",
    "fall",
    "loss",
    "miss",
    "risk",
    "weak",
    "下跌",
    "下降",
    "亏损",
    "利空",
    "风险",
    "减持",
    "流出",
    "违约",
}


def score_text(text: str) -> int:
    """Return positive=1, neutral=0, negative=-1."""

    raw = str(text or "").lower()
    tokens = set(tokenize(raw))
    positive = sum(1 for term in POSITIVE_TERMS if term in tokens or term in raw)
    negative = sum(1 for term in NEGATIVE_TERMS if term in tokens or term in raw)
    if positive > negative:
        return 1
    if negative > positive:
        return -1
    return 0


def score_texts(texts: list[str]) -> list[int]:
    return [score_text(text) for text in texts]

