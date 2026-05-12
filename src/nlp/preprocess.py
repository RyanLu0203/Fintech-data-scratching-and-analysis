"""Text preprocessing utilities for financial news."""

from __future__ import annotations

import re

ENGLISH_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "for",
    "from",
    "has",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


def tokenize(text: str) -> list[str]:
    """Tokenize English words/numbers and keep Chinese character spans."""

    return re.findall(r"[\u4e00-\u9fff]+|[A-Za-z]+|\d+(?:\.\d+)?", str(text or "").lower())


def lemmatize_token(token: str) -> str:
    """Small dependency-free lemmatizer/stemmer for notebook portability."""

    if re.search(r"[\u4e00-\u9fff]", token):
        return token
    for suffix in ("ing", "edly", "edly", "ed", "ly", "s"):
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            return token[: -len(suffix)]
    return token


def preprocess_text(text: str) -> str:
    tokens = [lemmatize_token(token) for token in tokenize(text)]
    tokens = [token for token in tokens if token not in ENGLISH_STOPWORDS]
    return " ".join(tokens)

