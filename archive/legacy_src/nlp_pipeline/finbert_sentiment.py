"""FinBERT sentiment inference wrapper."""

from __future__ import annotations

from typing import List

from transformers import pipeline


class FinBERTSentiment:
    def __init__(self, model_name: str = "ProsusAI/finbert") -> None:
        self.model_name = model_name
        self._pipeline = None

    @property
    def classifier(self):
        if self._pipeline is None:
            self._pipeline = pipeline("sentiment-analysis", model=self.model_name)
        return self._pipeline

    def score(self, texts: List[str]) -> List[float]:
        label_to_score = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
        outputs = self.classifier(texts, truncation=True)
        return [label_to_score.get(item["label"].lower(), 0.0) * float(item["score"]) for item in outputs]

