"""FinBERT inference with explicit cache/download status and probabilities."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)
DEFAULT_MODEL = "ProsusAI/finbert"


@dataclass
class FinBERTResult:
    labels: list[str]
    positive_probs: list[float]
    neutral_probs: list[float]
    negative_probs: list[float]
    scores: list[float]
    status: str
    error: str = ""
    warning: str = ""
    model_name: str = DEFAULT_MODEL
    allow_download: bool = False
    local_files_only: bool = True


class FinBERTSentiment:
    def __init__(self, model_name: str | None = None, allow_download: bool | None = None) -> None:
        self.model_name = model_name or os.getenv("FINBERT_MODEL_NAME", DEFAULT_MODEL)
        self.allow_download = (
            os.getenv("FINBERT_ALLOW_DOWNLOAD", "0") == "1" if allow_download is None else bool(allow_download)
        )
        self._classifier = None

    def _load(self):
        if self._classifier is None:
            from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

            local_only = not self.allow_download
            tokenizer = AutoTokenizer.from_pretrained(self.model_name, local_files_only=local_only)
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name, local_files_only=local_only)
            self._classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, return_all_scores=True)
        return self._classifier

    @property
    def local_files_only(self) -> bool:
        return not self.allow_download

    @staticmethod
    def cache_roots() -> list[Path]:
        roots = []
        for env_name in ("FINBERT_MODEL_DIR", "TRANSFORMERS_CACHE", "HF_HOME", "HUGGINGFACE_HUB_CACHE"):
            value = os.getenv(env_name)
            if value:
                roots.append(Path(value).expanduser())
        roots.append(Path.home() / ".cache" / "huggingface" / "hub")
        return roots

    def local_cache_hint(self) -> str:
        model_key = str(self.model_name or DEFAULT_MODEL).replace("/", "--")
        matches = []
        for root in self.cache_roots():
            if root.exists():
                matches.extend(sorted(root.glob(f"*{model_key}*")))
        return "; ".join(str(path) for path in matches[:3])

    def score(self, texts: list[str]) -> FinBERTResult:
        if not texts:
            return FinBERTResult([], [], [], [], [], "no_text", model_name=str(self.model_name), allow_download=self.allow_download, local_files_only=self.local_files_only)
        try:
            classifier = self._load()
            outputs = classifier(texts, truncation=True)
            labels: list[str] = []
            positive: list[float] = []
            neutral: list[float] = []
            negative: list[float] = []
            scores: list[float] = []
            for item in outputs:
                probs = {entry["label"].lower(): float(entry["score"]) for entry in item}
                pos = probs.get("positive", 0.0)
                neu = probs.get("neutral", 0.0)
                neg = probs.get("negative", 0.0)
                label = max({"positive": pos, "neutral": neu, "negative": neg}, key={"positive": pos, "neutral": neu, "negative": neg}.get)
                labels.append(label)
                positive.append(pos)
                neutral.append(neu)
                negative.append(neg)
                scores.append(pos - neg)
            return FinBERTResult(
                labels,
                positive,
                neutral,
                negative,
                scores,
                "ok",
                model_name=str(self.model_name),
                allow_download=self.allow_download,
                local_files_only=self.local_files_only,
            )
        except Exception as exc:
            warning = (
                "FinBERT skipped. Set FINBERT_ALLOW_DOWNLOAD=1 to download/cache the model, "
                "or pre-populate the Hugging Face cache. "
                f"Local cache hint: {self.local_cache_hint() or 'no matching local cache found'}"
            )
            LOGGER.warning("%s Error: %s", warning, exc)
            zeros = [0.0 for _ in texts]
            return FinBERTResult(
                ["skipped" for _ in texts],
                zeros,
                zeros,
                zeros,
                zeros,
                "skipped",
                str(exc),
                warning,
                model_name=str(self.model_name),
                allow_download=self.allow_download,
                local_files_only=self.local_files_only,
            )
