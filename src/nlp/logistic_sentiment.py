"""TF-IDF + Logistic Regression sentiment model."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from src.nlp.lexicon_sentiment import score_texts
from src.nlp.preprocess import preprocess_text


@dataclass
class LogisticSentimentResult:
    scores: list[int]
    evaluation: dict[str, float | str]


def build_model() -> Pipeline:
    return Pipeline(
        [
            ("tfidf", TfidfVectorizer(max_features=10000, ngram_range=(1, 2), preprocessor=preprocess_text)),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )


def score_with_logistic(news: pd.DataFrame, text_column: str = "text", label_column: str = "label") -> LogisticSentimentResult:
    """Train/evaluate when labels exist, otherwise use lexicon pseudo-labels."""

    texts = news[text_column].fillna("").astype(str).tolist()
    if not texts:
        return LogisticSentimentResult([], {"method": "logistic_tfidf", "status": "no_text"})

    if label_column in news.columns and news[label_column].notna().any():
        labels = news[label_column].dropna().astype(int)
        labelled_texts = news.loc[labels.index, text_column].fillna("").astype(str)
        return _fit_predict(labelled_texts.tolist(), labels.tolist(), texts, labelled=True)

    pseudo_labels = score_texts(texts)
    if len(set(pseudo_labels)) < 2:
        return LogisticSentimentResult(
            pseudo_labels,
            {
                "method": "logistic_tfidf",
                "status": "fallback_lexicon_one_class",
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0,
            },
        )
    return _fit_predict(texts, pseudo_labels, texts, labelled=False)


def _fit_predict(train_texts: list[str], labels: list[int], predict_texts: list[str], labelled: bool) -> LogisticSentimentResult:
    model = build_model()
    evaluation: dict[str, float | str] = {"method": "logistic_tfidf"}
    if labelled and len(labels) >= 10 and len(set(labels)) > 1:
        x_train, x_test, y_train, y_test = train_test_split(
            train_texts,
            labels,
            test_size=0.25,
            random_state=42,
            stratify=labels if min(pd.Series(labels).value_counts()) > 1 else None,
        )
        model.fit(x_train, y_train)
        pred = model.predict(x_test)
        evaluation.update(_classification_metrics(y_test, pred))
        evaluation["status"] = "labelled_eval"
    else:
        model.fit(train_texts, labels)
        evaluation.update({"status": "pseudo_label_eval", "accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0})
    scores = [int(value) for value in model.predict(predict_texts)]
    return LogisticSentimentResult(scores, evaluation)


def _classification_metrics(y_true: list[int], y_pred: list[int]) -> dict[str, float]:
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, average="macro", zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, average="macro", zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
    }

