"""Gold-label NLP evaluation and template generation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT
from src.nlp.finbert_sentiment import FinBERTSentiment
from src.nlp.lexicon_sentiment import score_texts
from src.nlp.logistic_sentiment import score_with_logistic

GOLD_LABEL_FILENAMES = [
    "gold_labels.csv",
    "labeled_news.csv",
    "manual_sentiment_labels.csv",
    "nlp_gold_labels.csv",
]
LABEL_TO_INT = {"negative": -1, "neutral": 0, "positive": 1, -1: -1, 0: 0, 1: 1}
INT_TO_LABEL = {-1: "negative", 0: "neutral", 1: "positive"}


def find_gold_label_file(search_root: Path | None = None) -> Path | None:
    root = search_root or PROJECT_ROOT
    for name in GOLD_LABEL_FILENAMES:
        matches = sorted(root.rglob(name))
        if matches:
            return matches[0]
    return None


def generate_gold_label_template(
    output_path: Path | None = None,
    sample_size: int = 400,
    stock_root: Path | None = None,
) -> Path:
    output_path = output_path or PROJECT_ROOT / "reports" / "tables" / "nlp_gold_label_template.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = sample_existing_news(sample_size=sample_size, stock_root=stock_root)
    if frame.empty:
        frame = pd.DataFrame(columns=["id", "symbol", "date", "title", "content", "text", "gold_label"])
    else:
        frame["gold_label"] = ""
        frame = frame[["id", "symbol", "date", "title", "content", "text", "gold_label"]]
    frame.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def sample_existing_news(sample_size: int = 400, stock_root: Path | None = None) -> pd.DataFrame:
    root = stock_root or STOCK_OUTPUT_ROOT
    rows = []
    for path in sorted(root.glob("*/reports/*_item_sentiment.csv")):
        try:
            data = pd.read_csv(path)
        except Exception:
            continue
        if data.empty:
            continue
        symbol = path.parts[-3] if len(path.parts) >= 3 else ""
        data = data.copy()
        data["symbol"] = data.get("ticker", symbol)
        data["id"] = data.get("news_id", pd.Series([f"{path.stem}_{idx}" for idx in data.index], index=data.index))
        if "text" not in data.columns:
            data["text"] = (data.get("title", "").fillna("") + " " + data.get("content", "").fillna("")).str.strip()
        for column in ["date", "title", "content"]:
            if column not in data.columns:
                data[column] = ""
        rows.append(data[["id", "symbol", "date", "title", "content", "text"]])
    if not rows:
        for path in sorted(root.glob("*/data/*_finance_text_*.csv")):
            if path.name.endswith("_master.csv"):
                continue
            try:
                data = pd.read_csv(path)
            except Exception:
                continue
            if data.empty:
                continue
            symbol = path.parts[-3] if len(path.parts) >= 3 else ""
            data = data.copy()
            data["symbol"] = data.get("symbol", symbol)
            data["id"] = [f"{path.stem}_{idx}" for idx in data.index]
            title_col = "event_title" if "event_title" in data.columns else "title"
            content_col = "event_summary" if "event_summary" in data.columns else "content"
            data["title"] = data.get(title_col, "")
            data["content"] = data.get(content_col, "")
            data["text"] = (data["title"].fillna("") + " " + data["content"].fillna("")).str.strip()
            rows.append(data[["id", "symbol", "date", "title", "content", "text"]])
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["text"])
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date"]).sort_values(["symbol", "date"])
    if len(combined) > sample_size:
        per_symbol = max(1, sample_size // max(combined["symbol"].nunique(), 1))
        sampled_groups = [
            group.sample(min(len(group), per_symbol), random_state=42)
            for _, group in combined.groupby("symbol", group_keys=False)
        ]
        combined = pd.concat(sampled_groups, ignore_index=True)
        if len(combined) < sample_size:
            remaining = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["text"])
            remaining = remaining.loc[~remaining["id"].isin(combined["id"])]
            combined = pd.concat([combined, remaining.sample(min(len(remaining), sample_size - len(combined)), random_state=42)])
        combined = combined.head(sample_size)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return combined.reset_index(drop=True)


def run_gold_label_evaluation(
    gold_label_path: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Path | pd.DataFrame | str]:
    output_dir = output_dir or PROJECT_ROOT / "reports" / "tables"
    output_dir.mkdir(parents=True, exist_ok=True)
    gold_label_path = gold_label_path or find_gold_label_file()
    template_path = generate_gold_label_template(output_dir / "nlp_gold_label_template.csv")

    if gold_label_path is None:
        evaluation = pd.DataFrame(
            [
                {
                    "method": method,
                    "evaluation_type": "skipped",
                    "status": "missing_gold_labels",
                    "accuracy": np.nan,
                    "precision_macro": np.nan,
                    "recall_macro": np.nan,
                    "macro_f1": np.nan,
                    "weighted_f1": np.nan,
                    "warning": f"No gold-label file found. Fill template: {template_path}",
                }
                for method in ["lexicon", "logistic_tfidf", "finbert"]
            ]
        )
    else:
        labels = _load_gold_labels(gold_label_path)
        evaluation = evaluate_models_on_gold(labels)

    evaluation_path = output_dir / "nlp_gold_label_evaluation.csv"
    final_path = output_dir / "nlp_model_comparison_final.csv"
    evaluation.to_csv(evaluation_path, index=False, encoding="utf-8-sig")
    evaluation.to_csv(final_path, index=False, encoding="utf-8-sig")
    return {
        "gold_label_file": str(gold_label_path) if gold_label_path else "",
        "template_csv": template_path,
        "evaluation": evaluation,
        "evaluation_csv": evaluation_path,
        "final_comparison_csv": final_path,
    }


def evaluate_models_on_gold(labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty:
        return pd.DataFrame()
    texts = labels["text"].fillna("").astype(str).tolist()
    y_true = labels["gold_label_int"].astype(int).tolist()
    rows = []
    rows.append(_metrics_row("lexicon", y_true, score_texts(texts), "gold_label_eval", "ok"))

    logistic_input = labels[["text", "gold_label_int"]].rename(columns={"gold_label_int": "label"})
    logistic_result = score_with_logistic(logistic_input, text_column="text", label_column="label")
    rows.append(_metrics_row("logistic_tfidf", y_true, logistic_result.scores, "gold_label_eval", logistic_result.evaluation.get("status", "ok")))

    finbert_result = FinBERTSentiment().score(texts)
    if finbert_result.status == "ok":
        finbert_pred = [_score_to_int(score) for score in finbert_result.scores]
        rows.append(_metrics_row("finbert", y_true, finbert_pred, "gold_label_eval", "ok"))
    else:
        rows.append(
            {
                "method": "finbert",
                "evaluation_type": "skipped",
                "status": finbert_result.status,
                "accuracy": np.nan,
                "precision_macro": np.nan,
                "recall_macro": np.nan,
                "macro_f1": np.nan,
                "weighted_f1": np.nan,
                "warning": finbert_result.warning,
                "error": finbert_result.error,
            }
        )
    return pd.DataFrame(rows)


def _load_gold_labels(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if "text" not in frame.columns:
        frame["text"] = (frame.get("title", "").fillna("") + " " + frame.get("content", "").fillna("")).str.strip()
    frame["gold_label"] = frame["gold_label"].astype(str).str.strip().str.lower()
    frame["gold_label_int"] = frame["gold_label"].map(LABEL_TO_INT)
    return frame.dropna(subset=["text", "gold_label_int"]).copy()


def _metrics_row(method: str, y_true: list[int], y_pred: list[int], evaluation_type: str, status: str) -> dict[str, object]:
    return {
        "method": method,
        "evaluation_type": evaluation_type,
        "status": status,
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision_macro": float(precision_score(y_true, y_pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y_true, y_pred, average="macro", zero_division=0)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "weighted_f1": float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
        "warning": "",
        "error": "",
    }


def _score_to_int(score: float) -> int:
    if score > 0.05:
        return 1
    if score < -0.05:
        return -1
    return 0
