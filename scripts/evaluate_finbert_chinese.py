"""External human-label benchmark; never treats model predictions as gold."""

from pathlib import Path
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import torch  # noqa: E402
from sklearn.metrics import accuracy_score, f1_score  # noqa: E402
from src.nlp.finbert_sentiment import FinBERTSentiment  # noqa: E402
from src.nlp.gold_label_evaluation import evaluate_models_on_gold, split_gold_labels  # noqa: E402

MODEL_ID = "yiyanghkust/finbert-tone-chinese"
MODEL_REVISION = "e91b1a3af10e1e8c9c03429d3cd7d5e9a1c8000d"
DATASET_ID = "TongjiFinLab/CFBenchmark"
DATASET_REVISION = "3c825c2c9a63634736630e207c15cd27ab9633b6"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gold-csv",
        type=Path,
        default=ROOT / "data/benchmarks/cfbenchmark_sentiment.csv",
    )
    parser.add_argument("--checkpoint-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--device", choices=["cpu", "mps", "cuda"], default="cpu")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    checkpoint = args.checkpoint_directory.resolve()
    registry = json.loads((ROOT / "config/finbert_chinese_checkpoint.json").read_text())
    for filename, digest in registry["file_sha256"].items():
        if hashlib.sha256((checkpoint / filename).read_bytes()).hexdigest() != digest:
            raise ValueError(
                f"Checkpoint file differs from the pinned revision: {filename}"
            )
    source_registry = json.loads(
        (ROOT / "data/benchmarks/cfbenchmark_source.json").read_text()
    )
    if (
        hashlib.sha256(args.gold_csv.read_bytes()).hexdigest()
        != source_registry["csv_sha256"]
    ):
        raise ValueError(
            "Gold CSV differs from the frozen external benchmark and split"
        )
    config = json.loads((checkpoint / "config.json").read_text())
    if {int(k): v.lower() for k, v in config["id2label"].items()} != {
        0: "neutral",
        1: "positive",
        2: "negative",
    }:
        raise ValueError(
            "Chinese checkpoint label mapping differs from the verified model card"
        )
    gold = pd.read_csv(args.gold_csv)
    if (
        not gold["annotation_source"]
        .eq("CFBenchmark_three_professional_consensus")
        .all()
    ):
        raise ValueError(
            "Benchmark must preserve the external human annotation provenance"
        )
    os.environ.update(
        {
            "FINBERT_MODEL_NAME": str(checkpoint),
            "FINBERT_MODEL_REVISION": MODEL_REVISION,
            "FINBERT_ALLOW_DOWNLOAD": "0",
            "FINBERT_DEVICE": args.device,
        }
    )
    torch.set_num_threads(2)
    started = time.monotonic()
    evaluation = evaluate_models_on_gold(gold)
    if not evaluation["status"].eq("ok").all():
        raise RuntimeError(evaluation.to_json(orient="records"))
    training, test = split_gold_labels(gold)
    predictions = evaluation.attrs["predictions"]
    predictions.to_csv(args.output / "common_test_predictions.csv", index=False)
    evaluation.attrs["confusion"].to_csv(
        args.output / "common_test_confusion.csv", index=False
    )
    evaluation.to_csv(args.output / "common_test_metrics.csv", index=False)
    scorer = FinBERTSentiment(str(checkpoint), allow_download=False, device=args.device)
    tokenizer = scorer._load().tokenizer
    token_lengths = [
        len(tokens)
        for tokens in tokenizer(gold.text.tolist(), truncation=False)["input_ids"]
    ]
    all_result = scorer.score(gold.text.tolist())
    if all_result.status != "ok":
        raise RuntimeError(all_result.error)
    label_map = {"negative": -1, "neutral": 0, "positive": 1}
    full = gold[["id", "text", "gold_label_int", "split"]].copy()
    full["prediction"] = [label_map[x] for x in all_result.labels]
    full["positive_probability"] = all_result.positive_probs
    full["neutral_probability"] = all_result.neutral_probs
    full["negative_probability"] = all_result.negative_probs
    full["token_length"] = token_lengths
    full["truncated"] = full.token_length.gt(512)
    full.to_csv(args.output / "finbert_all_external_predictions.csv", index=False)
    if not np.allclose(
        full[
            ["positive_probability", "neutral_probability", "negative_probability"]
        ].sum(axis=1),
        1,
        atol=1e-5,
    ):
        raise ValueError("Probabilities do not sum to one")
    manifest = {
        "status": "complete",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": DATASET_ID,
        "dataset_revision": DATASET_REVISION,
        "model": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "annotation": "External labels: three professional researchers' consensus, CFBenchmark paper section 3.3",
        "gold_csv_sha256": hashlib.sha256(args.gold_csv.read_bytes()).hexdigest(),
        "model_file_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(checkpoint.iterdir())
            if p.is_file()
        },
        "common_split": {
            "train_rows": len(training),
            "test_rows": len(test),
            "policy": "stratified 75/25 seed 42; normalized duplicate exclusion; train/test frozen before inference",
        },
        "all_external_finbert": {
            "rows": len(full),
            "accuracy": accuracy_score(full.gold_label_int, full.prediction),
            "macro_f1": f1_score(
                full.gold_label_int, full.prediction, average="macro", zero_division=0
            ),
        },
        "runtime": {
            "torch": torch.__version__,
            "device": args.device,
            "batch_size": 8,
            "max_length": 512,
            "truncated_rows": int(full.truncated.sum()),
            "elapsed_seconds": time.monotonic() - started,
        },
        "limitations": [
            "checkpoint private training corpus is unavailable: overlap with this public benchmark is unknown",
            "external document benchmark does not establish accuracy on the project's disclosure titles",
            "all-external FinBERT score uses unchanged checkpoint; LR comparison uses only common held-out test",
            "no new human annotation of project titles and no FinBERT-based trading claim",
        ],
        "files": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(args.output.glob("*.csv"))
        },
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(evaluation.to_string(index=False), flush=True)
    print(json.dumps(manifest["all_external_finbert"]), flush=True)
