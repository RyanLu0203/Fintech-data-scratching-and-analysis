# Chinese FinBERT evaluation — 2026-10-10

Actual inference completed with a pinned Chinese financial sentiment checkpoint.
On the same 149 external human-labelled test texts, measured macro F1 is
**0.391 for FinBERT**, **0.605 for TF-IDF/LR** and **0.453 for the lexicon**.
The unchanged FinBERT checkpoint underperforms these baselines in this setting.
No checkpoint tuning or winning-test selection was performed.

## Model and human labels

The [Chinese FinBERT model card](https://huggingface.co/yiyanghkust/finbert-tone-chinese)
identifies a model trained on private Chinese financial analyst-report text.
We use immutable revision `e91b1a3af10e1e8c9c03429d3cd7d5e9a1c8000d`, verify
all six checkpoint file hashes, and confirm labels **0 neutral, 1 positive,
2 negative** from its config and card. Weights SHA256 is
`0c25b105902f90d1e039d511944af4f49d1490f6698e701f2704a714ca74e66b`.
The checkpoint is pretrained inference; the team did not fine-tune it.

Gold labels come from [TongjiFinLab/CFBenchmark](https://huggingface.co/datasets/TongjiFinLab/CFBenchmark)
at revision `3c825c2c9a63634736630e207c15cd27ab9633b6`. The
[CFBenchmark paper, §3.3](https://arxiv.org/abs/2311.05812) describes annotation
by three professional financial researchers and retention of unanimous labels.
This is **external human annotation**, not new team annotation of project titles.
The pinned sentiment Arrow contains 593 rows and 593 unique normalized texts,
despite its dataset card stating 591; all 593 observed rows are retained.
Dataset provenance, hashes and label conversion are in the
[source registry](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/data/benchmarks/cfbenchmark_source.json); its Apache-2.0
[license](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/data/benchmarks/CFBenchmark-LICENSE) accompanies the frozen CSV.

## Common test protocol

The [frozen CSV](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/data/benchmarks/cfbenchmark_sentiment.csv) has an explicit
stratified 75/25 split, seed 42: 444 LR training texts and 149 test texts.
No normalized text duplicate crosses the boundary. All three methods score
the **same 149 test texts**, containing 41 negative, 49 neutral and 59 positive
labels. TF-IDF vocabulary and LR fitting use only the 444 training texts.
Labels and split were frozen before inference; neither predictions nor weak
labels replace gold labels.

| Method | Test texts | Accuracy | Macro F1 |
|---|---:|---:|---:|
| Lexicon | 149 | 0.450 | 0.453 |
| TF-IDF / logistic regression | 149 | 0.604 | 0.605 |
| Chinese FinBERT | 149 | 0.436 | 0.391 |

[Common-test scores and FinBERT confusion](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/assets/finbert-benchmark.png)

As a separate diagnostic, unchanged FinBERT also scored all 593 external texts:
accuracy **0.4064**, macro F1 **0.3739**. This is not the common-test LR comparison,
because 444 of those texts train LR. Saved rows preserve the split so the
populations cannot be confused.

Inference used CPU, two PyTorch threads, batch size 8 and maximum length
512 tokens. **219/593 texts (36.9%) exceed that limit and were truncated**.
The entire evaluation script, including both inference passes, took 98.81
seconds locally. Probabilities, predicted classes, original gold labels,
confusion counts and truncation flags are saved. Domain differences and
truncation are plausible explanations for performance, but this run cannot
identify their causal contribution.

## What “private training overlap unknown” means

The FinBERT authors do not publish the checkpoint's training texts. We therefore
cannot check whether a CFBenchmark text was previously used to train it. If
overlap exists, a measured score might be optimistic about unseen-text accuracy.
**Unknown overlap is not evidence that overlap occurred.** The actual scores
remain reportable, but this is not a verified contamination-free test of that
checkpoint. Our own LR train/test separation is directly verified.

This external document benchmark also does not establish FinBERT accuracy on
the project's shorter corporate disclosure titles. No new title annotation,
FinBERT-derived trading signal or FinBERT profitability claim is made. The core
[trading experiments](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/real_experiments.md) use peer weak-label models separately.

## Evidence and reproduction

- [Evaluation manifest](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/nlp_evaluation/cfbenchmark_20261010/manifest.json)
  records immutable model/data revisions, hashes, population sizes and runtime.
- [Common-test metrics](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/nlp_evaluation/cfbenchmark_20261010/common_test_metrics.csv),
  [149 predictions](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/nlp_evaluation/cfbenchmark_20261010/common_test_predictions.csv)
  and [confusion counts](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/nlp_evaluation/cfbenchmark_20261010/common_test_confusion.csv).
- [All 593 FinBERT predictions and truncation flags](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/nlp_evaluation/cfbenchmark_20261010/finbert_all_external_predictions.csv).
- [Checkpoint registry](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/config/finbert_chinese_checkpoint.json),
  [download script](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/scripts/download_finbert_chinese.py) and
  [evaluation script](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/scripts/evaluate_finbert_chinese.py).

The independent saved-evidence validator recomputes scores from original gold
labels and saved predictions, checks class/probability agreement, confusion
counts and hashes. CI runs this validation without downloading weights or
claiming to rerun FinBERT inference. Downloading the approximately 409 MB weights
and rerunning inference is an explicit reproducible command in
[reproduction](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/reproduction.md).
