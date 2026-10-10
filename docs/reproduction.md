# Reproduction and demonstration

## Installation

Use Python 3.11. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-validation.txt
```

`requirements-validation.txt` records the installed package versions used for
this local verification (Python 3.9.6 on macOS). GitHub Actions also passed in a fresh
Linux/Python 3.11 environment using these package pins and CPU PyTorch.
The broader `requirements.txt` declares the stack; use the pins above to
reproduce the verified versions. No personal installation
path is needed by the project.

## Demo: frozen output reader

```bash
python -m streamlit run src/dashboard/streamlit_app.py --server.address 127.0.0.1
```

The default reader has Overview, News to Decision, Experiments, and Methods &
Project. Opening it reads local files; it does not fetch data or train models.
The default showcase opens the completed 002475 Full run; 601138 is also
selectable. Select a Smoke version explicitly to inspect the fixture pipeline. A frozen
Smoke viewer bundle is included in `outputs/demo/`; weights are omitted there,
and rerunning Smoke produces weights and scalers in its run directory. Advanced
research controls open the original workspace, where collection/training can
be triggered by the user. Those older paths remain diagnostic.

## Smoke: offline implementation check

```bash
python scripts/run_coursework.py --mode smoke
```

This creates a deterministic synthetic input fixture and actually fits six
DQN agents: three groups × two folds × one seed, two episodes each. It also
evaluates Buy-and-Hold per fold. Each run gets a new `outputs/runs/<run_id>/`
directory, with a manifest, input profile, training rewards, model/scaler files,
test logs, signals, news traces and paired differences. The UI labels Smoke
prominently. Smoke cannot be promoted to the formal showcase.

## Full: source-bound historical re-evaluation

The frozen four-symbol source snapshot is included. Each command uses a **fresh**
output root; existing protocol files are preserved and cause an explicit error.
These commands actually retrain 18 agents per target, each for 200 episodes.
The result reports include both stocks rather than selecting the better one.

```bash
python scripts/run_real_coursework.py \
  --snapshot data/frozen/electronics_2023_2025_20261010 \
  --target 002475 --output-root outputs/runs/reproduction_002475
python scripts/run_real_coursework.py \
  --snapshot data/frozen/electronics_2023_2025_20261010 \
  --target 601138 --output-root outputs/runs/reproduction_601138
```

The saved manifests identify the fixed configuration, upstream implementation,
actual code hash, source hashes, folds, seeds, matured labels and all checkpoint/
scaler hashes. Results may differ across hardware/library platforms despite
fixed seeds. No reduced-budget run is relabelled as Full. The immutable market
cache speeds access; parity tests compare it with the original environment.

To collect a **new** snapshot rather than overwrite the included evidence:

```bash
python scripts/collect_coursework_sources.py \
  --start 2023-01-01 --end 2025-03-31 \
  --destination work/new_source_snapshot
```

Provider histories and qfq adjustments can change on recollection. Preserve
both manifests; do not call a later download the original frozen input.
Date-only disclosure availability is a documented end-of-public-date assumption.
See [data provenance](data.md) and [real results](real_experiments.md).
The generic four-input runner `scripts/run_coursework.py --mode full` remains
available for independently supplied source-bound files.

## Actual Chinese FinBERT inference

The committed benchmark CSV preserves external human labels and the explicit
444/149 split. Checkpoint weights are about 409 MB and remain outside Git.
The downloader resolves the immutable revision and verifies weights; evaluation
checks all six registry hashes and the gold CSV hash before fitting/inference.
Use a new evaluation output directory to preserve the saved benchmark.

```bash
python scripts/download_finbert_chinese.py \
  --destination work/finbert-tone-chinese
python scripts/evaluate_finbert_chinese.py \
  --checkpoint-directory work/finbert-tone-chinese \
  --output outputs/runs/new_finbert_evaluation --device cpu
```

The evaluation fits LR only on 444 training texts, scores all three methods on
149 shared test texts, then separately diagnoses unchanged FinBERT on all 593.
CPU batch size is 8, maximum length 512 tokens and two PyTorch threads.
The completed local script took 98.81 seconds; download time is excluded.
Unavailable inference raises an error rather than returning neutral scores.
The default English checkpoint in the legacy API is unsuitable for Chinese;
this script explicitly sets the verified Chinese local model.

External annotation does not prove independence from the checkpoint's private
training corpus. Unknown overlap, 219 truncated texts, and document-versus-title
scope are explained in [FinBERT evaluation](finbert_evaluation.md).

## Saved evidence and figures

```bash
python scripts/validate_saved_evidence.py
python scripts/build_real_evidence_figures.py
```

Validation independently recomputes saved ledger and NLP scores, checks hashes
and population/configuration completeness. It requires no model download or
retraining. GitHub CI performs this validation plus tests and synthetic Smoke;
it does not pretend to repeat the full training or FinBERT inference.

## Checks and failures

```bash
python -m compileall -q src scripts tests
python -m pytest -q
python scripts/run_coursework.py --help
```

Run errors leave a `failed` manifest once an output directory exists; the
reader refuses incomplete runs. Preflight errors create no result claim.
An output checksum mismatch means that a result file changed after completion;
select a consistent version or rerun, rather than editing the manifest hash to
hide the mismatch. Zero-volatility Sharpe is N/A. No-news test periods are not
formal NLP evidence. The original grid and CLI remain available for historical
diagnostics, with their detailed options in the preserved legacy documentation.
