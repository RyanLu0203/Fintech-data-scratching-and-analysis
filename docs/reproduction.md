# Reproduction and demonstration

## Installation

Use Python 3.11. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

`requirements-validation.txt` records the installed package versions used for
this local verification (Python 3.9.6 on macOS). It is evidence of that environment,
not a claim that a clean Python 3.11 installation has been tested. CI installs
the recorded package pins on Python 3.11, with CPU PyTorch. No personal installation
path is needed by the project.

## Demo: frozen output reader

```bash
python -m streamlit run src/dashboard/streamlit_app.py --server.address 127.0.0.1
```

The default reader has Overview, News to Decision, Experiments, and Methods &
Project. Opening it reads local files; it does not fetch data or train models.
No formal result is selected until a completed Full run is promoted. Select a
completed Smoke version explicitly to inspect the fixture pipeline. A frozen
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

Prepare the four files described in `docs/data.md`, then run:

```bash
python scripts/run_coursework.py --mode full --target-symbol 002475 \
  --market work/002475_market.csv --target-news work/002475_news.csv \
  --peer-news work/sector_peer_news.csv --peer-market work/sector_peer_market.csv \
  --showcase
```

Full uses the fixed protocol in `docs/methodology.md`. It fails when files,
availability timestamps, source provenance, adequate coverage or matured
training classes are missing. It never falls back to a different stock or
the synthetic fixture. `--showcase` writes a pointer to that specific completed
result version. Its hashes protect the reader against mixed or changed files.
Two targets are run separately, retaining each manifest; the final cross-stock
report must include both rather than selecting the better target.

No Full run was executed in this refinement: the required raw input snapshots
are absent from the public repository. Full duration therefore remains unmeasured.

## Independent NLP benchmark

```bash
python -c "from pathlib import Path; from src.nlp.gold_label_evaluation import run_gold_label_evaluation; run_gold_label_evaluation(Path('work/gold_labels.csv'))"
```

Provide `text`, `gold_label` and preferably `split` (`train`/`test`). Labels are
negative/neutral/positive. Use genuine manual labels, a recorded annotation
rubric and near-duplicate review. FinBERT defaults to local cache only. Set
`FINBERT_MODEL_NAME` and a pinned `FINBERT_MODEL_REVISION` after checking language
and label mapping. `FINBERT_ALLOW_DOWNLOAD=1` permits an explicit model download.
Missing/unsuitable checkpoints stay unavailable; they do not become neutral.

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
