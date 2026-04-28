# NLP-Driven Reinforcement Learning Trading Platform

This repository implements a runnable fintech research platform for the project guideline:

```text
raw financial news + OHLCV market data
-> NLP sentiment signal
-> market money-flow pressure signal
-> RL trading decision
-> evaluation, report artifacts, and dashboard
```

The current data ingestion workflow is preserved, but generated artifacts now live under one per-stock output folder:

```text
outputs/stocks/<symbol>/
  data/       integrated scraper CSVs and the stock master timeline
  reports/    NLP tables, SVG figures, diagnostics, report drafts
  results/    ablation metrics, portfolio curves, trading logs
  models/     trained DQN checkpoints
```

This keeps the uploadable program code separate from local experiment outputs.
`outputs/`, `.venv/`, and cache folders are ignored by `.gitignore`, so the repository is ready to upload without committing generated data or the local Python environment.

## Installation

```bash
cd /Users/luxinyu/Desktop/Fintech/fintechgp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The notebook kernel has been registered as:

```text
Python (.venv fintechgp)
```

## Run The Full Pipeline

Example with the required CLI options:

```bash
.venv/bin/python main.py \
  --symbol 002475 \
  --company-name 立讯精密 \
  --start-date 2023-01-01 \
  --end-date 2024-12-31 \
  --run-ingestion \
  --run-nlp \
  --run-rl \
  --run-ablation \
  --episodes 200
```

For a quick local test using an existing CSV:

```bash
.venv/bin/python main.py \
  --symbol 002475 \
  --company-name 立讯精密 \
  --start-date 2026-04-15 \
  --end-date 2026-04-22 \
  --run-nlp \
  --run-ablation \
  --episodes 5
```

Optional SQLite persistence:

```bash
.venv/bin/python main.py --symbol 002475 --company-name 立讯精密 --start-date 2026-04-15 --end-date 2026-04-22 --run-ingestion --run-nlp --run-ablation --use-sqlite
```

## Run The Notebook

```bash
.venv/bin/jupyter notebook notebooks/full_report_pipeline.ipynb
```

Use the `Python (.venv fintechgp)` kernel. The first notebook section has a control panel where you can select a preset stock or type a custom 6-digit A-share code, choose start/end dates, set data sources, and decide whether to re-run ingestion.

Important notebook rule:

- `Symbol` controls the market-data ticker. `Company` is the stronger news-search keyword.
- For a custom symbol, the panel tries to auto-fill `Company` from `config/stock_aliases.json`, then from an existing master CSV in `outputs/stocks/<symbol>/data/`.
- If `Company` is empty, the scraper still runs, but it searches mostly by stock-code variants such as `000625`/`sz000625`, which is weaker for Chinese news matching.
- If the expected CSV already exists in `outputs/stocks/<symbol>/data/`, leave `Run data ingestion` unchecked.
- If the exact CSV does not exist but a same-stock master CSV covers the requested range, the notebook automatically slices the master and analyzes that range.
- If you choose a new stock or a date range not covered by the master CSV, check `Run data ingestion`, click `Apply custom run settings`, then run section 2 onward.

The notebook generates tables, figures, daily net inflow/outflow proxy, ablation metrics, walk-forward split summaries, and report draft markdown under `outputs/stocks/<symbol>/reports/`.

## Data Cache And Cleanup

The ingestion layer keeps one canonical timeline per stock:

```text
outputs/stocks/<symbol>/data/<symbol>_finance_text_master.csv
```

Before scraping, the platform checks whether this master already covers the requested date range. If yes, it slices the master into the requested analysis CSV. If not, it scrapes the requested range, merges it back into the master, deduplicates by trading date, and keeps the higher-quality row when overlaps exist.

To clean generated clutter:

```bash
.venv/bin/python scripts/cleanup_redundant_data.py --apply
```

Run without `--apply` for a dry run.

To migrate older `data/integrated/`, `result/`, `reports/`, and `results/` files into the final per-stock layout:

```bash
.venv/bin/python scripts/reorganize_project_outputs.py --apply --remove-caches
```

## Launch Dashboard

```bash
.venv/bin/python main.py --mode dashboard
```

or:

```bash
.venv/bin/streamlit run src/dashboard/streamlit_app.py
```

The dashboard displays selected ticker/date range, close price, daily net inflow/outflow, daily sentiment, trading actions, portfolio curves, ablation metrics, and system health.

## Project Structure

```text
config/                         Default run configuration
docs/                           Guidelines, project structure, data-source notes
notebooks/full_report_pipeline.ipynb
program/                        Existing working scraper
outputs/stocks/<symbol>/data/   Integrated scraper CSV cache and per-stock master timeline
outputs/stocks/<symbol>/reports/ Report tables, SVG figures, markdown drafts
outputs/stocks/<symbol>/results/ RL/evaluation outputs
outputs/database/               Optional SQLite database
scripts/                        Cleanup and maintenance scripts
src/data_ingestion/             Scraper adapter and function-based ingestion API
src/storage/                    SQLite schema and persistence helpers
src/nlp/                        Preprocess, lexicon, logistic TF-IDF, FinBERT, aggregation
src/features/                   Technical indicators, money-flow proxy, and state-vector validation
src/rl/                         FinancialTradingEnv, replay buffer, DQN agent, training
src/evaluation/                 Metrics, walk-forward validation, ablation study
src/reporting/                  Report artifact generation
src/dashboard/                  Streamlit dashboard
tests/                          Unit tests
archive/legacy_src/             Archived earlier module versions for reference only
```

## Module Summary

1. **Data Ingestion**
   `src/data_ingestion/ingestion.py` exposes `fetch_market_data`, `fetch_news_data`, and `run_ingestion`. It preserves the existing scraper and logs row counts, missing values, and paths.

2. **NLP Pipeline**
   `src/nlp/` includes lexicon sentiment, Logistic Regression + TF-IDF, and FinBERT. FinBERT uses local HuggingFace cache by default and safely falls back when the model is unavailable.

3. **Data Storage**
   `src/storage/schema.sql` defines `news_table`, `market_table`, `sentiment_table`, and `trading_log_table`. CSV remains the default fallback.

4. **Feature Engineering**
   `src/features/` computes technical indicators, validates the RL state vector, and adds a daily net inflow/outflow proxy. If a reported net-flow column is present it is used directly; otherwise the proxy is estimated from OHLCV and traded value.

5. **RL Trading Engine**
   `src/rl/` implements `FinancialTradingEnv` and DQN from scratch with PyTorch, replay buffer, target network, and epsilon-greedy exploration. No Stable-Baselines3 is used.

6. **Evaluation and Dashboard**
   `src/evaluation/ablation.py` compares buy-and-hold, DQN without NLP, and DQN with NLP. `src/dashboard/streamlit_app.py` visualizes the outputs.

## Generated Artifacts

Primary outputs:

```text
outputs/stocks/<symbol>/reports/*_daily_sentiment.csv
outputs/stocks/<symbol>/reports/*_daily_net_flow.csv
outputs/stocks/<symbol>/reports/*_signal_diagnostics.csv
outputs/stocks/<symbol>/reports/*_nlp_evaluation.csv
outputs/stocks/<symbol>/reports/*_state_vector_compliance.csv
outputs/stocks/<symbol>/reports/*_walk_forward_splits.csv
outputs/stocks/<symbol>/reports/*_report_draft.md
outputs/stocks/<symbol>/reports/*_sentiment_trend.svg
outputs/stocks/<symbol>/reports/*_daily_net_flow.svg
outputs/stocks/<symbol>/reports/*_drawdown_curves.svg
outputs/stocks/<symbol>/reports/*_portfolio_curves.svg
outputs/stocks/<symbol>/reports/*_risk_return_scatter.svg
outputs/stocks/<symbol>/reports/*_strategy_metric_comparison.svg
outputs/stocks/<symbol>/reports/*_action_distribution.svg
outputs/stocks/<symbol>/results/ablation_metrics.csv
outputs/stocks/<symbol>/results/drawdown_curves.csv
outputs/stocks/<symbol>/results/portfolio_curves.csv
outputs/stocks/<symbol>/results/trading_logs.csv
```

## Known Limitations

- Current scraper wrapper is optimized for China A-share 6-digit symbols.
- Daily net inflow/outflow is a reported value only when the data source provides a net-flow column. Otherwise it is an OHLCV-derived proxy and should be described as estimated market pressure.
- FinBERT is attempted only when the HuggingFace model is available locally, unless `FINBERT_ALLOW_DOWNLOAD=1` is set. If unavailable, the pipeline logs the skip and continues with lexicon/logistic sentiment.
- Generated OHLCV summary text can act as fallback text when external news coverage is sparse.
- Long DQN runs with 200+ episodes are slower on large date ranges; use fewer episodes only for quick debugging, not final reporting.
