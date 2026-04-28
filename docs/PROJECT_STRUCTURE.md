# Project Structure

The repository has been reorganized so the root stays focused on runnable entry
points and major project folders.

```text
fintechgp/
├── main.py
├── README.md
├── requirements.txt
├── config/
├── data/
├── docs/
│   ├── Project_Guidelines.docx
│   ├── PROJECT_STRUCTURE.md
│   └── statement/
├── notebooks/
│   └── full_report_pipeline.ipynb
├── program/
│   ├── finance_text_scraper.py
│   └── run_scraper.py
├── outputs/
│   ├── stocks/
│   │   └── <symbol>/
│   │       ├── data/
│   │       ├── reports/
│   │       ├── results/
│   │       └── models/
│   ├── database/
│   └── system/
├── scripts/
├── src/
│   ├── config/
│   ├── dashboard/
│   ├── data_ingestion/
│   ├── evaluation/
│   ├── experiments/
│   ├── features/
│   ├── nlp/
│   ├── reporting/
│   ├── rl/
│   ├── storage/
│   └── utils/
├── tests/
└── archive/
    └── legacy_src/
```

## Root Files

- `main.py`: CLI entry point for ingestion, NLP, RL, ablation, artifacts, and dashboard launch.
- `README.md`: Main usage guide.
- `requirements.txt`: Python dependency list.
- `.env.example`: Optional environment template.

## Main Folders

- `config/`: Default run configuration.
- `docs/`: Project guideline document, project structure notes, and historical data-source statements.
- `notebooks/`: High-level experiment/report notebook.
- `program/`: Existing scraper implementation preserved for backward compatibility.
- `outputs/stocks/<symbol>/data/`: Integrated CSV cache and per-stock master timeline.
- `outputs/stocks/<symbol>/reports/`: Report-ready tables, SVG figures, diagnostics, and markdown drafts.
- `outputs/stocks/<symbol>/results/`: RL/evaluation CSV outputs such as portfolio curves, drawdowns, and trading logs.
- `outputs/stocks/<symbol>/models/`: DQN checkpoint files.
- `outputs/database/`: Optional SQLite database.
- `outputs/system/`: Shared system-level artifacts such as architecture Mermaid text.
- `scripts/`: Maintenance scripts such as generated-data cleanup.
- `src/`: Production project modules.
- `tests/`: Unit tests.
- `archive/legacy_src/`: Archived earlier module versions kept for reference only.

## Active Source Modules

- `src/data_ingestion/`: Cache-aware scraper interface and master CSV reuse/merge logic.
- `src/storage/`: SQLite schema and persistence helpers.
- `src/nlp/`: Text preprocessing, lexicon sentiment, TF-IDF logistic sentiment, FinBERT fallback, aggregation.
- `src/features/`: Technical indicators, money-flow proxy, and RL state validation.
- `src/rl/`: Financial trading environment, replay buffer, DQN agent, and training utilities.
- `src/evaluation/`: Metrics, signal diagnostics, walk-forward validation, and ablation study.
- `src/reporting/`: Report tables and SVG artifact generation.
- `src/dashboard/`: Streamlit dashboard.
