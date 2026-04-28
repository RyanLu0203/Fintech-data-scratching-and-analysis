# Implementation Report Draft: NLP-Driven RL Trading Platform

## Experiment Configuration

- Symbol: `301607`
- Company name: `富特科技`
- Date range: `2024-12-30` to `2026-04-22`
- Data source order: `tencent`
- Input CSV: `data/integrated/301607_finance_text_2024-12-30_2026-04-22.csv`
- Daily net-flow CSV: `reports/301607_finance_text_2024-12-30_2026-04-22_daily_net_flow.csv`
- Rows after loading: `316`
- Event coverage ratio: `100.00%`
- Total text/news/event count: `4004`

## System Pipeline

The platform follows the required end-to-end flow: raw market and text data are collected by the data ingestion module, converted into daily sentiment signals by the NLP pipeline, stored as CSV/report artifacts for reproducible experiments, merged into the RL state representation, and evaluated through ablation metrics and benchmark comparisons.

## Market Money-Flow Signal

Daily net inflow/outflow is computed as a market-pressure proxy. If a reported net-flow column exists it is used directly; otherwise the notebook estimates signed money flow from OHLCV and traded value. This complements close price because price shows the final transaction level, while net flow approximates whether buying or selling pressure dominated the day.

## NLP Signal Construction

For this notebook run, daily sentiment is generated through the reusable NLP pipeline. Lexicon and TF-IDF Logistic Regression run locally; FinBERT is used when the HuggingFace model is available and otherwise logs a safe fallback. The generated file is `reports/301607_finance_text_2024-12-30_2026-04-22_daily_sentiment.csv`.

## RL State Vector

The checked state vector is `['price', 'MA50', 'MA200', 'RSI', 'MACD', 'position', 'cash', 'sentiment_score']`. It includes price indicators, portfolio status, and `sentiment_score`, satisfying the guideline requirement that NLP information must be part of the RL state.

## Ablation Results

- With NLP final equity: `100000.00`, Sharpe: `0.0000`, MDD: `0.0000`
- Without NLP final equity: `184940.95`, Sharpe: `1.1710`, MDD: `-0.3065`
- Buy-and-hold final equity: `196023.53`, Sharpe: `1.2062`, MDD: `-0.3446`
- Best experiment by final equity: `buy_and_hold`

## Walk-Forward Validation

The notebook calls `split_frame_walk_forward` with `252` train days and `63` test days. Chronological windows ensure the training period ends before the testing period starts, preventing look-ahead bias. If the selected sample period is too short, no full split is produced; use a longer date range for the final experiment.

## Critical Reflection

This notebook demonstrates the full integration path required by the project guideline. The final written submission should discuss the available sentiment methods, FinBERT fallback status, DQN training episode count, and whether the NLP signal improves Sharpe Ratio, Max Drawdown, and final equity relative to both the no-NLP ablation and buy-and-hold baseline.