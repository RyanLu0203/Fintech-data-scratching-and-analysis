# NLP-Driven Reinforcement Learning Trading Platform Final Report

## Executive Summary

The official current experiment is **Peer-Sector NLP Transfer**. For each held-out target stock, the target stock is excluded from NLP training. Sector-peer NLP is trained from other stocks in the same sector, while marketwide-peer NLP is trained from all other available A-share stocks. The peer-trained sentiment scores are then applied to the target stock's own high-density news window and entered into a from-scratch DQN trading state.

The cautious conclusion remains conditional: peer NLP may improve, hurt, or have mixed effects depending on peer-corpus sufficiency, target news coverage, market regime, and DQN stability. Legacy stock-level NLP is deprecated as a main experiment because it can mix target-stock training and testing evidence.

## Research Question

Can peer-trained NLP sentiment improve DQN trading performance for a held-out target stock?

The main comparisons are:

- `dqn_without_nlp`
- `dqn_with_sector_peer_nlp`
- `dqn_with_marketwide_peer_nlp`

Effects are measured as:

- `sector_nlp_effect = dqn_with_sector_peer_nlp - dqn_without_nlp`
- `marketwide_nlp_effect = dqn_with_marketwide_peer_nlp - dqn_without_nlp`
- `sector_vs_marketwide_effect = dqn_with_sector_peer_nlp - dqn_with_marketwide_peer_nlp`

## Peer-Sector NLP Transfer Design

For each target stock:

1. Detect the recent high-information-density evaluation window using the 80% news-density rule.
2. Use the earlier target-stock market data as the DQN market-learning window.
3. Build a sector-peer NLP corpus from other stocks in the same sector, excluding the target.
4. Build a marketwide-peer NLP corpus from all other available A-share stocks, excluding the target.
5. Score the target stock's own high-density news with the peer-trained models.
6. Run DQN ablation with identical train/test windows, seeds, transaction costs, reward function, and architecture.

No-news days are marked with explicit availability and missing flags. They are not treated as genuine neutral sentiment.

## State and Leakage Control

The no-NLP DQN state is:

`[price, MA50, MA200, RSI, MACD, position, cash]`

The sector-peer NLP DQN state is:

`[price, MA50, MA200, RSI, MACD, position, cash, sector_sentiment_score]`

The marketwide-peer NLP DQN state is:

`[price, MA50, MA200, RSI, MACD, position, cash, marketwide_sentiment_score]`

Market features and sentiment features are lagged by one trading day before action selection:

`features at t-1 -> action at t -> reward from t to t+1`

## Required Outputs

The official output files are:

- `reports/tables/stock_sector_mapping.csv`
- `reports/tables/peer_nlp_corpus_summary.csv`
- `outputs/stocks/<symbol>/results/peer_nlp_daily_sentiment.csv`
- `outputs/stocks/<symbol>/results/peer_nlp_ablation_metrics.csv`
- `outputs/stocks/<symbol>/results/peer_nlp_portfolio_curves.csv`
- `outputs/stocks/<symbol>/results/peer_nlp_trading_logs.csv`
- `reports/tables/peer_nlp_effect_summary.csv`
- `outputs/system/peer_nlp_cross_stock_summary.csv`
- `outputs/system/peer_nlp_cross_stock_diagnostics.csv`
- `outputs/system/peer_nlp_cross_stock_discussion.md`
- `reports/tables/peer_nlp_integrity_check.csv`
- `reports/peer_nlp_integrity_check.md`

## Legacy Experiment Policy

The old stock-level NLP experiment is deprecated and may only be used as a robustness or historical reference. It is no longer an official result and must not be used for the main dashboard, notebook conclusion, or report conclusion.

## Dashboard and System Integration

The Streamlit dashboard defaults to the Peer-Sector NLP Transfer experiment. It shows sector mapping, corpus readiness, peer sentiment trends, DQN ablation results, cross-stock comparison, and reliability diagnostics. It does not scrape or retrain by default when reviewing existing outputs.

## Limitations

- Some sectors currently have fewer than four usable local peer stocks. Those sector-peer corpora are marked insufficient rather than faked.
- FinBERT is only used if available locally or explicitly enabled; fallback methods are reported honestly.
- The current news endpoint is denser near recent dates, so the official experiment focuses NLP testing on recent high-density windows.
- Cross-stock claims are only reliable when corpus sufficiency, sentiment coverage, non-flat DQN curves, and test-trade checks pass.

## Conclusion

Peer NLP is a stricter and more defensible design than stock-level self-training. It directly tests transfer: whether sentiment learned from related stocks improves trading for a held-out target. The final conclusion should remain cautious and evidence-based: sector-peer or marketwide-peer NLP is useful only when the corpus is sufficient, the target has enough high-density news, and the DQN comparison passes reliability checks.
