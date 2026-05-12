# Model Optimization and Reliability Summary

This report is generated from cached outputs. It does not scrape data or retrain DQN by default.

## DQN Stability
- High-variance experiment rows: `2`
- If improvement is smaller than seed-level standard deviation, treat the NLP effect as inconclusive.

## Action Behavior
- Action-warning rows: `83`
- Hold ratio above 90% means the model may be too conservative.
- High turnover means the model may be overtrading.

## Enhanced NLP Ablation
- Enhanced NLP status values: `existing_cached_result, not_run_by_default`
- `dqn_with_enhanced_nlp` is not trained by default. Use an explicit configured run before claiming enhanced NLP performance.

## Reward Modes
- Default: `portfolio_return`.
- Optional: `portfolio_return_minus_turnover_penalty`, `portfolio_return_minus_drawdown_penalty`.
- The chosen reward mode is written to trading logs when DQN is run.

## Enhanced Signal Diagnostics
- Enhanced feature correlations are saved in `signal_diagnostics_enhanced.csv`.
- Low correlations or low sentiment coverage should lower confidence in NLP-driven conclusions.