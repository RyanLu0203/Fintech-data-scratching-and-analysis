# Q&A Preparation

## Did NLP improve trading performance?

Sometimes. The evidence is mixed, so the conclusion is cautious rather than universal.

## Why can NLP hurt?

Sentiment coverage may be sparse, news can be delayed, and noisy sentiment can push the DQN toward overtrading.

## Is FinBERT really used?

Only when `finbert_status=ok`. If the model is unavailable, outputs state `skipped` and the RL input falls back to logistic or lexicon sentiment.

## How do you avoid look-ahead bias?

Technical features are shifted and news is aligned conservatively to tradable dates. Diagnostics are saved with each run.

## Why not use Stable-Baselines3?

The guideline requires a DQN from scratch, so the project implements the network, replay buffer, and target updates directly.