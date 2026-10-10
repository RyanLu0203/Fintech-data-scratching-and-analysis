# Methodology

## Research question and fixed comparison

Does non-target peer news provide incremental information for DQN trading?
The core groups are market-only DQN, sector sentiment DQN, sector impact DQN,
and Buy-and-Hold. The first three share the same eight input slots, three
actions, network, training budget, costs and seeds. Only the text signal changes.
The control's text slot is zero. The existing 12-dimensional normalized state,
five target-position actions and DQN variants remain available as explorations;
their results cannot be subtracted from a differently configured control.

The fixed runner uses vanilla DQN, two 64-unit hidden layers, Huber loss,
gradient clipping at 1.0, gamma 0.99 and learning rate 0.001. It scales inputs
using the RL training window and freezes that scaler in evaluation. Flat
penalties and exploration trade bonuses are disabled. Full uses 200 episodes,
seeds 42/123/2024 and two non-overlapping 63-session test folds. Parameters are
fixed before any test results are inspected; there is no model leaderboard.

## Chronological protocol

For fold 1: 126-session NLP prefix → 252-session RL training → 63-session test.
Fold 2 advances the boundaries by 63 sessions. Each fold fits new peer models,
trains new DQN agents and fits new training scalers. The second fold's expanded
prefix may contain information that was unavailable in the first fold, but
never changes the first fold's frozen models. Previously inspected historical
periods are **re-evaluations**, not newly unseen market evidence. This protocol
has no validation-based tuning; any future tuning needs a separate chronological
validation interval before the test windows.

Peer membership comes from `config/stock_sector_mapping.csv`. Target news and
target future returns never train the peer models. Only peer news available by
the NLP cutoff enters fitting. Impact labels use the peer close-to-close return
over three trading sessions, with thresholds +1.5% and −1.5%; the label's last
close must have occurred by the cutoff. These are weak return labels, not causal
estimates of a story's impact. Sentiment uses lexicon weak labels to fit TF-IDF
logistic regression. Both scorers return P(positive) − P(negative), not discrete
predicted class IDs.

Normalized duplicate texts in peer training are excluded from target scoring.
This prevents text re-use across stock codes from masquerading as transfer.
Human review is still required for semantic near duplicates and gold labels.

## Availability and state

News requires a timezone-aware `available_at`. A story first becomes usable at
the first market open **strictly after** that timestamp (09:30 Asia/Shanghai).
After-close and weekend stories therefore wait for the next session. This
mapping is the single text-delay layer: the new runner does not shift it again.
Market indicators use information through the previous close, including
price, moving averages, RSI and MACD. Account ratios use the previous close,
not today's close. Missing news retains a missing flag; its numeric zero is
not an assertion of neutral sentiment.

The state is `[price, MA50, MA200, RSI, MACD, position, cash, nlp_signal_score]`.
`nlp_signal_score` is the course sentiment slot, populated with the selected
sentiment or impact signal. The network sees training-window normalized values.
In the retained 12-dimensional exploration, all four text-derived channels
(score, magnitude, rolling mean and change) are zero in the control.

## Trading ledger and metrics

At session t: form state through t−1 → execute at t open → mark at t close.
Initial NAV is initial cash before the first trade. Reward is
`NAV_close(t) / NAV_close(t−1) − 1`, including previous-position overnight
gains, trading fees and current-position intraday changes. Every session,
including the last, is processed. The final position is marked at close without
forced liquidation. The log separates intended action, actual execution,
gross traded value and monetary fees. Turnover is traded value / previous NAV.

Buy-and-Hold uses the same environment, integer shares, entry cost and final
mark convention. It is evaluated once per fold, without replicated DQN seeds.
Sharpe uses daily NAV returns, a zero cash/risk-free return and 252 sessions per
year. Initial NAV enters Sharpe, annualization and drawdown. Zero-volatility
Sharpe is N/A. MDD is positive loss magnitude; negative paired ΔMDD is better.
Paired differences are NLP minus market-only within stock × fold × seed.
Seeds quantify optimization variation; they are not independent market samples.

This is a course-level daily model: it omits slippage, order-book liquidity,
board lots, A-share T+1/price-limit enforcement and corporate-action execution
details. Adjusted OHLC must use a consistent basis. Backtests do not establish
live tradability. The existing three/five-day reward variants remain
counterfactual holding-value experiments, not correct n-step Bellman returns.

## Independent NLP benchmark

`gold_label_evaluation.py` uses a shared held-out test set for lexicon, LR
and FinBERT. An explicit `split` column (`train` / `test`) is preferred; without
it, sufficiently large labelled data uses a fixed 75/25 split. Normalized text
duplicates cannot cross the boundary. Small sets leave LR unavailable rather
than reporting training-set F1. Class counts, test predictions and confusion
matrices accompany metrics.

FinBERT is pretrained inference here; no team fine-tuning is claimed. The
[ProsusAI model card](https://huggingface.co/ProsusAI/finbert) identifies the
default checkpoint as English, with positive/neutral/negative labels. Chinese
input therefore needs a verified suitable checkpoint. Revision, language and
label mapping must be checked; unavailable inference returns missing values.
The core weak-label experiment does not replace the course's FinBERT benchmark.
Actual Chinese FinBERT inference now uses a pinned suitable checkpoint and
CFBenchmark's external human consensus labels. The common test contains 149
texts; LR fits only the other 444. Checkpoint training texts are private, so
its overlap with this benchmark cannot be verified. “External” does not imply
verified independence from pretraining. See [FinBERT evaluation](finbert_evaluation.md)
for model/data revisions, results, truncation and this limitation.

## Evidence status

The included Smoke run tests orchestration on explicitly synthetic data, with
two episodes per group. It supports implementation checks only. May 2026
diagnostic CSVs predate the accounting/state corrections and are preserved as
history. They are excluded from the new main-results reader. A formal finding
requires real source-bound inputs, complete Full runs and the external NLP
benchmark. Both target Full runs and actual Chinese inference are now complete;
the [real report](real_experiments.md) includes all seeds/folds and limitations.
Source dates are conservatively mapped to availability bounds, not represented
as observed publication times. Retrospective qfq is a frozen adjustment snapshot.
Run manifests record inputs, configuration, cutoffs, seeds, code
hashes and output hashes; changed or incomplete outputs fail the reader check.
