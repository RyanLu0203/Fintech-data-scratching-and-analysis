# Real-data experiments — 2026-10-10

The fixed Full protocol has completed for **002475 and 601138**: 36 DQN agents,
200 episodes each, all three seeds and both chronological folds. Results vary
by stock, fold and seed. They do **not** establish a stable NLP return advantage.
Eleven of the 36 agents execute no test trades; these flat results remain included.

## Frozen sources and timing

The [source snapshot](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/data/frozen/electronics_2023_2025_20261010/source_manifest.json)
was retrieved on 2026-10-10. It retains raw API responses, request URLs,
retrieval times and SHA256 hashes alongside normalized CSVs. Tencent supplies
retrospective qfq OHLCV; Eastmoney supplies corporate disclosure **titles**.
These are actual provider records, not generated news or price summaries.

| Role | Symbol | Market sessions | Disclosure titles |
|---|---|---:|---:|
| Target | 002475 | 541 | 363 |
| Target | 601138 | 541 | 319 |
| Fixed electronics peer | 002241 | 541 | 386 |
| Fixed electronics peer | 601231 | 541 | 421 |

Market dates span 2023-01-03 through 2025-03-31. The collected interval is
2023-01-01 through 2025-03-31. Both targets use the same two peers; neither
target trains either peer model. Title counts above are distinct provider IDs;
normalized duplicate text is deduplicated within the fitting/scoring boundary.
Monthly source counts and usable target coverage are in each `data_quality.json`.
No-news days stay in the market sequence.

Historical disclosure listings omit precise `display_time`. We therefore wait
until the **entire provider public-notice date has ended**, also respecting any
later precise production time, then use the first market open strictly after
that bound. For example, a notice dated Friday is eligible from Saturday
00:00 Asia/Shanghai and first enters Monday's opening state. This is a declared
conservative **availability assumption**, not an observed historical publication
timestamp. Provider history downloaded today cannot prove its point-in-time
completeness or eliminate revised/backfilled records.

The qfq snapshot uses a consistent provider adjustment basis. Adjusted share
units and corporate-action/dividend treatment are approximations. The daily
ledger omits board lots, T+1 sellability, price limits, slippage and asymmetric
stamp tax. It is a teaching backtest, not an executable A-share strategy.

## Protocol and complete comparisons

The configuration was saved before training in the two
[protocol files](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/protocol_002475.json) ([601138](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/protocol_601138.json)).
The first 504 sessions serve the fixed protocol; the remaining 37 collected
sessions are unused. No test window was moved after returns were observed.

| Fold | NLP fitting cutoff (Shanghai close) | RL training | Test (63 sessions) |
|---|---|---|---|
| 1 | 2023-07-12 | 2023-07-13 – 2024-07-25 | 2024-07-26 – 2024-10-31 |
| 2 | 2023-10-17 | 2023-10-18 – 2024-10-31 | 2024-11-01 – 2025-02-06 |

Each target × fold trains market-only, sector sentiment and sector impact DQN
with seeds 42/123/2024, vanilla 64×64 network, eight state slots, three actions,
200 episodes, initial cash 1,000,000 and proportional cost 0.001. Sentiment uses
peer lexicon weak labels; impact uses matured peer three-session return labels.
**FinBERT is evaluated separately and does not produce these trading signals.**
Buy-and-Hold is evaluated once per fold on the same integer-share ledger.
See [methodology](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/methodology.md) for reward, state and risk definitions.

## Results

The table shows mean NLP-minus-market-only cumulative return in **percentage
points**, with the full three-seed range. A positive mean is descriptive, not
a significance test. Seeds measure optimization variation; they do not create
independent market observations.

| Target | Fold | Sentiment mean Δ (range), pp | Impact mean Δ (range), pp | Buy-and-Hold return |
|---|---:|---:|---:|---:|
| 002475 | 1 | +2.753 (−5.617, +13.876) | −1.916 (−5.617, 0.000) | +14.594% |
| 002475 | 2 | +2.733 (−2.060, +6.347) | +0.671 (−3.716, +4.317) | −1.656% |
| 601138 | 1 | +15.182 (+14.971, +15.320) | +15.266 (−4.470, +32.390) | +7.654% |
| 601138 | 2 | −5.967 (−15.049, +5.817) | −3.727 (−15.244, +15.244) | −15.244% |

[Every paired seed and fold](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/assets/real-paired-returns.png)

Across six seed/fold observations per target, sentiment beats its matched
control in 3/6 observations for 002475 and 4/6 for 601138; impact does so in
2/6 and 3/6 respectively. The strong first-fold sentiment difference for
601138 reverses in the second fold. Means across these observations are
unweighted descriptive summaries, not pooled portfolio returns.

| Target | Mean control return | Mean sentiment return | Mean impact return | Zero-trade agents |
|---|---:|---:|---:|---:|
| 002475 | +0.352% | +3.094% | −0.271% | 8/18 |
| 601138 | −7.828% | −3.220% | −2.058% | 3/18 |

Full per-seed return, annualized return, Sharpe, MDD, trade count and turnover
remain in the metrics CSVs below. Zero-volatility Sharpe is undefined and kept
as N/A. Negative returns and no-trade policies are retained, without choosing
a winning seed. Shared peers, two targets, only two non-overlapping test periods,
weak labels and previously inspected historical periods limit generalization.
This is historical re-evaluation, not prospective unseen-market evidence.

## Saved evidence and validation

- [002475 manifest](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624247113Z_full_002475/manifest.json),
  [all metrics](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624247113Z_full_002475/metrics.csv),
  [paired differences](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624247113Z_full_002475/paired_differences.csv).
- [601138 manifest](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624246574Z_full_601138/manifest.json),
  [all metrics](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624246574Z_full_601138/metrics.csv),
  [paired differences](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/outputs/real/20261010T110624246574Z_full_601138/paired_differences.csv).

Together these contain 7,200 episode records and 2,520 test ledger rows,
36 DQN checkpoints and their frozen scalers, matured peer labels, daily signals
and source-to-decision traces. Manifests record source, code, output and model
hashes. The default showcase points to 002475 for navigation; the other stock
is selectable and is included in every cross-stock conclusion.

[Independent validation](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/scripts/validate_saved_evidence.py) checks frozen
source/output/model hashes, all configured episodes/folds/seeds, cash/shares,
fees, NAV, rewards, turnover, risk/return metrics, paired differences, zero
control text channels, label maturity and news-before-opening boundaries.
The [saved validation result](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/reports/real_evidence_validation.json) passes
both targets and the separate NLP benchmark. Cached immutable market rows
speed training without changing the algorithm; parity tests compare states,
rewards and ledgers with the original path. See [reproduction](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/blob/main/docs/reproduction.md).

**Assessment: share with caveats.** Saved calculations and artifacts are
validated. Historical provider timing and adjustment limitations must accompany
any presentation; the results do not support live-profit or stable-NLP claims.
