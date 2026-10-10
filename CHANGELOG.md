# Changelog

## 2026-10-10 — Real-data and Chinese FinBERT follow-up

- Freeze actual Tencent qfq OHLCV and Eastmoney disclosure titles for both
  targets and two fixed peers, retaining raw responses, retrieval times and hashes.
- Declare date-only disclosure availability after the whole public notice date;
  document historical timing and price-adjustment limitations.
- Complete both Full runs: 36 DQN agents × 200 episodes, every fold/seed,
  7,200 training records and 2,520 ledger rows. Retain 11 zero-trade policies.
- Verify a pinned Chinese FinBERT checkpoint and evaluate external CFBenchmark
  human consensus labels. Shared-test macro F1: lexicon 0.453, LR 0.605,
  FinBERT 0.391. Document unknown private training overlap and truncation.
- Add reproducible collectors, inference/retraining commands, full saved
  evidence validation in CI, result figures and a real default showcase.
- Prevent future duplicate text from removing earlier prefix training rows;
  accelerate immutable market access with tested state/reward/ledger parity.

The section below records the earlier first pass; its pending empirical status
is superseded by this follow-up.

## 2026-10-10 — Coursework refinement

- Reorganize the homepage around the research question, implementation,
  conceptual journey, controlled comparison and evidence limits.
- Add a default four-page frozen-output reader, explicit version selection,
  checksum validation and news/state/action/ledger tracing.
- Fix same-day normalized returns, account valuation time, fee-inclusive
  rewards, overnight returns, final-session handling and turnover semantics.
- Reject duplicate market/signal grain; correct flat/zero-loss RSI and causal
  news-count scaling.
- Preserve FinBERT classification labels, use peer probability differences,
  expose unavailable checkpoint/language/mapping failures and record revisions.
- Use a common independent gold test, exclude normalized cross-boundary
  duplicates and export predictions and confusion counts.
- Add fixed chronological peer fitting and actual per-fold DQN retraining,
  paired differences and same-ledger Buy-and-Hold. Smoke is synthetic; Full
  requires recorded real inputs and at least 200 episodes.
- Preserve original reports and extended configurations. Historical result
  files require re-evaluation after these corrections.

No real-data Full run, manual-label benchmark, verified Chinese FinBERT
inference or release is claimed. The remote Linux/Python 3.11 install, tests
and Smoke check subsequently passed in GitHub Actions.
