# Changelog

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
