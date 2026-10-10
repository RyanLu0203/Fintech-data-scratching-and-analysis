# Coursework refinement — 2026-10-10

> Historical first-pass addendum. The empirical work described as pending below
> has since been completed in the [real-data report](../docs/real_experiments.md)
> and [Chinese FinBERT evaluation](../docs/finbert_evaluation.md): 36 × 200-episode
> agents, both targets/all folds/seeds, and actual external human-label inference.
> Those reports supersede this addendum's empirical status; old results remain
> preserved history. New project-title annotation and prospective validation
> remain separate future work.

## Outcome and evidence boundary

The project now has a research-oriented homepage, three distinct system/journey/
experiment diagrams, a four-page frozen-output reader, corrected accounting
and state paths, independent gold evaluation, and an actual chronological
comparison runner. Existing scrapers, handwritten DQN, SQLite integration,
expanded configurations and historical reports remain available.

The verified empirical artifact is **synthetic Smoke**, not a stock-performance
study. It fits six DQN agents (three groups × two folds × one seed), two episodes
each, and evaluates Buy-and-Hold once per fold. Formal market findings and the
course's independent FinBERT comparison remain pending real source inputs.

## Source and baseline

Implementation was based on public main commit
`f66e264874163d09a6556f50a0405e4df57089d0`, obtained from GitHub's source archive
and checked against the commit API. The original README, course specification,
report, model/evaluation/dashboard code and all 39 baseline tests were read or
executed. The supplied revision plan served as reference context; it was not
treated as proof that its proposed tasks or experiments had already occurred.

The public snapshot contains historical model-upgrade outputs but lacks the
source-complete target/peer market and timestamped news needed by the fixed
Full protocol. The local historical coursework folder was used only for its
existing Python environment; its source/results did not replace upstream code.

## Findings and corrections

| Finding | Why it matters | Implemented correction |
|---|---|---|
| Normalized `daily_return` read current close | same-day information entered the opening state | use lagged return/previous-close price history |
| Account ratios used current close | portfolio state could reveal unavailable price | value account inputs at the preceding close |
| Reward denominator followed the trade | fees disappeared from ordinary reward; overnight sell gains were omitted | previous-close NAV → current-close NAV; costs included |
| Last row was never executed | displayed/evaluated interval lost its last session | execute and mark every session |
| Turnover was fee amount / NAV | gross activity and monetary cost were conflated | record executed traded value separately |
| Intended action was counted as execution | cash-constrained Buy could inflate trade count | record actual shares change as executed action |
| RSI became NaN in zero-loss/flat sequences | tests could pass on an empty feature frame | RSI 100 for positive/zero-loss, 50 for flat periods |
| Unchecked duplicate joins | one-to-many news expansion can repeat market returns | reject duplicate daily market/signal grain |
| FinBERT continuous differences were truncated | nonzero probabilities collapsed to neutral class IDs | map classification labels; retain floating signal scores |
| Peer sentiment returned `predict` IDs | advertised probability-difference semantics differed | use positive minus negative probabilities |
| Final gold evaluation included LR training rows | F1 overstated independent model performance | one common test set; separate fitting; duplicate exclusions |
| Legacy corpus generation had no chronological prefix | target exclusion alone did not isolate future peer data | explicit frozen prefix and matured impact labels in new runner |

These are analytical-integrity corrections. They invalidate reuse of old
performance as evidence for the new implementation. The historical CSVs were
preserved, not silently recalculated or relabelled as current results.

## Fixed experimental design

The core uses the existing eight-slot state with training-window scaling,
three actions and vanilla DQN. This provides a controlled teaching baseline
while preserving the 12-dimensional/target-position/Double/Dueling explorations.
Shared conditions are initial cash, transaction costs, architecture, seeds,
training budget, market interval and reward. Only the text signal changes.

Each Full fold fits NLP on an earlier peer prefix, trains RL on the following
252 sessions and evaluates on 63 later sessions. Two folds have non-overlapping
test intervals; all groups retrain per fold. Impact labels have explicit
availability and maturity cutoffs. The benchmark uses the same integer-share
ledger and costs. Initial cash enters every risk measure. Paired effects are
computed within stock, fold and seed, with N/A retained for undefined Sharpe.

There is no hyperparameter selection on the test windows. Previously inspected
market periods remain historical re-evaluations. The core weak-label sentiment
comparison does not satisfy the separate FinBERT/manual-label benchmark.
See [methodology](../docs/methodology.md) for the exact definitions and limitations.

## Presentation and reader review

1. Overview: the research question, four concrete modules, two development cases
   and a visible missing-formal-results state replace the parameter-heavy start.
2. News to Decision: availability, first decision session, model score, actual
   normalized state, intended action and executed ledger are connected.
3. Experiments: a selected fold's NAV includes initial cash and costs, with
   matched per-seed metrics and paired differences alongside it.
4. Methods & Project: methodology, data contract, conceptual journey,
   reproduction and contribution-confirmation notes have direct entries.

All four pages were exercised in the in-app browser. Screenshots were inspected
at a laptop viewport, including before/after first screens. Static Mermaid SVGs
were rendered locally from the retained sources and inspected at README width.
The requested narrow viewport override remained at 1280 pixels, so mobile
layout verification is still pending. Screenshots do not establish full
assistive-technology or accessibility compliance.

The default reader reads local files and never starts fitting or collection.
Formal showcase is separate from explicitly selected synthetic Smoke versions.
Historical model-upgrade files remain behind the original Advanced workspace,
with a diagnostic-use notice. Output hashes reject incomplete/changed run files.

## Checks actually executed

- Original baseline: 39 tests passed.
- After corrections: 52 tests passed, including 13 new semantic counterexamples.
- Python compilation of source, scripts and tests passed.
- Ruff check passed for the new runner, chronology, reader, CLI and tests.
- Offline Smoke completed genuine per-fold NLP/DQN fitting and evaluation;
  the final frozen bundle contains 80 test ledger rows, 8 metric rows and 4
  paired-difference rows. Its seven data files pass the reader's hash checks.
- The data-quality companion's code cells executed successfully: its synthetic
  fixture has 70 target market sessions, 24 target stories and 140 peer stories.
- Full without source files exited with an explicit input error; it did not
  generate replacement results.
- Current-document links and exported diagram/source paths were checked.

Verification used the existing Python 3.9.6/macOS environment. Exact installed
versions are in `requirements-validation.txt`. The new offline CI workflow uses those pins and CPU
PyTorch. GitHub Actions [run 38042482465](https://github.com/RyanLu0203/Fintech-data-scratching-and-analysis/actions/runs/38042482465) subsequently passed in a fresh Linux/Python 3.11 environment: pinned installation, compilation, tests and the two-fold Smoke run. A fresh macOS installation and an installation from unrestricted lower bounds remain unverified.

## Remaining empirical and handover work

Freeze real target/peer source snapshots for 002475 and 601138, including
provider availability times and consistent price adjustments. Check coverage,
run the 200-episode comparisons and include every configured seed and fold.
Complete the manual annotation rubric and independent test set, verify an
appropriate checkpoint language/labels/revision, and perform real FinBERT
inference. Confirm the original team contributions and complete the final
course report/presentation around those actual results. A fresh macOS installation,
release and optional social preview are separate later checks.

The original report and notebooks retain historical context. Notebook outputs
were cleared to avoid stale/error traces; the CLI remains the training owner.
No invented team assignment, historical schedule, positive return, passing
badge, published-paper claim or release is included.
