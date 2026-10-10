# Project journey

These are conceptual stages reconstructed from the existing README, course
report and implementation. They are not a dated weekly schedule or proof that
each research issue was resolved at the time.

| Stage | Question or observation | Existing evidence |
|---|---|---|
| Initial prototype | Can news enter a DQN state? | `src/nlp/`, `src/rl/`, original course report |
| Coverage review | Historical news is uneven | `src/evaluation/information_density.py`, data source statement |
| Peer-trained signals | Can non-target peers supply training text? | `src/nlp/peer_sentiment.py`, `market_impact.py` |
| Trading behavior diagnosis | Why do policies hold so often? | historical action/exposure diagnostic CSVs |
| State/action exploration | Does representation affect learning? | normalized_plus, target_position and DQN variants |
| Integrated analysis | How can the system be explained? | Streamlit, notebooks, report and exported diagnostics |

## Case A: uneven historical news

Observation: the project's existing source notes describe limited remote
historical coverage. Impact: a target-text train/test split alone may provide
little usable training data. Response: sector/marketwide peer corpora and
coverage diagnostics. Limitation: excluding the target does not exclude future
peer text or future return labels. The new fixed runner adds an explicit NLP
prefix, availability checks and label maturity. Real coverage still needs the
source snapshots; no density improvement is claimed from a synthetic fixture.

## Case B: hold-heavy DQN

Observation: the existing model-upgrade notes describe empty/low-exposure
policies. Response: training-window state scaling, normalized features,
target-position actions and action/Q-value diagnostics. Limitation: extra
actions or a less-flat curve do not establish economic gain. This refinement
also fixes same-day returns and ledger rewards, so historical behavior and
performance need re-evaluation. The fixed core uses the existing eight-slot
state, training scaler and three actions; the extensions remain available.

## Refinement on 2026-10-10

| Milestone | Status | Evidence / remaining work |
|---|---|---|
| M1 Story and repository | Implemented locally | new homepage, three diagram sources and document navigation |
| M2 Data and accounting | Tested for targeted semantics | grain, availability, fee/overnight/final-session counterexamples |
| M3 NLP and evaluation | Runner implemented; formal evidence pending | independent gold split and true per-fold fitting; real FinBERT/gold inputs absent |
| M4 Results and demo | Smoke completed; real results pending | frozen reader and synthetic two-fold run; no formal market claim |
| M5 Handover | Local package prepared | local checks and browser review; Linux/Python 3.11 CI install/tests/Smoke passed |

The next empirical step is to freeze the real 002475/601138 source data,
verify a suitable language checkpoint and complete the independent NLP and
200-episode comparisons. The roadmap has no invented completion percentage,
team assignment, historical PR review or delivery date.
