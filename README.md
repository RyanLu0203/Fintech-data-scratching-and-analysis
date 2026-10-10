# NLP-Driven Reinforcement Learning Trading Platform

### From Financial News to Trading Decisions

A group project for **Data Analysis and FinTech Fundamentals**.

[Overview](#project-at-a-glance) · [Journey](#project-journey) · [Experiments](#experiments-and-evidence) · [Demo](#explore-the-dashboard) · [Report](reports/refinement_report.md) · [中文](README.zh-CN.md)

This project investigates whether financial news provides incremental information for DQN-based stock trading. We connect market and news data, peer-trained text signals, a custom trading environment and a Streamlit dashboard. The development process examines two practical challenges: uneven historical news coverage and hold-heavy trading behavior. The repository presents the system, its design iterations, experimental comparisons and the limits of the available evidence.

**Evidence status:** offline tests and a synthetic two-fold pipeline check have been executed. Source-complete, 200-episode market experiments and an independent FinBERT benchmark remain pending. Preserved May 2026 diagnostic returns predate the accounting/state corrections and are excluded from new headline results. No profitable-trading or positive NLP-increment claim is made.

## Project at a glance

| What we built | What you can inspect |
|---|---|
| Financial data pipeline | existing scraper, cache, source statement and [input/coverage contract](docs/data.md) |
| Peer-trained NLP signals | sentiment and return-labelled impact paths with [availability and maturity boundaries](docs/methodology.md) |
| Handwritten DQN experiment | replay buffer, target network, chronological trading ledger and [fixed comparison runner](scripts/run_coursework.py) |
| Interactive analysis | project overview, news-to-decision trace, paired results and methods in [Streamlit](src/dashboard/streamlit_app.py) |

The teaching baseline uses eight state slots and Buy/Hold/Sell. Existing normalized 12-dimensional states, target-position actions and DQN variants remain available as supplementary explorations. The fixed core comparison uses the eight-slot state with training-window scaling and three actions. Its configuration is fixed before viewing test returns.

## Research question

Can signals learned from non-target peer news improve the target stock's DQN decisions under the same market window, costs, architecture and training budget? Target exclusion and chronological isolation are separate requirements. A corpus can exclude the target and still contain future peer information; the new runner therefore fits upstream models before the RL training window.

## How it works

![System Overview](docs/assets/system-overview.svg)

[Editable diagram source](docs/diagrams/system-overview.mmd)

Market features and each agent's account state are common inputs. Sentiment and impact are parallel alternative signals. Peer future returns supply matured training labels only. The control receives no text content. SQLite is a storage layer; the new result package stores full traces as files, while the existing SQLite integration persists core data through the legacy batch workflow.

## Project journey

![Project Journey](docs/assets/project-journey.svg)

[Editable diagram source](docs/diagrams/project-journey.mmd)

These are conceptual stages reconstructed from existing materials, not a dated schedule. In [the project journey](docs/project_journey.md), two cases connect observations to design responses: sparse historical news motivated peer-corpus organization; hold-heavy policies motivated state scaling, action exploration and behavior diagnostics. Neither response by itself demonstrates better economic performance.

## Experiments and evidence

![Experiment Design](docs/assets/experiment-design.svg)

[Editable diagram source](docs/diagrams/experiment-design.mmd)

Full uses an NLP training prefix, RL training and two non-overlapping test folds, with 200 episodes and three fixed DQN seeds. Every fold fits and trains again. The runner records input/code hashes, model configuration, cutoffs, label maturity, training rewards, scalers, test decisions and monetary costs. Buy-and-Hold uses the same execution and valuation conventions, once per fold.

All strategy risk measures include initial cash. Zero-volatility Sharpe is N/A. NLP effects are paired differences within stock, fold and seed. Seed variation describes optimization randomness and does not supply independent market observations. Expanded action/reward configurations belong in separate controlled comparisons.

| Evidence | Current interpretation |
|---|---|
| Original model-upgrade CSVs | historical diagnostics; require re-evaluation |
| New two-fold Smoke run | verifies fitting, training, ledger and trace paths on synthetic fixtures |
| Real-data Full comparisons | pending frozen source data; no new market conclusion |
| Independent NLP / FinBERT benchmark | evaluation API repaired; manual labels and suitable checkpoint pending |

The [current report addendum](reports/refinement_report.md) records changes and limits. The [original course report](reports/final_report.md) remains available with a revision notice. Negative, flat or unstable results should be included when formal comparisons are completed.

## Explore the dashboard

```bash
python -m streamlit run src/dashboard/streamlit_app.py
```

![Actual read-only dashboard overview](docs/assets/dashboard-overview.jpg)

*Actual local reader, showing that formal data/results are still pending.*

Follow **Overview → News to Decision → Experiments → Methods & Project**. Choose an explicit result version, inspect when one story became available, see its model scores and actual DQN state/action, then compare the matching fold. This is traceability, not a causal explanation of a trade; Q-value differences are not profit probabilities.

The default reader opens frozen local outputs without training or network collection. If no formal run exists, it states which inputs are missing. Smoke versions must be selected explicitly and carry a synthetic-data warning. Advanced controls retain the original research workspace.

## Run it

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-validation.txt
python -m pytest -q
python scripts/run_coursework.py --mode smoke
```

Full requires four real input files and never substitutes generated data. See [reproduction](docs/reproduction.md) for actual commands, input requirements, FinBERT setup and failure handling. The [legacy guide](docs/legacy_repository_guide.md) preserves earlier grid/CLI details for historical tracing.

## Team and deliverables

The five workstreams are data, NLP, storage, RL and the dashboard, with analysis and reporting across them. [Team contributions](docs/team.md) must be confirmed from team records; repository ownership is not a contribution assignment. Deliverables include the course specification, original report, presentation outline, live-demo script, implementation and reproducible notebooks.

## Limits and references

Historical news coverage, weak labels, checkpoint language, previously viewed test periods and stochastic training limit interpretation. The daily ledger omits market microstructure and exchange trading restrictions. This is a course research platform, with no claim of live execution or stable profits. See [methodology](docs/methodology.md), [data provenance](docs/data.md), the [original course specification](docs/Project_Guidelines.docx), and the [FinBERT model card](https://huggingface.co/ProsusAI/finbert).
