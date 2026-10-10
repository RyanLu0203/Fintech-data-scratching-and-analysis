# Reader and chart contract

Primary surface: existing Streamlit app, preserving its native layout and
research controls. The default reader uses four task pages and an explicit
result version. It opens no network/fit path; missing formal data is visible.

| Visual | Question / supported reading | Grain and form | Source |
|---|---|---|---|
| Portfolio NAV | How do strategies evolve in the same fold? | 10 Smoke / 63 Full daily marks; mean across seeds plus initial NAV; multi-series line | frozen `trading_logs.csv` |
| Metric table | What are the exact mean per-seed measures? | strategy × fold, mean seed metric; lookup table | `metrics.csv` |
| Paired differences | How does each NLP run differ from its matched control? | stock × fold × seed × signal; exact lookup (small Smoke sample) | `paired_differences.csv` |
| News trace | When was text available, scored and acted on? | one source story, one decision session and selected seed/group | `news_trace.csv`, actual normalized state and ledger |

Palette follows the supplied revision plan: deep blue, teal, amber and grey.
Strategy identity also uses solid/dashed/dotted/dash-dot lines and labels.
The teal is a signal category, not a positive-return encoding. Titles are
descriptive. Units, window, cost and seed denominator accompany the chart.
Initial cash is included; folds are selected rather than averaged together.
The metric table uses mean per-seed risk measures, not risk of the mean curve.
Smoke has a visible synthetic-evidence warning; no economic inference is offered.

Before-change browser capture: the original first screen shows a long parameter
sidebar and workflow table ahead of the project question; one selected-stock
tab reports missing integrated data. These observations motivated the default
reader. Changes retain Advanced controls for existing research tasks.

Browser review checks navigation, empty state, run selection, trace, portfolio
chart and Methods. Screenshots establish visible layout only; they do not prove
full screen-reader or WCAG compliance. Inspect at laptop and narrow widths.
The final handover records the actual completed checks and remaining limits.
