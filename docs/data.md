# Data overview and input contract

The downloaded public main snapshot contains historical model-upgrade result
CSVs. It does **not** contain the target/peer source market and timestamped news
needed to reproduce those results. Do not infer coverage from the result curves.
The existing scraper/cache remains the collection entry point; obtaining inputs
and running Full are separate from the read-only Demo.

## Four source files for Full

| File | Grain | Required columns |
|---|---|---|
| Target market | one target stock/session | symbol, date, open, high, low, close, volume, source |
| Target news | one distinct target story | news_id, symbol, text, available_at, source |
| Peer news | one distinct peer story | news_id, symbol, text, available_at, source |
| Peer market | one peer stock/session | symbol, date, open, high, low, close, volume, source |

Keep codes as strings: `002475`, `601138`, etc. News stays separate from OHLCV;
a story-to-market join must not duplicate a market session. Prices must be
finite and positive, volume nonnegative, and high/low consistent with open
and close. Duplicate stock/session keys fail validation. Select peer stocks
from the same configured sector, excluding the target. Full needs at least two
independent peers and actual target news in each test interval.

`available_at` must contain time and timezone, for example
`2025-03-07T18:30:00+08:00`. It means the earliest time the story could have
been read using the recorded source. Store `published_at`, `fetched_at`, URL,
provider ID and failure reason when available; retain them in the source audit.
If only a date is known, do not invent a timestamp. A conservative documented
availability policy must first be established from the provider evidence.

Program-generated price summaries remain useful ingestion diagnostics but
are excluded from the external-news increment experiment. The Full gate rejects
sources marked generated, synthetic or fixture. It never fills missing news
with synthetic text. Normalized duplicate stories are counted once, using the
later availability timestamp conservatively. Semantic near duplicates still
need review. Source permissions govern whether full articles can be redistributed;
this repository refinement does not grant redistribution rights.

## Coverage and quality evidence

Each completed run writes `data_quality.json`, `daily_signals.csv` and
`news_trace.csv`. These record row counts, date range, nulls, peer identities,
daily target coverage and the availability-to-decision mapping. Inspect news
counts by period, source and symbol before interpreting a model comparison.
No-news days remain in the market sequence, with explicit missing flags.

The reproducible `notebooks/coursework_data_quality.ipynb` profiles the included
synthetic fixture and explains how to substitute the four source files. Its
fixture counts are not estimates of real historical news density.

For the fixed default Full protocol, each target needs 504 sessions
(126 NLP-prefix + 252 RL-training + 2 × 63 test). Insufficient data is an error,
not a reason to silently shorten or move the test windows. The raw source
snapshot, provider timing and price-adjustment basis must be frozen before
the run; a result manifest records hashes of the inputs actually used.
