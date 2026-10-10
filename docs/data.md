# Data overview and input contract

The repository includes a frozen real-data snapshot for 002475/601138 and fixed
peers 002241/601231 under `data/frozen/electronics_2023_2025_20261010/`. Each has
541 Tencent qfq sessions; Eastmoney supplies 1,489 disclosure titles across the
four symbols. Raw responses, URLs, retrieval times and CSV hashes are retained
in `source_manifest.json`. See [real experiments](real_experiments.md) for source
counts, test dates and limitations. Older model-upgrade outputs remain historical
diagnostics and do not supply source evidence for this run.

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
`2025-03-07T18:30:00+08:00`. It means the observed or explicitly assumed time
after which the story may enter a decision. Store `published_at`, `fetched_at`, URL,
provider ID and failure reason when available; retain them in the source audit.
If only a date is known, do not invent a timestamp. A conservative documented
availability policy must first be established from the provider evidence.
The included historical listings omit precise display times. Their policy
waits until the whole public-notice date is over (next calendar day 00:00
Asia/Shanghai), also respecting any later precise production time.
`availability_inferred` is true. This assumption is conservative with respect
to the recorded date; it cannot verify point-in-time completeness of historical
listings. First eligible opening is strictly later than that bound.

Program-generated price summaries remain useful ingestion diagnostics but
are excluded from the external-news increment experiment. The Full gate rejects
sources marked generated, synthetic or fixture. It never fills missing news
with synthetic text. Revisions of one provider ID retain the later timestamp.
Distinct IDs with the same normalized text survive chronological selection:
peer fitting deduplicates within the cutoff, target scoring counts the first
eligible occurrence and excludes texts fitted on peers. A future repeat cannot
erase an earlier training story. Semantic near duplicates still
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
