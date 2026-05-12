# Result Overview Summary

## Coverage-Controlled Project Logic

Long historical OHLCV data is used to learn market-based trading behavior. NLP value is evaluated mainly in each stock's recent high-information-density window, defined by the most recent 80% of news observations.

No-news days are not treated as neutral sentiment. The RL state keeps explicit `news_available` and `sentiment_missing_flag` columns, and all NLP state features are lagged before trading decisions.

## Current Density Summary

- Stocks analyzed: `3`
- MAIN_EXPERIMENT stocks: `3`
- Cross-stock high-density common window: `2026-03-20 to 2026-04-22`
- Cross-stock comparability: `NOT_RELIABLE`

## Interpretation

NLP does not universally improve DQN performance. The high-density design makes this conclusion more honest by asking whether sentiment helps when textual information is actually available.