# GPT Diagnostic Bundle for 002475

- Generated at: `2026-05-17T22:26:33`
- Purpose: diagnose sector-only and marketwide peer NLP / market-impact DQN experiment outputs.
- Official logic: default sector sentiment + sector impact; optional marketwide sentiment + marketwide impact add-on; buy-and-hold is benchmark only.

## Official Market-Impact Ablation Metrics

File: `outputs/stocks/002475/results/market_impact_ablation_metrics.csv`; rows=6, columns=11

| experiment                        | official_experiment                     |   seed_count |     final_equity |   cumulative_return |   annualized_return |   annualized_volatility |   sharpe_ratio |   max_drawdown |   number_of_trades |    win_rate |
|:----------------------------------|:----------------------------------------|-------------:|-----------------:|--------------------:|--------------------:|------------------------:|---------------:|---------------:|-------------------:|------------:|
| buy_and_hold                      | sector_sentiment_impact_plus_marketwide |            1 |      1.03268e+06 |           0.0326757 |           0.0692567 |               0.44186   |       0.365524 |      0.286683  |                1   | nan         |
| dqn_without_nlp                   | sector_sentiment_impact_plus_marketwide |            5 | 952814           |          -0.0471861 |          -0.0633574 |               0.149527  |      -0.68232  |      0.104051  |                1.8 |   0.127273  |
| dqn_with_sector_sentiment_nlp     | sector_sentiment_impact_plus_marketwide |            5 | 905807           |          -0.0941932 |          -0.117735  |               0.139699  |      -0.795893 |      0.09929   |                3.6 |   0.0876033 |
| dqn_with_sector_impact_nlp        | sector_sentiment_impact_plus_marketwide |            5 | 928365           |          -0.0716353 |          -0.0924603 |               0.122156  |      -0.586484 |      0.092435  |                1.6 |   0.0661157 |
| dqn_with_marketwide_sentiment_nlp | sector_sentiment_impact_plus_marketwide |            5 | 985118           |          -0.0148823 |           0.0295965 |               0.265663  |       0.319083 |      0.151844  |                3.8 |   0.204959  |
| dqn_with_marketwide_impact_nlp    | sector_sentiment_impact_plus_marketwide |            5 | 960376           |          -0.0396236 |          -0.0567834 |               0.0836296 |      -0.65087  |      0.0561611 |                1.8 |   0.0512397 |

## Market-Impact Effect Summary

File: `outputs/stocks/002475/results/market_impact_effect_summary.csv`; rows=1, columns=32

|   target_symbol | target_company_name   | target_sector   | official_experiment                     | baseline_peer_sentiment_experiment   | best_strategy   | best_nlp_type   |   sector_sentiment_effect |   marketwide_sentiment_effect |   sector_impact_effect |   marketwide_impact_effect |   sector_impact_vs_sector_sentiment |   marketwide_impact_vs_marketwide_sentiment |   sector_sentiment_sharpe_effect |   marketwide_sentiment_sharpe_effect |   sector_impact_sharpe_effect |   marketwide_impact_sharpe_effect |   target_sentiment_coverage |   sector_sentiment_training_news |   marketwide_sentiment_training_news |   sector_impact_labeled_news |   marketwide_impact_labeled_news | sector_sentiment_label   | marketwide_sentiment_label   | sector_impact_label   | marketwide_impact_label   | sentiment_corpus_status        | impact_corpus_status           | peer_corpus_scope      | marketwide_enabled   | reliability_status     |   reason_if_not_reliable |
|----------------:|:----------------------|:----------------|:----------------------------------------|:-------------------------------------|:----------------|:----------------|--------------------------:|------------------------------:|-----------------------:|---------------------------:|------------------------------------:|--------------------------------------------:|---------------------------------:|-------------------------------------:|------------------------------:|----------------------------------:|----------------------------:|---------------------------------:|-------------------------------------:|-----------------------------:|---------------------------------:|:-------------------------|:-----------------------------|:----------------------|:--------------------------|:-------------------------------|:-------------------------------|:-----------------------|:---------------------|:-----------------------|-------------------------:|
|            2475 | 立讯精密              | electronics     | sector_sentiment_impact_plus_marketwide | peer_sector_nlp_transfer             | buy_and_hold    | none            |                  -47007.1 |                       32303.8 |               -24449.2 |                    7562.56 |                             22557.9 |                                    -24741.2 |                        -0.113573 |                               1.0014 |                     0.0958361 |                         0.0314502 |                    0.996721 |                            11909 |                                23821 |                        10709 |                            21421 | Inconclusive             | Inconclusive                 | Inconclusive          | Inconclusive              | sector=READY; marketwide=READY | sector=READY; marketwide=READY | sector_plus_marketwide | True                 | READY_FOR_PRESENTATION |                      nan |

## Market-Impact Reliability Check

File: `outputs/stocks/002475/reports/market_impact_reliability_check.csv`; rows=10, columns=5

|   target_symbol | check                                        | passed   | evidence                                                                  | final_status           |
|----------------:|:---------------------------------------------|:---------|:--------------------------------------------------------------------------|:-----------------------|
|            2475 | target_excluded_from_peer_training           | True     | Corpus builders exclude target_symbol from sector and marketwide corpora. | READY_FOR_PRESENTATION |
|            2475 | target_high_density_window_length            | True     | test_rows=122                                                             | READY_FOR_PRESENTATION |
|            2475 | target_news_coverage                         | True     | coverage=99.7%                                                            | READY_FOR_PRESENTATION |
|            2475 | sector_sentiment_training_news_threshold     | True     | sector_sentiment_training_news=11909                                      | READY_FOR_PRESENTATION |
|            2475 | sector_impact_training_news_threshold        | True     | sector_impact_labeled_news=10709                                          | READY_FOR_PRESENTATION |
|            2475 | sector_dqn_non_flat_portfolio_curves         | True     | Sector impact DQN curve should move.                                      | READY_FOR_PRESENTATION |
|            2475 | dqn_test_trades_positive                     | True     | At least one test trade.                                                  | READY_FOR_PRESENTATION |
|            2475 | marketwide_sentiment_training_news_threshold | True     | marketwide_sentiment_training_news=23821                                  | READY_FOR_PRESENTATION |
|            2475 | marketwide_impact_training_news_threshold    | True     | marketwide_impact_labeled_news=21421                                      | READY_FOR_PRESENTATION |
|            2475 | marketwide_dqn_non_flat_portfolio_curves     | True     | Marketwide impact DQN curve should move.                                  | READY_FOR_PRESENTATION |

## Peer Corpus Summary for Target

File: `reports/tables/peer_nlp_corpus_summary.csv`; rows=2, columns=11

|   target_symbol | target_company_name   | corpus_type     | corpus_status   |   number_of_peer_stocks |   peer_sector_count | peer_sectors                                    | corpus_scope           | marketwide_distinct_from_sector   | included_symbols                                                                    |   reason_if_not_ready |
|----------------:|:----------------------|:----------------|:----------------|------------------------:|--------------------:|:------------------------------------------------|:-----------------------|:----------------------------------|:------------------------------------------------------------------------------------|----------------------:|
|            2475 | 立讯精密              | sector_peer     | READY           |                       6 |                   1 | electronics                                     | sector_only            | False                             | 002241,002456,300136,300433,601138,601231                                           |                   nan |
|            2475 | 立讯精密              | marketwide_peer | READY           |                      12 |                   4 | automobile,consumer_staples,electronics,finance | sector_plus_marketwide | True                              | 000568,000625,000858,002241,002456,002594,300136,300433,600030,600036,601138,601231 |                   nan |

## Information Density Split

File: `outputs/stocks/002475/reports/information_density_split.csv`; rows=1, columns=18

|   symbol |   total_news_count |   total_trading_days | density_cutoff_date   | low_density_start_date   | low_density_end_date   | high_density_start_date   | high_density_end_date   |   low_density_news_count |   high_density_news_count |   low_density_trading_days |   high_density_trading_days |   low_density_avg_news_per_day |   high_density_avg_news_per_day |   high_density_coverage_ratio | density_status   |   target_sentiment_coverage | peer_official_experiment   |
|---------:|-------------------:|---------------------:|:----------------------|:-------------------------|:-----------------------|:--------------------------|:------------------------|-------------------------:|--------------------------:|---------------------------:|----------------------------:|-------------------------------:|--------------------------------:|------------------------------:|:-----------------|----------------------------:|:---------------------------|
|     2475 |               2294 |                  562 | 2025-01-23            | 2024-01-02               | 2025-01-22             | 2025-01-23                | 2026-04-30              |                      458 |                      1836 |                        257 |                         305 |                         1.7821 |                         6.01967 |                             1 | OK               |                    0.996721 | peer_sector_nlp_transfer   |

## Market-Impact State Diagnostics

File: `outputs/stocks/002475/reports/market_impact_group_state_diagnostics.csv`; rows=80, columns=13

| experiment                    | period   | state_column     | source_signal_column   |   rows |   non_missing_count |   missing_count |   nonzero_count |       mean |       std |       min |       max | is_unified_nlp_signal   |
|:------------------------------|:---------|:-----------------|:-----------------------|-------:|--------------------:|----------------:|----------------:|-----------:|----------:|----------:|----------:|:------------------------|
| dqn_without_nlp               | train    | price            | nan                    |    181 |                 181 |               0 |             181 | 40.4671    |  9.73533  | 28.32     | 70.04     | False                   |
| dqn_without_nlp               | train    | MA50             | nan                    |    181 |                 181 |               0 |             181 | 38.0744    |  4.90606  | 31.1138   | 52.3826   | False                   |
| dqn_without_nlp               | train    | MA200            | nan                    |    181 |                 181 |               0 |             181 | 38.2608    |  0.800798 | 36.8355   | 40.365    | False                   |
| dqn_without_nlp               | train    | RSI              | nan                    |    181 |                 181 |               0 |             181 | 55.8387    | 18.8681   | 10.7492   | 90.7199   | False                   |
| dqn_without_nlp               | train    | MACD             | nan                    |    181 |                 181 |               0 |             181 |  0.655073  |  1.90497  | -3.05205  |  6.26546  | False                   |
| dqn_without_nlp               | train    | position         | nan                    |    181 |                 181 |               0 |               0 |  0         |  0        |  0        |  0        | False                   |
| dqn_without_nlp               | train    | cash             | nan                    |    181 |                 181 |               0 |             181 |  1e+06     |  0        |  1e+06    |  1e+06    | False                   |
| dqn_without_nlp               | train    | nlp_signal_score | constant_zero_control  |    181 |                 181 |               0 |               0 |  0         |  0        |  0        |  0        | True                    |
| dqn_without_nlp               | test     | price            | nan                    |    122 |                 122 |               0 |             122 | 55.6537    |  5.50928  | 46.28     | 71.97     | False                   |
| dqn_without_nlp               | test     | MA50             | nan                    |    122 |                 122 |               0 |             122 | 55.7455    |  2.99547  | 50.4808   | 59.9932   | False                   |
| dqn_without_nlp               | test     | MA200            | nan                    |    122 |                 122 |               0 |             122 | 45.8755    |  3.18291  | 40.4956   | 52.1798   | False                   |
| dqn_without_nlp               | test     | RSI              | nan                    |    122 |                 122 |               0 |             122 | 47.2582    | 18.0399   |  9.35601  | 89.6169   | False                   |
| dqn_without_nlp               | test     | MACD             | nan                    |    122 |                 122 |               0 |             122 | -0.0127229 |  1.59374  | -1.97063  |  5.01241  | False                   |
| dqn_without_nlp               | test     | position         | nan                    |    122 |                 122 |               0 |               0 |  0         |  0        |  0        |  0        | False                   |
| dqn_without_nlp               | test     | cash             | nan                    |    122 |                 122 |               0 |             122 |  1e+06     |  0        |  1e+06    |  1e+06    | False                   |
| dqn_without_nlp               | test     | nlp_signal_score | constant_zero_control  |    122 |                 122 |               0 |               0 |  0         |  0        |  0        |  0        | True                    |
| dqn_with_sector_sentiment_nlp | train    | price            | nan                    |    181 |                 181 |               0 |             181 | 40.4671    |  9.73533  | 28.32     | 70.04     | False                   |
| dqn_with_sector_sentiment_nlp | train    | MA50             | nan                    |    181 |                 181 |               0 |             181 | 38.0744    |  4.90606  | 31.1138   | 52.3826   | False                   |
| dqn_with_sector_sentiment_nlp | train    | MA200            | nan                    |    181 |                 181 |               0 |             181 | 38.2608    |  0.800798 | 36.8355   | 40.365    | False                   |
| dqn_with_sector_sentiment_nlp | train    | RSI              | nan                    |    181 |                 181 |               0 |             181 | 55.8387    | 18.8681   | 10.7492   | 90.7199   | False                   |
| dqn_with_sector_sentiment_nlp | train    | MACD             | nan                    |    181 |                 181 |               0 |             181 |  0.655073  |  1.90497  | -3.05205  |  6.26546  | False                   |
| dqn_with_sector_sentiment_nlp | train    | position         | nan                    |    181 |                 181 |               0 |               0 |  0         |  0        |  0        |  0        | False                   |
| dqn_with_sector_sentiment_nlp | train    | cash             | nan                    |    181 |                 181 |               0 |             181 |  1e+06     |  0        |  1e+06    |  1e+06    | False                   |
| dqn_with_sector_sentiment_nlp | train    | nlp_signal_score | sector_sentiment_score |    181 |                 181 |               0 |             130 | -0.0256716 |  0.827593 | -1        |  1        | True                    |
| dqn_with_sector_sentiment_nlp | test     | price            | nan                    |    122 |                 122 |               0 |             122 | 55.6537    |  5.50928  | 46.28     | 71.97     | False                   |
| dqn_with_sector_sentiment_nlp | test     | MA50             | nan                    |    122 |                 122 |               0 |             122 | 55.7455    |  2.99547  | 50.4808   | 59.9932   | False                   |
| dqn_with_sector_sentiment_nlp | test     | MA200            | nan                    |    122 |                 122 |               0 |             122 | 45.8755    |  3.18291  | 40.4956   | 52.1798   | False                   |
| dqn_with_sector_sentiment_nlp | test     | RSI              | nan                    |    122 |                 122 |               0 |             122 | 47.2582    | 18.0399   |  9.35601  | 89.6169   | False                   |
| dqn_with_sector_sentiment_nlp | test     | MACD             | nan                    |    122 |                 122 |               0 |             122 | -0.0127229 |  1.59374  | -1.97063  |  5.01241  | False                   |
| dqn_with_sector_sentiment_nlp | test     | position         | nan                    |    122 |                 122 |               0 |               0 |  0         |  0        |  0        |  0        | False                   |
| dqn_with_sector_sentiment_nlp | test     | cash             | nan                    |    122 |                 122 |               0 |             122 |  1e+06     |  0        |  1e+06    |  1e+06    | False                   |
| dqn_with_sector_sentiment_nlp | test     | nlp_signal_score | sector_sentiment_score |    122 |                 122 |               0 |              99 |  0.0382108 |  0.52089  | -1        |  1        | True                    |
| dqn_with_sector_impact_nlp    | train    | price            | nan                    |    181 |                 181 |               0 |             181 | 40.4671    |  9.73533  | 28.32     | 70.04     | False                   |
| dqn_with_sector_impact_nlp    | train    | MA50             | nan                    |    181 |                 181 |               0 |             181 | 38.0744    |  4.90606  | 31.1138   | 52.3826   | False                   |
| dqn_with_sector_impact_nlp    | train    | MA200            | nan                    |    181 |                 181 |               0 |             181 | 38.2608    |  0.800798 | 36.8355   | 40.365    | False                   |
| dqn_with_sector_impact_nlp    | train    | RSI              | nan                    |    181 |                 181 |               0 |             181 | 55.8387    | 18.8681   | 10.7492   | 90.7199   | False                   |
| dqn_with_sector_impact_nlp    | train    | MACD             | nan                    |    181 |                 181 |               0 |             181 |  0.655073  |  1.90497  | -3.05205  |  6.26546  | False                   |
| dqn_with_sector_impact_nlp    | train    | position         | nan                    |    181 |                 181 |               0 |               0 |  0         |  0        |  0        |  0        | False                   |
| dqn_with_sector_impact_nlp    | train    | cash             | nan                    |    181 |                 181 |               0 |             181 |  1e+06     |  0        |  1e+06    |  1e+06    | False                   |
| dqn_with_sector_impact_nlp    | train    | nlp_signal_score | sector_impact_score    |    181 |                 181 |               0 |             181 | -0.0547224 |  0.193697 | -0.521071 |  0.582021 | True                    |

## Derived Diagnostics

### Action Distribution
| experiment                        |   Buy |   Hold |   Sell |
|:----------------------------------|------:|-------:|-------:|
| dqn_with_marketwide_impact_nlp    |     5 |    596 |      4 |
| dqn_with_marketwide_sentiment_nlp |    11 |    586 |      8 |
| dqn_with_sector_impact_nlp        |     4 |    597 |      4 |
| dqn_with_sector_sentiment_nlp     |    10 |    587 |      8 |
| dqn_without_nlp                   |     6 |    596 |      3 |

### Portfolio Curve Non-Flat Check
| experiment                        |   unique_portfolio_values |
|:----------------------------------|--------------------------:|
| buy_and_hold                      |                       119 |
| dqn_with_marketwide_impact_nlp    |                        76 |
| dqn_with_marketwide_sentiment_nlp |                       275 |
| dqn_with_sector_impact_nlp        |                        98 |
| dqn_with_sector_sentiment_nlp     |                       104 |
| dqn_without_nlp                   |                       187 |

### Sector vs Marketwide Signal Difference
| comparison                               | exactly_equal   |   correlation |
|:-----------------------------------------|:----------------|--------------:|
| sector_sentiment vs marketwide_sentiment | False           |      0.974563 |
| sector_impact vs marketwide_impact       | False           |      0.814756 |

## Suggested Questions for GPT

1. Are the five DQN groups correctly configured as no-NLP, sector sentiment, sector impact, marketwide sentiment, marketwide impact?
2. Does marketwide appear genuinely cross-sector, or is it still too correlated with sector?
3. Is the DQN policy overly conservative based on action distribution and curve behavior?
4. Are the eight official metrics enough to compare groups, or should additional diagnostics be reviewed?
5. Does marketwide sentiment improvement look meaningful or likely dominated by noise/seed variance?
