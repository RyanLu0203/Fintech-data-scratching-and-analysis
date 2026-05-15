# SQLite Storage Demo

- Selected stock: `000002`
- Demo database: `reports/sqlite_demo.db`
- Purpose: demonstrate schema creation, write, read-back, row counts, date ranges, and sample query output.

## Sample Query Result

| table_name | row_count | date_range | sample |
| --- | --- | --- | --- |
| news_table | 479 | 2024-12-30 to 2026-04-22 | [{"news_id":"000002_0","ticker":"000002","date":"2024-12-30","title":"","content":"","source":"integrated_csv"}] |
| market_table | 479 | 2024-12-30 to 2026-04-22 | [{"ticker":"000002.SZ","date":"2024-12-30","open":7.5,"high":7.51,"low":7.33,"close":7.36,"volume":1338891.0}] |
| sentiment_table | 351 | 2024-12-30 to 2026-04-22 | [{"ticker":"000002.SZ","date":"2024-12-30","method":"ensemble","sentiment_score":-1.0}] |
| trading_log_table | 3150 | 2024-12-30 to 2026-04-21 | [{"id":1,"episode":0,"date":"2024-12-30","action":"Hold","reward":0.0,"position":0.0,"cash":100000.0,"portfolio_value":100000.0,"experiment":"dqn_without_nlp"}] |

SQLite remains optional; the main pipeline can continue using CSV artifacts.
