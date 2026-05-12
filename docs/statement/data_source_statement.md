# Data Source Statement

## Yahoo Finance

Yahoo Finance is attempted first by `program/run_scraper.py` because it is widely used for financial data and text/news collection. During testing, Yahoo blocked the current network with `Too Many Requests`, likely because the VPN exit IP was rate-limited.

Yahoo ticker examples:

- `AAPL`
- `TSLA`
- `002475.SZ`
- `600519.SS`

## China A-Share Fallbacks

For China A-shares, the scripts support practical fallback sources:

- Tencent Securities for daily adjusted K-line data.
- Eastmoney for company announcements and event titles.
- Program-generated daily OHLCV text summaries for trading dates where no external
  Eastmoney text event is available. These rows are labeled `程序生成行情文本摘要`.

For `002475`, the script automatically normalizes:

```text
002475 -> 002475.SZ
```

## Diagnostic Reports

Every direct run of `program/run_scraper.py` writes a JSON diagnostic report into `outputs/stocks/<symbol>/data/`.

The report is useful when comparing VPN vs non-VPN behavior because it records each source's exact error reason.
