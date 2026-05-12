# Fintech GP Stock Text Scraper

This archived note describes the stock data and text-event scraping program.

Important scope statement: this customized fintech research function only supports China A-share stocks. Use 6-digit A-share codes such as `002475`, `600519`, or `300750`. It is not designed for US tickers such as `AAPL`.

## Folder Structure

- `program/`: crawler programs.
- `outputs/stocks/<symbol>/data/`: generated integrated CSV files and diagnostic JSON reports.
- `docs/statement/`: usage notes and experiment explanation.

## Main Programs

### `program/finance_text_scraper.py`

Single-source scraper. It can collect:

- Daily stock price data.
- Basic stock metadata where available.
- Related text events, such as Yahoo Finance news or Eastmoney announcements.
- Extracted keywords from event text.

Supported source options:

- `yahoo`: Yahoo Finance, more international and authoritative for US stocks, but currently rate-limited on this network.
- `eastmoney`: Eastmoney A-share K-line and announcement source.
- `tencent`: Tencent A-share K-line source, with Eastmoney news search and announcements for text events.
- `auto`: Try Yahoo first, then fallback for China A-shares.

Example:

```bash
cd /Users/luxinyu/Desktop/Fintech/fintechgp
python3 program/finance_text_scraper.py 002475 -o outputs/stocks/002475/data/luxshare_yahoo_or_fallback.csv --source auto --news-count 50 --retries 6 --pause 1.5
```

### `program/run_scraper.py`

Recommended low-level runner for A-share fintech research. The main platform now calls it through `src/data_ingestion/ingestion.py`, which also handles master CSV reuse and merge logic. The runner can write diagnostic JSON files when scraping is required.

- Source attempted.
- Full command.
- Return code.
- Standard output.
- Standard error.
- Inferred failure reason.
- Successful output file path.

Command example:

```bash
cd /Users/luxinyu/Desktop/Fintech/fintechgp
python3 program/run_scraper.py 600519 --start-date 2026-04-15 --end-date 2026-04-22 --sources tencent --require-news
```

Interactive mode:

```bash
cd /Users/luxinyu/Desktop/Fintech/fintechgp
python3 program/run_scraper.py
```

Interactive mode asks only for:

- A-share stock code, such as `002475`, `600519`, or `300750`.
- Start date in `YYYY-MM-DD` format.
- End date in `YYYY-MM-DD` format.

Everything else is automated:

- Daily OHLCV source: Tencent A-share K-line.
- Text/news/event source: Eastmoney news search and company announcements.
- Text cap: high default search cap instead of the old 50-item prompt.
- Data quality: every trading day with stock data will have text fields. External Eastmoney news
  and announcements are used first; dates still missing external text receive a transparent
  program-generated OHLCV text summary labeled `程序生成行情文本摘要`.
- Output naming: the platform standardizes integrated files as `outputs/stocks/<stock_code>/data/<stock_code>_finance_text_<start_date>_<end_date>.csv` and maintains `outputs/stocks/<stock_code>/data/<stock_code>_finance_text_master.csv`.

Fintech research command example, requiring both daily stock data and text events:

```bash
python3 program/run_scraper.py 600519 --start-date 2026-04-15 --end-date 2026-04-22 --sources tencent --require-news
```

For A-shares, `--sources tencent` is currently the most stable choice for daily OHLCV data in this environment. Text events are collected from Eastmoney news and announcements first. If a trading day still has no matched external text event, the script adds a clearly labeled daily OHLCV text summary so the fintech dataset remains complete by date.

If Yahoo succeeds, the final CSV will be:

```text
outputs/stocks/002475/data/002475_finance_text_<date-range>.csv
```

If Yahoo fails, the program will print the reason and continue to Eastmoney/Tencent.

## Why Yahoo May Be Blocked

Yahoo Finance often rate-limits or blocks requests based on:

- Shared VPN exit IPs.
- Too many requests from the same IP.
- Data-center or proxy IP reputation.
- Missing browser session behavior.
- Regional or anti-bot edge rules.

In our tests, Yahoo returned `Too Many Requests` for `002475.SZ`, `000001.SS`, and `AAPL`, so the issue was network-level blocking rather than a wrong ticker.

## Existing Result

Current platform data is stored as master timelines, for example:

```text
outputs/stocks/002475/data/002475_finance_text_master.csv
```

It was generated using Tencent daily K-line data plus Eastmoney stock news and announcement text events.

Important note: stock prices are available for every trading day, but stock-specific text events only exist on days when a news article or announcement is found. Empty event columns mean that no matching text item was found for that date, not that the price row failed.
