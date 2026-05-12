# Program Structure

This directory contains the command-line scraping tools used by the project.

```text
program/
  finance_text_scraper.py        Legacy-compatible scraper entry point
  run_scraper.py                 Legacy-compatible diagnostic runner entry point
  finance_scraper/
    scraper.py                   Price, text-event, and CSV generation logic
    runner.py                    Source retry orchestration and diagnostics
```

Use the existing commands as before:

```bash
python3 program/run_scraper.py 002475 --start-date 2024-01-01 --end-date 2026-04-22
python3 program/finance_text_scraper.py 002475 --source tencent -o outputs/stocks/002475/data/sample.csv
```

For future maintenance, add new source-specific logic inside
`program/finance_scraper/` rather than expanding the top-level wrapper scripts.
