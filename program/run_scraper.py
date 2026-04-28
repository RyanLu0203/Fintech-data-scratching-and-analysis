#!/usr/bin/env python3
"""
Run the A-share finance text scraper with diagnostics.

This runner is customized for the fintech research workflow: collect daily
A-share OHLCV data together with daily text/news/event data.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.paths import stock_data_dir, stock_reports_dir  # noqa: E402

PROGRAM_DIR = PROJECT_ROOT / "program"
SCRAPER = PROGRAM_DIR / "finance_text_scraper.py"


def is_china_a_share_symbol(symbol: str) -> bool:
    return bool(re.fullmatch(r"\d{6}(\.(SZ|SS))?", symbol.strip().upper()))


def unsupported_source_reason(symbol: str, source: str) -> str:
    normalized = symbol.strip().upper()
    if not is_china_a_share_symbol(normalized):
        return (
            "This fintech data function only supports China A-share 6-digit symbols "
            f"such as 002475, 600519, or 300750. Got '{symbol}'."
        )
    if source in {"eastmoney", "tencent"} and not is_china_a_share_symbol(normalized):
        hint = " Did you mean AAPL?" if normalized == "APPL" else ""
        return (
            f"Source '{source}' only supports China A-share 6-digit symbols "
            f"such as 600519 or 300750, but got '{symbol}'. "
            f"For US stocks, use source 'yahoo' and the correct ticker.{hint}"
        )
    return ""


def validate_args(args: argparse.Namespace) -> None:
    if args.symbol and not is_china_a_share_symbol(args.symbol):
        raise SystemExit(
            "This function only supports China A-share 6-digit stock codes, "
            f"but got '{args.symbol}'. Examples: 002475, 600519, 300750."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect China A-share daily OHLCV data and Eastmoney text/news "
            "events, then write detailed diagnostic reports."
        )
    )
    parser.add_argument("symbol", nargs="?", help="China A-share code, e.g. 002475, 600519, 300750")
    parser.add_argument("--years", type=float, default=2.0, help="Years of daily data. Default: 2")
    parser.add_argument("--start-date", default=None, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="End date in YYYY-MM-DD format")
    parser.add_argument("--news-count", type=int, default=5000, help="Text event search cap. Default: 5000")
    parser.add_argument(
        "--company-name",
        default=None,
        help="Optional company name used as an extra news search keyword, e.g. 贵州茅台.",
    )
    parser.add_argument(
        "--require-news",
        action="store_true",
        help="Treat the run as failed if no news/event item is collected.",
    )
    parser.add_argument("--retries", type=int, default=3, help="Retries per request. Default: 3")
    parser.add_argument("--pause", type=float, default=0.8, help="Pause between requests. Default: 0.8")
    parser.add_argument(
        "--sources",
        default="tencent",
        help="Comma-separated source order. Default: tencent. This runner only supports A-share sources.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Final CSV path. Default: fintechgp/outputs/stocks/<symbol>/data/<symbol>_<range>.csv",
    )
    parser.add_argument(
        "--keep-failed-outputs",
        action="store_true",
        help="Keep source-specific CSV paths even when a source fails before writing.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for symbol, date range, and options interactively.",
    )
    parser.add_argument(
        "--run-analysis-after",
        action="store_true",
        help="Run all post-scraping analysis tasks automatically after a successful scrape.",
    )
    args = parser.parse_args()
    if args.interactive or not args.symbol:
        args.interactive = True
        fill_interactive_args(args)
    validate_args(args)
    return args


def fill_interactive_args(args: argparse.Namespace) -> None:
    print("A-share Fintech Data Scraper")
    print("This function only supports China A-share stocks.")
    print("It automatically collects daily OHLCV data and daily text/news/event data.")
    while not args.symbol:
        args.symbol = input("A-share stock code (e.g. 002475, 600519, 300750): ").strip()

    while not args.start_date:
        args.start_date = input("Start date YYYY-MM-DD: ").strip()

    while not args.end_date:
        args.end_date = input("End date YYYY-MM-DD: ").strip()

    args.sources = "tencent"
    args.news_count = 5000
    args.require_news = True
    args.retries = 3
    args.pause = 0.8


def safe_symbol_for_path(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "_")


def run_source(args: argparse.Namespace, source: str, output_path: Path) -> Dict[str, object]:
    unsupported_reason = unsupported_source_reason(args.symbol, source)
    if unsupported_reason:
        return {
            "source": source,
            "command": [],
            "started_at": dt.datetime.now().isoformat(timespec="seconds"),
            "ended_at": dt.datetime.now().isoformat(timespec="seconds"),
            "returncode": 2,
            "stdout": "",
            "stderr": unsupported_reason,
            "output_csv": str(output_path),
            "output_exists": False,
            "error_reason": unsupported_reason,
        }

    cmd = [
        sys.executable,
        str(SCRAPER),
        args.symbol,
        "-o",
        str(output_path),
        "--source",
        source,
        "--years",
        str(args.years),
        "--news-count",
        str(args.news_count),
        "--retries",
        str(args.retries),
        "--pause",
        str(args.pause),
    ]
    if args.start_date:
        cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        cmd.extend(["--end-date", args.end_date])
    if args.company_name:
        cmd.extend(["--company-name", args.company_name])
    if args.require_news:
        cmd.append("--require-news")
    started = dt.datetime.now().isoformat(timespec="seconds")
    completed = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    ended = dt.datetime.now().isoformat(timespec="seconds")
    return {
        "source": source,
        "command": cmd,
        "started_at": started,
        "ended_at": ended,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "output_csv": str(output_path),
        "output_exists": output_path.exists(),
        "error_reason": infer_error_reason(completed.stdout, completed.stderr, completed.returncode),
    }


def infer_error_reason(stdout: str, stderr: str, returncode: int) -> str:
    text = f"{stdout}\n{stderr}"
    if returncode == 0:
        return ""
    patterns = [
        ("Too Many Requests", "Source rate-limited the current network or VPN IP."),
        ("rate-limiting or blocking", "Source is rate-limiting or blocking the current network."),
        ("sad-panda", "Yahoo returned its anti-abuse error page."),
        ("cookie/crumb", "Yahoo blocked the request even after cookie/crumb session fallback."),
        ("Cannot infer Eastmoney market", "Eastmoney only supports China A-share 6-digit symbols."),
        ("Cannot infer Tencent market", "Tencent only supports China A-share 6-digit symbols."),
        ("All Eastmoney kline endpoints failed", "All Eastmoney historical K-line endpoint variants failed."),
        ("No rows to write", "No daily rows were returned for the requested date range."),
        (
            "No text/news/event item was collected for these trading dates",
            "Some trading dates have stock data but no matched text/news/event item.",
        ),
        ("No news/event items were collected", "No text/news/event items were collected."),
        ("JSONP", "Source rejected both JSON and JSONP request styles."),
        ("Remote end closed connection", "Remote server closed the connection, often temporary anti-bot behavior."),
        ("curl exited 52", "Remote server returned an empty response."),
        ("curl exited 28", "Remote server timed out."),
        ("curl exited 6", "DNS/network access failed."),
        ("Empty reply from server", "Remote server returned an empty response."),
        ("timed out", "Remote server timed out or the network is too slow."),
        ("HTTP 400", "Source rejected the request parameters or blocked the request."),
        ("HTTP 403", "Source denied access from this network or request profile."),
        ("HTTP 502", "Source gateway error."),
        ("nodename nor servname", "DNS/network access failed."),
    ]
    for needle, reason in patterns:
        if needle in text:
            return reason
    return "Unknown failure. See stdout/stderr in the diagnostic report."


def write_report(path: Path, report: Dict[str, object]) -> None:
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def run_post_scrape_analysis(csv_path: Path) -> int:
    symbol = safe_symbol_for_path(csv_path.name.split("_finance_text")[0])
    report_dir = stock_reports_dir(symbol)
    cmd = [
        sys.executable,
        "-m",
        "src.experiments.run_all_analysis",
        "--input-csv",
        str(csv_path),
        "--output-dir",
        str(report_dir),
    ]
    completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=False)
    return completed.returncode


def should_run_analysis(args: argparse.Namespace) -> bool:
    if args.run_analysis_after:
        return True
    if not args.interactive:
        return False
    answer = input("Scraping finished. Input 1 to run all analysis tasks, or press Enter to stop: ").strip()
    return answer == "1"


def main() -> int:
    args = parse_args()
    integrated_dir = stock_data_dir(args.symbol)
    report_dir = stock_reports_dir(args.symbol)
    integrated_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    sources: List[str] = [s.strip() for s in args.sources.split(",") if s.strip()]
    range_suffix = (
        f"{args.start_date or 'auto'}_{args.end_date or 'today'}"
        if args.start_date or args.end_date
        else f"{args.years:g}y"
    )
    final_output = (
        Path(args.output).resolve()
        if args.output
        else integrated_dir / f"{safe_symbol_for_path(args.symbol)}_{range_suffix}.csv"
    )
    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = integrated_dir / f"{safe_symbol_for_path(args.symbol)}_diagnostic_{run_id}.json"

    report: Dict[str, object] = {
        "symbol": args.symbol,
        "years": args.years,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "news_count": args.news_count,
        "company_name": args.company_name,
        "require_news": args.require_news,
        "sources": sources,
        "final_output": str(final_output),
        "attempts": [],
        "success": False,
    }

    for source in sources:
        temp_output = integrated_dir / f"{safe_symbol_for_path(args.symbol)}_{source}_{run_id}.csv"
        print(f"Trying source: {source}")
        attempt = run_source(args, source, temp_output)
        report["attempts"].append(attempt)
        write_report(report_path, report)

        if attempt["returncode"] == 0 and temp_output.exists():
            if final_output.exists():
                final_output.unlink()
            temp_output.replace(final_output)
            report["success"] = True
            report["successful_source"] = source
            report["final_output"] = str(final_output)
            write_report(report_path, report)
            print(f"Success with {source}: {final_output}")
            print(f"Diagnostic report: {report_path}")
            if should_run_analysis(args):
                print("Running all post-scraping analysis tasks...")
                analysis_code = run_post_scrape_analysis(final_output)
                if analysis_code != 0:
                    print("Analysis tasks failed. Check terminal output above.")
                    return analysis_code
                print(f"Analysis outputs saved in: {report_dir}")
            return 0

        print(f"Failed with {source}: {attempt['error_reason']}")
        if not args.keep_failed_outputs and temp_output.exists():
            temp_output.unlink()

    write_report(report_path, report)
    print("All sources failed.")
    print(f"Diagnostic report: {report_path}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
