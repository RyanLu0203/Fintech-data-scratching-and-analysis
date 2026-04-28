#!/usr/bin/env python3
"""
Collect two years of Yahoo Finance daily stock data plus related text events.

The script uses Yahoo's public chart/search endpoints and writes one CSV row per
trading day. News/events are matched to the trading date on which they were
published. Yahoo often limits how much historical news it returns, so the CSV
also records how many news items were actually found.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import http.client
import http.cookiejar
import html
import json
import os
import random
import re
import shutil
import sys
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple


YAHOO_CHART_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
]
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YAHOO_QUOTE_PAGE_URL = "https://finance.yahoo.com/quote/{symbol}"
EASTMONEY_KLINE_URLS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "http://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://61.push2his.eastmoney.com/api/qt/stock/kline/get",
    "http://61.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://27.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://28.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://29.push2his.eastmoney.com/api/qt/stock/kline/get",
]
EASTMONEY_UT_VALUES = [
    "fa5fd1943c7b386f172d6893dbfba10b",
    "7eea3edcaed734bea9cbfc24409ed989",
]
EASTMONEY_QUOTE_PAGE_URL = "https://quote.eastmoney.com/{market}{code}.html"
EASTMONEY_CENTER_URL = "https://quote.eastmoney.com/center/gridlist.html"
EASTMONEY_ANN_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"
EASTMONEY_SEARCH_URL = "https://search-api-web.eastmoney.com/search/jsonp"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
MARKET_NEWS_KEYWORDS = ["A股", "沪深", "大盘", "市场", "上证指数", "创业板"]
DAILY_CONTEXT_PUBLISHER = "程序生成行情文本摘要"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "company",
    "could",
    "day",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "market",
    "more",
    "new",
    "news",
    "of",
    "on",
    "or",
    "over",
    "says",
    "shares",
    "stock",
    "stocks",
    "than",
    "that",
    "the",
    "their",
    "this",
    "to",
    "today",
    "up",
    "us",
    "was",
    "with",
    "yahoo",
    "finance",
}

YAHOO_COOKIE_JAR = http.cookiejar.CookieJar()
YAHOO_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(YAHOO_COOKIE_JAR))
YAHOO_COOKIE_FILE = os.path.join(tempfile.gettempdir(), "fintechgp_yahoo_cookies.txt")
YAHOO_CRUMB: Optional[str] = None

EASTMONEY_COOKIE_JAR = http.cookiejar.CookieJar()
EASTMONEY_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(EASTMONEY_COOKIE_JAR))
EASTMONEY_COOKIE_FILE = os.path.join(tempfile.gettempdir(), "fintechgp_eastmoney_cookies.txt")
EASTMONEY_WARMED = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Yahoo Finance daily OHLCV data, basic quote metadata, "
            "daily news/event text, and extracted keywords into a CSV file."
        )
    )
    parser.add_argument("symbol", nargs="?", help="Stock ticker, e.g. AAPL, MSFT, TSLA")
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path. Default: yahoo_finance_<SYMBOL>_2y.csv",
    )
    parser.add_argument(
        "--years",
        type=float,
        default=2.0,
        help="How many years of daily price data to collect when no date range is given. Default: 2",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date in YYYY-MM-DD format. Overrides --years when used with --end-date.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to today when --start-date is provided.",
    )
    parser.add_argument(
        "--news-count",
        type=int,
        default=5000,
        help="Maximum text/news/event items to request. Default: 5000",
    )
    parser.add_argument(
        "--company-name",
        default=None,
        help="Optional company name used as an extra Eastmoney news search keyword, e.g. 贵州茅台.",
    )
    parser.add_argument(
        "--require-news",
        action="store_true",
        help="Fail the run if no news/event item is collected for the date range.",
    )
    parser.add_argument(
        "--region",
        default="US",
        help="Yahoo Finance region parameter. Default: US",
    )
    parser.add_argument(
        "--lang",
        default="en-US",
        help="Yahoo Finance language parameter. Default: en-US",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=0.4,
        help="Pause in seconds between Yahoo requests. Default: 0.4",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retries for each Yahoo request. Default: 3",
    )
    parser.add_argument(
        "--source",
        choices=["auto", "yahoo", "eastmoney", "tencent"],
        default="auto",
        help="Data source. auto tries Yahoo first, then Tencent/Eastmoney for China A-shares. Default: auto",
    )
    args = parser.parse_args()
    if not args.symbol:
        parser.error("symbol is required unless you use the interactive wrapper run_scraper.py")
    return args


def resolve_date_range(years: float, start_date: Optional[str], end_date: Optional[str]) -> Tuple[dt.date, dt.date]:
    end = parse_user_date(end_date) if end_date else dt.datetime.now(tz=dt.timezone.utc).date()
    if start_date:
        start = parse_user_date(start_date)
    else:
        start = end - dt.timedelta(days=int(years * 365.25) + 10)
    if start > end:
        raise ValueError(f"start date {start} is after end date {end}")
    return start, end


def parse_user_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Please use YYYY-MM-DD.") from exc


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if re.fullmatch(r"\d{6}", symbol):
        if symbol.startswith(("0", "2", "3")):
            return f"{symbol}.SZ"
        if symbol.startswith(("5", "6", "9")):
            return f"{symbol}.SS"
    return symbol


def is_china_a_share(symbol: str) -> bool:
    return bool(re.fullmatch(r"\d{6}(\.(SZ|SS))?", symbol.upper()))


def plain_stock_code(symbol: str) -> str:
    return symbol.upper().split(".")[0]


def eastmoney_secid(symbol: str) -> str:
    code = plain_stock_code(symbol)
    if code.startswith(("0", "2", "3")):
        return f"0.{code}"
    if code.startswith(("5", "6", "9")):
        return f"1.{code}"
    raise ValueError(f"Cannot infer Eastmoney market for symbol: {symbol}")


def tencent_symbol(symbol: str) -> str:
    code = plain_stock_code(symbol)
    if code.startswith(("0", "2", "3")):
        return f"sz{code}"
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    raise ValueError(f"Cannot infer Tencent market for symbol: {symbol}")


def build_url(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    if params:
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}{urllib.parse.urlencode(params)}"
    return url


def browser_headers(url: str, accept: str = "json", referer: Optional[str] = None) -> Dict[str, str]:
    host = urllib.parse.urlparse(url).netloc.lower()
    is_yahoo = "yahoo" in host
    is_eastmoney = "eastmoney" in host

    if accept == "html":
        accept_value = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    elif accept == "text":
        accept_value = "text/javascript,application/javascript,text/plain,*/*"
    else:
        accept_value = "application/json,text/plain,*/*"

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept_value,
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7" if is_eastmoney else "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "close",
        "Pragma": "no-cache",
    }
    if referer:
        headers["Referer"] = referer
    elif is_yahoo:
        headers["Referer"] = "https://finance.yahoo.com/"
    elif is_eastmoney:
        headers["Referer"] = "https://quote.eastmoney.com/"
    if is_eastmoney:
        headers["X-Requested-With"] = "XMLHttpRequest"
    return headers


def retry_pause(attempt: int) -> None:
    time.sleep(min(60.0, 1.2 * (2 ** (attempt - 1)) + random.uniform(0.1, 0.7)))


def request_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    headers: Optional[Dict[str, str]] = None,
    opener: Optional[Any] = None,
    curl_cookie_file: Optional[str] = None,
) -> Dict[str, Any]:
    url = build_url(url, params)
    request_headers = headers or browser_headers(url)
    req = urllib.request.Request(url, headers=request_headers)

    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            open_fn = opener.open if opener else urllib.request.urlopen
            with open_fn(req, timeout=30) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if "Too Many Requests" in body or "sad-panda" in body:
                    raise RuntimeError("Source returned an anti-abuse or rate-limit response.")
                try:
                    return json.loads(body)
                except json.JSONDecodeError as exc:
                    snippet = clean_text(body[:500])
                    last_error = RuntimeError(f"Invalid JSON response: {snippet}") if snippet else exc
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            if exc.code == 429 or "Too Many Requests" in body or "sad-panda" in body:
                last_error = RuntimeError(
                    "Source is rate-limiting or blocking this network right now. "
                    "Please wait and retry, "
                    "increase --retries, lower --news-count, use another network, "
                    "or let the source fallback run."
                )
                if attempt < retries:
                    retry_pause(attempt)
                    continue
                raise last_error from exc
            last_error = RuntimeError(f"HTTP {exc.code}: {clean_text(body)}")
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected, RuntimeError) as exc:
            last_error = exc
        if attempt < retries:
            retry_pause(attempt)

    try:
        return request_json_with_curl(url, headers=request_headers, cookie_file=curl_cookie_file)
    except Exception as curl_error:
        raise RuntimeError(f"Request failed after {retries} attempts: {url}\n{last_error}\ncurl fallback: {curl_error}")


def request_json_with_curl(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    cookie_file: Optional[str] = None,
) -> Dict[str, Any]:
    curl = shutil.which("curl")
    if not curl:
        raise RuntimeError("curl is not available")
    cmd = [
        curl,
        "-sS",
        "-L",
        "--compressed",
        "--connect-timeout",
        "12",
        "--max-time",
        "45",
        "--retry",
        "2",
        "--retry-delay",
        "1",
        "--retry-connrefused",
        "-A",
        USER_AGENT,
    ]
    if cookie_file:
        cmd.extend(["-b", cookie_file, "-c", cookie_file])
    for key, value in (headers or {}).items():
        if key.lower() == "user-agent":
            continue
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(url)
    completed = subprocess.run(
        cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=80,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or f"curl exited {completed.returncode}")
    body = completed.stdout.strip()
    if "Too Many Requests" in body or "sad-panda" in body:
        raise RuntimeError("Too Many Requests")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        snippet = clean_text(body[:500])
        raise RuntimeError(f"Invalid JSON response from curl: {snippet}") from exc


def request_text(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    headers: Optional[Dict[str, str]] = None,
    opener: Optional[Any] = None,
    curl_cookie_file: Optional[str] = None,
) -> str:
    url = build_url(url, params)
    request_headers = headers or browser_headers(url, accept="text")
    req = urllib.request.Request(url, headers=request_headers)
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            open_fn = opener.open if opener else urllib.request.urlopen
            with open_fn(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                retry_pause(attempt)
    try:
        return request_text_with_curl(url, headers=request_headers, cookie_file=curl_cookie_file)
    except Exception:
        raise RuntimeError(f"Text request failed after {retries} attempts: {url}\n{last_error}")


def request_text_with_curl(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    cookie_file: Optional[str] = None,
) -> str:
    curl = shutil.which("curl")
    if not curl:
        raise RuntimeError("curl is not available")
    cmd = [
        curl,
        "-sS",
        "-L",
        "--compressed",
        "--connect-timeout",
        "12",
        "--max-time",
        "45",
        "--retry",
        "2",
        "--retry-delay",
        "1",
        "--retry-connrefused",
        "-A",
        USER_AGENT,
    ]
    if cookie_file:
        cmd.extend(["-b", cookie_file, "-c", cookie_file])
    for key, value in (headers or {}).items():
        if key.lower() == "user-agent":
            continue
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(url)
    completed = subprocess.run(
        cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=80,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or f"curl exited {completed.returncode}")
    return completed.stdout


def yahoo_referer(symbol: str) -> str:
    return YAHOO_QUOTE_PAGE_URL.format(symbol=urllib.parse.quote(symbol))


def warm_yahoo_session(symbol: str, retries: int, force: bool = False) -> Optional[str]:
    global YAHOO_CRUMB
    if YAHOO_CRUMB and not force:
        return YAHOO_CRUMB

    errors: List[str] = []
    quote_url = yahoo_referer(symbol)
    try:
        request_text(
            quote_url,
            retries=max(1, min(retries, 2)),
            headers=browser_headers(quote_url, accept="html", referer="https://finance.yahoo.com/"),
            opener=YAHOO_OPENER,
            curl_cookie_file=YAHOO_COOKIE_FILE,
        )
    except Exception as exc:
        errors.append(f"quote page: {exc}")

    try:
        crumb = request_text(
            YAHOO_CRUMB_URL,
            retries=max(1, min(retries, 2)),
            headers=browser_headers(YAHOO_CRUMB_URL, accept="text", referer=quote_url),
            opener=YAHOO_OPENER,
            curl_cookie_file=YAHOO_COOKIE_FILE,
        ).strip()
        if crumb and len(crumb) < 200 and "<" not in crumb and " " not in crumb:
            YAHOO_CRUMB = crumb
            return YAHOO_CRUMB
        errors.append(f"crumb endpoint returned unexpected body: {clean_text(crumb[:120])}")
    except Exception as exc:
        errors.append(f"crumb endpoint: {exc}")

    if errors:
        raise RuntimeError("Yahoo cookie/crumb warm-up failed: " + " | ".join(errors))
    return None


def request_yahoo_json(
    url: str,
    params: Optional[Dict[str, Any]],
    retries: int,
    symbol: str,
) -> Dict[str, Any]:
    headers = browser_headers(url, referer=yahoo_referer(symbol))
    try:
        return request_json(url, params, retries=retries, headers=headers)
    except Exception as first_error:
        session_params = dict(params or {})
        session_error = ""
        try:
            crumb = warm_yahoo_session(symbol, retries, force=True)
            if crumb:
                session_params.setdefault("crumb", crumb)
        except Exception as exc:
            session_error = f"\nsession warm-up: {exc}"
        try:
            return request_json(
                url,
                session_params,
                retries=retries,
                headers=headers,
                opener=YAHOO_OPENER,
                curl_cookie_file=YAHOO_COOKIE_FILE,
            )
        except Exception as second_error:
            raise RuntimeError(
                "Yahoo request failed without and with cookie/crumb fallback:"
                f"\ninitial: {first_error}{session_error}\nsession: {second_error}"
            ) from second_error


def eastmoney_market_prefix(symbol: str) -> str:
    return "sz" if eastmoney_secid(symbol).startswith("0.") else "sh"


def eastmoney_referer(symbol: str) -> str:
    return EASTMONEY_QUOTE_PAGE_URL.format(market=eastmoney_market_prefix(symbol), code=plain_stock_code(symbol))


def warm_eastmoney_session(symbol: str, retries: int, force: bool = False) -> None:
    global EASTMONEY_WARMED
    if EASTMONEY_WARMED and not force:
        return

    errors: List[str] = []
    for url in (eastmoney_referer(symbol), EASTMONEY_CENTER_URL):
        try:
            request_text(
                url,
                retries=max(1, min(retries, 2)),
                headers=browser_headers(url, accept="html", referer="https://www.eastmoney.com/"),
                opener=EASTMONEY_OPENER,
                curl_cookie_file=EASTMONEY_COOKIE_FILE,
            )
            EASTMONEY_WARMED = True
            return
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("Eastmoney session warm-up failed: " + " | ".join(errors))


def request_eastmoney_json(
    url: str,
    params: Optional[Dict[str, Any]],
    retries: int,
    symbol: str,
) -> Dict[str, Any]:
    headers = browser_headers(url, referer=eastmoney_referer(symbol))
    try:
        return request_json(
            url,
            params,
            retries=retries,
            headers=headers,
            opener=EASTMONEY_OPENER,
            curl_cookie_file=EASTMONEY_COOKIE_FILE,
        )
    except Exception as first_error:
        warm_error = ""
        try:
            warm_eastmoney_session(symbol, retries)
        except Exception as exc:
            warm_error = f"\nsession warm-up: {exc}"
        try:
            return request_json(
                url,
                params,
                retries=retries,
                headers=headers,
                opener=EASTMONEY_OPENER,
                curl_cookie_file=EASTMONEY_COOKIE_FILE,
            )
        except Exception as second_error:
            raise RuntimeError(
                "Eastmoney JSON request failed after browser headers/cookie fallback:"
                f"\ninitial: {first_error}{warm_error}\nsession: {second_error}"
            ) from second_error


def request_eastmoney_jsonp(
    url: str,
    params: Dict[str, Any],
    retries: int,
    symbol: str,
) -> Dict[str, Any]:
    callback = f"jQuery{int(time.time() * 1000)}{random.randint(100, 999)}"
    jsonp_params = dict(params)
    jsonp_params.setdefault("cb", callback)
    jsonp_params.setdefault("_", str(int(time.time() * 1000)))
    text = request_text(
        url,
        jsonp_params,
        retries=retries,
        headers=browser_headers(url, accept="text", referer=eastmoney_referer(symbol)),
        opener=EASTMONEY_OPENER,
        curl_cookie_file=EASTMONEY_COOKIE_FILE,
    )
    return parse_jsonp(text)


def eastmoney_kline_param_variants(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    for ut in EASTMONEY_UT_VALUES:
        candidate = dict(params)
        candidate["ut"] = ut
        candidate.setdefault("rtntype", "6")
        variants.append(candidate)

        lmt_candidate = dict(candidate)
        lmt_candidate.pop("beg", None)
        lmt_candidate.setdefault("lmt", params.get("lmt", "260"))
        variants.append(lmt_candidate)

    plain = dict(params)
    variants.append(plain)

    unique: List[Dict[str, Any]] = []
    seen = set()
    for item in variants:
        key = tuple(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def fetch_eastmoney_kline_payload(symbol: str, params: Dict[str, Any], retries: int) -> Dict[str, Any]:
    errors: List[str] = []
    error_count = 0
    for url in EASTMONEY_KLINE_URLS:
        for candidate_params in eastmoney_kline_param_variants(params):
            mode = "lmt" if "lmt" in candidate_params and "beg" not in candidate_params else "range"
            try:
                return request_eastmoney_json(url, candidate_params, retries, symbol)
            except Exception as exc:
                error_count += 1
                if len(errors) < 20:
                    errors.append(f"{url} {mode} JSON: {exc}")
            try:
                return request_eastmoney_jsonp(url, candidate_params, retries, symbol)
            except Exception as exc:
                error_count += 1
                if len(errors) < 20:
                    errors.append(f"{url} {mode} JSONP: {exc}")
    if error_count > len(errors):
        errors.append(f"... omitted {error_count - len(errors)} additional failed Eastmoney attempts")
    raise RuntimeError("All Eastmoney kline endpoints failed:\n" + "\n".join(errors))


def parse_jsonp(text: str) -> Dict[str, Any]:
    text = text.strip()
    start = text.find("(")
    end = text.rfind(")")
    if start != -1 and end != -1 and end > start:
        text = text[start + 1 : end]
    return json.loads(text)


def utc_date_from_timestamp(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).date().isoformat()


def fetch_daily_prices(
    symbol: str,
    years: float,
    retries: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    use_explicit_dates = start_date is not None or end_date is not None
    if use_explicit_dates:
        start, end = resolve_date_range(years, start_date.isoformat() if start_date else None, end_date.isoformat() if end_date else None)
        params = {
            "period1": int(dt.datetime.combine(start, dt.time.min, tzinfo=dt.timezone.utc).timestamp()),
            "period2": int(dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min, tzinfo=dt.timezone.utc).timestamp()),
            "interval": "1d",
            "events": "div,splits",
            "includeAdjustedClose": "true",
        }
    else:
        range_value = yahoo_range_for_years(years)
        if range_value:
            params = {
                "range": range_value,
                "interval": "1d",
                "events": "div,splits",
                "includeAdjustedClose": "true",
            }
        else:
            end_dt = dt.datetime.now(tz=dt.timezone.utc)
            start_dt = end_dt - dt.timedelta(days=int(years * 365.25) + 10)
            params = {
                "period1": int(start_dt.timestamp()),
                "period2": int(end_dt.timestamp()),
                "interval": "1d",
                "events": "div,splits",
                "includeAdjustedClose": "true",
            }
    payload = fetch_chart_payload(symbol, params, retries)
    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo chart error for {symbol}: {error}")

    result = (chart.get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"No chart data returned for {symbol}")

    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adjclose = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    meta = result.get("meta") or {}
    events = result.get("events") or {}

    dividends_by_date = _event_values_by_date(events.get("dividends") or {}, "amount")
    splits_by_date = _split_values_by_date(events.get("splits") or {})

    rows: List[Dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        date = utc_date_from_timestamp(ts)
        if start_date and date < start_date.isoformat():
            continue
        if end_date and date > end_date.isoformat():
            continue
        rows.append(
            {
                "date": date,
                "symbol": symbol.upper(),
                "open": _safe_index(quote.get("open"), i),
                "high": _safe_index(quote.get("high"), i),
                "low": _safe_index(quote.get("low"), i),
                "close": _safe_index(quote.get("close"), i),
                "adjclose": _safe_index(adjclose, i),
                "volume": _safe_index(quote.get("volume"), i),
                "dividend": dividends_by_date.get(date, ""),
                "split": splits_by_date.get(date, ""),
            }
        )

    return rows, meta


def fetch_chart_payload(symbol: str, params: Dict[str, Any], retries: int) -> Dict[str, Any]:
    errors = []
    quoted_symbol = urllib.parse.quote(symbol)
    for template in YAHOO_CHART_URLS:
        try:
            return request_yahoo_json(template.format(symbol=quoted_symbol), params, retries=retries, symbol=symbol)
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError("All Yahoo chart endpoints failed:\n" + "\n".join(errors))


def yahoo_range_for_years(years: float) -> Optional[str]:
    """Return a Yahoo chart range when possible.

    Using range=2y avoids failures when the local machine clock is ahead of
    Yahoo's available market data, which can make period2 look like the future.
    """
    rounded = round(years)
    if abs(years - rounded) < 0.001 and rounded in {1, 2, 5, 10}:
        return f"{rounded}y"
    return None


def _event_values_by_date(events: Dict[str, Any], field: str) -> Dict[str, str]:
    values: Dict[str, List[str]] = defaultdict(list)
    for item in events.values():
        date = utc_date_from_timestamp(int(item["date"]))
        values[date].append(str(item.get(field, "")))
    return {date: "; ".join(v for v in vals if v) for date, vals in values.items()}


def _split_values_by_date(events: Dict[str, Any]) -> Dict[str, str]:
    values: Dict[str, List[str]] = defaultdict(list)
    for item in events.values():
        date = utc_date_from_timestamp(int(item["date"]))
        numerator = item.get("numerator")
        denominator = item.get("denominator")
        if numerator and denominator:
            values[date].append(f"{numerator}:{denominator}")
    return {date: "; ".join(vals) for date, vals in values.items()}


def _safe_index(values: Optional[List[Any]], index: int) -> Any:
    if not values or index >= len(values):
        return ""
    value = values[index]
    return "" if value is None else value


def fetch_quote_metadata(symbol: str, region: str, lang: str, retries: int) -> Dict[str, Any]:
    params = {
        "symbols": symbol,
        "region": region,
        "lang": lang,
        "fields": ",".join(
            [
                "symbol",
                "shortName",
                "longName",
                "regularMarketPrice",
                "regularMarketChangePercent",
                "marketCap",
                "exchange",
                "quoteType",
                "currency",
                "fiftyTwoWeekHigh",
                "fiftyTwoWeekLow",
                "trailingPE",
                "forwardPE",
            ]
        ),
    }
    payload = request_yahoo_json(YAHOO_QUOTE_URL, params, retries=retries, symbol=symbol)
    result = (payload.get("quoteResponse") or {}).get("result") or []
    return result[0] if result else {}


def fetch_eastmoney_daily_prices(
    symbol: str,
    years: float,
    retries: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    begin, end = resolve_date_range(
        years,
        start_date.isoformat() if start_date else None,
        end_date.isoformat() if end_date else None,
    )
    lmt = max(1, int((end - begin).days * 1.6) + 40)
    params = {
        "secid": eastmoney_secid(symbol),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": begin.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "lmt": str(lmt),
        "rtntype": "6",
        "ut": EASTMONEY_UT_VALUES[0],
    }
    payload = fetch_eastmoney_kline_payload(symbol, params, retries)
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    rows: List[Dict[str, Any]] = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 11:
            continue
        date, open_, close, high, low, volume, amount, amplitude, pct_chg, change, turnover = parts[:11]
        if date < begin.isoformat() or date > end.isoformat():
            continue
        rows.append(
            {
                "date": date,
                "symbol": normalize_symbol(symbol),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "adjclose": close,
                "volume": volume,
                "dividend": "",
                "split": "",
                "amount": amount,
                "amplitude_pct": amplitude,
                "change_pct": pct_chg,
                "change": change,
                "turnover_pct": turnover,
                "data_source": "eastmoney",
            }
        )
    meta = {
        "symbol": normalize_symbol(symbol),
        "longName": data.get("name") or "",
        "exchangeName": "SZSE" if eastmoney_secid(symbol).startswith("0.") else "SSE",
        "currency": "CNY",
        "instrumentType": "EQUITY",
    }
    return rows, meta


def fetch_tencent_daily_prices(
    symbol: str,
    years: float,
    retries: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    begin, end = resolve_date_range(
        years,
        start_date.isoformat() if start_date else None,
        end_date.isoformat() if end_date else None,
    )
    ts_symbol = tencent_symbol(symbol)
    stock_data: Dict[str, Any] = {}
    lines: List[List[Any]] = []
    current = begin
    max_chunk_days = 560

    while current <= end:
        chunk_end = min(end, current + dt.timedelta(days=max_chunk_days))
        day_limit = (chunk_end - current).days + 60
        params = {
            "param": ",".join(
                [
                    ts_symbol,
                    "day",
                    current.isoformat(),
                    chunk_end.isoformat(),
                    str(day_limit),
                    "qfq",
                ]
            )
        }
        url = f"{TENCENT_KLINE_URL}?{urllib.parse.urlencode(params, safe=',')}"
        payload = request_json(url, retries=retries)
        chunk_data = (payload.get("data") or {}).get(ts_symbol) or {}
        if chunk_data:
            stock_data = chunk_data
        lines.extend(chunk_data.get("qfqday") or chunk_data.get("day") or [])
        current = chunk_end + dt.timedelta(days=1)

    rows: List[Dict[str, Any]] = []
    seen_dates = set()
    for item in lines:
        if len(item) < 6:
            continue
        date, open_, close, high, low, volume = item[:6]
        if date in seen_dates:
            continue
        seen_dates.add(date)
        rows.append(
            {
                "date": date,
                "symbol": normalize_symbol(symbol),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "adjclose": close,
                "volume": volume,
                "dividend": "",
                "split": "",
                "data_source": "tencent",
            }
        )
    rows.sort(key=lambda row: row["date"])
    inferred_name = infer_tencent_name(stock_data, symbol)
    meta = {
        "symbol": normalize_symbol(symbol),
        "longName": inferred_name,
        "exchangeName": "SZSE" if ts_symbol.startswith("sz") else "SSE",
        "currency": "CNY",
        "instrumentType": "EQUITY",
    }
    return rows, meta


def infer_tencent_name(stock_data: Dict[str, Any], symbol: str) -> str:
    for key in ("qt", "name", "info"):
        value = stock_data.get(key)
        if isinstance(value, list) and value:
            for item in value:
                if isinstance(item, str) and re.search(r"[\u4e00-\u9fff]", item):
                    return clean_text(item)
        if isinstance(value, str) and re.search(r"[\u4e00-\u9fff]", value):
            return clean_text(value)
    return plain_stock_code(symbol)


def fetch_news(symbol: str, region: str, lang: str, count: int, retries: int) -> List[Dict[str, Any]]:
    params = {
        "q": symbol,
        "quotesCount": 0,
        "newsCount": count,
        "listsCount": 0,
        "enableFuzzyQuery": "false",
        "quotesQueryId": "tss_match_phrase_query",
        "multiQuoteQueryId": "multi_quote_single_token_query",
        "newsQueryId": "news_cie_vespa",
        "enableCb": "true",
        "enableNavLinks": "false",
        "enableEnhancedTrivialQuery": "true",
        "region": region,
        "lang": lang,
    }
    payload = request_yahoo_json(YAHOO_SEARCH_URL, params, retries=retries, symbol=symbol)
    news = payload.get("news") or []
    normalized = []
    for item in news:
        published = item.get("providerPublishTime")
        if not published:
            continue
        title = clean_text(item.get("title") or "")
        summary = clean_text(item.get("summary") or "")
        normalized.append(
            {
                "date": utc_date_from_timestamp(int(published)),
                "title": title,
                "summary": summary,
                "publisher": clean_text(item.get("publisher") or ""),
                "link": item.get("link") or "",
                "published_utc": dt.datetime.fromtimestamp(
                    int(published), tz=dt.timezone.utc
                ).isoformat(),
            }
        )
    return normalized


def fetch_eastmoney_announcements(symbol: str, years: float, count: int, retries: int) -> List[Dict[str, Any]]:
    cutoff = dt.datetime.now(tz=dt.timezone.utc).date() - dt.timedelta(days=int(years * 365.25))
    end_filter: Optional[dt.date] = None
    return fetch_eastmoney_announcements_between(symbol, cutoff, end_filter, count, retries)


def fetch_eastmoney_announcements_between(
    symbol: str,
    start_date: dt.date,
    end_date: Optional[dt.date],
    count: int,
    retries: int,
) -> List[Dict[str, Any]]:
    code = plain_stock_code(symbol)
    normalized: List[Dict[str, Any]] = []
    page_size = 100
    max_pages = max(1, min(80, (count + page_size - 1) // page_size + 2))

    for page in range(1, max_pages + 1):
        params = {
            "sr": "-1",
            "page_size": page_size,
            "page_index": page,
            "ann_type": "A",
            "client_source": "web",
            "stock_list": code,
        }
        payload = request_eastmoney_json(EASTMONEY_ANN_URL, params, retries, symbol)
        items = ((payload.get("data") or {}).get("list") or [])
        if not items:
            break

        old_seen = 0
        for item in items:
            raw_date = item.get("notice_date") or item.get("display_time") or ""
            date = parse_eastmoney_date(raw_date)
            if not date:
                continue
            if date < start_date.isoformat():
                old_seen += 1
                continue
            if end_date and date > end_date.isoformat():
                continue
            title = clean_text(item.get("title_ch") or item.get("title") or "")
            columns = item.get("columns") or []
            column_names = [clean_text(col.get("column_name") or "") for col in columns if col.get("column_name")]
            normalized.append(
                {
                    "date": date,
                    "title": title,
                    "summary": "；".join(column_names),
                    "publisher": "东方财富公告",
                    "link": f"https://data.eastmoney.com/notices/detail/{code}/{item.get('art_code', '')}.html",
                    "published_utc": raw_date,
                    "source_type": "external",
                }
            )
            if len(normalized) >= count:
                return deduplicate_events(normalized)
        if old_seen == len(items):
            break

    return deduplicate_events(normalized)


def fetch_eastmoney_stock_news(symbol: str, years: float, count: int, retries: int) -> List[Dict[str, Any]]:
    cutoff = dt.datetime.now(tz=dt.timezone.utc).date() - dt.timedelta(days=int(years * 365.25))
    return fetch_eastmoney_stock_news_between(symbol, cutoff, None, count, retries)


def fetch_eastmoney_stock_news_between(
    symbol: str,
    start_date: dt.date,
    end_date: Optional[dt.date],
    count: int,
    retries: int,
    keywords: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    search_keywords = keywords or [plain_stock_code(symbol)]
    combined: List[Dict[str, Any]] = []
    for keyword in search_keywords:
        combined.extend(fetch_eastmoney_stock_news_for_keyword(symbol, keyword, start_date, end_date, count, retries))
    return deduplicate_events(combined)[:count]


def fetch_eastmoney_stock_news_for_keyword(
    symbol: str,
    keyword: str,
    start_date: dt.date,
    end_date: Optional[dt.date],
    count: int,
    retries: int,
) -> List[Dict[str, Any]]:
    page_size = 100
    max_pages = max(1, min(50, (count + page_size - 1) // page_size + 2))
    normalized: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        param = {
            "uid": "",
            "keyword": keyword,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": page,
                    "pageSize": page_size,
                    "preTag": "",
                    "postTag": "",
                }
            },
        }
        params = {
            "cb": "jQuery",
            "param": json.dumps(param, ensure_ascii=False, separators=(",", ":")),
            "_": str(int(time.time() * 1000)),
        }
        payload = parse_jsonp(
            request_text(
                EASTMONEY_SEARCH_URL,
                params,
                retries=retries,
                headers=browser_headers(EASTMONEY_SEARCH_URL, accept="text", referer=eastmoney_referer(symbol)),
                opener=EASTMONEY_OPENER,
                curl_cookie_file=EASTMONEY_COOKIE_FILE,
            )
        )
        articles = ((payload.get("result") or {}).get("cmsArticleWebOld") or [])
        if not articles:
            break

        old_seen = 0
        for item in articles:
            date = parse_eastmoney_date(item.get("date") or "")
            if not date:
                continue
            if date < start_date.isoformat():
                old_seen += 1
                continue
            if end_date and date > end_date.isoformat():
                continue
            normalized.append(
                {
                    "date": date,
                    "title": clean_text(item.get("title") or ""),
                    "summary": clean_text(item.get("content") or ""),
                    "publisher": clean_text(item.get("mediaName") or "东方财富资讯"),
                    "link": item.get("url") or "",
                    "published_utc": item.get("date") or "",
                    "source_type": "external",
                }
            )
            if len(normalized) >= count:
                return deduplicate_events(normalized)
        if old_seen == len(articles):
            break

    return deduplicate_events(normalized)


def news_search_keywords(symbol: str, company_name: Optional[str], *metas: Dict[str, Any]) -> List[str]:
    code = plain_stock_code(symbol)
    candidates = [
        code,
        normalize_symbol(symbol),
        tencent_symbol(symbol) if is_china_a_share(symbol) else "",
    ]
    if company_name:
        candidates.append(company_name.strip())
    for meta in metas:
        for key in ("longName", "shortName", "name"):
            value = clean_text(str(meta.get(key) or ""))
            if value and value != code:
                candidates.append(value)

    keywords: List[str] = []
    seen = set()
    for item in candidates:
        item = clean_text(item)
        if not item or item in seen:
            continue
        seen.add(item)
        keywords.append(item)
    return keywords


def deduplicate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for item in events:
        key = (item.get("date"), item.get("title"), item.get("link"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def event_dates(events: List[Dict[str, Any]]) -> set:
    return {item.get("date", "") for item in events if item.get("date")}


def price_dates(price_rows: List[Dict[str, Any]]) -> List[str]:
    return [row["date"] for row in price_rows if row.get("open") not in {"", None}]


def missing_text_dates(price_rows: List[Dict[str, Any]], events: List[Dict[str, Any]]) -> List[str]:
    dates_with_events = event_dates(events)
    return [date for date in price_dates(price_rows) if date not in dates_with_events]


def safe_float(value: Any) -> Optional[float]:
    try:
        if value in {"", None}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def format_number(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return ""
    return f"{number:.2f}".rstrip("0").rstrip(".")


def daily_return_pct(row: Dict[str, Any]) -> Optional[float]:
    open_ = safe_float(row.get("open"))
    close = safe_float(row.get("close"))
    if open_ in {None, 0} or close is None:
        return None
    return (close - open_) / open_ * 100


def build_daily_market_context_events(
    price_rows: List[Dict[str, Any]],
    quote_meta: Dict[str, Any],
    chart_meta: Dict[str, Any],
    missing_dates: List[str],
) -> List[Dict[str, Any]]:
    price_by_date = {row["date"]: row for row in price_rows}
    company_name = (
        quote_meta.get("longName")
        or quote_meta.get("shortName")
        or chart_meta.get("longName")
        or chart_meta.get("symbol")
        or ""
    )
    symbol = chart_meta.get("symbol") or quote_meta.get("symbol") or ""
    events: List[Dict[str, Any]] = []

    for date in missing_dates:
        row = price_by_date.get(date)
        if not row:
            continue
        return_pct = daily_return_pct(row)
        if return_pct is None:
            move_text = "当日涨跌幅无法由开盘价和收盘价计算"
        else:
            direction = "上涨" if return_pct > 0 else "下跌" if return_pct < 0 else "平收"
            move_text = f"按开盘价计算当日{direction}{abs(return_pct):.2f}%"

        title_name = company_name or symbol or row.get("symbol") or "A股标的"
        title = f"{title_name} {date} 日行情文本摘要"
        summary_parts = [
            f"{date}，{title_name}A股交易日行情记录",
            f"开盘价{format_number(row.get('open'))}",
            f"最高价{format_number(row.get('high'))}",
            f"最低价{format_number(row.get('low'))}",
            f"收盘价{format_number(row.get('close'))}",
            f"成交量{format_number(row.get('volume'))}",
            move_text,
            "该条为程序根据当日OHLCV行情生成的文本补充，用于保证每日行情数据均有对应文本字段；不代表外部新闻报道。",
        ]
        events.append(
            {
                "date": date,
                "title": title,
                "summary": "；".join(part for part in summary_parts if part),
                "publisher": DAILY_CONTEXT_PUBLISHER,
                "link": "",
                "published_utc": date,
                "source_type": "generated_ohlcv_summary",
            }
        )

    return events


def infer_company_name_from_events(events: List[Dict[str, Any]], fallback: str) -> str:
    for item in events:
        title = clean_text(item.get("title") or "")
        match = re.match(r"([\u4e00-\u9fffA-Za-z0-9]{2,12})[:：，,（(]", title)
        if match:
            candidate = match.group(1)
            if not re.fullmatch(r"\d{6}", candidate):
                return candidate
    return fallback


def parse_eastmoney_date(value: str) -> str:
    match = re.search(r"\d{4}-\d{2}-\d{2}", value or "")
    return match.group(0) if match else ""


def clean_text(value: str) -> str:
    value = html.unescape(value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def extract_keywords(texts: Iterable[str], top_n: int = 12) -> str:
    counter: Counter[str] = Counter()
    for text in texts:
        for token in re.findall(r"[A-Za-z][A-Za-z0-9&.+-]{2,}|[\u4e00-\u9fff]{2,}", text.lower()):
            if token in STOPWORDS or token.isdigit():
                continue
            counter[token] += 1
    return "; ".join(word for word, _ in counter.most_common(top_n))


def combine_rows(
    price_rows: List[Dict[str, Any]],
    news: List[Dict[str, Any]],
    quote_meta: Dict[str, Any],
    chart_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not price_rows:
        return []

    price_by_date = {row["date"]: row for row in price_rows}
    news_by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in news:
        news_by_date[item["date"]].append(item)

    company_name = quote_meta.get("longName") or quote_meta.get("shortName") or chart_meta.get("longName") or ""
    exchange = quote_meta.get("exchange") or chart_meta.get("exchangeName") or ""
    currency = quote_meta.get("currency") or chart_meta.get("currency") or ""
    instrument_type = quote_meta.get("quoteType") or chart_meta.get("instrumentType") or ""

    combined = []
    for date in calendar_dates(price_rows[0]["date"], price_rows[-1]["date"]):
        row = price_by_date.get(
            date,
            {
                "date": date,
                "symbol": (chart_meta.get("symbol") or quote_meta.get("symbol") or "").upper(),
                "open": "",
                "high": "",
                "low": "",
                "close": "",
                "adjclose": "",
                "volume": "",
                "dividend": "",
                "split": "",
            },
        )
        items = news_by_date.get(date, [])
        event_titles = [item["title"] for item in items if item.get("title")]
        event_summaries = [item["summary"] for item in items if item.get("summary")]
        publishers = sorted({item["publisher"] for item in items if item.get("publisher")})
        links = [item["link"] for item in items if item.get("link")]
        source_types = sorted({item.get("source_type", "external") for item in items})
        external_event_count = sum(1 for item in items if item.get("source_type", "external") == "external")
        generated_event_count = sum(1 for item in items if item.get("source_type") == "generated_ohlcv_summary")
        keywords = extract_keywords(event_titles + event_summaries)
        combined.append(
            {
                **row,
                "company_name": company_name,
                "exchange": exchange,
                "currency": currency,
                "instrument_type": instrument_type,
                "market_cap_latest": quote_meta.get("marketCap", ""),
                "trailing_pe_latest": quote_meta.get("trailingPE", ""),
                "forward_pe_latest": quote_meta.get("forwardPE", ""),
                "fifty_two_week_high_latest": quote_meta.get("fiftyTwoWeekHigh", ""),
                "fifty_two_week_low_latest": quote_meta.get("fiftyTwoWeekLow", ""),
                "event_count": len(items),
                "event_titles": " || ".join(event_titles),
                "event_summaries": " || ".join(event_summaries),
                "event_publishers": " || ".join(publishers),
                "event_links": " || ".join(links),
                "event_source_types": " || ".join(source_types),
                "external_event_count": external_event_count,
                "generated_event_count": generated_event_count,
                "has_external_text": "1" if external_event_count else "0",
                "keywords": keywords,
            }
        )
    return combined


def calendar_dates(start_date: str, end_date: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    while current <= end:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No rows to write")
    fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_symbol = args.symbol.strip().upper()
    symbol = normalize_symbol(input_symbol)
    output_symbol = symbol.replace(".", "_")
    start_date, end_date = resolve_date_range(args.years, args.start_date, args.end_date)
    explicit_range = args.start_date is not None or args.end_date is not None
    default_suffix = (
        f"{start_date.isoformat()}_{end_date.isoformat()}"
        if explicit_range
        else f"{args.years:g}y"
    )
    output = args.output or f"finance_text_{output_symbol}_{default_suffix}.csv"

    if symbol != input_symbol:
        print(f"Normalized China A-share symbol: {input_symbol} -> {symbol}")

    print(f"[1/4] Fetching daily price data for {symbol} from {start_date} to {end_date}...")
    source_used = args.source
    if args.source == "eastmoney":
        price_rows, chart_meta = fetch_eastmoney_daily_prices(symbol, args.years, args.retries, start_date, end_date)
        source_used = "eastmoney"
    elif args.source == "tencent":
        price_rows, chart_meta = fetch_tencent_daily_prices(symbol, args.years, args.retries, start_date, end_date)
        source_used = "tencent"
    else:
        try:
            price_rows, chart_meta = fetch_daily_prices(
                symbol,
                args.years,
                args.retries,
                start_date if explicit_range else None,
                end_date if explicit_range else None,
            )
            source_used = "yahoo"
        except Exception as exc:
            if args.source == "auto" and is_china_a_share(symbol):
                print(f"Yahoo price fetch failed, trying Eastmoney: {exc}", file=sys.stderr)
                try:
                    price_rows, chart_meta = fetch_eastmoney_daily_prices(symbol, args.years, args.retries, start_date, end_date)
                    source_used = "eastmoney"
                except Exception as eastmoney_exc:
                    print(f"Eastmoney price fetch failed, falling back to Tencent: {eastmoney_exc}", file=sys.stderr)
                    price_rows, chart_meta = fetch_tencent_daily_prices(symbol, args.years, args.retries, start_date, end_date)
                    source_used = "tencent"
            else:
                raise
    time.sleep(args.pause)

    print("[2/4] Fetching latest quote/basic stock metadata...")
    if source_used in {"eastmoney", "tencent"}:
        quote_meta = {
            "symbol": symbol,
            "longName": args.company_name or chart_meta.get("longName", ""),
            "exchange": chart_meta.get("exchangeName", ""),
            "currency": chart_meta.get("currency", "CNY"),
            "quoteType": chart_meta.get("instrumentType", "EQUITY"),
        }
    else:
        try:
            quote_meta = fetch_quote_metadata(symbol, args.region, args.lang, args.retries)
            if args.company_name:
                quote_meta["longName"] = args.company_name
        except Exception as exc:
            print(f"Warning: quote metadata failed: {exc}", file=sys.stderr)
            quote_meta = {"longName": args.company_name or ""}
    time.sleep(args.pause)

    if source_used in {"eastmoney", "tencent"}:
        keywords = news_search_keywords(symbol, args.company_name, quote_meta, chart_meta)
        print(f"[3/4] Fetching up to {args.news_count} Eastmoney news and announcement/event items...")
        print(f"Search keywords: {', '.join(keywords)}")
        try:
            stock_news = fetch_eastmoney_stock_news_between(
                symbol,
                start_date,
                end_date,
                args.news_count,
                args.retries,
                keywords=keywords,
            )
            announcements = fetch_eastmoney_announcements_between(symbol, start_date, end_date, args.news_count, args.retries)
            news = deduplicate_events(stock_news + announcements)
            missing_dates = missing_text_dates(price_rows, news)
            if missing_dates:
                print(
                    "Some trading days have no stock-specific text events; "
                    "fetching market-wide news fallback..."
                )
                market_news = fetch_eastmoney_stock_news_between(
                    symbol,
                    start_date,
                    end_date,
                    args.news_count,
                    args.retries,
                    keywords=MARKET_NEWS_KEYWORDS,
                )
                news = deduplicate_events(news + market_news)
            missing_dates = missing_text_dates(price_rows, news)
            if missing_dates:
                print(
                    "Some trading days still have no external text events; "
                    "adding transparent daily OHLCV text summaries..."
                )
                daily_context = build_daily_market_context_events(
                    price_rows,
                    quote_meta,
                    chart_meta,
                    missing_dates,
                )
                news = deduplicate_events(news + daily_context)
            if not quote_meta.get("longName"):
                quote_meta["longName"] = infer_company_name_from_events(news, plain_stock_code(symbol))
        except Exception as exc:
            print(f"Warning: Eastmoney text/event fetch failed: {exc}", file=sys.stderr)
            print("Adding transparent daily OHLCV text summaries for all trading days...")
            news = build_daily_market_context_events(
                price_rows,
                quote_meta,
                chart_meta,
                price_dates(price_rows),
            )
    else:
        print(f"[3/4] Fetching up to {args.news_count} Yahoo Finance news/event items...")
        try:
            news = [
                item
                for item in fetch_news(symbol, args.region, args.lang, args.news_count, args.retries)
                if start_date.isoformat() <= item.get("date", "") <= end_date.isoformat()
            ]
        except Exception as exc:
            print(f"Warning: news fetch failed: {exc}", file=sys.stderr)
            news = []

    print("[4/4] Combining daily data and writing CSV...")
    if args.require_news and not news:
        raise RuntimeError("No news/event items were collected, but --require-news was set.")
    if args.require_news:
        missing_dates = missing_text_dates(price_rows, news)
        if missing_dates:
            raise RuntimeError(
                "No text/news/event item was collected for these trading dates: "
                + ", ".join(missing_dates)
            )
    rows = combine_rows(price_rows, news, quote_meta, chart_meta)
    write_csv(output, rows)

    dated_news = len({item["date"] for item in news})
    print(f"Done: wrote {len(rows)} daily rows to {output}")
    print(f"{source_used} returned {len(news)} news/event items across {dated_news} dates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
