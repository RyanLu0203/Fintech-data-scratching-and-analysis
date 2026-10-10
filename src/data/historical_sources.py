"""Freeze public historical OHLCV and disclosure titles without invented times."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import urlencode

import pandas as pd
import requests

PRICE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
ANNOUNCEMENT_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"


def fetch_bytes(url: str) -> bytes:
    """Use the platform curl TLS stack when the Python TLS stack is unavailable."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.RequestException:
        result = subprocess.run(
            [
                "curl",
                "-fLsS",
                "--connect-timeout",
                "15",
                "--max-time",
                "90",
                "--retry",
                "2",
                url,
            ],
            check=True,
            capture_output=True,
        )
        return result.stdout


def display_timestamp(value: str) -> pd.Timestamp | None:
    """Provider display_time uses HH:MM:SS:milliseconds, in Chinese local time."""
    if not value or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?::\d{1,3})?", str(value)
    ):
        return None
    fixed = re.sub(r"(\d{2}:\d{2}:\d{2}):(\d+)$", r"\1.\2", str(value))
    return pd.Timestamp(fixed).tz_localize("Asia/Shanghai")


def freeze_sources(
    symbols: tuple[str, ...], start: str, end: str, destination: Path
) -> Path:
    if pd.Timestamp(start) > pd.Timestamp(end):
        raise ValueError("Start must precede end")
    destination.mkdir(parents=True, exist_ok=False)
    raw_dir = destination / "raw"
    raw_dir.mkdir()
    fetched_at = datetime.now(timezone.utc).isoformat()
    requests_audit, profiles = [], []

    def fetch_json(url: str, filename: str) -> dict:
        content = fetch_bytes(url)
        (raw_dir / filename).write_bytes(content)
        requests_audit.append(
            {
                "url": url,
                "file": "raw/" + filename,
                "sha256": hashlib.sha256(content).hexdigest(),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        return json.loads(content)

    for symbol in symbols:
        code = ("sh" if symbol.startswith("6") else "sz") + symbol
        price_rows = []
        current = pd.Timestamp(start)
        chunk = 0
        while current <= pd.Timestamp(end):
            chunk_end = min(pd.Timestamp(end), current + pd.Timedelta(days=500))
            params = {
                "param": f"{code},day,{current.date()},{chunk_end.date()},560,qfq"
            }
            payload = fetch_json(
                PRICE_URL + "?" + urlencode(params), f"{symbol}_market_{chunk}.json"
            )
            if payload.get("code") != 0:
                raise ValueError(f"Provider rejected market request: {symbol}")
            data = payload["data"][code]
            # An unadjusted fallback would silently change the experiment basis.
            if "qfqday" not in data:
                raise ValueError(f"Missing explicitly requested qfq bars: {symbol}")
            for item in data["qfqday"]:
                date, opening, close, high, low, volume = item[:6]
                if start <= date <= end:
                    price_rows.append(
                        {
                            "symbol": symbol,
                            "date": date,
                            "open": float(opening),
                            "close": float(close),
                            "high": float(high),
                            "low": float(low),
                            "volume": float(volume),
                            "source": "tencent_qfq",
                            "price_basis": "provider_qfq_snapshot",
                            "volume_unit": "provider_reported_lots",
                            "fetched_at": fetched_at,
                        }
                    )
            current = chunk_end + pd.Timedelta(days=1)
            chunk += 1
        market = (
            pd.DataFrame(price_rows)
            .drop_duplicates(["symbol", "date"])
            .sort_values("date")
        )
        if market.empty:
            raise ValueError(f"Empty market history: {symbol}")
        market.to_csv(destination / f"{symbol}_market.csv", index=False)
        news_rows, excluded = [], []
        page = 1
        expected = None
        seen_ids = set()
        while True:
            params = {
                "sr": "-1",
                "page_size": 100,
                "page_index": page,
                "ann_type": "A",
                "client_source": "web",
                "stock_list": symbol,
                "begin_time": start,
                "end_time": end,
            }
            payload = fetch_json(
                ANNOUNCEMENT_URL + "?" + urlencode(params),
                f"{symbol}_announcements_{page}.json",
            )
            data = payload.get("data") or {}
            expected = int(data["total_hits"])
            items = data.get("list") or []
            if not items:
                if len(seen_ids) < expected:
                    raise ValueError(f"Truncated announcement pagination: {symbol}")
                break
            new_ids = 0
            for item in items:
                identifier = item["art_code"]
                if identifier in seen_ids:
                    continue
                seen_ids.add(identifier)
                new_ids += 1
                displayed = display_timestamp(item.get("display_time", ""))
                produced = display_timestamp(item.get("eiTime", ""))
                # Historical public listings retain the notice date but omit
                # display_time. Wait until that ENTIRE calendar date is over.
                # This is a declared availability assumption, not an observed
                # intraday publication timestamp or a production-time proxy.
                policy = "precise_display_time"
                if displayed is None:
                    notice = str(item.get("notice_date", ""))
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2} 00:00:00", notice):
                        excluded.append(
                            {
                                "news_id": identifier,
                                "reason": "missing_public_notice_date",
                            }
                        )
                        continue
                    displayed = pd.Timestamp(notice).tz_localize(
                        "Asia/Shanghai"
                    ) + pd.Timedelta(days=1)
                    policy = "after_end_of_public_notice_date"
                available = max(
                    [displayed] + ([produced] if produced is not None else [])
                )
                title = str(item.get("title_ch") or item.get("title") or "").strip()
                if not title:
                    excluded.append({"news_id": identifier, "reason": "missing_title"})
                    continue
                if not start <= available.date().isoformat() <= end:
                    excluded.append(
                        {
                            "news_id": identifier,
                            "reason": "display_time_outside_frozen_interval",
                        }
                    )
                    continue
                news_rows.append(
                    {
                        "news_id": identifier,
                        "symbol": symbol,
                        "text": title,
                        "available_at": available.isoformat(),
                        "source": "eastmoney_disclosure_title",
                        "published_at": item.get("notice_date", ""),
                        "display_time": item.get("display_time", ""),
                        "production_time": item.get("eiTime", ""),
                        "url": f"https://data.eastmoney.com/notices/detail/{symbol}/{identifier}.html",
                        "fetched_at": fetched_at,
                        "text_scope": "title_only",
                    }
                )
                news_rows[-1]["availability_policy"] = policy
                news_rows[-1]["availability_inferred"] = (
                    policy != "precise_display_time"
                )
            if len(seen_ids) >= expected:
                break
            if new_ids == 0 or page >= 100:
                raise ValueError(
                    f"Announcement pagination stopped before coverage: {symbol}"
                )
            page += 1
        news = pd.DataFrame(news_rows)
        if news.empty:
            raise ValueError(f"No precisely timestamped disclosure titles: {symbol}")
        news.sort_values(["available_at", "news_id"]).to_csv(
            destination / f"{symbol}_news.csv", index=False
        )
        profiles.append(
            {
                "symbol": symbol,
                "market_rows": len(market),
                "news_rows": len(news),
                "provider_records": len(seen_ids),
                "provider_total_hits": expected,
                "exclusions": excluded,
            }
        )
        print(
            f"source symbol={symbol} bars={len(market)} titles={len(news)} excluded={len(excluded)}",
            flush=True,
        )
    manifest = {
        "schema_version": 1,
        "start": start,
        "end": end,
        "symbols": symbols,
        "fetched_at": fetched_at,
        "requests": requests_audit,
        "profiles": profiles,
        "availability_policy": "precise display_time when present; otherwise after the entire public notice date (00:00 of following calendar day); never before production time",
        "scope": "retrospective provider qfq OHLCV and disclosure titles; not a complete media-news archive",
        "file_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(destination.glob("*.csv"))
        },
    }
    path = destination / "source_manifest.json"
    path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return path
