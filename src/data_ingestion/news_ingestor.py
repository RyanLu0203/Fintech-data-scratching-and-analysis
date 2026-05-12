"""Fetch financial news from RSS feeds and NewsAPI-compatible providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import feedparser
import requests


@dataclass
class NewsItem:
    symbol: str
    published_at: str
    title: str
    summary: str
    source: str
    url: str


class NewsIngestor:
    def fetch_rss(self, symbol: str, feed_urls: Iterable[str]) -> List[NewsItem]:
        items: List[NewsItem] = []
        for feed_url in feed_urls:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                items.append(
                    NewsItem(
                        symbol=symbol,
                        published_at=getattr(entry, "published", ""),
                        title=getattr(entry, "title", ""),
                        summary=getattr(entry, "summary", ""),
                        source=feed.feed.get("title", "rss"),
                        url=getattr(entry, "link", ""),
                    )
                )
        return items

    def fetch_newsapi(self, symbol: str, api_key: str, query: str, page_size: int = 100) -> List[NewsItem]:
        if not api_key:
            return []
        response = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": query, "apiKey": api_key, "pageSize": page_size, "sortBy": "publishedAt"},
            timeout=30,
        )
        response.raise_for_status()
        return [
            NewsItem(
                symbol=symbol,
                published_at=article.get("publishedAt", ""),
                title=article.get("title", ""),
                summary=article.get("description") or article.get("content") or "",
                source=(article.get("source") or {}).get("name", "NewsAPI"),
                url=article.get("url", ""),
            )
            for article in response.json().get("articles", [])
        ]

