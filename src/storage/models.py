"""Relational table definitions for news, prices, sentiment, trades, and portfolio values."""

from __future__ import annotations

from sqlalchemy import Column, Date, Float, Integer, String, Text
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class NewsArticle(Base):
    __tablename__ = "news_articles"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    title = Column(Text)
    summary = Column(Text)
    source = Column(String)
    url = Column(Text)


class MarketBar(Base):
    __tablename__ = "market_bars"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


class SentimentSignal(Base):
    __tablename__ = "sentiment_signals"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    sentiment_score = Column(Float)
    method = Column(String)


class TradeLog(Base):
    __tablename__ = "trade_logs"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    action = Column(String)
    price = Column(Float)
    position = Column(Float)
    cash = Column(Float)
    portfolio_value = Column(Float)

