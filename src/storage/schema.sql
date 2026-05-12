-- SQLite schema for the NLP-driven RL trading platform.
CREATE TABLE IF NOT EXISTS news_table (
    news_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    title TEXT,
    content TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS market_table (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS sentiment_table (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    method TEXT NOT NULL,
    sentiment_score REAL,
    PRIMARY KEY (ticker, date, method)
);

CREATE TABLE IF NOT EXISTS trading_log_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    episode INTEGER,
    date TEXT,
    action TEXT,
    reward REAL,
    position REAL,
    cash REAL,
    portfolio_value REAL,
    experiment TEXT
);

-- Backward-compatible views for earlier module names.
CREATE VIEW IF NOT EXISTS news_articles AS
SELECT news_id AS id, ticker AS symbol, date, title, content AS summary, source, NULL AS url
FROM news_table;

CREATE VIEW IF NOT EXISTS market_bars AS
SELECT NULL AS id, ticker AS symbol, date, open, high, low, close, volume
FROM market_table;

CREATE VIEW IF NOT EXISTS sentiment_signals AS
SELECT NULL AS id, ticker AS symbol, date, sentiment_score, method
FROM sentiment_table;

CREATE VIEW IF NOT EXISTS trade_logs AS
SELECT id, NULL AS symbol, date, action, NULL AS price, position, cash, portfolio_value
FROM trading_log_table;
