from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.information_density import detect_information_density_split
from src.features.technical_indicators import STATE_COLUMNS, add_trading_features, leakage_diagnostics
from src.nlp.aggregate_sentiment import run_nlp_pipeline
from src.storage.database import initialize_database, load_table, save_market_data, save_news_data, save_sentiment_data, save_trading_logs


def _tiny_market(symbol: str = "000001") -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=260, freq="D")
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": dates,
            "open": range(100, 360),
            "high": range(101, 361),
            "low": range(99, 359),
            "close": range(100, 360),
            "volume": [1000] * len(dates),
            "event_title": ["profit growth"] * len(dates),
            "event_summary": ["company profit growth improves"] * len(dates),
            "event_source": ["unit_test"] * len(dates),
        }
    )


def test_state_vector_columns_and_no_lookahead() -> None:
    market = _tiny_market()
    sentiment = pd.DataFrame({"date": market["date"], "sentiment_score": 0.1, "news_count": 1, "sentiment_method": "lexicon"})
    features = add_trading_features(market, sentiment)
    assert STATE_COLUMNS == ["price", "MA50", "MA200", "RSI", "MACD", "position", "cash", "sentiment_score"]
    assert list(features[STATE_COLUMNS].columns) == STATE_COLUMNS
    assert {"news_available", "sentiment_missing_flag", "sentiment_rolling_3d", "sentiment_rolling_5d"}.issubset(features.columns)
    diagnostics = leakage_diagnostics(features, STATE_COLUMNS)
    assert diagnostics.loc[diagnostics["metric"] == "lookahead_bias_detected", "value"].iloc[0] == False
    assert diagnostics.loc[diagnostics["metric"] == "same_day_proxy_leakage_warning", "value"].iloc[0] == False


def test_information_density_split_uses_recent_80_percent_news() -> None:
    market = _tiny_market()
    news_count = [1] * 20 + [8] * 80 + [0] * 160
    sentiment = pd.DataFrame({"date": market["date"], "sentiment_score": 0.1, "news_count": news_count, "sentiment_method": "lexicon"})
    split = detect_information_density_split("000001", market, sentiment)
    assert split["total_news_count"] == 660
    assert split["high_density_news_count"] >= 528
    assert split["density_cutoff_date"] >= "2025-01-20"
    assert split["high_density_start_date"] <= split["high_density_end_date"]
    assert split["high_density_coverage_ratio"] > 0


def test_sentiment_missing_is_distinct_from_neutral(tmp_path: Path) -> None:
    input_csv = tmp_path / "000001_finance_text_2025-01-01_2025-09-17.csv"
    _tiny_market().to_csv(input_csv, index=False)
    outputs = run_nlp_pipeline(input_csv, tmp_path, "000001", methods=("lexicon",))
    daily = outputs["daily_sentiment"]
    assert "news_count" in daily.columns
    assert "sentiment_score" in daily.columns
    assert (daily["news_count"] == 0).all()
    assert (daily["sentiment_score"] == 0).all()
    assert (daily["sentiment_method"] == "event_count_proxy").all()


def test_storage_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "demo.sqlite"
    initialize_database(db_path)
    market = _tiny_market().head(3)
    news = pd.DataFrame(
        {
            "news_id": ["n1"],
            "symbol": ["000001"],
            "date": ["2025-01-01"],
            "title": ["profit growth"],
            "content": ["profit growth improves"],
            "source": ["unit_test"],
        }
    )
    sentiment = pd.DataFrame({"ticker": ["000001"], "date": ["2025-01-01"], "sentiment_score": [0.5], "method": ["lexicon"]})
    logs = pd.DataFrame(
        {
            "episode": [1],
            "date": ["2025-01-01"],
            "action": ["Hold"],
            "reward": [0.0],
            "position": [0.0],
            "cash": [100000.0],
            "portfolio_value": [100000.0],
            "experiment": ["unit_test"],
        }
    )
    save_market_data(market, db_path)
    save_news_data(news, db_path)
    save_sentiment_data(sentiment, db_path)
    save_trading_logs(logs, db_path)
    assert len(load_table("market_table", db_path)) == 3
    assert len(load_table("news_table", db_path)) == 1
    assert len(load_table("sentiment_table", db_path)) == 1
    assert len(load_table("trading_log_table", db_path)) == 1


def test_cross_stock_common_window(tmp_path: Path) -> None:
    root = tmp_path / "stocks"
    for symbol, start in [("000001", "2025-01-01"), ("000002", "2025-01-05")]:
        data_dir = root / symbol / "data"
        reports_dir = root / symbol / "reports"
        results_dir = root / symbol / "results"
        data_dir.mkdir(parents=True)
        reports_dir.mkdir()
        results_dir.mkdir()
        market = _tiny_market(symbol).assign(date=pd.date_range(start, periods=260, freq="D"))
        market.to_csv(data_dir / f"{symbol}_finance_text_2025.csv", index=False)
        pd.DataFrame({"date": market["date"], "sentiment_score": 0.1, "news_count": 1, "sentiment_method": "lexicon"}).to_csv(
            reports_dir / f"{symbol}_daily_sentiment.csv", index=False
        )
        pd.DataFrame(
            {
                "metric": ["sentiment_coverage", "sentiment_next_day_return_corr", "net_flow_next_day_return_corr"],
                "value": [1.0, 0.1, 0.0],
            }
        ).to_csv(reports_dir / f"{symbol}_signal_diagnostics.csv", index=False)
        pd.DataFrame(
            {
                "experiment": ["buy_and_hold", "dqn_without_nlp", "dqn_with_nlp"],
                "final_equity": [100000, 101000, 102000],
                "cumulative_return": [0, 0.01, 0.02],
                "sharpe_ratio": [0, 0.5, 0.6],
                "max_drawdown": [0, 0.1, 0.1],
            }
        ).to_csv(results_dir / "ablation_metrics.csv", index=False)
        pd.DataFrame(
            {
                "date": list(market["date"]) * 3,
                "portfolio_value": list(range(100000, 100260)) * 3,
                "experiment": ["buy_and_hold"] * 260 + ["dqn_without_nlp"] * 260 + ["dqn_with_nlp"] * 260,
            }
        ).to_csv(results_dir / "portfolio_curves.csv", index=False)
    payload = build_cross_stock_summary(stock_root=root, output_dir=tmp_path / "system")
    summary = payload["summary"]
    assert not summary.empty
    assert "comparability_status" in summary.columns
    assert summary["common_start_date"].iloc[0] == "2025-01-05"
