"""Boundary checks for source timing, frozen inference and fast training ledgers."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.run_real_coursework import verify_snapshot
from scripts.run_coursework import synthetic_inputs
from src.data.historical_sources import display_timestamp
from src.nlp.chronological import normalize_news, fit_frozen_peer_models, session_time
from src.rl.trading_env import FinancialTradingEnv

ROOT = Path(__file__).resolve().parents[1]


def test_provider_timestamp_precision_and_offset():
    assert (
        display_timestamp("2023-04-27 18:32:09:123").isoformat()
        == "2023-04-27T18:32:09.123000+08:00"
    )
    assert display_timestamp("2023-04-27") is None
    assert display_timestamp("") is None


def test_real_snapshot_hashes_and_conservative_public_date_timing():
    directory = ROOT / "data/frozen/electronics_2023_2025_20261010"
    manifest = verify_snapshot(directory)
    assert len(manifest["profiles"]) == 4
    for symbol in manifest["symbols"]:
        market = pd.read_csv(directory / f"{symbol}_market.csv")
        news = pd.read_csv(directory / f"{symbol}_news.csv")
        assert len(market) >= 504 and not market.date.duplicated().any()
        inferred = news[news.availability_policy == "after_end_of_public_notice_date"]
        public_date_end = pd.to_datetime(inferred.published_at).dt.tz_localize(
            "Asia/Shanghai"
        ) + pd.Timedelta(days=1)
        assert (
            pd.to_datetime(inferred.available_at, utc=True) >= public_date_end
        ).all()
        assert news.source.eq("eastmoney_disclosure_title").all()


@pytest.mark.parametrize("mode", ["discrete_trade", "target_position"])
@pytest.mark.parametrize(
    "reward",
    ["one_day_return", "three_day_return", "five_day_return", "risk_adjusted_return"],
)
def test_cached_training_preserves_actual_state_rewards_fees_and_full_ledger(
    mode, reward
):
    dates = pd.bdate_range("2025-01-01", periods=15)
    data = pd.DataFrame(
        {
            "date": dates,
            "open": np.linspace(10, 20, 15),
            "close": np.linspace(11, 19, 15),
            "price": np.linspace(9, 18, 15),
            "position": 0.0,
            "cash": 1000.0,
            "position_ratio": 0.0,
            "cash_ratio": 1.0,
        }
    )
    columns = ["price", "position", "cash", "position_ratio", "cash_ratio"]
    options = dict(
        initial_cash=1000,
        transaction_cost=0.003,
        action_mode=mode,
        reward_variant=reward,
    )
    reference = FinancialTradingEnv(data, columns, **options)
    cached = FinancialTradingEnv(data, columns, cache_market_rows=True, **options)
    np.testing.assert_array_equal(reference.reset(), cached.reset())
    for action in np.random.default_rng(14).integers(
        0, 3 if mode == "discrete_trade" else 5, len(data)
    ):
        before, after = reference.step(action), cached.step(action)
        np.testing.assert_array_equal(before[0], after[0])
        assert before[1:] == after[1:]
    pd.testing.assert_frame_equal(reference.trading_log(), cached.trading_log())


def test_future_duplicate_with_distinct_provider_id_cannot_erase_past_fit_text():
    market, _, peer_news, peer_market = synthetic_inputs()
    end = session_time(market.date.iloc[[19]], 15).iloc[0]
    base = normalize_news(peer_news)
    duplicate = peer_news.iloc[[0]].copy()
    duplicate["news_id"] = "future-republication"
    duplicate["available_at"] = "2026-01-02T18:00:00+08:00"
    changed = normalize_news(pd.concat([peer_news, duplicate]))
    models, meta, labels = fit_frozen_peer_models(base, peer_market, "002475", end)
    new_models, new_meta, new_labels = fit_frozen_peer_models(
        changed, peer_market, "002475", end
    )
    assert meta == new_meta
    pd.testing.assert_frame_equal(labels, new_labels)
    for name in models:
        np.testing.assert_allclose(
            models[name].predict_proba(["profit growth"]),
            new_models[name].predict_proba(["profit growth"]),
            rtol=0,
            atol=1e-12,
        )
