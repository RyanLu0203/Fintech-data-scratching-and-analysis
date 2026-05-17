from __future__ import annotations

import pandas as pd

from src.evaluation.peer_nlp_ablation import (
    _high_density_internal_split,
    _needs_nlp_aware_high_density_split,
    _peer_state_feature_diagnostics,
)


def _feature_rows(start: str, periods: int, signal: float) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=periods)
    frame = pd.DataFrame(
        {
            "date": dates,
            "price": 10.0,
            "MA50": 10.0,
            "MA200": 10.0,
            "RSI": 50.0,
            "MACD": 0.0,
            "position": 0.0,
            "cash": 1000000.0,
            "sector_sentiment_score": signal,
            "marketwide_sentiment_score": signal,
            "target_news_available": 1 if signal else 0,
        }
    )
    return frame


def test_peer_nlp_split_moves_training_into_high_density_when_old_train_has_zero_signal() -> None:
    old_train = _feature_rows("2025-01-01", 80, 0.0)
    high_density = _feature_rows("2025-05-01", 100, 0.25)
    assert _needs_nlp_aware_high_density_split(old_train, high_density)

    train, test, split = _high_density_internal_split(high_density, {"window_status": "READY"})

    assert split["window_status"] == "READY_NLP_AWARE_HIGH_DENSITY_SPLIT"
    assert split["split_reason"] == "pre_high_density_training_window_has_no_nonzero_peer_nlp_signal"
    assert len(train) >= 3
    assert len(test) >= 30
    assert train["date"].max() < test["date"].min()
    assert train["sector_sentiment_score"].abs().sum() > 0


def test_peer_state_feature_diagnostics_counts_signal_variation() -> None:
    train = _feature_rows("2025-01-01", 10, 0.2)
    test = _feature_rows("2025-02-01", 10, 0.3)
    diagnostics = _peer_state_feature_diagnostics(
        train,
        test,
        {"dqn_with_sector_peer_nlp": {"columns": ["price", "sector_sentiment_score"]}},
    )

    row = diagnostics[
        (diagnostics["experiment"] == "dqn_with_sector_peer_nlp")
        & (diagnostics["period"] == "train")
        & (diagnostics["state_column"] == "sector_sentiment_score")
    ].iloc[0]
    assert row["nonzero_count"] == 10
    assert bool(row["is_peer_nlp_signal"]) is True
