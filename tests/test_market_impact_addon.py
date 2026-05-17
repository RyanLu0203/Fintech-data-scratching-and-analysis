from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.evaluation.market_impact_ablation import EXPERIMENT_SIGNAL_COLUMNS, _peer_sentiment_cache_matches_scope
from src.nlp.peer_sentiment import _marketwide_scope_ready


def test_official_market_impact_experiment_defines_ordered_dqn_groups_only() -> None:
    assert list(EXPERIMENT_SIGNAL_COLUMNS) == [
        "dqn_without_nlp",
        "dqn_with_sector_sentiment_nlp",
        "dqn_with_sector_impact_nlp",
        "dqn_with_marketwide_sentiment_nlp",
        "dqn_with_marketwide_impact_nlp",
    ]
    assert "buy_and_hold" not in EXPERIMENT_SIGNAL_COLUMNS


def test_dashboard_uses_marketwide_checkbox_without_label_tuning() -> None:
    source = Path("src/dashboard/streamlit_app.py").read_text(encoding="utf-8")

    assert 'st.sidebar.checkbox(\n    "Add marketwide peer benchmark groups"' in source
    assert '"Experiment type"' not in source
    assert '"Market-impact label settings"' not in source
    assert "sector-only 三组 DQN 对比" in source
    assert "五组 DQN 对比" in source
    assert "Official Sector Sentiment / Impact DQN Comparison" in source
    assert "CORE_EXPERIMENT_METRICS" in source
    assert "buy-and-hold 只是 benchmark" in source
    assert "run_market_impact_nlp = True" in source
    assert "include_marketwide_peer=add_marketwide_peer_groups" in source
    assert "density_from_peer_daily" in source
    assert "peer_nlp_information_density_split.csv" in source
    assert '"src.rl.train"' in source
    assert '"state_scaler" not in inspect.signature(rl_train_module.evaluate_agent).parameters' in source


def test_pipeline_summary_names_marketwide_as_add_on() -> None:
    source = Path("main.py").read_text(encoding="utf-8")

    assert "sector_sentiment_impact_plus_marketwide" in source
    assert '"marketwide_peer_add_on": bool(include_marketwide_peer)' in source
    assert '"dqn_group_count": 5 if (run_market_impact_nlp and include_marketwide_peer) else 3' in source


def test_marketwide_cache_rejects_old_sector_only_scope(tmp_path: Path) -> None:
    stale = tmp_path / "peer_nlp_daily_sentiment.csv"
    pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "marketwide_corpus_status": "READY",
                "marketwide_peer_stock_count": 6,
                "marketwide_peer_sector_count": 1,
                "marketwide_distinct_from_sector": 0,
            }
        ]
    ).to_csv(stale, index=False)

    assert not _peer_sentiment_cache_matches_scope(stale, include_marketwide_peer=True)

    fresh = tmp_path / "fresh_peer_nlp_daily_sentiment.csv"
    pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "marketwide_corpus_status": "READY",
                "marketwide_peer_stock_count": 12,
                "marketwide_peer_sector_count": 4,
                "marketwide_distinct_from_sector": 1,
            }
        ]
    ).to_csv(fresh, index=False)

    assert _peer_sentiment_cache_matches_scope(fresh, include_marketwide_peer=True)


def test_marketwide_scope_requires_cross_sector_peers() -> None:
    same_sector = pd.DataFrame(
        {
            "symbol": [f"{idx:06d}" for idx in range(12)],
            "sector": ["electronics"] * 12,
        }
    )
    cross_sector = pd.DataFrame(
        {
            "symbol": [f"{idx:06d}" for idx in range(12)],
            "sector": ["electronics", "banking", "software", "pharma"] * 3,
        }
    )

    assert not _marketwide_scope_ready(same_sector, "electronics")
    assert _marketwide_scope_ready(cross_sector, "electronics")
