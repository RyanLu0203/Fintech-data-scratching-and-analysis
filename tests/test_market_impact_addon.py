from __future__ import annotations

from pathlib import Path

from src.evaluation.market_impact_ablation import EXPERIMENT_SIGNAL_COLUMNS


def test_market_impact_add_on_defines_five_dqn_groups_only() -> None:
    assert list(EXPERIMENT_SIGNAL_COLUMNS) == [
        "dqn_without_nlp",
        "dqn_with_sector_sentiment_nlp",
        "dqn_with_marketwide_sentiment_nlp",
        "dqn_with_sector_impact_nlp",
        "dqn_with_marketwide_impact_nlp",
    ]
    assert "buy_and_hold" not in EXPERIMENT_SIGNAL_COLUMNS


def test_dashboard_uses_market_impact_checkbox_add_on_copy() -> None:
    source = Path("src/dashboard/streamlit_app.py").read_text(encoding="utf-8")

    assert 'st.sidebar.checkbox(\n    "Add market-impact NLP groups"' in source
    assert '"Experiment type"' not in source
    assert "基础三组 DQN 对比" in source
    assert "五组 DQN 对比" in source
    assert "Market-Impact Add-on: Five-Group Comparison" in source


def test_pipeline_summary_names_market_impact_as_add_on() -> None:
    source = Path("main.py").read_text(encoding="utf-8")

    assert "peer_sentiment_plus_market_impact" in source
    assert "Add the two peer market-impact NLP DQN groups" in source
