from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.evaluation.model_upgrade import NORMALIZED_PLUS_COLUMNS, _model_summary, _reward_comparison


def test_normalized_plus_contains_requested_nlp_derived_features() -> None:
    assert "abs_nlp_signal" in NORMALIZED_PLUS_COLUMNS
    assert "nlp_signal_3day_mean" in NORMALIZED_PLUS_COLUMNS
    assert "nlp_signal_5day_mean" in NORMALIZED_PLUS_COLUMNS
    assert "nlp_signal_change" in NORMALIZED_PLUS_COLUMNS
    assert "relative_signal" in NORMALIZED_PLUS_COLUMNS
    assert "marketwide_residual_signal" in NORMALIZED_PLUS_COLUMNS


def test_model_upgrade_summary_schema() -> None:
    seed = pd.DataFrame(
        [
            {
                "run_id": "002475_vanilla_dqn_one_day_return_official_8d_seed1",
                "target_symbol": "002475",
                "experiment": "dqn_without_nlp",
                "model_variant": "vanilla_dqn",
                "reward_variant": "one_day_return",
                "state_feature_mode": "official_8d",
                "seed": 1,
                "final_equity": 1_010_000,
                "cumulative_return": 0.01,
                "sharpe_ratio": 0.5,
                "max_drawdown": 0.02,
                "number_of_trades": 2,
                "exposure_ratio": 0.4,
                "turnover": 0.01,
            }
        ]
    )
    summary = _model_summary(seed)
    expected = {
        "seed_count",
        "mean_final_equity",
        "std_final_equity",
        "mean_cumulative_return",
        "std_cumulative_return",
        "mean_sharpe_ratio",
        "std_sharpe_ratio",
        "mean_max_drawdown",
        "mean_number_of_trades",
        "mean_exposure_ratio",
        "mean_turnover",
    }
    assert expected.issubset(summary.columns)
    reward = _reward_comparison(seed)
    assert {"reward_variant", "mean_final_equity", "mean_exposure_ratio"}.issubset(reward.columns)


def test_dashboard_has_model_upgrade_missing_file_fallback() -> None:
    source = Path("src/dashboard/streamlit_app.py").read_text(encoding="utf-8")
    assert "Model Upgrade Diagnostics" in source
    assert "No model upgrade results found. Run scripts/run_model_upgrade_grid.py first." in source
    assert "model_upgrade_summary.csv" in source
    assert "action_distribution_diagnostics.csv" in source
