from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.evaluation.model_upgrade import _write_run_id_output
from src.evaluation.model_upgrade_diagnostics import (
    discover_model_upgrade_diagnostics,
    generate_model_upgrade_diagnostic_reports,
)


def test_diagnostic_discovery_loads_global_and_per_stock_paths(tmp_path: Path) -> None:
    global_dir = tmp_path / "outputs" / "model_upgrade"
    stock_dir = tmp_path / "outputs" / "stocks" / "601138" / "results" / "model_upgrade"
    global_dir.mkdir(parents=True)
    stock_dir.mkdir(parents=True)
    pd.DataFrame(
        [{"run_id": "002475_run_seed1", "experiment": "dqn_without_nlp", "seed": 1, "final_equity": 1_000_000}]
    ).to_csv(global_dir / "seed_level_metrics.csv", index=False)
    pd.DataFrame(
        [{"run_id": "601138_run_seed1", "experiment": "dqn_without_nlp", "seed": 1, "portfolio_after": 1_010_000}]
    ).to_csv(stock_dir / "env_dynamics_diagnostics.csv", index=False)

    bundle = discover_model_upgrade_diagnostics(tmp_path)

    assert bundle.target_symbols == ["002475", "601138"]
    loaded_paths = {item.path for item in bundle.loaded_files}
    assert any(path.endswith("outputs/model_upgrade/seed_level_metrics.csv") for path in loaded_paths)
    assert any(path.endswith("outputs/stocks/601138/results/model_upgrade/env_dynamics_diagnostics.csv") for path in loaded_paths)
    assert bundle.get("seed_level_metrics.csv")["target_symbol"].iloc[0] == "002475"
    assert bundle.get("env_dynamics_diagnostics.csv")["target_symbol"].iloc[0] == "601138"


def test_diagnostic_discovery_normalizes_short_a_share_symbols(tmp_path: Path) -> None:
    global_dir = tmp_path / "outputs" / "model_upgrade"
    global_dir.mkdir(parents=True)
    pd.DataFrame(
        [{"target_symbol": "2475", "run_id": "2475_quick_seed1", "experiment": "dqn_without_nlp"}]
    ).to_csv(global_dir / "model_upgrade_summary.csv", index=False)

    bundle = discover_model_upgrade_diagnostics(tmp_path)

    assert bundle.target_symbols == ["002475"]
    assert bundle.get("model_upgrade_summary.csv")["target_symbol"].iloc[0] == "002475"


def test_write_run_id_output_replaces_exact_run_id_only(tmp_path: Path) -> None:
    path = tmp_path / "seed_level_metrics.csv"
    pd.DataFrame(
        [
            {"run_id": "old_same", "target_symbol": "002475", "value": 1},
            {"run_id": "other_stock", "target_symbol": "601138", "value": 2},
        ]
    ).to_csv(path, index=False)

    _write_run_id_output(path, pd.DataFrame([{"run_id": "old_same", "target_symbol": "002475", "value": 3}]))

    result = pd.read_csv(path)
    assert result[result["run_id"] == "old_same"]["value"].tolist() == [3]
    assert result[result["run_id"] == "other_stock"]["value"].tolist() == [2]


def test_generate_diagnostic_reports_creates_readme_and_chatgpt_summary(tmp_path: Path) -> None:
    out = tmp_path / "outputs" / "model_upgrade"
    out.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "target_symbol": "002475",
                "experiment": "dqn_without_nlp",
                "run_mode": "quick_test",
                "model_variant": "vanilla_dqn",
                "reward_variant": "one_day_return",
                "state_feature_mode": "normalized_plus",
                "action_mode": "target_position",
                "seed": 1,
                "final_equity": 1_000_000,
                "sharpe_ratio": 0.1,
                "hold_ratio": 0.5,
                "action_buy_count": 1,
                "action_sell_count": 1,
                "average_exposure": 0.3,
                "unique_portfolio_values": 20,
            }
        ]
    ).to_csv(out / "seed_level_metrics.csv", index=False)

    paths = generate_model_upgrade_diagnostic_reports(tmp_path)

    assert paths["readme"].exists()
    assert paths["chatgpt_summary"].exists()
    assert "Hold-Heavy Policy Diagnosis" in paths["readme"].read_text(encoding="utf-8")
    assert "Specific Questions For Further Analysis" in paths["chatgpt_summary"].read_text(encoding="utf-8")
