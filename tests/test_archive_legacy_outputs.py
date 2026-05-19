from __future__ import annotations

from pathlib import Path

from scripts.archive_legacy_official_outputs import run_archive


def test_archive_dry_run_writes_plan_and_moves_nothing(tmp_path: Path) -> None:
    results = tmp_path / "outputs" / "stocks" / "002475" / "results"
    model_upgrade = tmp_path / "outputs" / "model_upgrade"
    results.mkdir(parents=True)
    model_upgrade.mkdir(parents=True)
    legacy = results / "peer_nlp_portfolio_curves.csv"
    preserved = results / "stock_profile.csv"
    upgrade = model_upgrade / "seed_level_metrics.csv"
    legacy.write_text("legacy\n", encoding="utf-8")
    preserved.write_text("preserved\n", encoding="utf-8")
    upgrade.write_text("upgrade\n", encoding="utf-8")

    plan = run_archive(apply=False, project_root=tmp_path, timestamp="20260101_000000")

    assert legacy.exists()
    assert preserved.exists()
    assert upgrade.exists()
    assert (plan.archive_dir / "ARCHIVE_PLAN.md").exists()
    assert not (plan.archive_dir / "stocks" / "002475" / "results" / legacy.name).exists()


def test_archive_apply_moves_only_legacy_outputs(tmp_path: Path) -> None:
    results = tmp_path / "outputs" / "stocks" / "601138" / "results"
    data = tmp_path / "outputs" / "stocks" / "601138" / "data"
    results.mkdir(parents=True)
    data.mkdir(parents=True)
    legacy = results / "market_impact_ablation_metrics.csv"
    preserved_result = results / "manual_review_notes.csv"
    preserved_data = data / "601138_final.csv"
    legacy.write_text("legacy\n", encoding="utf-8")
    preserved_result.write_text("preserved\n", encoding="utf-8")
    preserved_data.write_text("data\n", encoding="utf-8")

    plan = run_archive(apply=True, project_root=tmp_path, timestamp="20260101_000001")

    archived = plan.archive_dir / "stocks" / "601138" / "results" / legacy.name
    assert archived.exists()
    assert not legacy.exists()
    assert preserved_result.exists()
    assert preserved_data.exists()
    assert (plan.archive_dir / "ARCHIVE_LOG.md").exists()
