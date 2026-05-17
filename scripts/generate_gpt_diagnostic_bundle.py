"""Generate a portable GPT diagnostic bundle from cached experiment outputs."""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

from src.config.paths import OUTPUTS_ROOT, PROJECT_ROOT, SYSTEM_OUTPUT_DIR, stock_data_dir, stock_reports_dir, stock_results_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create GPT-ready diagnostics for one target symbol.")
    parser.add_argument("--target-symbol", default="002475")
    parser.add_argument("--target-company", default="立讯精密")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbol = str(args.target_symbol).zfill(6)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bundle_dir = SYSTEM_OUTPUT_DIR / "gpt_diagnostics" / f"gpt_diagnostic_{symbol}_{timestamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    zip_path = bundle_dir.with_suffix(".zip")

    paths = _collect_paths(symbol)
    summary = _summary_markdown(symbol, args.target_company, timestamp, paths)
    summary_path = bundle_dir / "GPT_DIAGNOSTIC_SUMMARY.md"
    manifest_path = bundle_dir / "MANIFEST.json"
    summary_path.write_text(summary, encoding="utf-8")
    manifest = {
        "target_symbol": symbol,
        "target_company": args.target_company,
        "generated_at": timestamp,
        "summary": str(summary_path.relative_to(PROJECT_ROOT)),
        "zip": str(zip_path.relative_to(PROJECT_ROOT)),
        "included_files": [str(path.relative_to(PROJECT_ROOT)) for path in paths if path.exists()],
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.write(summary_path, "GPT_DIAGNOSTIC_SUMMARY.md")
        bundle.write(manifest_path, "MANIFEST.json")
        for path in paths:
            if path.exists() and path.is_file():
                bundle.write(path, str(path.relative_to(PROJECT_ROOT)))

    print(summary_path)
    print(zip_path)


def _collect_paths(symbol: str) -> list[Path]:
    data_dir = stock_data_dir(symbol)
    reports_dir = stock_reports_dir(symbol)
    results_dir = stock_results_dir(symbol)
    paths: list[Path] = []
    paths.extend(sorted(data_dir.glob(f"{symbol}_finance_text*.csv")))
    paths.extend(sorted(data_dir.glob(f"{symbol}_diagnostic*.json")))
    for name in [
        "market_impact_ablation_metrics.csv",
        "market_impact_ablation_metrics_by_seed.csv",
        "market_impact_portfolio_curves.csv",
        "market_impact_drawdown_curves.csv",
        "market_impact_trading_logs.csv",
        "market_impact_training_rewards_all_seeds.csv",
        "market_impact_effect_summary.csv",
        "peer_market_impact_daily_signal.csv",
        "peer_market_impact_item_signal.csv",
        "peer_nlp_ablation_metrics.csv",
        "peer_nlp_ablation_metrics_by_seed.csv",
        "peer_nlp_daily_sentiment.csv",
        "peer_nlp_item_sentiment.csv",
        "peer_nlp_portfolio_curves.csv",
        "peer_nlp_trading_logs.csv",
        "peer_nlp_effect_summary.csv",
    ]:
        paths.append(results_dir / name)
    for name in [
        "information_density_split.csv",
        "daily_news_density.csv",
        "market_impact_group_state_diagnostics.csv",
        "market_impact_reliability_check.csv",
        "market_impact_state_vector_compliance.csv",
        "market_impact_train_eval_windows.csv",
        "peer_nlp_group_state_diagnostics.csv",
        "peer_nlp_integrity_check.csv",
        "peer_nlp_state_vector_compliance.csv",
        "peer_nlp_train_eval_windows.csv",
        "signal_validity_summary.csv",
    ]:
        paths.append(reports_dir / name)
    for name in [
        "model_upgrade_summary.csv",
        "seed_level_metrics.csv",
        "action_distribution_diagnostics.csv",
        "reward_variant_comparison.csv",
        "state_feature_diagnostics.csv",
        "upgrade_run_log.md",
    ]:
        paths.append(OUTPUTS_ROOT / "model_upgrade" / name)
    paths.extend(sorted((PROJECT_ROOT / "reports" / "tables").glob("*corpus_summary.csv")))
    for name in [
        "peer_nlp_cross_stock_summary.csv",
        "market_impact_cross_stock_summary.csv",
        "latest_peer_training_progress.csv",
    ]:
        paths.append(SYSTEM_OUTPUT_DIR / name)
    return _dedupe(paths)


def _summary_markdown(symbol: str, company: str, timestamp: str, paths: list[Path]) -> str:
    sections = [
        f"# GPT Diagnostic Bundle for {symbol}",
        "",
        f"- Generated at: `{timestamp}`",
        f"- Target company: `{company}`",
        "- Purpose: diagnose official sector/marketwide NLP + DQN outputs and the new DQN model-upgrade grid.",
        "- Official logic: sector sentiment + sector impact are default; marketwide sentiment + marketwide impact are add-on groups; buy-and-hold is benchmark only.",
        "",
    ]
    sections.extend(_table_section("Official Market-Impact Ablation Metrics", stock_results_dir(symbol) / "market_impact_ablation_metrics.csv", limit=20))
    sections.extend(_table_section("Market-Impact Effect Summary", stock_results_dir(symbol) / "market_impact_effect_summary.csv", limit=5))
    sections.extend(_table_section("Market-Impact Reliability Check", stock_reports_dir(symbol) / "market_impact_reliability_check.csv", limit=20))
    sections.extend(_table_section("Model Upgrade Summary", OUTPUTS_ROOT / "model_upgrade" / "model_upgrade_summary.csv", limit=20))
    sections.extend(_table_section("Model Upgrade Seed-Level Metrics", OUTPUTS_ROOT / "model_upgrade" / "seed_level_metrics.csv", limit=20))
    sections.extend(_table_section("Model Upgrade Action Diagnostics", OUTPUTS_ROOT / "model_upgrade" / "action_distribution_diagnostics.csv", limit=20))
    sections.extend(_table_section("Reward Variant Comparison", OUTPUTS_ROOT / "model_upgrade" / "reward_variant_comparison.csv", limit=20))
    sections.extend(_table_section("Information Density Split", stock_reports_dir(symbol) / "information_density_split.csv", limit=5))
    sections.extend(_derived_diagnostics(symbol))
    sections.extend(
        [
            "## Included Files",
            "",
            *[f"- `{path.relative_to(PROJECT_ROOT)}`" for path in paths if path.exists()],
            "",
            "## Suggested Questions for GPT",
            "",
            "1. Do the model-upgrade variants reduce Hold-heavy policy collapse versus the official baseline?",
            "2. Which model/reward/state combination improves trade count, exposure, final equity, and Sharpe without overtrading?",
            "3. Are sector and marketwide NLP signals distinct enough to justify both add-on groups?",
            "4. Does normalized_plus add useful signal or introduce instability relative to official_8d?",
            "5. Are the remaining weak strategies explainable by signal quality, reward design, or DQN exploration?",
            "",
        ]
    )
    return "\n".join(sections)


def _table_section(title: str, path: Path, limit: int = 10) -> list[str]:
    if not path.exists():
        return [f"## {title}", "", f"Missing file: `{path.relative_to(PROJECT_ROOT)}`", ""]
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        return [f"## {title}", "", f"Could not read `{path.relative_to(PROJECT_ROOT)}`: {exc}", ""]
    lines = [f"## {title}", "", f"File: `{path.relative_to(PROJECT_ROOT)}`; rows={len(frame)}, columns={len(frame.columns)}", ""]
    if frame.empty:
        return [*lines, "No rows.", ""]
    display = frame.head(limit).copy()
    lines.append(display.to_markdown(index=False))
    lines.append("")
    return lines


def _derived_diagnostics(symbol: str) -> list[str]:
    lines = ["## Derived Diagnostics", ""]
    logs = _read_csv(stock_results_dir(symbol) / "market_impact_trading_logs.csv")
    if not logs.empty and {"experiment", "action"}.issubset(logs.columns):
        action_counts = logs.groupby(["experiment", "action"]).size().unstack(fill_value=0)
        lines.extend(["### Official Action Distribution", "", action_counts.to_markdown(), ""])
    upgrade_actions = _read_csv(OUTPUTS_ROOT / "model_upgrade" / "action_distribution_diagnostics.csv")
    if not upgrade_actions.empty:
        grouped = upgrade_actions.groupby(["model_variant", "experiment"])[["hold_ratio", "buy_ratio", "sell_ratio"]].mean().reset_index()
        lines.extend(["### Model Upgrade Mean Action Ratios", "", grouped.to_markdown(index=False), ""])
    daily = _read_csv(stock_results_dir(symbol) / "peer_market_impact_daily_signal.csv")
    comparisons = []
    for left, right, label in [
        ("sector_sentiment_score", "marketwide_sentiment_score", "sector_sentiment vs marketwide_sentiment"),
        ("sector_impact_score", "marketwide_impact_score", "sector_impact vs marketwide_impact"),
    ]:
        if {left, right}.issubset(daily.columns):
            l = pd.to_numeric(daily[left], errors="coerce").fillna(0)
            r = pd.to_numeric(daily[right], errors="coerce").fillna(0)
            comparisons.append({"comparison": label, "exactly_equal": bool(l.equals(r)), "correlation": float(l.corr(r))})
    if comparisons:
        lines.extend(["### Sector vs Marketwide Signal Difference", "", pd.DataFrame(comparisons).to_markdown(index=False), ""])
    return lines


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _dedupe(paths: list[Path]) -> list[Path]:
    seen = set()
    out = []
    for path in paths:
        key = path.resolve()
        if key not in seen:
            out.append(path)
            seen.add(key)
    return out


if __name__ == "__main__":
    main()
