"""Model-upgrade diagnostic discovery and markdown reporting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import numpy as np
import pandas as pd

from src.config.paths import OUTPUTS_ROOT, normalize_symbol_for_path
from src.config.settings import PROJECT_ROOT


MODEL_UPGRADE_FILENAMES = [
    "model_upgrade_summary.csv",
    "seed_level_metrics.csv",
    "env_dynamics_diagnostics.csv",
    "action_distribution_diagnostics.csv",
    "q_value_diagnostics.csv",
    "exploration_diagnostics.csv",
    "reward_diagnostics.csv",
    "portfolio_curves.csv",
    "nlp_incremental_effect.csv",
    "nlp_incremental_effect_summary.csv",
    "reward_variant_comparison.csv",
    "state_feature_diagnostics.csv",
]


@dataclass(frozen=True)
class LoadedDiagnosticFile:
    filename: str
    path: str
    row_count: int
    column_count: int
    target_symbols: tuple[str, ...]


@dataclass
class ModelUpgradeDiagnosticsBundle:
    frames: dict[str, pd.DataFrame]
    checked_paths: list[str]
    loaded_files: list[LoadedDiagnosticFile]
    warnings: list[str]

    @property
    def found_any(self) -> bool:
        return any(not frame.empty for frame in self.frames.values())

    @property
    def target_symbols(self) -> list[str]:
        symbols: set[str] = set()
        for frame in self.frames.values():
            if not frame.empty and "target_symbol" in frame.columns:
                symbols.update(frame["target_symbol"].dropna().astype(str).map(_normalize_symbol).tolist())
        return sorted(symbol for symbol in symbols if symbol)

    def get(self, filename: str) -> pd.DataFrame:
        return self.frames.get(filename, pd.DataFrame())


def discover_model_upgrade_diagnostics(base_dir: Path | None = None) -> ModelUpgradeDiagnosticsBundle:
    root = Path(base_dir) if base_dir is not None else PROJECT_ROOT
    output_root = root / "outputs"
    checked_paths: list[str] = []
    warnings: list[str] = []
    frames: dict[str, list[pd.DataFrame]] = {name: [] for name in MODEL_UPGRADE_FILENAMES}
    loaded_files: list[LoadedDiagnosticFile] = []

    candidates_by_name = _candidate_paths_by_name(output_root, checked_paths)
    for filename, paths in candidates_by_name.items():
        for path in paths:
            if not path.exists() or not path.is_file():
                continue
            try:
                frame = pd.read_csv(path)
            except pd.errors.EmptyDataError:
                frame = pd.DataFrame()
            except Exception as exc:  # pragma: no cover - defensive dashboard/reporting path
                warnings.append(f"Could not load {path}: {exc}")
                continue
            frame = _ensure_target_symbol(frame, path, warnings)
            frame = _ensure_target_company(frame)
            frames[filename].append(frame)
            loaded_files.append(
                LoadedDiagnosticFile(
                    filename=filename,
                    path=_display_path(path),
                    row_count=int(len(frame)),
                    column_count=int(len(frame.columns)),
                    target_symbols=tuple(_frame_target_symbols(frame)),
                )
            )

    merged: dict[str, pd.DataFrame] = {}
    for filename, parts in frames.items():
        if parts:
            merged_frame = pd.concat(parts, ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)
        else:
            merged_frame = pd.DataFrame()
        merged[filename] = merged_frame
    return ModelUpgradeDiagnosticsBundle(
        frames=merged,
        checked_paths=checked_paths,
        loaded_files=loaded_files,
        warnings=warnings,
    )


def generate_model_upgrade_diagnostic_reports(base_dir: Path | None = None) -> dict[str, Path]:
    root = Path(base_dir) if base_dir is not None else PROJECT_ROOT
    output_dir = root / "outputs" / "model_upgrade"
    report_dir = output_dir / "diagnostic_reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    bundle = discover_model_upgrade_diagnostics(root)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    full_text = build_diagnostic_readme(bundle, timestamp=timestamp)
    compact_text = build_chatgpt_diagnostic_summary(bundle, timestamp=timestamp)

    latest = output_dir / "README_diagnostics.md"
    latest_compact = output_dir / "CHATGPT_DIAGNOSTIC_SUMMARY.md"
    timestamped = report_dir / f"model_upgrade_diagnostic_{timestamp}.md"
    latest.write_text(full_text, encoding="utf-8")
    latest_compact.write_text(compact_text, encoding="utf-8")
    timestamped.write_text(full_text, encoding="utf-8")

    paths: dict[str, Path] = {
        "readme": latest,
        "chatgpt_summary": latest_compact,
        "timestamped_report": timestamped,
    }
    for symbol in bundle.target_symbols:
        stock_bundle = _filter_bundle_by_symbol(bundle, symbol)
        stock_path = report_dir / f"{symbol}_diagnostic_{timestamp}.md"
        stock_path.write_text(build_diagnostic_readme(stock_bundle, timestamp=timestamp), encoding="utf-8")
        paths[f"{symbol}_report"] = stock_path
    return paths


def build_diagnostic_readme(bundle: ModelUpgradeDiagnosticsBundle, *, timestamp: str) -> str:
    sections = [
        "# Model Upgrade Diagnostic Report",
        "",
        *_run_overview(bundle, timestamp=timestamp),
        *_data_availability(bundle),
        *_cross_stock_summary(bundle),
        *_model_upgrade_summary(bundle),
        *_hold_heavy_diagnosis(bundle),
        *_trade_behavior_summary(bundle),
        *_q_value_diagnosis(bundle),
        *_reward_diagnosis(bundle),
        *_nlp_incremental_effect(bundle),
        *_benchmark_comparison(bundle),
        *_stability_across_seeds(bundle),
        *_key_findings(bundle),
        *_recommended_next_experiments(bundle),
        *_report_ready_paragraph(bundle),
        *_files_generated(bundle),
    ]
    return "\n".join(sections).rstrip() + "\n"


def build_chatgpt_diagnostic_summary(bundle: ModelUpgradeDiagnosticsBundle, *, timestamp: str) -> str:
    lines = [
        "# ChatGPT Diagnostic Summary",
        "",
        f"- generated_timestamp: {timestamp}",
        f"- run_mode: {', '.join(_unique_values(bundle.get('seed_level_metrics.csv'), 'run_mode')) or 'UNKNOWN'}",
        f"- detected_target_symbols: {', '.join(bundle.target_symbols) or 'NONE'}",
        "",
        "## Files Loaded And Missing",
        _loaded_files_table(bundle, compact=True),
        "",
        "## Top Configurations",
        _top_configs_table(bundle, limit=5),
        "",
        "## Hold-Heavy Diagnosis",
        _hold_heavy_table(bundle, limit=20),
        "",
        "## Trade Behavior Summary",
        _trade_behavior_table(bundle, limit=20),
        "",
        "## Q-Value Diagnosis",
        _q_value_table(bundle, limit=20),
        "",
        "## NLP Incremental Effect Diagnosis",
        _nlp_effect_text(bundle),
        "",
        "## Benchmark Comparison",
        _benchmark_text(bundle),
        "",
        "## Stability Across Seeds",
        _stability_table(bundle, limit=20),
        "",
        "## Key Findings",
        *_key_findings(bundle, compact=True),
        "",
        "## Recommended Next Experiments",
        *_recommended_next_experiments(bundle, compact=True),
        "",
        "## Specific Questions For Further Analysis",
        "- Which model/reward/state combination should be promoted beyond medium-test?",
        "- Are Q-value margins high enough to trust the selected actions?",
        "- Which NLP group improves over no-NLP on paired seeds, if any?",
        "- Are any stocks still hold-heavy despite target-position actions?",
        "- Should the next run prioritize full-grid seeds or reward diagnostics?",
    ]
    return "\n".join(lines).rstrip() + "\n"


def _candidate_paths_by_name(output_root: Path, checked_paths: list[str]) -> dict[str, list[Path]]:
    by_name: dict[str, list[Path]] = {name: [] for name in MODEL_UPGRADE_FILENAMES}
    for filename in MODEL_UPGRADE_FILENAMES:
        candidates = [
            output_root / "model_upgrade" / filename,
            *sorted((output_root / "stocks").glob(f"*/model_upgrade/{filename}")),
            *sorted((output_root / "stocks").glob(f"*/results/model_upgrade/{filename}")),
        ]
        for path in candidates:
            display = _display_path(path)
            checked_paths.append(display)
            if path not in by_name[filename]:
                by_name[filename].append(path)
    return by_name


def _ensure_target_symbol(frame: pd.DataFrame, path: Path, warnings: list[str]) -> pd.DataFrame:
    out = frame.copy()
    if "target_symbol" in out.columns and out["target_symbol"].notna().any():
        out["target_symbol"] = out["target_symbol"].astype(str).map(_normalize_symbol)
        return out
    inferred = _infer_symbol_from_path(path)
    if inferred is None and "run_id" in out.columns and out["run_id"].notna().any():
        inferred = out["run_id"].astype(str).map(_infer_symbol_from_run_id)
    if isinstance(inferred, pd.Series):
        out["target_symbol"] = inferred.fillna("UNKNOWN").map(_normalize_symbol)
        if (out["target_symbol"] == "UNKNOWN").any():
            warnings.append(f"Some target_symbol values could not be inferred for {_display_path(path)}.")
    elif inferred:
        out["target_symbol"] = _normalize_symbol(inferred)
    else:
        out["target_symbol"] = "UNKNOWN"
        warnings.append(f"target_symbol missing and could not be inferred for {_display_path(path)}.")
    return out


def _ensure_target_company(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "target_company" not in out.columns:
        out["target_company"] = ""
    return out


def _normalize_symbol(value: object) -> str:
    text = str(value).strip()
    if text in {"", "nan", "None", "UNKNOWN"}:
        return "UNKNOWN" if text == "UNKNOWN" else ""
    if re.fullmatch(r"\d{1,6}", text):
        return text.zfill(6)
    return normalize_symbol_for_path(text)


def _infer_symbol_from_path(path: Path) -> str | None:
    parts = list(path.parts)
    if "stocks" in parts:
        index = parts.index("stocks")
        if index + 1 < len(parts):
            candidate = parts[index + 1]
            if re.fullmatch(r"\d{1,6}", candidate):
                return candidate.zfill(6)
    return None


def _infer_symbol_from_run_id(run_id: object) -> str | None:
    first = str(run_id).split("_", 1)[0].strip()
    if re.fullmatch(r"\d{1,6}", first):
        return first.zfill(6)
    return None


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _frame_target_symbols(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "target_symbol" not in frame.columns:
        return []
    return sorted(symbol for symbol in frame["target_symbol"].dropna().astype(str).map(_normalize_symbol).unique().tolist() if symbol)


def _unique_values(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    return sorted(str(value) for value in frame[column].dropna().unique().tolist())


def _filter_bundle_by_symbol(bundle: ModelUpgradeDiagnosticsBundle, symbol: str) -> ModelUpgradeDiagnosticsBundle:
    normalized = _normalize_symbol(symbol)
    filtered: dict[str, pd.DataFrame] = {}
    for filename, frame in bundle.frames.items():
        if not frame.empty and "target_symbol" in frame.columns:
            filtered[filename] = frame[frame["target_symbol"].astype(str).map(_normalize_symbol) == normalized].copy()
        else:
            filtered[filename] = frame.copy()
    loaded = [
        item
        for item in bundle.loaded_files
        if not item.target_symbols or normalized in {_normalize_symbol(symbol) for symbol in item.target_symbols}
    ]
    return ModelUpgradeDiagnosticsBundle(
        frames=filtered,
        checked_paths=bundle.checked_paths,
        loaded_files=loaded,
        warnings=bundle.warnings,
    )


def _run_overview(bundle: ModelUpgradeDiagnosticsBundle, *, timestamp: str) -> list[str]:
    seed = bundle.get("seed_level_metrics.csv")
    return [
        "## 1. Run Overview",
        f"- generated timestamp: {timestamp}",
        f"- run_mode: {', '.join(_unique_values(seed, 'run_mode')) or 'UNKNOWN'}",
        f"- episodes: {', '.join(_unique_values(seed, 'episodes')) or 'UNKNOWN'}",
        f"- max_seeds: {', '.join(_unique_values(seed, 'max_seeds')) or 'UNKNOWN'}",
        f"- target_symbol list: {', '.join(bundle.target_symbols) or 'NONE'}",
        f"- target_company list: {', '.join(_unique_values(seed, 'target_company')) or 'UNKNOWN'}",
        f"- action_mode list: {', '.join(_unique_values(seed, 'action_mode')) or 'UNKNOWN'}",
        f"- model_variant list: {', '.join(_unique_values(seed, 'model_variant')) or 'UNKNOWN'}",
        f"- reward_variant list: {', '.join(_unique_values(seed, 'reward_variant')) or 'UNKNOWN'}",
        f"- state_feature_mode list: {', '.join(_unique_values(seed, 'state_feature_mode')) or 'UNKNOWN'}",
        f"- data source paths used: {len(bundle.loaded_files)} file(s) loaded",
        f"- validation status: see upgrade_run_log.md if available",
        "",
    ]


def _data_availability(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    lines = ["## 2. Data Availability Check", _loaded_files_table(bundle), ""]
    if bundle.warnings:
        lines.extend(["Warnings:", *[f"- {warning}" for warning in bundle.warnings], ""])
    return lines


def _loaded_files_table(bundle: ModelUpgradeDiagnosticsBundle, *, compact: bool = False) -> str:
    rows = []
    loaded_by_name = {item.filename: item for item in bundle.loaded_files}
    for filename in MODEL_UPGRADE_FILENAMES:
        matches = [item for item in bundle.loaded_files if item.filename == filename]
        if matches:
            for item in matches:
                rows.append(
                    {
                        "file": filename,
                        "exists": "yes",
                        "path": item.path,
                        "rows": item.row_count,
                        "columns": item.column_count,
                        "targets": ", ".join(item.target_symbols),
                    }
                )
        elif not compact:
            rows.append({"file": filename, "exists": "no", "path": "", "rows": 0, "columns": 0, "targets": ""})
    return _markdown_table(pd.DataFrame(rows))


def _cross_stock_summary(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    seed = bundle.get("seed_level_metrics.csv")
    env = bundle.get("env_dynamics_diagnostics.csv")
    q = bundle.get("q_value_diagnostics.csv")
    rows = []
    if not seed.empty:
        for symbol, part in seed.groupby("target_symbol", dropna=False):
            rows.append(
                {
                    "target_symbol": symbol,
                    "runs": len(part),
                    "experiments": ", ".join(_unique_values(part, "experiment")),
                    "best_final_equity_experiment": _best_by(part, "final_equity"),
                    "best_sharpe_experiment": _best_by(part, "sharpe_ratio"),
                    "average_exposure": _mean(part, "average_exposure"),
                    "average_trades": _mean(part, "number_of_trades"),
                    "hold_ratio": _mean(part, "hold_ratio"),
                    "trade_diagnostics": "yes" if not env.empty and str(symbol) in set(env["target_symbol"].astype(str)) else "no",
                    "q_diagnostics": "yes" if not q.empty and str(symbol) in set(q["target_symbol"].astype(str)) else "no",
                }
            )
    return ["## 3. Cross-Stock Summary", _markdown_table(pd.DataFrame(rows)) if rows else "No seed-level metrics available.", ""]


def _model_upgrade_summary(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 4. Model Upgrade Summary", _top_configs_table(bundle, limit=10), ""]


def _top_configs_table(bundle: ModelUpgradeDiagnosticsBundle, *, limit: int) -> str:
    summary = bundle.get("model_upgrade_summary.csv")
    if summary.empty:
        return "model_upgrade_summary.csv is missing or empty."
    sort_col = "mean_final_equity" if "mean_final_equity" in summary.columns else None
    top = summary.copy()
    if sort_col:
        top[sort_col] = pd.to_numeric(top[sort_col], errors="coerce")
        top = top.sort_values(sort_col, ascending=False)
    cols = [
        col
        for col in [
            "target_symbol",
            "experiment",
            "model_variant",
            "reward_variant",
            "state_feature_mode",
            "action_mode",
            "seed_count",
            "mean_final_equity",
            "mean_cumulative_return",
            "mean_sharpe_ratio",
            "mean_max_drawdown",
            "mean_number_of_trades",
            "mean_average_exposure",
            "mean_unique_portfolio_values",
        ]
        if col in top.columns
    ]
    return _markdown_table(top[cols].head(limit))


def _hold_heavy_diagnosis(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 5. Hold-Heavy Policy Diagnosis", _hold_heavy_table(bundle, limit=200), ""]


def _hold_heavy_table(bundle: ModelUpgradeDiagnosticsBundle, *, limit: int) -> str:
    seed = bundle.get("seed_level_metrics.csv")
    if seed.empty:
        return "seed_level_metrics.csv is missing or empty."
    rows = []
    group_cols = [col for col in ["target_symbol", "experiment", "model_variant", "reward_variant", "state_feature_mode", "action_mode"] if col in seed.columns]
    for keys, part in seed.groupby(group_cols, dropna=False):
        key_map = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        hold_ratio = _mean(part, "hold_ratio")
        buy_count = _mean(part, "action_buy_count")
        sell_count = _mean(part, "action_sell_count")
        exposure = _mean(part, "average_exposure")
        unique_values = _mean(part, "unique_portfolio_values")
        failures = sum(
            [
                bool(pd.notna(hold_ratio) and hold_ratio > 0.90),
                bool(pd.notna(exposure) and exposure < 0.10),
                bool(pd.notna(buy_count) and buy_count == 0),
                bool(pd.notna(sell_count) and sell_count == 0),
                bool(pd.notna(unique_values) and unique_values <= 10),
            ]
        )
        label = "NOT_HOLD_HEAVY" if failures == 0 else "PARTIALLY_HOLD_HEAVY" if failures <= 2 else "HOLD_HEAVY"
        rows.append({**key_map, "avg_hold_ratio": hold_ratio, "avg_buy_count": buy_count, "avg_sell_count": sell_count, "avg_exposure": exposure, "avg_unique_portfolio_values": unique_values, "label": label})
    return _markdown_table(pd.DataFrame(rows).head(limit))


def _trade_behavior_summary(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 6. Trade Behavior Summary", _trade_behavior_table(bundle, limit=200), ""]


def _trade_behavior_table(bundle: ModelUpgradeDiagnosticsBundle, *, limit: int) -> str:
    env = bundle.get("env_dynamics_diagnostics.csv")
    if env.empty:
        return "env_dynamics_diagnostics.csv is missing or empty."
    label_col = "mapped_action_label" if "mapped_action_label" in env.columns else "action"
    rows = []
    group_cols = [col for col in ["target_symbol", "experiment", "model_variant", "reward_variant", "state_feature_mode", "action_mode"] if col in env.columns]
    for keys, part in env.groupby(group_cols, dropna=False):
        key_map = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        labels = part[label_col].astype(str)
        rows.append(
            {
                **key_map,
                "buy_count": int((labels == "Buy").sum()),
                "sell_count": int((labels == "Sell").sum()),
                "hold_count": int((labels == "Hold").sum()),
                "avg_exposure": _mean(part, "current_exposure_after"),
                "max_exposure": _max(part, "current_exposure_after"),
                "min_exposure": _min(part, "current_exposure_after"),
                "avg_transaction_cost": _mean(part, "transaction_cost"),
                "final_portfolio_value": _last_numeric(part, "portfolio_after"),
                "total_return": _total_return(part),
                "number_of_trades": int((labels.isin(["Buy", "Sell"])).sum()),
            }
        )
    return _markdown_table(pd.DataFrame(rows).head(limit))


def _q_value_diagnosis(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 7. Q-Value Decision Diagnosis", _q_value_table(bundle, limit=200), ""]


def _q_value_table(bundle: ModelUpgradeDiagnosticsBundle, *, limit: int) -> str:
    q = bundle.get("q_value_diagnostics.csv")
    if q.empty:
        return "Q_VALUE_DIAGNOSTICS_MISSING"
    rows = []
    group_cols = [col for col in ["target_symbol", "experiment", "model_variant", "reward_variant", "state_feature_mode", "action_mode"] if col in q.columns]
    for keys, part in q.groupby(group_cols, dropna=False):
        key_map = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        margin = _mean(part, "q_value_margin")
        action0_ratio = float((part.get("raw_action_id", pd.Series(dtype=object)).astype(str) == "0").mean()) if "raw_action_id" in part.columns else np.nan
        if pd.notna(action0_ratio) and action0_ratio > 0.80:
            label = "Q_VALUE_ACTION0_DOMINANT"
        elif pd.notna(margin) and margin < 0.001:
            label = "Q_VALUE_LOW_MARGIN"
        else:
            label = "Q_VALUE_CONFIDENT"
        rows.append({**key_map, "avg_selected_q_value": _mean(part, "selected_q_value"), "avg_q_value_margin": margin, "action0_ratio": action0_ratio, "label": label})
    return _markdown_table(pd.DataFrame(rows).head(limit))


def _reward_diagnosis(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    reward = bundle.get("reward_diagnostics.csv")
    if reward.empty:
        body = "Reward diagnostics are missing. Reward attribution cannot be fully analyzed."
    else:
        cols = [col for col in ["portfolio_return", "benchmark_return", "base_reward", "final_reward", "transaction_cost", "flat_penalty_applied", "trade_bonus_applied"] if col in reward.columns]
        body = _markdown_table(reward[cols].describe().reset_index()) if cols else "Reward diagnostics file has no recognized reward columns."
    return ["## 8. Reward Diagnosis", body, ""]


def _nlp_incremental_effect(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 9. NLP Incremental Effect Diagnosis", _nlp_effect_text(bundle), ""]


def _nlp_effect_text(bundle: ModelUpgradeDiagnosticsBundle) -> str:
    effect = bundle.get("nlp_incremental_effect.csv")
    if effect.empty:
        return "NLP_EFFECT_DIAGNOSTICS_MISSING"
    rows = []
    for _, row in effect.iterrows():
        final_effect = pd.to_numeric(pd.Series([row.get("mean_final_equity_effect")]), errors="coerce").iloc[0]
        positive_rate = pd.to_numeric(pd.Series([row.get("positive_effect_rate")]), errors="coerce").iloc[0]
        sharpe_effect = pd.to_numeric(pd.Series([row.get("mean_sharpe_effect")]), errors="coerce").iloc[0]
        label = "NLP_IMPROVES" if final_effect > 0 and positive_rate >= 0.60 and sharpe_effect >= 0 else "NLP_MIXED" if final_effect > 0 or sharpe_effect >= 0 else "NLP_DOES_NOT_IMPROVE"
        rows.append({**row.to_dict(), "label": label})
    return _markdown_table(pd.DataFrame(rows))


def _benchmark_comparison(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 10. Benchmark Comparison", _benchmark_text(bundle), ""]


def _benchmark_text(bundle: ModelUpgradeDiagnosticsBundle) -> str:
    summary = bundle.get("model_upgrade_summary.csv")
    portfolio = bundle.get("portfolio_curves.csv")
    if summary.empty and portfolio.empty:
        return "Benchmark curve or benchmark metrics are unavailable for model-upgrade diagnostics."
    if "experiment" not in summary.columns:
        return "Benchmark curve or benchmark metrics are unavailable for model-upgrade diagnostics."
    benchmark = summary[summary.get("experiment", pd.Series(dtype=str)).astype(str).eq("buy_and_hold")] if "experiment" in summary.columns else pd.DataFrame()
    if benchmark.empty:
        return "Benchmark curve or benchmark metrics are unavailable for model-upgrade diagnostics."
    return _markdown_table(benchmark.head(20))


def _stability_across_seeds(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    return ["## 11. Stability Across Seeds", _stability_table(bundle, limit=200), ""]


def _stability_table(bundle: ModelUpgradeDiagnosticsBundle, *, limit: int) -> str:
    summary = bundle.get("model_upgrade_summary.csv")
    if summary.empty:
        return "model_upgrade_summary.csv is missing or empty."
    frame = summary.copy()
    mean = pd.to_numeric(frame.get("mean_final_equity"), errors="coerce")
    std = pd.to_numeric(frame.get("std_final_equity"), errors="coerce")
    seed_count = pd.to_numeric(frame.get("seed_count"), errors="coerce")
    frame["coefficient_of_variation"] = std / mean.abs().replace(0, np.nan)
    frame["stability_label"] = np.where(seed_count < 5, "INSUFFICIENT_SEEDS", np.where(frame["coefficient_of_variation"] > 0.20, "UNSTABLE", "STABLE"))
    cols = [col for col in ["target_symbol", "experiment", "model_variant", "reward_variant", "state_feature_mode", "seed_count", "std_final_equity", "std_sharpe_ratio", "coefficient_of_variation", "stability_label"] if col in frame.columns]
    return _markdown_table(frame[cols].head(limit))


def _key_findings(bundle: ModelUpgradeDiagnosticsBundle, *, compact: bool = False) -> list[str]:
    hold_text = _hold_heavy_table(bundle, limit=200)
    hold_heavy = "HOLD_HEAVY" in hold_text or "PARTIALLY_HOLD_HEAVY" in hold_text
    q_text = _q_value_table(bundle, limit=200)
    stable_text = _stability_table(bundle, limit=200)
    return [
        "## 12. Key Findings" if not compact else "",
        f"- Did the model-upgrade path solve flat/Hold-heavy behavior? {'Not fully; review HOLD_HEAVY/PARTIALLY_HOLD_HEAVY rows.' if hold_heavy else 'No strong hold-heavy warning detected in available metrics.'}",
        f"- Best model/reward/action/state combination: see top configurations table.",
        "- NLP value-add: use NLP Incremental Effect Diagnosis; do not claim improvement unless labels say NLP_IMPROVES.",
        f"- Q-value confidence: {'low-margin/action0 dominance appears in available diagnostics.' if ('LOW_MARGIN' in q_text or 'ACTION0' in q_text) else 'no low-margin warning detected in available Q diagnostics.'}",
        f"- Seed stability: {'some configurations are unstable or insufficiently seeded.' if ('UNSTABLE' in stable_text or 'INSUFFICIENT_SEEDS' in stable_text) else 'available configurations look stable by the configured rule.'}",
        "- Report suitability: medium-test is preliminary; full-grid with sufficient seeds is preferred for formal claims.",
        "",
    ]


def _recommended_next_experiments(bundle: ModelUpgradeDiagnosticsBundle, *, compact: bool = False) -> list[str]:
    return [
        "## 13. Recommended Next Experiments" if not compact else "",
        "- Run full-grid with 20 seeds for the most promising configurations.",
        "- Add or enable reward diagnostics for step-level reward attribution.",
        "- Add paired NLP incremental effect analysis if missing.",
        "- Run transaction-cost sensitivity before formal trading conclusions.",
        "- Investigate any stock-specific HOLD_HEAVY rows before adding model complexity.",
        "- Compare target_position against discrete_trade only after diagnostics are complete.",
        "",
    ]


def _report_ready_paragraph(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    paragraph = (
        "The model-upgrade diagnostics evaluate DQN behavior across cached target-stock experiments using "
        "seed-level metrics, action distributions, environment dynamics, and Q-value traces where available. "
        "The dashboard and reports make it possible to inspect whether each DQN policy actively traded, how "
        "exposure evolved over time, whether NLP-enhanced groups improved over the no-NLP baseline, and whether "
        "performance was stable across seeds. Current conclusions should be treated as diagnostic unless full-grid "
        "seed coverage and paired NLP effect evidence are available."
    )
    return ["## 14. Report-Ready Summary Paragraph", paragraph, ""]


def _files_generated(bundle: ModelUpgradeDiagnosticsBundle) -> list[str]:
    rows = [{"file": item.filename, "path": item.path, "rows": item.row_count, "targets": ", ".join(item.target_symbols)} for item in bundle.loaded_files]
    return ["## 15. Files Generated", _markdown_table(pd.DataFrame(rows)) if rows else "No diagnostic CSVs were loaded.", ""]


def _best_by(frame: pd.DataFrame, metric: str) -> str:
    if frame.empty or metric not in frame.columns or "experiment" not in frame.columns:
        return ""
    values = pd.to_numeric(frame[metric], errors="coerce")
    if values.notna().any():
        return str(frame.loc[values.idxmax(), "experiment"])
    return ""


def _mean(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return np.nan
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.mean()) if values.notna().any() else np.nan


def _max(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return np.nan
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.max()) if values.notna().any() else np.nan


def _min(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return np.nan
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.min()) if values.notna().any() else np.nan


def _last_numeric(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return np.nan
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(values.iloc[-1]) if not values.empty else np.nan


def _total_return(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    start = _last_numeric(frame.head(1), "portfolio_before")
    end = _last_numeric(frame, "portfolio_after")
    if pd.isna(start) or not start or pd.isna(end):
        return np.nan
    return float(end / start - 1)


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No rows available."
    display = frame.copy()
    for col in display.columns:
        if pd.api.types.is_float_dtype(display[col]):
            display[col] = display[col].map(lambda value: "" if pd.isna(value) else f"{value:.6g}")
    display = display.fillna("").astype(str)
    headers = [str(col) for col in display.columns]
    rows = display.values.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(cell).replace("\n", " ") for cell in row) + " |")
    return "\n".join(lines)
