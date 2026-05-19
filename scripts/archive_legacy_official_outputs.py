"""Dry-run-first archive tool for deprecated official experiment outputs.

The model-upgrade diagnostics are now the active dashboard experiment source.
This script only archives generated legacy result files under
``outputs/stocks/<symbol>/results``. It never deletes files permanently.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LEGACY_PATTERNS = (
    "peer_nlp_*",
    "peer_market_impact_*",
    "market_impact_*",
    "dqn_without_nlp*",
    "dqn_with_*peer_nlp*",
    "dqn_with_*sentiment_nlp*",
    "dqn_with_*impact_nlp*",
    "shared_market_only_pretrain*",
    "official_*",
)

PRESERVE_NOTES = (
    "raw input data and crawler cache outside outputs/stocks/<symbol>/results",
    "cleaned stock price/input CSVs under outputs/stocks/<symbol>/data",
    "peer corpus data needed to regenerate NLP scoring",
    "all outputs/model_upgrade files and diagnostic reports",
    "README_diagnostics.md, CHATGPT_DIAGNOSTIC_SUMMARY.md, and UPGRADE_REASON_README.md",
)


@dataclass(frozen=True)
class ArchivePlan:
    archive_dir: Path
    candidates: tuple[Path, ...]
    preserved_notes: tuple[str, ...]
    warnings: tuple[str, ...]


def collect_archive_plan(
    *,
    project_root: Path = PROJECT_ROOT,
    timestamp: str | None = None,
) -> ArchivePlan:
    """Collect generated legacy official outputs that are safe to archive."""

    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = project_root / "outputs"
    stock_root = output_root / "stocks"
    archive_dir = output_root / "archive" / f"official_legacy_{ts}"
    candidates: set[Path] = set()
    warnings: list[str] = []

    if not stock_root.exists():
        warnings.append(f"Stock output root does not exist: {_display_path(stock_root, project_root)}")
    else:
        for results_dir in sorted(stock_root.glob("*/results")):
            if not results_dir.is_dir():
                continue
            for pattern in LEGACY_PATTERNS:
                for path in results_dir.glob(pattern):
                    if path.is_file():
                        candidates.add(path)

    model_upgrade_dir = output_root / "model_upgrade"
    for candidate in sorted(candidates):
        try:
            candidate.relative_to(model_upgrade_dir)
        except ValueError:
            continue
        warnings.append(f"Refusing to archive model-upgrade output: {_display_path(candidate, project_root)}")

    filtered = tuple(
        sorted(
            path
            for path in candidates
            if not _is_under(path, model_upgrade_dir)
        )
    )
    return ArchivePlan(
        archive_dir=archive_dir,
        candidates=filtered,
        preserved_notes=PRESERVE_NOTES,
        warnings=tuple(warnings),
    )


def run_archive(
    *,
    apply: bool = False,
    project_root: Path = PROJECT_ROOT,
    timestamp: str | None = None,
) -> ArchivePlan:
    """Write an archive plan or move files when ``apply`` is true."""

    plan = collect_archive_plan(project_root=project_root, timestamp=timestamp)
    plan.archive_dir.mkdir(parents=True, exist_ok=True)
    if apply:
        moved: list[tuple[Path, Path]] = []
        for source in plan.candidates:
            destination = plan.archive_dir / source.relative_to(project_root / "outputs")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))
            moved.append((source, destination))
        _write_report(plan, project_root=project_root, applied=True, moved=moved)
    else:
        _write_report(plan, project_root=project_root, applied=False, moved=[])
    return plan


def _write_report(
    plan: ArchivePlan,
    *,
    project_root: Path,
    applied: bool,
    moved: list[tuple[Path, Path]],
) -> Path:
    report_path = plan.archive_dir / ("ARCHIVE_LOG.md" if applied else "ARCHIVE_PLAN.md")
    mode = "--apply" if applied else "dry-run"
    lines = [
        f"# Official Legacy Output Archive {'Log' if applied else 'Plan'}",
        "",
        f"- mode: {mode}",
        f"- archive folder: {_display_path(plan.archive_dir, project_root)}",
        "- permanent deletion: disabled",
        "- reason: model-upgrade outputs are now the active dashboard experiment source; deprecated generated official result outputs are archived out of the active workflow.",
        "",
        "## Files To Archive" if not applied else "## Files Archived",
    ]
    if applied:
        if moved:
            for source, destination in moved:
                lines.append(
                    f"- {_display_path(source, project_root)} -> {_display_path(destination, project_root)}"
                )
        else:
            lines.append("- none")
    else:
        if plan.candidates:
            for source in plan.candidates:
                destination = plan.archive_dir / source.relative_to(project_root / "outputs")
                lines.append(
                    f"- {_display_path(source, project_root)} -> {_display_path(destination, project_root)}"
                )
        else:
            lines.append("- none")

    lines.extend(["", "## Files Preserved"])
    lines.extend(f"- {note}" for note in plan.preserved_notes)
    lines.extend(["", "## Safety Warnings"])
    if plan.warnings:
        lines.extend(f"- {warning}" for warning in plan.warnings)
    else:
        lines.append("- none")
    lines.extend(["", "## Confirmation"])
    lines.append("- outputs/model_upgrade remains the active model-upgrade diagnostic source.")
    lines.append("- No raw data, crawler cache, cleaned stock data, or model-upgrade output is permanently deleted.")
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_path


def _display_path(path: Path, project_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve()))
    except ValueError:
        return str(path)


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually move legacy generated outputs into the archive folder.")
    args = parser.parse_args()

    plan = run_archive(apply=args.apply)
    mode = "--apply" if args.apply else "dry-run"
    report_name = "ARCHIVE_LOG.md" if args.apply else "ARCHIVE_PLAN.md"
    print(f"Archive mode: {mode}")
    print(f"Archive folder: {_display_path(plan.archive_dir, PROJECT_ROOT)}")
    print(f"Report: {_display_path(plan.archive_dir / report_name, PROJECT_ROOT)}")
    print(f"Legacy files {'archived' if args.apply else 'that would be archived'}: {len(plan.candidates)}")
    for path in plan.candidates:
        print(f"- {_display_path(path, PROJECT_ROOT)}")
    print("Preserved:")
    for note in plan.preserved_notes:
        print(f"- {note}")
    if plan.warnings:
        print("Safety warnings:")
        for warning in plan.warnings:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
