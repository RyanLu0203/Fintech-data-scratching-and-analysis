"""Run all post-scraping analysis tasks from one generated finance_text CSV."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.config.paths import stock_reports_dir, stock_results_dir
from src.evaluation.ablation import run_ablation_study
from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.diagnostics import build_run_diagnostics
from src.evaluation.signals import signal_diagnostics
from src.evaluation.walk_forward import walk_forward_summary
from src.features.money_flow import compute_daily_net_flow
from src.features.technical_indicators import add_trading_features
from src.nlp.aggregate_sentiment import run_nlp_pipeline
from src.reporting.artifacts import generate_report_artifacts


def run_all_analysis(input_csv: Path, output_dir: Path, episodes: int = 200) -> dict:
    data = pd.read_csv(input_csv)
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date").reset_index(drop=True)
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data["event_count"] = pd.to_numeric(data.get("event_count", 0), errors="coerce").fillna(0)
    data = data.dropna(subset=["close"])

    stem = input_csv.stem
    daily_net_flow = compute_daily_net_flow(data)
    daily_net_flow_path = output_dir / f"{stem}_daily_net_flow.csv"
    daily_net_flow.to_csv(daily_net_flow_path, index=False, encoding="utf-8-sig")

    symbol = str(data["symbol"].dropna().iloc[0]) if "symbol" in data.columns and data["symbol"].notna().any() else stem.split("_")[0]
    if str(output_dir) == "reports":
        output_dir = stock_reports_dir(symbol)
    output_dir.mkdir(parents=True, exist_ok=True)
    company_name = str(data["company_name"].dropna().iloc[0]) if "company_name" in data.columns and data["company_name"].notna().any() else ""
    start_date = str(data["date"].min().date())
    end_date = str(data["date"].max().date())

    nlp_outputs = run_nlp_pipeline(
        input_csv=input_csv,
        reports_dir=output_dir,
        symbol=symbol,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
    )
    daily_sentiment_path = Path(nlp_outputs["daily_sentiment_proxy_csv"])
    sentiment_csv = Path(nlp_outputs["daily_sentiment_csv"])

    ablation_outputs = run_ablation_study(
        input_csv=input_csv,
        sentiment_csv=sentiment_csv,
        output_dir=stock_results_dir(symbol),
        reports_dir=output_dir,
        episodes=episodes,
    )
    ablation = ablation_outputs["metrics"]
    ablation_path = Path(ablation_outputs["report_ablation_metrics_csv"])

    features = add_trading_features(data, nlp_outputs["daily_sentiment"])
    signal_diag = signal_diagnostics(data, nlp_outputs["daily_sentiment"], daily_net_flow)
    signal_diag_path = output_dir / f"{stem}_signal_diagnostics.csv"
    signal_diag.to_csv(signal_diag_path, index=False, encoding="utf-8-sig")

    walk_forward = walk_forward_summary(features, start_date, end_date)
    walk_forward_path = output_dir / f"{stem}_walk_forward_splits.csv"
    walk_forward.to_csv(walk_forward_path, index=False, encoding="utf-8-sig")

    diagnostics = build_run_diagnostics(
        raw_market=data,
        feature_frame=features,
        daily_sentiment=nlp_outputs["daily_sentiment"],
        signal_diagnostics_table=signal_diag,
        walk_forward_table=walk_forward,
        ablation_metrics=ablation_outputs["metrics"],
        seed_metrics=ablation_outputs["metrics_by_seed"],
        split_info=ablation_outputs["split_info"],
        leakage_diagnostics=ablation_outputs["leakage_diagnostics"],
        trading_logs=ablation_outputs["trading_logs"],
    )
    diagnostics_path = output_dir / f"{stem}_diagnostics.csv"
    diagnostics.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")

    artifact_paths = generate_report_artifacts(
        reports_dir=output_dir,
        input_csv=input_csv,
        market_data=data,
        daily_sentiment=nlp_outputs["daily_sentiment"],
        nlp_evaluation=nlp_outputs["nlp_evaluation"],
        state_compliance=ablation_outputs["state_compliance"],
        ablation_metrics=ablation_outputs["metrics"],
        ablation_seed_metrics=ablation_outputs["metrics_by_seed"],
        portfolio_curves=ablation_outputs["portfolio_curves"],
        trading_logs=ablation_outputs["trading_logs"],
        walk_forward_table=walk_forward,
        daily_net_flow=daily_net_flow,
        drawdown_curves=ablation_outputs["drawdown_curves"],
        signal_diagnostics_table=signal_diag,
        diagnostics_table=diagnostics,
        leakage_table=ablation_outputs["leakage_diagnostics"],
    )

    cross_stock_outputs = build_cross_stock_summary()

    summary = {
        "input_csv": str(input_csv),
        "rows": int(len(data)),
        "date_start": str(data["date"].min().date()) if not data.empty else "",
        "date_end": str(data["date"].max().date()) if not data.empty else "",
        "event_rows": int((data["event_count"] > 0).sum()),
        "total_events": int(data["event_count"].sum()),
        "event_coverage_ratio": float((data["event_count"] > 0).mean()) if not data.empty else 0.0,
        "daily_sentiment_proxy_csv": str(daily_sentiment_path),
        "daily_sentiment_csv": str(sentiment_csv),
        "daily_net_flow_csv": str(daily_net_flow_path),
        "signal_diagnostics_csv": str(signal_diag_path),
        "diagnostics_csv": str(diagnostics_path),
        "nlp_evaluation_csv": str(nlp_outputs["nlp_evaluation_csv"]),
        "ablation_metrics_csv": str(ablation_path),
        "ablation_metrics_by_seed_csv": str(ablation_outputs["ablation_metrics_by_seed_csv"]),
        "portfolio_curves_csv": str(ablation_outputs["portfolio_curves_csv"]),
        "drawdown_curves_csv": str(ablation_outputs["drawdown_curves_csv"]),
        "trading_logs_csv": str(ablation_outputs["trading_logs_csv"]),
        "walk_forward_splits_csv": str(walk_forward_path),
        "cross_stock_summary_csv": str(cross_stock_outputs["summary_csv"]),
        "cross_stock_discussion_md": str(cross_stock_outputs["discussion_md"]),
        "report_draft_markdown": str(artifact_paths["report_draft"]),
        "metrics": ablation.to_dict(orient="records"),
    }
    summary_path = output_dir / f"{stem}_analysis_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Analysis summary: {summary_path}")
    print(f"Daily sentiment: {sentiment_csv}")
    print(f"Ablation metrics: {ablation_path}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all post-scraping analysis tasks.")
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--episodes", type=int, default=200)
    args = parser.parse_args()
    run_all_analysis(Path(args.input_csv), Path(args.output_dir), episodes=args.episodes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
