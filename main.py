"""CLI entry point for the NLP-driven RL trading platform."""

from __future__ import annotations

import argparse
import inspect
import logging
import subprocess
import sys
from pathlib import Path
from typing import Callable

import pandas as pd

from src.config.paths import database_path, ensure_stock_dirs, stock_reports_dir, stock_results_dir
from src.config.settings import PROJECT_ROOT, settings
from src.data_ingestion.cache import resolve_cached_csv
from src.data_ingestion.ingestion import IngestionConfig, default_output_csv, run_ingestion
from src.evaluation.ablation import run_ablation_study, run_coverage_controlled_ablation_study
from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.diagnostics import build_run_diagnostics
from src.evaluation.feasibility_audit import run_feasibility_audit
from src.evaluation.information_density import generate_information_density_outputs
from src.evaluation.market_impact_ablation import (
    build_market_impact_cross_stock_summary,
    run_market_impact_official_experiment,
)
from src.evaluation.peer_nlp_ablation import (
    build_peer_nlp_cross_stock_summary,
    run_peer_nlp_official_experiment,
    write_peer_nlp_integrity_report,
)
from src.evaluation.sector_peer_bootstrap import ensure_sector_peer_data
from src.evaluation.signals import signal_diagnostics
from src.evaluation.walk_forward import walk_forward_summary
from src.features.money_flow import compute_daily_net_flow
from src.features.technical_indicators import add_trading_features
from src.nlp.aggregate_sentiment import run_nlp_pipeline
from src.reporting.artifacts import generate_report_artifacts
from src.storage.database import initialize_database, save_sentiment_data, save_trading_logs

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NLP-driven reinforcement learning trading platform.")
    parser.add_argument("--mode", choices=["all", "ingest", "ablation", "dashboard"], default=None, help="Backward-compatible shortcut mode.")
    parser.add_argument("--symbol", default=settings.default_symbol)
    parser.add_argument("--company-name", default="")
    parser.add_argument("--start-date", default=settings.default_start_date)
    parser.add_argument("--end-date", default=settings.default_end_date)
    parser.add_argument("--sources", default="tencent")
    parser.add_argument("--news-count", type=int, default=5000)
    parser.add_argument("--run-ingestion", action="store_true")
    parser.add_argument("--run-nlp", action="store_true")
    parser.add_argument("--run-rl", action="store_true")
    parser.add_argument("--run-ablation", action="store_true")
    parser.add_argument("--skip-peer-nlp-experiment", action="store_true", help="Skip the official peer-sector NLP transfer experiment.")
    parser.add_argument("--run-legacy-stock-level-nlp", action="store_true", help="Run deprecated stock-level NLP outputs as legacy robustness only.")
    parser.add_argument("--allow-fetch-missing-sector-peers", action="store_true", help="Allow future sector-peer fetching hooks when peer corpus is insufficient.")
    parser.add_argument("--supplement-sector-peers", action="store_true", help="Fetch missing configured peer stocks for selected target sectors before peer NLP.")
    parser.add_argument("--supplement-sector-peers-only", action="store_true", help="Only fetch missing sector-peer data and exit.")
    parser.add_argument("--run-peer-cross-analysis", action="store_true", help="Run the official peer NLP workflow for multiple held-out targets and build cross-stock summary.")
    parser.add_argument("--skip-high-density-ablation", action="store_true", help="Skip the coverage-controlled high-density NLP evaluation.")
    parser.add_argument("--episodes", type=int, default=200)
    parser.add_argument("--initial-cash", type=float, default=settings.default_initial_cash)
    parser.add_argument("--use-sqlite", action="store_true")
    parser.add_argument("--reuse-existing-csv", action="store_true", default=True)
    parser.add_argument("--require-news", action="store_true")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--peer-dashboard", action="store_true", help="Launch the new clean peer sentiment + market-impact dashboard.")
    parser.add_argument("--run-market-impact-nlp", action="store_true", help="Run the improved peer market-impact NLP experiment.")
    parser.add_argument("--market-impact-horizon-days", type=int, default=3)
    parser.add_argument("--market-impact-pos-threshold", type=float, default=0.015)
    parser.add_argument("--market-impact-neg-threshold", type=float, default=-0.015)
    parser.add_argument("--run-feasibility-audit", action="store_true")
    parser.add_argument("--audit-mode", choices=["dry_run", "full_run"], default="dry_run")
    parser.add_argument("--allow-fetch-missing-data", action="store_true")
    parser.add_argument("--audit-years", type=int, default=5)
    parser.add_argument("--symbols", default="", help="Comma-separated symbols for cross-stock audit.")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    _apply_legacy_mode(args)

    if args.peer_dashboard:
        return run_peer_dashboard()
    if args.dashboard:
        return run_dashboard()

    if args.run_feasibility_audit:
        symbols = args.symbols if args.symbols else (args.symbol if "--symbol" in sys.argv else None)
        audit_start_date = args.start_date if "--start-date" in sys.argv else None
        audit_end_date = args.end_date if "--end-date" in sys.argv else None
        outputs = run_feasibility_audit(
            symbols=symbols,
            start_date=audit_start_date,
            end_date=audit_end_date,
            years=args.audit_years,
            mode=args.audit_mode,
            allow_fetch_missing_data=args.allow_fetch_missing_data,
            sources=args.sources,
            news_count=args.news_count,
            episodes=args.episodes,
        )
        summary = outputs["summary"]
        LOGGER.info("Feasibility audit status: %s", outputs["cross_stock_status"])
        if not summary.empty:
            print(summary.to_string(index=False))
        print(f"Audit outputs saved under: {Path(outputs['paths']['feasibility_audit_csv']).parent}")
        return 0

    symbol_list = _parse_symbol_list(args.symbols) or [_normalize_symbol(args.symbol)]
    if args.supplement_sector_peers_only:
        outputs = ensure_sector_peer_data(
            target_symbols=symbol_list,
            start_date=args.start_date,
            end_date=args.end_date,
            sources=args.sources,
            news_count=args.news_count,
            allow_fetch=True,
        )
        print(outputs["readiness"].to_string(index=False) if not outputs["readiness"].empty else "No sector readiness rows.")
        return 0

    if args.run_peer_cross_analysis:
        run_peer_cross_analysis(
            symbols=symbol_list,
            start_date=args.start_date,
            end_date=args.end_date,
            sources=args.sources,
            news_count=args.news_count,
            episodes=args.episodes,
            initial_cash=args.initial_cash,
            run_ingestion_flag=args.run_ingestion,
            reuse_existing_csv=args.reuse_existing_csv,
            require_news=args.require_news,
            use_sqlite=args.use_sqlite,
            supplement_sector_peers=args.supplement_sector_peers,
            allow_fetch_missing_sector_peers=args.allow_fetch_missing_sector_peers or args.supplement_sector_peers,
        )
        return 0

    if not any([args.run_ingestion, args.run_nlp, args.run_rl, args.run_ablation]):
        args.run_ingestion = True
        args.run_nlp = True
        args.run_ablation = True

    run_pipeline_for_symbol(
        symbol=args.symbol,
        company_name=args.company_name,
        start_date=args.start_date,
        end_date=args.end_date,
        sources=args.sources,
        news_count=args.news_count,
        run_ingestion_flag=args.run_ingestion,
        run_nlp_flag=args.run_nlp,
        run_rl_flag=args.run_rl,
        run_ablation_flag=args.run_ablation,
        episodes=args.episodes,
        initial_cash=args.initial_cash,
        use_sqlite=args.use_sqlite,
        reuse_existing_csv=args.reuse_existing_csv,
        require_news=args.require_news,
        build_cross_stock_outputs=True,
        run_high_density_ablation=not args.skip_high_density_ablation,
        run_peer_nlp_experiment=not args.skip_peer_nlp_experiment,
        run_legacy_stock_level_nlp=args.run_legacy_stock_level_nlp,
        allow_fetch_missing_sector_peers=args.allow_fetch_missing_sector_peers,
        run_market_impact_nlp=args.run_market_impact_nlp,
        market_impact_horizon_days=args.market_impact_horizon_days,
        market_impact_pos_threshold=args.market_impact_pos_threshold,
        market_impact_neg_threshold=args.market_impact_neg_threshold,
    )

    return 0


def run_peer_cross_analysis(
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    sources: str = "tencent",
    news_count: int = 5000,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    run_ingestion_flag: bool = True,
    reuse_existing_csv: bool = True,
    require_news: bool = False,
    use_sqlite: bool = False,
    supplement_sector_peers: bool = False,
    allow_fetch_missing_sector_peers: bool = False,
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, object]:
    """Run the official held-out peer NLP workflow as a cross-stock experiment."""

    targets = [_normalize_symbol(symbol) for symbol in symbols if str(symbol).strip()]
    if supplement_sector_peers:
        ensure_sector_peer_data(
            target_symbols=targets,
            start_date=start_date,
            end_date=end_date,
            sources=sources,
            news_count=news_count,
            allow_fetch=True,
            status_callback=status_callback,
        )

    completed: list[str] = []
    failures: list[dict[str, str]] = []
    for symbol in targets:
        try:
            company = _company_from_config(symbol)
            run_pipeline_for_symbol(
                symbol=symbol,
                company_name=company,
                start_date=start_date,
                end_date=end_date,
                sources=sources,
                news_count=news_count,
                run_ingestion_flag=run_ingestion_flag,
                run_nlp_flag=True,
                run_rl_flag=True,
                run_ablation_flag=True,
                episodes=episodes,
                initial_cash=initial_cash,
                use_sqlite=use_sqlite,
                reuse_existing_csv=reuse_existing_csv,
                require_news=require_news,
                build_cross_stock_outputs=False,
                status_callback=status_callback,
                run_high_density_ablation=False,
                run_peer_nlp_experiment=True,
                run_legacy_stock_level_nlp=False,
                allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
            )
            completed.append(symbol)
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
            LOGGER.exception("Peer cross target failed: %s", symbol)

    cross = build_peer_nlp_cross_stock_summary(selected_symbols=completed)
    integrity = write_peer_nlp_integrity_report(selected_symbols=completed)
    return {"completed_symbols": completed, "failures": failures, "cross": cross, "integrity": integrity}


def run_pipeline_for_symbol(
    symbol: str,
    company_name: str,
    start_date: str,
    end_date: str,
    sources: str = "tencent",
    news_count: int = 5000,
    run_ingestion_flag: bool = True,
    run_nlp_flag: bool = True,
    run_rl_flag: bool = False,
    run_ablation_flag: bool = True,
    episodes: int = 200,
    initial_cash: float = 1000000.0,
    use_sqlite: bool = False,
    reuse_existing_csv: bool = True,
    require_news: bool = False,
    build_cross_stock_outputs: bool = False,
    status_callback: Callable[[str, str], None] | None = None,
    run_high_density_ablation: bool = True,
    run_peer_nlp_experiment: bool = True,
    run_legacy_stock_level_nlp: bool = False,
    allow_fetch_missing_sector_peers: bool = False,
    include_marketwide_peer: bool = True,
    run_market_impact_nlp: bool = False,
    market_impact_horizon_days: int = 3,
    market_impact_pos_threshold: float = 0.015,
    market_impact_neg_threshold: float = -0.015,
) -> dict[str, object]:
    """Run the end-to-end workflow for one symbol and return output metadata."""

    def emit(stage: str, message: str) -> None:
        LOGGER.info("[%s] %s", stage, message)
        if status_callback is not None:
            status_callback(stage, message)

    ensure_stock_dirs(symbol)
    input_csv = default_output_csv(symbol, start_date, end_date)
    sqlite_path = database_path()

    if run_ingestion_flag:
        emit("ingestion", f"Preparing held-out target market/news data for {symbol} from {start_date} to {end_date}; target news remains excluded from peer NLP training.")
        input_csv = run_ingestion(
            IngestionConfig(
                symbol=symbol,
                company_name=company_name,
                start_date=start_date,
                end_date=end_date,
                sources=sources,
                news_count=news_count,
                reuse_existing_csv=reuse_existing_csv,
                require_news=require_news,
                use_sqlite=use_sqlite,
                sqlite_path=sqlite_path,
            )
        )
    elif not input_csv.exists():
        emit("cache", f"Trying to resolve {symbol} from local cache only.")
        cached = resolve_cached_csv(symbol, start_date, end_date, output_csv=input_csv)
        if cached is None:
            raise FileNotFoundError(
                f"Input CSV does not exist and no cached master covers the range: {input_csv}. "
                "Run with --run-ingestion or choose a date range already present in the stock output cache."
            )
        input_csv = cached.path
        LOGGER.info("Resolved requested range from cached data via %s: %s", cached.source, input_csv)
        emit("cache", f"Resolved {symbol} from cached data via {cached.source}.")

    reports_dir = stock_reports_dir(symbol)
    results_dir = stock_results_dir(symbol)
    reports_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    nlp_outputs = None
    peer_outputs: dict[str, object] | None = None
    market_impact_outputs: dict[str, object] | None = None
    raw_data = pd.read_csv(input_csv)
    raw_data["date"] = pd.to_datetime(raw_data["date"])
    daily_net_flow = pd.DataFrame()
    signal_diag = pd.DataFrame()
    ablation_outputs: dict[str, object] | None = None
    diagnostics = pd.DataFrame()
    walk_forward = pd.DataFrame()
    if run_nlp_flag or run_rl_flag or run_ablation_flag:
        if run_peer_nlp_experiment:
            emit("peer_nlp", f"Running official peer-sector NLP transfer experiment for {symbol}.")
            peer_kwargs = {
                "input_csv": input_csv,
                "symbol": symbol,
                "company_name": company_name,
                "start_date": start_date,
                "end_date": end_date,
                "sources": sources,
                "news_count": max(news_count, 100000),
                "episodes": episodes,
                "initial_cash": initial_cash,
                "allow_fetch_missing_sector_peers": allow_fetch_missing_sector_peers,
                "include_marketwide_peer": include_marketwide_peer,
            }
            if "status_callback" in inspect.signature(run_peer_nlp_official_experiment).parameters:
                peer_kwargs["status_callback"] = status_callback
            peer_outputs = run_peer_nlp_official_experiment(**peer_kwargs)
            peer_daily = peer_outputs.get("daily_sentiment", pd.DataFrame())
            if isinstance(peer_daily, pd.DataFrame) and not peer_daily.empty:
                official_signal = peer_daily.copy()
                official_signal["sentiment_score"] = pd.to_numeric(
                    official_signal.get("marketwide_sentiment_score", official_signal.get("sector_sentiment_score", 0)),
                    errors="coerce",
                ).fillna(0.0)
                official_signal["news_count"] = pd.to_numeric(official_signal.get("target_news_count", 0), errors="coerce").fillna(0).astype(int)
                official_signal["sentiment_method"] = official_signal.get(
                    "marketwide_sentiment_method",
                    pd.Series(["peer_nlp_transfer"] * len(official_signal), index=official_signal.index),
                )
                official_signal["alignment_rule"] = official_signal.get(
                    "alignment_rule",
                    pd.Series(["target_news_to_next_trading_day_no_intraday_timestamp"] * len(official_signal), index=official_signal.index),
                )
                nlp_outputs = {
                    "daily_sentiment": official_signal,
                    "daily_sentiment_csv": peer_outputs.get("peer_nlp_daily_sentiment_csv"),
                    "nlp_evaluation": pd.DataFrame(
                        [
                            {
                                "method": "peer_nlp_transfer",
                                "status": "official_current_experiment",
                                "sector_method": peer_daily.get("sector_sentiment_method", pd.Series([""])).iloc[0],
                                "marketwide_method": peer_daily.get("marketwide_sentiment_method", pd.Series([""])).iloc[0],
                            }
                        ]
                    ),
                }
            emit("peer_nlp", f"Saved official peer NLP outputs for {symbol}.")
        if run_market_impact_nlp:
            emit("market_impact_nlp", f"Running peer market-impact NLP experiment for {symbol}.")
            market_impact_outputs = run_market_impact_official_experiment(
                input_csv=input_csv,
                symbol=symbol,
                company_name=company_name,
                start_date=start_date,
                end_date=end_date,
                sources=sources,
                news_count=max(news_count, 100000),
                episodes=episodes,
                initial_cash=initial_cash,
                allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
                include_marketwide_peer=include_marketwide_peer,
                horizon_days=market_impact_horizon_days,
                positive_threshold=market_impact_pos_threshold,
                negative_threshold=market_impact_neg_threshold,
                status_callback=status_callback,
            )
            emit("market_impact_nlp", f"Saved market-impact NLP outputs for {symbol}.")
        if run_legacy_stock_level_nlp or not run_peer_nlp_experiment:
            emit("legacy_nlp", f"Running deprecated stock-level NLP pipeline for {symbol} as legacy robustness output.")
            nlp_outputs = run_nlp_pipeline(
                input_csv=input_csv,
                reports_dir=reports_dir,
                symbol=symbol,
                company_name=company_name,
                start_date=start_date,
                end_date=end_date,
                sources=sources,
                news_count=news_count,
            )
            if use_sqlite:
                initialize_database(sqlite_path)
                save_sentiment_data(nlp_outputs["daily_sentiment"], sqlite_path)
        daily_net_flow = compute_daily_net_flow(raw_data)
        daily_net_flow.to_csv(reports_dir / f"{input_csv.stem}_daily_net_flow.csv", index=False, encoding="utf-8-sig")
        signal_frame = nlp_outputs["daily_sentiment"] if nlp_outputs else pd.DataFrame()
        signal_diag = signal_diagnostics(raw_data, signal_frame, daily_net_flow)
        signal_diag.to_csv(reports_dir / f"{input_csv.stem}_signal_diagnostics.csv", index=False, encoding="utf-8-sig")
        emit("signals", f"Saved signal diagnostics and daily net-flow series for {symbol}.")

    if (run_rl_flag or run_ablation_flag) and (run_legacy_stock_level_nlp or not run_peer_nlp_experiment):
        emit("rl", f"Running RL training/evaluation and ablation for {symbol}.")
        if nlp_outputs is None:
            sentiment_csv = reports_dir / f"{input_csv.stem}_daily_sentiment.csv"
            if not sentiment_csv.exists():
                raise FileNotFoundError(f"Sentiment CSV not found: {sentiment_csv}. Run with --run-nlp first.")
        else:
            sentiment_csv = Path(nlp_outputs["daily_sentiment_csv"])

        ablation_outputs = run_ablation_study(
            input_csv=input_csv,
            sentiment_csv=sentiment_csv,
            output_dir=results_dir,
            reports_dir=reports_dir,
            episodes=episodes,
            initial_cash=initial_cash,
        )
        high_density_outputs: dict[str, object] | None = None
        if run_high_density_ablation:
            emit("high_density", f"Running coverage-controlled high-density NLP evaluation for {symbol}.")
            high_density_outputs = run_coverage_controlled_ablation_study(
                input_csv=input_csv,
                sentiment_csv=sentiment_csv,
                output_dir=results_dir,
                reports_dir=reports_dir,
                episodes=episodes,
                initial_cash=initial_cash,
            )
            emit("high_density", f"Saved high-density ablation outputs for {symbol}.")
        if use_sqlite:
            save_trading_logs(ablation_outputs["trading_logs"], sqlite_path)
        features = add_trading_features(raw_data, nlp_outputs["daily_sentiment"] if nlp_outputs else pd.DataFrame())
        walk_forward = walk_forward_summary(features, start_date, end_date)
        walk_forward.to_csv(reports_dir / f"{input_csv.stem}_walk_forward_splits.csv", index=False, encoding="utf-8-sig")
        diagnostics = build_run_diagnostics(
            raw_market=raw_data,
            feature_frame=features,
            daily_sentiment=nlp_outputs["daily_sentiment"] if nlp_outputs else pd.DataFrame(),
            signal_diagnostics_table=signal_diag,
            walk_forward_table=walk_forward,
            ablation_metrics=ablation_outputs["metrics"],
            seed_metrics=ablation_outputs["metrics_by_seed"],
            split_info=ablation_outputs["split_info"],
            leakage_diagnostics=ablation_outputs["leakage_diagnostics"],
            trading_logs=ablation_outputs["trading_logs"],
        )
        diagnostics.to_csv(reports_dir / f"{input_csv.stem}_diagnostics.csv", index=False, encoding="utf-8-sig")
        generate_report_artifacts(
            reports_dir=reports_dir,
            input_csv=input_csv,
            market_data=raw_data,
            daily_sentiment=nlp_outputs["daily_sentiment"] if nlp_outputs else pd.DataFrame(),
            nlp_evaluation=nlp_outputs["nlp_evaluation"] if nlp_outputs else pd.DataFrame(),
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
        emit("report", f"Generated report artifacts for {symbol}.")
        if build_cross_stock_outputs:
            emit("cross_stock", "Refreshing cross-stock summary outputs.")
            build_cross_stock_summary()
            if run_high_density_ablation:
                generate_information_density_outputs(selected_symbols=[symbol])
    elif build_cross_stock_outputs and peer_outputs is not None:
        emit("cross_stock", "Refreshing peer NLP cross-stock summary outputs.")
        build_peer_nlp_cross_stock_summary(selected_symbols=[symbol])
        if market_impact_outputs is not None:
            build_market_impact_cross_stock_summary(selected_symbols=[symbol])
        write_peer_nlp_integrity_report(selected_symbols=[symbol])

    summary: dict[str, object] = {
        "symbol": symbol,
        "company_name": company_name,
        "input_csv": str(input_csv),
        "reports_dir": str(reports_dir),
        "results_dir": str(results_dir),
        "daily_sentiment_csv": str(reports_dir / f"{input_csv.stem}_daily_sentiment.csv"),
        "diagnostics_csv": str(reports_dir / f"{input_csv.stem}_diagnostics.csv"),
        "signal_diagnostics_csv": str(reports_dir / f"{input_csv.stem}_signal_diagnostics.csv"),
        "walk_forward_csv": str(reports_dir / f"{input_csv.stem}_walk_forward_splits.csv"),
        "report_draft_markdown": str(reports_dir / f"{input_csv.stem}_report_draft.md"),
        "ablation_metrics_csv": str(results_dir / "ablation_metrics.csv"),
        "portfolio_curves_csv": str(results_dir / "portfolio_curves.csv"),
        "high_density_ablation_metrics_csv": str(results_dir / "high_density_ablation_metrics.csv"),
        "high_density_portfolio_curves_csv": str(results_dir / "high_density_portfolio_curves.csv"),
        "official_current_experiment": "peer_sector_nlp_transfer" if run_peer_nlp_experiment else "legacy_stock_level_nlp",
        "peer_nlp_daily_sentiment_csv": str(results_dir / "peer_nlp_daily_sentiment.csv"),
        "peer_nlp_ablation_metrics_csv": str(results_dir / "peer_nlp_ablation_metrics.csv"),
        "peer_nlp_portfolio_curves_csv": str(results_dir / "peer_nlp_portfolio_curves.csv"),
        "peer_nlp_trading_logs_csv": str(results_dir / "peer_nlp_trading_logs.csv"),
        "peer_nlp_effect_summary_csv": str(results_dir / "peer_nlp_effect_summary.csv"),
        "market_impact_daily_signal_csv": str(results_dir / "peer_market_impact_daily_signal.csv"),
        "market_impact_ablation_metrics_csv": str(results_dir / "market_impact_ablation_metrics.csv"),
        "market_impact_portfolio_curves_csv": str(results_dir / "market_impact_portfolio_curves.csv"),
        "market_impact_trading_logs_csv": str(results_dir / "market_impact_trading_logs.csv"),
        "market_impact_effect_summary_csv": str(results_dir / "market_impact_effect_summary.csv"),
        "drawdown_curves_csv": str(results_dir / "drawdown_curves.csv"),
        "trading_logs_csv": str(results_dir / "trading_logs.csv"),
        "rows": int(len(raw_data)),
        "has_nlp": bool(nlp_outputs is not None),
        "has_ablation": bool(ablation_outputs is not None or peer_outputs is not None or market_impact_outputs is not None),
    }
    LOGGER.info("Pipeline completed for %s. Reports: %s Results: %s", symbol, reports_dir, results_dir)
    emit("done", f"Completed workflow for {symbol}.")
    return summary


def _apply_legacy_mode(args: argparse.Namespace) -> None:
    if args.mode == "dashboard":
        args.dashboard = True
    elif args.mode == "ingest":
        args.run_ingestion = True
    elif args.mode == "ablation":
        args.run_nlp = True
        args.run_ablation = True
    elif args.mode == "all":
        args.run_ingestion = True
        args.run_nlp = True
        args.run_ablation = True


def _parse_symbol_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [_normalize_symbol(part) for part in str(value).replace("\n", ",").split(",") if part.strip()]


def _normalize_symbol(symbol: str) -> str:
    extracted = pd.Series([str(symbol)]).str.extract(r"(\d{6})", expand=False).iloc[0]
    return str(extracted) if pd.notna(extracted) else str(symbol).strip()


def _company_from_config(symbol: str) -> str:
    mapping_path = PROJECT_ROOT / "config" / "stock_sector_mapping.csv"
    if mapping_path.exists():
        try:
            mapping = pd.read_csv(mapping_path, dtype=str)
            row = mapping[mapping["symbol"].astype(str) == _normalize_symbol(symbol)]
            if not row.empty:
                return str(row["company_name"].iloc[0])
        except Exception:
            pass
    alias_path = PROJECT_ROOT / "config" / "stock_aliases.json"
    if alias_path.exists():
        try:
            import json

            aliases = json.loads(alias_path.read_text(encoding="utf-8"))
            return str(aliases.get(_normalize_symbol(symbol), symbol))
        except Exception:
            pass
    return _normalize_symbol(symbol)


def run_dashboard() -> int:
    subprocess.run([sys.executable, "-m", "streamlit", "run", "src/dashboard/streamlit_app.py"], cwd=str(PROJECT_ROOT), check=True)
    return 0


def run_peer_dashboard() -> int:
    subprocess.run([sys.executable, "-m", "streamlit", "run", "src/dashboard/peer_nlp_dashboard.py"], cwd=str(PROJECT_ROOT), check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
