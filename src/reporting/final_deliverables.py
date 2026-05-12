"""Generate final course-submission evidence without changing the pipeline."""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.config.paths import PROJECT_ROOT, STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR
from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.information_density import generate_information_density_outputs
from src.evaluation.trading_visualizations import generate_trading_visualizations
from src.evaluation.walk_forward import summarize_walk_forward_results
from src.nlp.gold_label_evaluation import run_gold_label_evaluation
from src.evaluation.model_reliability import generate_model_reliability_outputs
from src.storage.database import initialize_database, load_table, save_market_data, save_news_data, save_sentiment_data, save_trading_logs

REPORTS_DIR = PROJECT_ROOT / "reports"
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"


def prepare_final_submission_materials() -> dict[str, Path]:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    gold_outputs = run_gold_label_evaluation(output_dir=TABLES_DIR)
    paths["nlp_gold_label_template"] = Path(gold_outputs["template_csv"])
    paths["nlp_gold_label_evaluation"] = Path(gold_outputs["evaluation_csv"])
    paths["nlp_model_comparison_final"] = Path(gold_outputs["final_comparison_csv"])

    cross = build_cross_stock_summary()
    paths["cross_stock_summary"] = Path(cross["summary_csv"])
    paths["cross_stock_diagnostics"] = Path(cross["diagnostics_csv"])
    paths["cross_stock_discussion"] = Path(cross["discussion_md"])

    density = generate_information_density_outputs()
    paths["information_density_split"] = Path(density["information_density_split"])
    paths["experiment_window_summary"] = Path(density["experiment_window_summary"])
    paths["cross_stock_high_density_summary"] = Path(density["cross_stock_high_density_summary"])

    visuals = generate_trading_visualizations()
    paths["trading_visualization_index"] = Path(visuals["trading_visualization_index"])
    paths["cross_stock_visual_summary"] = Path(visuals["cross_stock_visual_summary"])

    paths.update(generate_walk_forward_evidence())
    paths.update(generate_model_reliability_outputs())
    paths.update(generate_sqlite_demo())
    paths.update(generate_final_report())
    paths.update(generate_presentation_materials())
    paths.update(generate_github_submission_checklist())
    update_notebook_checklist()
    return paths


def generate_walk_forward_evidence() -> dict[str, Path]:
    curves_path = _latest_existing(STOCK_OUTPUT_ROOT.glob("*/results/portfolio_curves.csv"))
    splits_path = _latest_existing(STOCK_OUTPUT_ROOT.glob("*/reports/*_walk_forward_splits.csv"))
    curves = _safe_read_csv(curves_path)
    splits = _safe_read_csv(splits_path)
    results_path = TABLES_DIR / "walk_forward_results.csv"
    results = summarize_walk_forward_results(curves, splits, output_csv=str(results_path))

    figure_path = FIGURES_DIR / "walk_forward_performance.png"
    _plot_walk_forward(results, figure_path)
    return {"walk_forward_results": results_path, "walk_forward_performance": figure_path}


def generate_sqlite_demo(symbol: str | None = None) -> dict[str, Path]:
    selected = symbol or _first_symbol_with_outputs()
    db_path = REPORTS_DIR / "sqlite_demo.db"
    initialize_database(db_path)

    data_path = _latest_existing((STOCK_OUTPUT_ROOT / selected / "data").glob("*_finance_text_*.csv"))
    market = _safe_read_csv(data_path)
    if not market.empty:
        market["symbol"] = market.get("symbol", selected)
        news = _news_from_integrated(market, selected)
        save_market_data(market, db_path)
        save_news_data(news, db_path)

    sentiment_path = _latest_existing((STOCK_OUTPUT_ROOT / selected / "reports").glob("*_daily_sentiment.csv"))
    sentiment = _safe_read_csv(sentiment_path)
    if not sentiment.empty:
        sentiment["ticker"] = sentiment.get("ticker", selected)
        save_sentiment_data(sentiment, db_path)

    logs_path = STOCK_OUTPUT_ROOT / selected / "results" / "trading_logs.csv"
    logs = _safe_read_csv(logs_path)
    if not logs.empty:
        save_trading_logs(logs, db_path)

    schema_summary = _sqlite_schema_summary(db_path)
    roundtrip = _sqlite_roundtrip_check(db_path)
    schema_path = TABLES_DIR / "sqlite_schema_summary.csv"
    roundtrip_path = TABLES_DIR / "sqlite_roundtrip_check.csv"
    schema_summary.to_csv(schema_path, index=False, encoding="utf-8-sig")
    roundtrip.to_csv(roundtrip_path, index=False, encoding="utf-8-sig")

    demo_path = REPORTS_DIR / "sqlite_demo.md"
    sample = _markdown_table(roundtrip.head(10)) if not roundtrip.empty else "No rows available."
    demo_path.write_text(
        "\n".join(
            [
                "# SQLite Storage Demo",
                "",
                f"- Selected stock: `{selected}`",
                f"- Demo database: `{db_path}`",
                "- Purpose: demonstrate schema creation, write, read-back, row counts, date ranges, and sample query output.",
                "",
                "## Sample Query Result",
                "",
                sample,
                "",
                "SQLite remains optional; the main pipeline can continue using CSV artifacts.",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "sqlite_schema_summary": schema_path,
        "sqlite_roundtrip_check": roundtrip_path,
        "sqlite_demo": demo_path,
    }


def generate_final_report() -> dict[str, Path]:
    figure_rows = _collect_index_rows(REPORTS_DIR.glob("**/*.*"), {".svg", ".png"}, "figure")
    table_rows = _collect_index_rows(REPORTS_DIR.glob("**/*.*"), {".csv"}, "table")
    figure_index = REPORTS_DIR / "final_report_figure_index.csv"
    table_index = REPORTS_DIR / "final_report_table_index.csv"
    pd.DataFrame(figure_rows).to_csv(figure_index, index=False, encoding="utf-8-sig")
    pd.DataFrame(table_rows).to_csv(table_index, index=False, encoding="utf-8-sig")

    cross_discussion = _read_text(SYSTEM_OUTPUT_DIR / "cross_stock_discussion.md")
    report = REPORTS_DIR / "final_report.md"
    report.write_text(_final_report_markdown(cross_discussion), encoding="utf-8")
    return {
        "final_report": report,
        "final_report_figure_index": figure_index,
        "final_report_table_index": table_index,
    }


def generate_presentation_materials() -> dict[str, Path]:
    presentation = REPORTS_DIR / "presentation_outline.md"
    demo = REPORTS_DIR / "live_demo_script.md"
    qa = REPORTS_DIR / "q_and_a_preparation.md"
    presentation.write_text(_presentation_outline(), encoding="utf-8")
    demo.write_text(_live_demo_script(), encoding="utf-8")
    qa.write_text(_qa_preparation(), encoding="utf-8")
    return {"presentation_outline": presentation, "live_demo_script": demo, "q_and_a_preparation": qa}


def generate_github_submission_checklist() -> dict[str, Path]:
    checklist = REPORTS_DIR / "github_submission_checklist.md"
    manifest = REPORTS_DIR / "submission_manifest.csv"
    rows = [
        ("README.md", (PROJECT_ROOT / "README.md").exists(), "Project overview and run instructions."),
        ("requirements.txt", (PROJECT_ROOT / "requirements.txt").exists(), "Python dependencies."),
        (".gitignore", (PROJECT_ROOT / ".gitignore").exists(), "Excludes .venv, outputs, caches, databases, logs."),
        ("notebooks/", (PROJECT_ROOT / "notebooks").exists(), "Final notebook workflow."),
        ("docs/", (PROJECT_ROOT / "docs").exists(), "Guideline and project documentation."),
        ("src/", (PROJECT_ROOT / "src").exists(), "Source code."),
        ("program/", (PROJECT_ROOT / "program").exists(), "Crawler programs."),
        (".git/", (PROJECT_ROOT / ".git").exists(), "Local git repository metadata."),
    ]
    pd.DataFrame(rows, columns=["item", "exists", "note"]).to_csv(manifest, index=False, encoding="utf-8-sig")
    checklist.write_text(
        "\n".join(
            [
                "# GitHub Submission Checklist",
                "",
                f"- `.git` exists: `{(PROJECT_ROOT / '.git').exists()}`",
                "- Do not push automatically from this script.",
                "- Keep `.venv/`, `outputs/`, cache folders, SQLite databases, logs, and large model files out of git.",
                "- Include `README.md`, `requirements.txt`, `src/`, `program/`, `docs/`, `notebooks/`, `tests/`, and final report materials.",
                "- Include only small sample outputs if needed for the demo; keep large generated artifacts outside the repo or in a release/archive.",
                "",
                "## Recommended Commands",
                "",
                "```bash",
                "git init",
                "git add README.md requirements.txt .gitignore src program docs notebooks tests reports/final_report.md reports/presentation_outline.md reports/live_demo_script.md reports/q_and_a_preparation.md",
                "git status",
                "git commit -m \"Prepare final NLP-RL trading platform submission\"",
                "git remote add origin <your-repo-url>",
                "git push -u origin main",
                "```",
            ]
        ),
        encoding="utf-8",
    )
    return {"github_submission_checklist": checklist, "submission_manifest": manifest}


def update_notebook_checklist() -> None:
    notebook = PROJECT_ROOT / "notebooks" / "full_report_pipeline.ipynb"
    if not notebook.exists():
        return
    payload = json.loads(notebook.read_text(encoding="utf-8"))
    checklist = [
        "## Final Submission Checklist",
        "",
        "- FinBERT actually run or explicitly skipped with fallback status",
        "- Gold-label NLP evaluation available",
        "- DQN from scratch",
        "- State vector compliance",
        "- No-lookahead diagnostics",
        "- Ablation study",
        "- Sharpe and MDD",
        "- Buy-and-hold benchmark",
        "- Walk-forward validation or honest split diagnostics",
        "- SQLite demo",
        "- Cross-stock common-window robustness",
        "- Dashboard output",
        "- Final report generated",
        "- Presentation/demo materials generated",
        "- GitHub submission checklist generated",
    ]
    cell = {"cell_type": "markdown", "metadata": {}, "source": [line + "\n" for line in checklist]}
    cells = payload.get("cells", [])
    cells = [existing for existing in cells if "Final Submission Checklist" not in "".join(existing.get("source", []))]
    cells.append(cell)
    payload["cells"] = cells
    notebook.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")


def _final_report_markdown(cross_discussion: str) -> str:
    return "\n".join(
        [
            "# NLP-Driven Reinforcement Learning Trading Platform Final Report",
            "",
            "## Executive Summary",
            "",
            "This project builds an end-to-end platform that converts financial news and OHLCV market data into daily NLP sentiment signals, feeds those signals into a from-scratch DQN trading agent, and evaluates whether NLP improves trading performance.",
            "",
            "The cautious answer is: NLP does not universally improve DQN performance. It may help some stocks and hurt others depending on sentiment coverage, signal quality, market regime, and time alignment.",
            "",
            "## Introduction and Research Question",
            "",
            "Research question: Do NLP-derived financial sentiment signals improve reinforcement-learning trading decisions compared with a DQN that only uses price/technical features and a buy-and-hold benchmark?",
            "",
            "## System Architecture",
            "",
            "The system keeps five modules connected: data ingestion, NLP sentiment, optional SQLite storage, DQN trading, and Streamlit dashboard/reporting. See `reports/system_architecture.mmd` and stock-level `system_architecture.mmd` files.",
            "",
            "## Data Ingestion and Data Quality",
            "",
            "The ingestion layer uses local master CSV caches by default and only scrapes when explicitly requested. A-share data uses Tencent/Eastmoney/CNINFO/Sina fallbacks; generated OHLCV summaries are explicitly labelled and should not be confused with external news.",
            "",
            "## NLP Pipeline and Gold-Label Evaluation",
            "",
            "The pipeline includes lexicon sentiment, logistic regression with TF-IDF, and FinBERT. FinBERT is preferred as the main RL sentiment input when it is available. If the local model is absent and `FINBERT_ALLOW_DOWNLOAD=1` is not set, FinBERT is marked as skipped and fallback sentiment is reported.",
            "",
            "Gold-label evidence is stored in `reports/tables/nlp_gold_label_evaluation.csv`. If no manual labels exist yet, `reports/tables/nlp_gold_label_template.csv` provides a 300-500 item annotation template.",
            "",
            "## Data Storage Design",
            "",
            "SQLite schema evidence is saved in `reports/tables/sqlite_schema_summary.csv`, with roundtrip checks in `reports/tables/sqlite_roundtrip_check.csv` and explanation in `reports/sqlite_demo.md`.",
            "",
            "## RL Trading Environment",
            "",
            "The environment uses state `[price, MA50, MA200, RSI, MACD, position, cash, sentiment_score]`, actions Hold/Buy/Sell, transaction costs, and portfolio-return rewards.",
            "",
            "## DQN Implementation from Scratch",
            "",
            "The DQN is implemented with PyTorch networks, epsilon-greedy exploration, replay buffer, and target network updates. Stable-Baselines3 or prebuilt RL agents are not used.",
            "",
            "## Experiment Design",
            "",
            "Experiments compare buy-and-hold, DQN without NLP, and DQN with NLP across fixed chronological splits and multiple seeds where available.",
            "",
            "## Coverage-Controlled Experimental Design",
            "",
            "Due to uneven temporal news density, we adopt a coverage-controlled experimental design. For each stock, the recent 80% of news observations defines a high-information-density evaluation window. The DQN agent learns general trading behavior from longer historical market data, while NLP sentiment signals are evaluated in the dense window where textual information is sufficiently available. Cross-stock robustness is performed only on the common overlapping dense window to ensure fair comparison.",
            "",
            "The low-density historical period remains useful for learning market-based trading behavior and for robustness checks, but it is not treated as reliable evidence for NLP-dependent policy learning. No-news days are represented with explicit `news_available` and `sentiment_missing_flag` features rather than being silently interpreted as neutral sentiment.",
            "",
            "## Walk-Forward / Chronological Validation",
            "",
            "Current evidence is saved in `reports/tables/walk_forward_results.csv`. Rows are explicitly labelled as chronological holdout + walk-forward split diagnostics unless generated by true rolling retraining. This avoids overclaiming validation strength.",
            "",
            "## Single-Stock Results",
            "",
            "Single-stock report drafts and figures are available under `outputs/stocks/<symbol>/reports/` and `outputs/stocks/<symbol>/results/`.",
            "",
            "## Cross-Stock Robustness Analysis",
            "",
            cross_discussion or "Cross-stock discussion is generated in `outputs/system/cross_stock_discussion.md`.",
            "",
            "## Ablation Study: With NLP vs Without NLP",
            "",
            "The key comparison is `dqn_with_nlp` versus `dqn_without_nlp`, using the same chronological test period. The conclusion should be interpreted together with sentiment coverage and market regime.",
            "",
            "## Risk Metrics: Sharpe, MDD, Drawdown",
            "",
            "The evaluation reports final equity, cumulative return, Sharpe ratio, maximum drawdown, drawdown curves, and portfolio curves.",
            "",
            "## Dashboard and System Integration",
            "",
            "The Streamlit dashboard supports cached-data workflows, status logs, feasibility audits, cross-stock summaries, and downloadable bundles. Live scraping is not required for the final presentation.",
            "",
            "## Critical Reflection",
            "",
            "NLP signals are noisy and unevenly distributed through time. High news density near recent dates can dominate training unless aggregation and diagnostics are used carefully.",
            "",
            "## Limitations",
            "",
            "- FinBERT requires a local model cache or explicit download permission.",
            "- Gold-label evaluation requires manual labels; pseudo-label F1 is not treated as gold-label F1.",
            "- Walk-forward diagnostics are not the same as full rolling retraining unless explicitly run.",
            "- Money-flow proxy is explanatory and should not be interpreted as true exchange-level capital flow.",
            "",
            "## Conclusion",
            "",
            "NLP can improve RL trading in selected settings, but it is not universally beneficial. The final answer is conditional on sentiment coverage, signal quality, market regime, and leakage-safe time alignment.",
            "",
            "## Appendix",
            "",
            "- Figure index: `reports/final_report_figure_index.csv`",
            "- Table index: `reports/final_report_table_index.csv`",
            "- GitHub checklist: `reports/github_submission_checklist.md`",
            "- Demo script: `reports/live_demo_script.md`",
        ]
    )


def _presentation_outline() -> str:
    return "\n".join(
        [
            "# Presentation Outline: 25 Minutes + Q&A",
            "",
            "1. Motivation and research question - 2 min",
            "2. System architecture - 3 min",
            "3. Data pipeline and data quality - 3 min",
            "4. NLP methods: lexicon, logistic TF-IDF, FinBERT - 4 min",
            "5. RL state, trading environment, and from-scratch DQN - 4 min",
            "6. Ablation design and validation controls - 3 min",
            "7. Results and risk metrics - 3 min",
            "8. Cross-stock robustness - 2 min",
            "9. Dashboard demo - 3 min",
            "10. Limitations and conclusion - 1 min",
            "",
            "Core conclusion: NLP does not universally improve DQN performance; it is conditional on coverage, signal quality, market regime, and time alignment.",
        ]
    )


def _live_demo_script() -> str:
    return "\n".join(
        [
            "# Live Demo Script",
            "",
            "Use cached data by default. Do not require live scraping during presentation.",
            "",
            "1. Start dashboard: `python main.py --dashboard`.",
            "2. Select a stock with existing outputs, such as `002475`, `300750`, or `600519`.",
            "3. Keep `Reuse cached CSV / master slice` enabled.",
            "4. Show preflight/audit status and explain data coverage.",
            "5. Open sentiment trend, news count, portfolio curves, ablation metrics, and diagnostics.",
            "6. Explain FinBERT status honestly: actual run when available, fallback when skipped.",
            "7. Show cross-stock reliability status and common-window diagnostics.",
            "",
            "Fallback plan: use screenshots, precomputed outputs, and dashboard bundles under `outputs/system/dashboard_exports/`.",
        ]
    )


def _qa_preparation() -> str:
    return "\n".join(
        [
            "# Q&A Preparation",
            "",
            "## Did NLP improve trading performance?",
            "",
            "Sometimes. The evidence is mixed, so the conclusion is cautious rather than universal.",
            "",
            "## Why can NLP hurt?",
            "",
            "Sentiment coverage may be sparse, news can be delayed, and noisy sentiment can push the DQN toward overtrading.",
            "",
            "## Is FinBERT really used?",
            "",
            "Only when `finbert_status=ok`. If the model is unavailable, outputs state `skipped` and the RL input falls back to logistic or lexicon sentiment.",
            "",
            "## How do you avoid look-ahead bias?",
            "",
            "Technical features are shifted and news is aligned conservatively to tradable dates. Diagnostics are saved with each run.",
            "",
            "## Why not use Stable-Baselines3?",
            "",
            "The guideline requires a DQN from scratch, so the project implements the network, replay buffer, and target updates directly.",
        ]
    )


def _news_from_integrated(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    title_col = "event_title" if "event_title" in frame.columns else "title"
    content_col = "event_summary" if "event_summary" in frame.columns else "content"
    news = pd.DataFrame(
        {
            "news_id": [f"{symbol}_{idx}" for idx in frame.index],
            "symbol": symbol,
            "date": frame["date"],
            "title": frame.get(title_col, ""),
            "content": frame.get(content_col, ""),
            "source": frame.get("event_source", "integrated_csv"),
        }
    )
    return news


def _sqlite_schema_summary(db_path: Path) -> pd.DataFrame:
    rows = []
    with sqlite3.connect(db_path) as conn:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn)
        for table in tables["name"].tolist():
            info = pd.read_sql_query(f"PRAGMA table_info({table})", conn)
            count = pd.read_sql_query(f"SELECT COUNT(*) AS row_count FROM {table}", conn)["row_count"].iloc[0]
            rows.append({"table_name": table, "row_count": int(count), "columns": ", ".join(info["name"].tolist())})
    return pd.DataFrame(rows)


def _sqlite_roundtrip_check(db_path: Path) -> pd.DataFrame:
    rows = []
    for table in ["news_table", "market_table", "sentiment_table", "trading_log_table"]:
        data = load_table(table, db_path)
        date_range = ""
        if not data.empty and "date" in data.columns:
            dates = pd.to_datetime(data["date"], errors="coerce")
            date_range = f"{dates.min().date()} to {dates.max().date()}" if dates.notna().any() else ""
        rows.append({"table_name": table, "row_count": int(len(data)), "date_range": date_range, "sample": data.head(1).to_json(force_ascii=False, orient="records")})
    return pd.DataFrame(rows)


def _plot_walk_forward(results: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(8, 4.5))
    if not results.empty and {"test_end", "strategy", "final_equity"}.issubset(results.columns):
        for strategy, group in results.dropna(subset=["final_equity"]).groupby("strategy"):
            plt.plot(pd.to_datetime(group["test_end"], errors="coerce"), pd.to_numeric(group["final_equity"], errors="coerce"), marker="o", label=strategy)
        plt.legend()
    plt.title("Walk-Forward / Chronological Split Performance")
    plt.xlabel("Test window end")
    plt.ylabel("Final equity")
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=160)
    plt.close()


def _collect_index_rows(paths, suffixes: set[str], item_type: str) -> list[dict[str, object]]:
    rows = []
    for index, path in enumerate(sorted(paths), start=1):
        if path.suffix.lower() in suffixes and path.name not in {"final_report_figure_index.csv", "final_report_table_index.csv"}:
            rows.append({"index": index, "type": item_type, "path": str(path.relative_to(PROJECT_ROOT)), "description": path.stem.replace("_", " ")})
    return rows


def _safe_read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _latest_existing(paths) -> Path | None:
    existing = [path for path in paths if path.exists()]
    return sorted(existing, key=lambda item: item.stat().st_mtime)[-1] if existing else None


def _first_symbol_with_outputs() -> str:
    for path in sorted(STOCK_OUTPUT_ROOT.iterdir()):
        if path.is_dir() and (path / "data").exists():
            return path.name
    return "002475"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    columns = list(frame.columns)
    rows = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for _, item in frame.iterrows():
        values = [str(item.get(column, "")).replace("\n", " ")[:160] for column in columns]
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join(rows)
