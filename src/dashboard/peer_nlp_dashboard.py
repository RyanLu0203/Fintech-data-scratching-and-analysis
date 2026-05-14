"""Clean dashboard for peer sentiment and market-impact NLP experiments."""

from __future__ import annotations

import json
import importlib
import inspect
import os
import re
import sys
import traceback
from datetime import date
from pathlib import Path
from typing import Callable
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/fintechgp_mpl_cache")

import main as main_module
from src.config.paths import STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR, stock_data_dir, stock_reports_dir, stock_results_dir
from src.config.settings import settings
from src.nlp.peer_sentiment import build_stock_sector_mapping

PALETTE = {
    "ink": "#3f3157",
    "plum": "#604771",
    "lavender": "#846992",
    "rose": "#995a70",
    "mist": "#d5bfc2",
    "sand": "#e7d5be",
    "blue": "#2d8de0",
    "green": "#4f8f63",
    "red": "#c14f58",
}
COLOR_SEQUENCE = [PALETTE["ink"], PALETTE["blue"], PALETTE["green"], PALETTE["rose"], "#f59e0b", "#7c3aed"]
STRATEGY_LABELS = {
    "buy_and_hold": "Buy & Hold",
    "dqn_without_nlp": "DQN Without NLP",
    "dqn_with_sector_peer_nlp": "DQN + Sector Sentiment",
    "dqn_with_marketwide_peer_nlp": "DQN + Marketwide Sentiment",
    "dqn_with_sector_sentiment_nlp": "DQN + Sector Sentiment",
    "dqn_with_marketwide_sentiment_nlp": "DQN + Marketwide Sentiment",
    "dqn_with_sector_impact_nlp": "DQN + Sector Impact",
    "dqn_with_marketwide_impact_nlp": "DQN + Marketwide Impact",
}
STRATEGY_COLORS = {
    "Buy & Hold": PALETTE["ink"],
    "DQN Without NLP": PALETTE["blue"],
    "DQN + Sector Sentiment": PALETTE["green"],
    "DQN + Marketwide Sentiment": PALETTE["rose"],
    "DQN + Sector Impact": "#f59e0b",
    "DQN + Marketwide Impact": "#7c3aed",
}
STATUS_JSON = SYSTEM_OUTPUT_DIR / "latest_peer_nlp_run_status.json"
STATUS_LOG = SYSTEM_OUTPUT_DIR / "latest_peer_nlp_run_log.csv"
PEER_TRAINING_LOG = SYSTEM_OUTPUT_DIR / "latest_peer_training_progress.csv"
STAGES = [
    "Load target stock config",
    "Load or fetch target market/news data",
    "Identify sector and peer stocks",
    "Check / fetch peer data",
    "Build sector sentiment corpus",
    "Build market-impact labelled corpus",
    "Train / fit peer sentiment NLP",
    "Score target stock news",
    "Aggregate daily signals",
    "Build lagged DQN features",
    "Pretrain shared market-only DQN backbone",
    "Run DQN without NLP",
    "Run DQN with peer sentiment NLP",
    "Run DQN with market-impact NLP",
    "Compute metrics",
    "Generate figures",
    "Save outputs",
    "Update dashboard result cache",
]


st.set_page_config(page_title="Peer NLP Trading Experiment", layout="wide")
st.markdown(
    f"""
    <style>
    html, body, [class*="css"] {{ color: {PALETTE['ink']}; }}
    .stApp {{ background: #ffffff; }}
    h1, h2, h3 {{ color: {PALETTE['ink']}; letter-spacing: 0; }}
    div[data-testid="stMetricValue"] {{ color: {PALETTE['ink']}; }}
    .caption {{ color: {PALETTE['lavender']}; font-size: 0.92rem; margin-top: -0.6rem; margin-bottom: 1.2rem; }}
    </style>
    """,
    unsafe_allow_html=True,
)
px.defaults.color_discrete_sequence = COLOR_SEQUENCE
px.defaults.template = "plotly_white"


def main() -> None:
    st.sidebar.title("Workflow Control")
    mapping = _load_mapping()
    symbol = _normalize_symbol(st.sidebar.text_input("Target symbol", value=settings.default_symbol))
    company = _company_from_mapping(symbol, mapping)
    target_meta = _target_meta(mapping, symbol, company)
    company = target_meta.get("company_name") or company
    st.sidebar.markdown(f"**Resolved company:** {company}")
    st.sidebar.markdown(f"**Resolved sector:** {target_meta['sector']}")
    start_date = st.sidebar.date_input("Start date", value=_parse_date(settings.default_start_date))
    end_date = st.sidebar.date_input("End date", value=_parse_date(settings.default_end_date))
    sources = st.sidebar.text_input("Market source priority", value="tencent")
    news_cap = st.sidebar.number_input("News cap", min_value=100, max_value=100000, value=5000, step=500)
    episodes = st.sidebar.number_input("DQN episodes", min_value=1, max_value=1000, value=200, step=10)
    st.sidebar.markdown(f"**Initial cash:** {settings.default_initial_cash:,.0f}")
    corpus_scope = st.sidebar.selectbox(
        "Peer corpus scope",
        ["sector_only", "sector_plus_marketwide"],
        index=0,
        help="sector_only 只跑目标股票同板块训练集；sector_plus_marketwide 额外跑全市场 peer benchmark，会明显更慢。",
    )
    include_marketwide_peer = corpus_scope == "sector_plus_marketwide"
    st.session_state["include_marketwide_peer"] = include_marketwide_peer
    run_ingestion = True
    reuse_cached = True
    allow_fetch = True
    use_sqlite = True
    run_peer_sentiment = True
    run_market_impact = True
    run_dqn = True
    st.sidebar.info(
        "每次实验会按完整流程运行：数据准备 → peer corpus → sentiment/impact NLP → DQN → 输出结果。"
        + ("当前为 sector-only，只处理同板块训练集。" if not include_marketwide_peer else "当前会额外处理全市场 marketwide benchmark。")
    )

    peer_plan = _peer_plan(mapping, symbol, target_meta["sector"])
    with st.sidebar.expander("Resolved peer/training plan", expanded=True):
        sidebar_plan = _role_peer_plan(peer_plan, symbol)
        target_rows = sidebar_plan[sidebar_plan["role"] == "experiment_target"]
        training_rows = sidebar_plan[sidebar_plan["role"] == "training_peer"]
        st.caption("Experiment set")
        st.dataframe(target_rows[["symbol", "company_name", "sector"]], hide_index=True, width="stretch")
        st.caption(f"Training peer set: {len(training_rows)} same-sector stocks, target excluded")
        st.dataframe(training_rows[["symbol", "company_name", "sector"]], hide_index=True, width="stretch")
        st.caption("输入目标股票后，这里会自动按板块列出同板块训练集股票；目标股只作为 experiment set，不进入自己的 NLP 训练语料。")

    if st.sidebar.button("Run target experiment", type="primary", width="stretch"):
        _reset_progress()
        _mark_stage("Load target stock config", "completed", f"Target={symbol}, company={company}, sector={target_meta['sector']}.", symbol=symbol, raw_stage="target_config")
        with st.spinner("Running peer NLP transfer workflow..."):
            try:
                pipeline_runner = _fresh_pipeline_runner()
                kwargs = {
                    "symbol": symbol,
                    "company_name": company,
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "sources": sources,
                    "news_count": int(news_cap),
                    "run_ingestion_flag": bool(run_ingestion),
                    "run_nlp_flag": bool(run_peer_sentiment or run_market_impact or run_dqn),
                    "run_rl_flag": bool(run_dqn),
                    "run_ablation_flag": bool(run_dqn),
                    "episodes": int(episodes),
                    "initial_cash": float(settings.default_initial_cash),
                    "use_sqlite": bool(use_sqlite),
                    "reuse_existing_csv": bool(reuse_cached),
                    "require_news": False,
                    "build_cross_stock_outputs": True,
                    "run_high_density_ablation": False,
                    "run_peer_nlp_experiment": bool(run_peer_sentiment or run_market_impact),
                    "run_legacy_stock_level_nlp": False,
                    "allow_fetch_missing_sector_peers": bool(allow_fetch),
                    "run_market_impact_nlp": bool(run_market_impact),
                    "market_impact_horizon_days": settings.market_impact_horizon_days,
                    "market_impact_pos_threshold": settings.market_impact_pos_threshold,
                    "market_impact_neg_threshold": settings.market_impact_neg_threshold,
                    "status_callback": _progress_callback(symbol),
                }
                if "include_marketwide_peer" in inspect.signature(pipeline_runner).parameters:
                    kwargs["include_marketwide_peer"] = include_marketwide_peer
                summary = pipeline_runner(**kwargs)
                _mark_stage("Compute metrics", "completed", "Metrics computed.")
                _mark_stage("Generate figures", "completed", "Figures and report artifacts generated.")
                _mark_stage("Save outputs", "completed", "Experiment CSV, logs, and report outputs saved.")
                _mark_stage("Update dashboard result cache", "completed", "Run finished.")
                st.session_state["last_summary"] = summary
            except Exception as exc:
                _mark_stage("Update dashboard result cache", "failed", str(exc))
                st.error(f"Run failed: {exc}")
                st.code(traceback.format_exc())

    st.title("Peer NLP Transfer Trading Experiment")
    st.caption("Baseline peer-sentiment NLP is preserved; market-impact NLP adds future-return-labelled peer text signals. Target stock is held out from all peer NLP training.")
    _progress_panel()

    stock_bundle = _load_stock_bundle(symbol)
    symbols = _available_symbols(symbol)
    selected_index = symbols.index(symbol) if symbol in symbols else 0
    selected_symbol = st.selectbox("Select target stock to inspect", symbols, index=selected_index)
    if selected_symbol != symbol:
        symbol = selected_symbol
        company = _company_from_mapping(symbol, mapping)
        target_meta = _target_meta(mapping, symbol, company)
        company = target_meta.get("company_name") or company
        peer_plan = _peer_plan(mapping, symbol, target_meta["sector"])
        stock_bundle = _load_stock_bundle(symbol)

    tabs = st.tabs(
        [
            "Experiment Overview",
            "Data & News Flow",
            "Peer Corpus Builder",
            "NLP Signal Lab",
            "DQN Ablation Results",
            "Trading Behavior",
            "Optional Cross-Stock Robustness",
            "Reliability & Export",
        ]
    )
    with tabs[0]:
        _overview_tab(symbol, company, target_meta, peer_plan, stock_bundle)
    with tabs[1]:
        _data_news_tab(stock_bundle)
    with tabs[2]:
        _peer_corpus_tab(symbol, mapping, peer_plan)
    with tabs[3]:
        _nlp_signal_tab(stock_bundle)
    with tabs[4]:
        _dqn_tab(stock_bundle)
    with tabs[5]:
        _trading_tab(stock_bundle)
    with tabs[6]:
        _cross_stock_tab()
    with tabs[7]:
        _reliability_export_tab(symbol, stock_bundle)


def _overview_tab(symbol: str, company: str, target_meta: dict[str, str], peer_plan: pd.DataFrame, bundle: dict[str, pd.DataFrame]) -> None:
    st.header("Experiment Overview")
    cols = st.columns(5)
    cols[0].metric("Target", symbol)
    cols[1].metric("Company", company or target_meta["company_name"])
    cols[2].metric("Sector", target_meta["sector"])
    cols[3].metric("Peer count", max(len(peer_plan) - 1, 0))
    cols[4].metric("Industry", target_meta["industry"])
    daily = bundle["sentiment_daily"]
    impact = bundle["impact_daily"]
    high_start = _first_value([impact, daily], "high_density_eval_start", "")
    high_end = _first_value([impact, daily], "high_density_eval_end", "")
    st.info(
        "Target stock held out → peer corpus from other stocks → sentiment/impact NLP scores target news → lagged scores enter DQN state → compare no NLP vs sentiment NLP vs market-impact NLP."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {"item": "Official version", "value": "Peer-Sector Sentiment + Market-Impact NLP"},
                {"item": "Market-learning window", "value": f"Before {high_start or 'detected high-density start'}"},
                {"item": "High-density evaluation window", "value": f"{high_start} to {high_end}"},
                {"item": "Baseline preserved", "value": "peer_sector_sentiment_nlp"},
                {"item": "Improved experiment", "value": "peer_market_impact_nlp"},
            ]
        ),
        hide_index=True,
        width="stretch",
    )
    _caption("This overview separates the experiment target from its training peers. The target stock is not used to train either sentiment or market-impact NLP models.")


def _data_news_tab(bundle: dict[str, pd.DataFrame]) -> None:
    st.header("Data & News Flow")
    market = bundle["market"]
    news_daily = _daily_news_counts(bundle)
    if market.empty:
        st.warning("No local market CSV found for this target. Run ingestion or choose a stock with outputs.")
        return
    fig = px.line(market, x="date", y="close", title="Close Price", labels={"date": "Date", "close": "Close price"})
    _plot(fig, "Close price tracks the target stock's market-learning and evaluation periods.")
    if "volume" in market.columns:
        fig = px.bar(market, x="date", y="volume", title="Daily Volume", labels={"date": "Date", "volume": "Volume"})
        _plot(fig, "Volume is shown as market activity context; it is not a replacement for text-based NLP signals.")
    if not news_daily.empty:
        fig = px.bar(news_daily, x="date", y="news_count", title="Daily News Count", labels={"date": "Date", "news_count": "News count"})
        high_start, high_end = _density_window(bundle)
        if high_start:
            fig.add_vrect(x0=high_start, x1=high_end, fillcolor=PALETTE["mist"], opacity=0.25, line_width=0)
        _plot(fig, "This chart shows information density. The shaded region is the high-density evaluation window when available.")
        news_daily = news_daily.sort_values("date").copy()
        total = max(news_daily["news_count"].sum(), 1)
        news_daily["cumulative_news_pct"] = news_daily["news_count"].cumsum() / total
        fig = px.line(news_daily, x="date", y="cumulative_news_pct", title="Cumulative News Percentage", labels={"date": "Date", "cumulative_news_pct": "Cumulative news share"})
        fig.add_hline(y=0.8, line_dash="dash", line_color=PALETTE["rose"])
        _plot(fig, "The 80% line helps identify where recent high-density news begins.")
    flow = bundle["net_flow"]
    if not flow.empty:
        value_col = _first_present(flow, ["net_flow", "net_flow_proxy", "daily_net_flow", "money_flow_proxy"])
        if value_col:
            fig = px.bar(flow, x="date", y=value_col, title="Money Flow / Net-Flow Proxy", labels={"date": "Date", value_col: "Net inflow / outflow"})
            _plot(fig, "Net flow is diagnostic context. If it is derived from same-day OHLCV, it should not be used as a predictive DQN state feature.")
    else:
        st.info("No net-flow file found for this stock.")


def _peer_corpus_tab(symbol: str, mapping: pd.DataFrame, peer_plan: pd.DataFrame) -> None:
    st.header("Peer Corpus Builder")
    st.subheader("Stock Sector Mapping")
    if mapping.empty:
        st.warning("No stock-sector mapping found. Run sector mapping generation or update config/stock_sector_mapping.csv.")
    else:
        st.dataframe(mapping, hide_index=True, width="stretch")
    st.subheader("Same-Sector Peer List")
    st.dataframe(_role_peer_plan(peer_plan, symbol), hide_index=True, width="stretch")
    _caption("The target row is the experiment set. Every other same-sector row is eligible for the sector-peer NLP training corpus.")
    corpus = _read_csv(PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv")
    impact_corpus = _read_csv(PROJECT_ROOT / "reports" / "tables" / "market_impact_corpus_summary.csv")
    for title, frame in [("Peer Sentiment Corpus Summary", corpus), ("Market-Impact Corpus Summary", impact_corpus)]:
        st.subheader(title)
        if frame.empty:
            st.warning(f"{title} is missing.")
        else:
            view = frame[frame.get("target_symbol", pd.Series(dtype=str)).astype(str) == symbol] if "target_symbol" in frame.columns else frame
            st.dataframe(view, hide_index=True, width="stretch")
    if not mapping.empty and "sector" in mapping.columns:
        counts = mapping.groupby("sector", as_index=False)["symbol"].count().rename(columns={"symbol": "stock_count"})
        fig = px.bar(counts, x="sector", y="stock_count", title="Peer Stock Count by Sector", labels={"sector": "Sector", "stock_count": "Stock count"})
        _plot(fig, "A sector needs enough peer stocks for a reliable held-out transfer experiment.")
    if not impact_corpus.empty and {"bullish_count", "neutral_count", "bearish_count"}.issubset(impact_corpus.columns):
        target_rows = impact_corpus[impact_corpus["target_symbol"].astype(str) == symbol].copy()
        if not target_rows.empty:
            melted = target_rows.melt(id_vars=["corpus_type"], value_vars=["bullish_count", "neutral_count", "bearish_count"], var_name="impact_class", value_name="count")
            fig = px.bar(melted, x="impact_class", y="count", color="corpus_type", barmode="group", title="Market-Impact Label Distribution", labels={"impact_class": "Pseudo-label", "count": "Labelled news rows"})
            _plot(fig, "The impact model is most trustworthy when bullish, neutral, and bearish labels are not extremely imbalanced.")


def _nlp_signal_tab(bundle: dict[str, pd.DataFrame]) -> None:
    st.header("NLP Signal Lab")
    signals = _combined_signals(bundle)
    if signals.empty:
        st.warning("No peer sentiment or market-impact daily signal files found.")
        return
    signal_cols = _filter_scope_signal_columns([col for col in ["sector_sentiment_score", "marketwide_sentiment_score", "sector_impact_score", "marketwide_impact_score"] if col in signals.columns])
    if signal_cols:
        fig = px.line(
            signals,
            x="date",
            y=signal_cols,
            title="Daily NLP Signals",
            labels={"date": "Trading date", "value": "NLP signal score", "variable": "Signal type"},
            color_discrete_sequence=[PALETTE["green"], PALETTE["rose"], "#f59e0b", "#7c3aed"],
        )
        _plot(fig, "Sentiment score measures tone; impact score measures learned peer market reaction.")
    validity = bundle["signal_validity"]
    if not validity.empty:
        st.subheader("Signal Validity Summary")
        st.dataframe(validity, hide_index=True, width="stretch")
        fig = px.bar(validity, x="signal", y="information_coefficient", color="horizon_days", barmode="group", title="Information Coefficient by Signal", labels={"signal": "Signal type", "information_coefficient": "Information coefficient", "horizon_days": "Future-return horizon"})
        _plot(fig, "IC shows whether higher signal values tend to rank future returns higher.")
    quantiles = bundle["signal_quantiles"]
    if not quantiles.empty:
        fig = px.bar(quantiles, x="signal_quantile", y="average_future_return", color="signal", barmode="group", title="Signal Quantile vs Future Return", labels={"signal_quantile": "Signal quantile bucket", "average_future_return": "Average future return", "signal": "Signal type"})
        _plot(fig, "A monotonic quantile pattern is stronger evidence that a signal has trading relevance.")
    if {"sector_sentiment_score", "sector_impact_score"}.issubset(signals.columns):
        fig = px.scatter(signals, x="sector_sentiment_score", y="sector_impact_score", title="Sentiment vs Market-Impact Score", labels={"sector_sentiment_score": "Sector peer sentiment score", "sector_impact_score": "Sector peer market-impact score"})
        _plot(fig, "This compares text tone with expected market reaction learned from peer future returns.")
    if "target_news_count" in signals.columns:
        fig = px.bar(signals, x="date", y="target_news_count", title="Daily Target News Count", labels={"date": "Trading date", "target_news_count": "Target news rows scored"})
        _plot(fig, "NLP signals are only meaningful on days where the target stock has news to score.")


def _dqn_tab(bundle: dict[str, pd.DataFrame]) -> None:
    st.header("DQN Ablation Results")
    metrics = bundle["market_metrics"]
    curves = bundle["market_curves"]
    if metrics.empty:
        st.warning("market_impact_ablation_metrics.csv is missing. Run the market-impact experiment to populate six-strategy results.")
        return
    metrics = _filter_scope_experiments(_with_strategy_labels(metrics))
    _dqn_readiness_messages(metrics, bundle)
    best = _best_metric_row(metrics, "final_equity")
    if best is not None:
        st.success(
            f"Best group by final equity: {best['strategy_label']} | "
            f"final equity={_fmt_number(best.get('final_equity'))}, "
            f"return={_fmt_pct(best.get('cumulative_return'))}, "
            f"Sharpe={_fmt_number(best.get('sharpe_ratio'))}."
        )
        cols = st.columns(4)
        cols[0].metric("Best Strategy", best["strategy_label"])
        cols[1].metric("Final Equity", _fmt_number(best.get("final_equity")))
        cols[2].metric("Cumulative Return", _fmt_pct(best.get("cumulative_return")))
        cols[3].metric("Sharpe Ratio", _fmt_number(best.get("sharpe_ratio")))
    st.dataframe(metrics, hide_index=True, width="stretch")
    if not curves.empty:
        curves = _mean_portfolio_curve(_filter_scope_experiments(_with_strategy_labels(curves)))
        fig = px.line(
            curves,
            x="date",
            y="portfolio_value",
            color="strategy_label",
            color_discrete_map=STRATEGY_COLORS,
            title="Portfolio Value Curve",
            labels={"date": "Trading date", "portfolio_value": "Portfolio value (cash + holdings)", "strategy_label": "Experiment group"},
        )
        _plot(fig, "All strategies use the same DQN settings; only the NLP signal source changes.")
    for metric, title, ylabel in [
        ("final_equity", "Final Equity by Strategy", "Final equity"),
        ("cumulative_return", "Cumulative Return by Strategy", "Cumulative return"),
        ("sharpe_ratio", "Sharpe Ratio by Strategy", "Sharpe ratio"),
        ("max_drawdown", "Max Drawdown by Strategy", "Max drawdown; lower is better"),
    ]:
        if metric in metrics.columns:
            plot_frame = metrics[pd.to_numeric(metrics[metric], errors="coerce").notna()].copy()
            if plot_frame.empty:
                st.info(f"{title} has no valid rows to plot. This usually means that corpus scope disabled or invalidated that strategy.")
                continue
            fig = px.bar(
                plot_frame,
                x="strategy_label",
                y=metric,
                color="strategy_label",
                color_discrete_map=STRATEGY_COLORS,
                title=title,
                labels={"strategy_label": "Experiment group", metric: ylabel},
            )
            fig.update_layout(xaxis_tickangle=-20)
            _plot(fig, f"{title} compares no NLP, peer sentiment NLP, and market-impact NLP under the same test window.")
    effect = bundle["market_effect"]
    if not effect.empty:
        st.subheader("Effect Summary")
        st.dataframe(effect, hide_index=True, width="stretch")


def _dqn_readiness_messages(metrics: pd.DataFrame, bundle: dict[str, pd.DataFrame]) -> None:
    """Explain missing/flat DQN groups without breaking the dashboard render."""

    if metrics.empty:
        return

    include_marketwide = st.session_state.get("include_marketwide_peer", False)
    expected = [
        "buy_and_hold",
        "dqn_without_nlp",
        "dqn_with_sector_sentiment_nlp",
        "dqn_with_sector_impact_nlp",
    ]
    if include_marketwide:
        expected.extend(["dqn_with_marketwide_sentiment_nlp", "dqn_with_marketwide_impact_nlp"])

    present = set(metrics.get("experiment", pd.Series(dtype=str)).astype(str))
    missing = [STRATEGY_LABELS.get(name, name) for name in expected if name not in present]
    invalid = []
    for name in expected:
        rows = metrics[metrics.get("experiment", pd.Series(dtype=str)).astype(str) == name]
        if rows.empty:
            continue
        equity = pd.to_numeric(rows.get("final_equity", pd.Series(dtype=float)), errors="coerce")
        trades = pd.to_numeric(rows.get("number_of_trades", pd.Series(dtype=float)), errors="coerce")
        if equity.isna().all():
            invalid.append(f"{STRATEGY_LABELS.get(name, name)} has no valid final equity")
        elif name.startswith("dqn_") and trades.fillna(0).sum() == 0:
            invalid.append(f"{STRATEGY_LABELS.get(name, name)} made zero trades")

    effect = bundle.get("market_effect", pd.DataFrame())
    reasons: list[str] = []
    if not effect.empty:
        row = effect.iloc[0]
        for col in ["corpus_status", "impact_corpus_status", "sentiment_corpus_status", "reliability_status", "reason_if_not_reliable"]:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                reasons.append(f"{col}: {row[col]}")

    if missing or invalid or reasons:
        with st.expander("Why some DQN groups may be missing or unreliable", expanded=False):
            if missing:
                st.warning("Missing strategy groups: " + ", ".join(missing))
            if invalid:
                for item in invalid:
                    st.info(item)
            if reasons:
                st.caption("Saved reliability/corpus notes")
                st.write("; ".join(reasons))
            st.caption(
                "Common causes: sector-only mode intentionally hides marketwide groups; an impact corpus below the labelled-news "
                "threshold skips impact DQN; zero trades means the learned policy stayed in Hold/cash during the test window."
            )


def _trading_tab(bundle: dict[str, pd.DataFrame]) -> None:
    st.header("Trading Behavior")
    logs = bundle["market_logs"]
    market = bundle["market"]
    metrics = _filter_scope_experiments(_with_strategy_labels(bundle["market_metrics"]))
    if logs.empty:
        st.warning("No market-impact trading log found.")
        return
    logs = _filter_scope_experiments(_with_strategy_labels(logs))
    label_to_raw = logs.drop_duplicates("strategy_label").set_index("strategy_label")["experiment"].to_dict()
    strategy_label = st.selectbox("Strategy", sorted(label_to_raw))
    strategy = label_to_raw[strategy_label]
    strategy_log = logs[logs["experiment"].astype(str) == str(strategy)].copy()
    if not market.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=market["date"], y=market["close"], mode="lines", name="Close", line=dict(color=PALETTE["ink"])))
        for action, color, symbol_marker in [("Buy", PALETTE["green"], "triangle-up"), ("Sell", PALETTE["red"], "triangle-down")]:
            subset = strategy_log[strategy_log.get("action", pd.Series(dtype=str)).astype(str) == action]
            if not subset.empty:
                fig.add_trace(go.Scatter(x=subset["date"], y=subset["price"], mode="markers", name=action, marker=dict(color=color, symbol=symbol_marker, size=9)))
        fig.update_layout(title=f"Price with Trading Actions: {strategy_label}", xaxis_title="Trading date", yaxis_title="Close price / execution price", legend_title="Action")
        _plot(fig, "Buy/Sell markers show how the selected strategy behaves during the high-density evaluation window.")
    action_counts = strategy_log.get("action", pd.Series(dtype=str)).astype(str).value_counts().reset_index()
    action_counts.columns = ["action", "count"]
    if not action_counts.empty:
        fig = px.bar(action_counts, x="action", y="count", color="action", title="Action Distribution", labels={"action": "Trading action", "count": "Number of actions"})
        _plot(fig, "Action distribution reveals whether the model overtrades or mostly holds.")
    curves = _mean_portfolio_curve(_filter_scope_experiments(_with_strategy_labels(bundle["market_curves"])))
    if not curves.empty and "portfolio_value" in curves.columns:
        drawdown = curves[["date", "experiment", "strategy_label", "portfolio_value"]].copy()
        drawdown["portfolio_value"] = pd.to_numeric(drawdown["portfolio_value"], errors="coerce")
        drawdown["drawdown"] = drawdown.groupby("experiment")["portfolio_value"].transform(lambda s: s / s.cummax() - 1)
        fig = px.line(
            drawdown,
            x="date",
            y="drawdown",
            color="strategy_label",
            color_discrete_map=STRATEGY_COLORS,
            title="Portfolio Drawdown by Strategy",
            labels={"date": "Trading date", "drawdown": "Drawdown from previous portfolio peak", "strategy_label": "Experiment group"},
        )
        _plot(fig, "Drawdown is portfolio value divided by its previous peak minus 1. More negative values mean deeper losses from the strategy's own earlier high.")
    for metric in ["exposure_ratio", "turnover"]:
        if metric in metrics.columns:
            fig = px.bar(
                metrics,
                x="strategy_label",
                y=metric,
                color="strategy_label",
                color_discrete_map=STRATEGY_COLORS,
                title=metric.replace("_", " ").title(),
                labels={"strategy_label": "Experiment group", metric: metric.replace("_", " ").title()},
            )
            _plot(fig, f"{metric.replace('_', ' ').title()} is shown separately to avoid mixing incompatible y-axis scales.")
    if not metrics.empty:
        st.subheader("Trading Performance Comparison")
        columns = [col for col in ["strategy_label", "final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown", "number_of_trades", "win_rate", "exposure_ratio", "turnover"] if col in metrics.columns]
        st.dataframe(metrics[columns], hide_index=True, width="stretch")
        st.markdown(_trading_summary(metrics))


def _cross_stock_tab() -> None:
    st.header("Optional Cross-Stock Robustness")
    st.info(
        "This section is optional. It checks whether the target-stock conclusion is consistent across other held-out stocks. "
        "It is not required for a single target experiment and only displays saved cross-stock outputs if they exist."
    )
    market_cross = _read_csv(SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_summary.csv")
    sentiment_cross = _read_csv(SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv")
    if market_cross.empty and sentiment_cross.empty:
        st.warning("No cross-stock summaries found yet.")
        return
    if not market_cross.empty:
        st.subheader("Market-Impact Cross-Stock Summary")
        st.dataframe(market_cross, hide_index=True, width="stretch")
        valid_targets = market_cross["target_symbol"].astype(str).nunique() if "target_symbol" in market_cross.columns else len(market_cross)
        if valid_targets < 2:
            st.warning(
                "Only one target stock is available, so these bars are not yet a true cross-stock robustness test. "
                "They are a single-target effect preview: each bar compares one NLP strategy against DQN without NLP."
            )
        for metric in ["sector_impact_effect", "marketwide_impact_effect", "sector_sentiment_effect", "marketwide_sentiment_effect"]:
            if metric in market_cross.columns:
                fig = px.bar(
                    market_cross,
                    x="target_symbol",
                    y=metric,
                    color="target_sector",
                    title=metric.replace("_", " ").title(),
                    labels={"target_symbol": "Target stock", metric: "Final equity effect versus DQN without NLP", "target_sector": "Sector"},
                )
                _plot(fig, "Positive bars mean the NLP strategy ended with higher final equity than DQN without NLP; negative bars mean it hurt performance.")
        if {"target_sentiment_coverage", "sector_impact_effect"}.issubset(market_cross.columns):
            fig = px.scatter(market_cross, x="target_sentiment_coverage", y="sector_impact_effect", color="target_sector", title="Coverage vs Sector Impact Effect", labels={"target_sentiment_coverage": "Target news coverage", "sector_impact_effect": "Sector impact equity effect"})
            _plot(fig, "This tests whether market-impact NLP works better when target news coverage is higher.")
    if not sentiment_cross.empty:
        st.subheader("Baseline Peer-Sentiment Cross-Stock Summary")
        st.dataframe(sentiment_cross, hide_index=True, width="stretch")


def _reliability_export_tab(symbol: str, bundle: dict[str, pd.DataFrame]) -> None:
    st.header("Reliability & Export")
    checks = bundle["market_reliability"]
    if checks.empty:
        st.warning("No market-impact reliability check found.")
    else:
        st.dataframe(checks, hide_index=True, width="stretch")
        passed = checks["passed"].astype(bool).mean() if "passed" in checks.columns and not checks.empty else 0
        status = "READY_FOR_PRESENTATION" if passed == 1 else "READY_WITH_WARNINGS" if passed >= 0.7 else "NOT_READY"
        st.metric("Final status", status)
    report_html = _build_html_report(symbol, bundle)
    st.download_button("Download visual report HTML", data=report_html.encode("utf-8"), file_name=f"{symbol}_peer_market_impact_report.html", mime="text/html")
    zip_bytes = _bundle_outputs(symbol)
    st.download_button("Download key CSV bundle", data=zip_bytes, file_name=f"{symbol}_peer_market_impact_outputs.zip", mime="application/zip")
    _caption("Downloads include only peer sentiment / market-impact experiment outputs, not deprecated stock-level NLP result tables.")


def _load_stock_bundle(symbol: str) -> dict[str, pd.DataFrame]:
    data_dir = stock_data_dir(symbol)
    results_dir = stock_results_dir(symbol)
    reports_dir = stock_reports_dir(symbol)
    latest_csv = _latest_file(data_dir, "*_finance_text_*.csv")
    return {
        "market": _market_from_csv(latest_csv),
        "sentiment_daily": _read_csv(results_dir / "peer_nlp_daily_sentiment.csv"),
        "impact_daily": _read_csv(results_dir / "peer_market_impact_daily_signal.csv"),
        "market_metrics": _read_csv(results_dir / "market_impact_ablation_metrics.csv"),
        "market_curves": _read_csv(results_dir / "market_impact_portfolio_curves.csv"),
        "market_logs": _read_csv(results_dir / "market_impact_trading_logs.csv"),
        "market_effect": _read_csv(results_dir / "market_impact_effect_summary.csv"),
        "market_reliability": _read_csv(reports_dir / "market_impact_reliability_check.csv"),
        "signal_validity": _read_csv(reports_dir / "signal_validity_summary.csv"),
        "signal_quantiles": _read_csv(reports_dir / "signal_quantile_future_returns.csv"),
        "net_flow": _latest_report_csv(reports_dir, "*daily_net_flow.csv"),
    }


def _progress_callback(symbol: str) -> Callable[[str, str], None]:
    def callback(stage: str, message: str) -> None:
        _record_peer_progress(stage, message)
        mapped = _map_stage(stage)
        status = "skipped" if "skipped" in stage else "running"
        _mark_stage(mapped, status, message, symbol=symbol, raw_stage=stage)
        if stage in {"peer_nlp_peer_processed", "peer_nlp_sentiment_saved", "impact_saved", "done"}:
            _mark_stage(mapped, "completed", message, symbol=symbol, raw_stage=stage)
    return callback


def _fresh_pipeline_runner() -> Callable[..., dict[str, object]]:
    """Reload pipeline modules so Streamlit hot-reload sees latest function signatures."""

    import src.evaluation.market_impact_ablation as market_impact_ablation_module
    import src.evaluation.peer_nlp_ablation as peer_nlp_ablation_module
    import src.nlp.market_impact as market_impact_module
    import src.nlp.peer_sentiment as peer_sentiment_module
    import src.rl.train as rl_train_module

    for module in [
        rl_train_module,
        peer_sentiment_module,
        market_impact_module,
        peer_nlp_ablation_module,
        market_impact_ablation_module,
        main_module,
    ]:
        importlib.reload(module)
    return main_module.run_pipeline_for_symbol


def _reset_progress() -> None:
    SYSTEM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    status = {stage: {"status": "pending", "message": ""} for stage in STAGES}
    STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(columns=["timestamp", "symbol", "raw_stage", "stage", "status", "message"]).to_csv(STATUS_LOG, index=False, encoding="utf-8-sig")
    pd.DataFrame(columns=["timestamp", "role", "corpus_type", "symbol", "company_name", "status", "raw_stage", "message"]).to_csv(
        PEER_TRAINING_LOG, index=False, encoding="utf-8-sig"
    )


def _record_peer_progress(stage: str, message: str) -> None:
    if not stage.startswith("peer_nlp_peer_"):
        return
    match = re.search(r"peer\s+(\d{6})\s+(.+?)(?:\s+with|\s+\[|;|$)", message)
    if not match:
        return
    symbol = _normalize_symbol(match.group(1))
    company = match.group(2).strip()
    corpus_type = message.split(":", 1)[0].strip() if ":" in message else ""
    status = "running"
    if stage.endswith("processed"):
        status = "completed"
    elif stage.endswith("skipped"):
        status = "skipped"
    row = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
                "role": "training_peer",
                "corpus_type": corpus_type,
                "symbol": symbol,
                "company_name": company,
                "status": status,
                "raw_stage": stage,
                "message": message,
            }
        ]
    )
    existing = _read_csv(PEER_TRAINING_LOG)
    if not existing.empty:
        existing = existing[~((existing.get("symbol", "") == symbol) & (existing.get("corpus_type", "") == corpus_type))]
    pd.concat([existing, row], ignore_index=True).to_csv(PEER_TRAINING_LOG, index=False, encoding="utf-8-sig")


def _mark_stage(stage: str, status: str, message: str, *, symbol: str = "", raw_stage: str = "") -> None:
    SYSTEM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    current = {name: {"status": "pending", "message": ""} for name in STAGES}
    if STATUS_JSON.exists():
        try:
            current.update(json.loads(STATUS_JSON.read_text(encoding="utf-8")))
        except Exception:
            pass
    if stage not in current:
        current[stage] = {"status": "pending", "message": ""}
    if status == "running":
        for name, payload in current.items():
            if name != stage and payload.get("status") == "running":
                current[name] = {"status": "completed", "message": payload.get("message", "")}
    current[stage] = {"status": status, "message": message}
    STATUS_JSON.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    row = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
                "symbol": symbol,
                "raw_stage": raw_stage,
                "stage": stage,
                "status": status,
                "message": message,
            }
        ]
    )
    existing = _read_csv(STATUS_LOG)
    pd.concat([existing, row], ignore_index=True).to_csv(STATUS_LOG, index=False, encoding="utf-8-sig")


def _progress_panel() -> None:
    status = {}
    if STATUS_JSON.exists():
        try:
            status = json.loads(STATUS_JSON.read_text(encoding="utf-8"))
        except Exception:
            status = {}
    completed = sum(1 for stage in STAGES if status.get(stage, {}).get("status") == "completed")
    running = [stage for stage in STAGES if status.get(stage, {}).get("status") == "running"]
    st.progress(completed / len(STAGES), text=f"{completed}/{len(STAGES)} stages completed" + (f"; running: {running[-1]}" if running else ""))
    status_rows = []
    for idx, stage in enumerate(STAGES, start=1):
        payload = status.get(stage, {})
        status_rows.append(
            {
                "step": idx,
                "stage": stage,
                "status": payload.get("status", "pending"),
                "latest_message": payload.get("message", ""),
                "why_this_can_take_time": _stage_runtime_hint(stage),
            }
        )
    st.dataframe(pd.DataFrame(status_rows), hide_index=True, width="stretch")
    _caption("This table is the real-time experiment flowchart: pending → running → completed/failed. It updates as target data, peer training corpus, NLP scoring, DQN, and exports finish.")
    peer_progress = _read_csv(PEER_TRAINING_LOG)
    if not peer_progress.empty:
        st.subheader("Training Peer Set Progress")
        if "company_name" in peer_progress.columns:
            peer_progress["company_name"] = peer_progress["company_name"].astype(str).str.replace(r"\s+with\s+.*$", "", regex=True)
        st.dataframe(peer_progress.sort_values(["corpus_type", "symbol"]), hide_index=True, width="stretch")
        _caption("These rows are the NLP training corpus candidates. They are same-sector or marketwide peers and are not the held-out experiment target.")
    log = _read_csv(STATUS_LOG)
    if not log.empty:
        with st.expander("Step-by-step progress log", expanded=True):
            st.dataframe(log.tail(60), hide_index=True, width="stretch")


def _map_stage(stage: str) -> str:
    mapping = {
        "ingestion": "Load or fetch target market/news data",
        "cache": "Load or fetch target market/news data",
        "peer_nlp_training_corpus": "Build sector sentiment corpus",
        "peer_nlp_peer_processing": "Check / fetch peer data",
        "peer_nlp_peer_processed": "Check / fetch peer data",
        "peer_nlp_model_training": "Train / fit peer sentiment NLP",
        "peer_nlp_target_data": "Score target stock news",
        "peer_nlp_target_scoring": "Score target stock news",
        "peer_nlp_sentiment_saved": "Aggregate daily signals",
        "impact_corpus": "Identify sector and peer stocks",
        "impact_labels": "Build market-impact labelled corpus",
        "impact_target_data": "Score target stock news",
        "impact_scoring": "Score target stock news",
        "impact_saved": "Save outputs",
        "market_impact_nlp": "Run DQN with market-impact NLP",
        "dqn_features": "Build lagged DQN features",
        "dqn_pretrain": "Pretrain shared market-only DQN backbone",
        "dqn_without_nlp": "Run DQN without NLP",
        "dqn_peer_sentiment": "Run DQN with peer sentiment NLP",
        "dqn_market_impact": "Run DQN with market-impact NLP",
        "dqn_skipped": "Run DQN with market-impact NLP",
        "metrics": "Compute metrics",
        "save_outputs": "Save outputs",
        "figures": "Generate figures",
        "signals": "Compute metrics",
        "cross_stock": "Update dashboard result cache",
        "dashboard_cache": "Update dashboard result cache",
        "done": "Update dashboard result cache",
    }
    return mapping.get(stage, "Update dashboard result cache")


def _stage_runtime_hint(stage: str) -> str:
    hints = {
        "Check / fetch peer data": "Reads every peer stock's local market/news files and builds high-density peer corpora.",
        "Train / fit peer sentiment NLP": "Fits TF-IDF/logistic sentiment models on large peer news corpora before scoring target news.",
        "Build market-impact labelled corpus": "Aligns peer news to future returns and creates bullish/neutral/bearish impact labels.",
        "Pretrain shared market-only DQN backbone": "Runs the shared market-only DQN pretraining for each random seed before NLP-specific fine-tuning starts.",
        "Run DQN without NLP": "Trains DQN across configured episodes and random seeds.",
        "Run DQN with peer sentiment NLP": "Trains sector and marketwide sentiment DQN groups across seeds.",
        "Run DQN with market-impact NLP": "Trains sector and marketwide impact DQN groups across seeds; this is usually the longest step.",
        "Compute metrics": "Aggregates seed-level metrics, portfolio curves, drawdowns, trade logs, and effect labels.",
    }
    return hints.get(stage, "")


def _plot(fig: go.Figure, caption: str) -> None:
    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color=PALETTE["ink"], size=13),
        title=dict(font=dict(size=20), x=0.02),
        margin=dict(l=76, r=32, t=72, b=82),
        height=430,
        hovermode="x unified",
        legend_title_text=fig.layout.legend.title.text if fig.layout.legend and fig.layout.legend.title else "",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(
        fixedrange=True,
        gridcolor="#eadfd6",
        showline=True,
        linecolor=PALETTE["mist"],
        ticks="outside",
        nticks=8,
        automargin=True,
        title_standoff=16,
    )
    fig.update_yaxes(
        fixedrange=True,
        gridcolor="#eadfd6",
        zeroline=True,
        zerolinecolor="#eadfd6",
        showline=True,
        linecolor=PALETTE["mist"],
        ticks="outside",
        nticks=7,
        automargin=True,
        title_standoff=16,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False, "staticPlot": False})
    _caption(caption)


def _caption(text: str) -> None:
    st.markdown(f"<div class='caption'>{text}</div>", unsafe_allow_html=True)


def _with_strategy_labels(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "experiment" not in frame.columns:
        return frame
    result = frame.copy()
    result["experiment"] = result["experiment"].astype(str)
    result["strategy_label"] = result["experiment"].map(STRATEGY_LABELS).fillna(result["experiment"])
    return result


def _filter_scope_experiments(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "experiment" not in frame.columns:
        return frame
    if st.session_state.get("include_marketwide_peer", False):
        return frame
    mask = ~frame["experiment"].astype(str).str.contains("marketwide", case=False, na=False)
    return frame[mask].copy()


def _filter_scope_signal_columns(columns: list[str]) -> list[str]:
    if st.session_state.get("include_marketwide_peer", False):
        return columns
    return [column for column in columns if "marketwide" not in column]


def _mean_portfolio_curve(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or not {"date", "experiment", "portfolio_value"}.issubset(frame.columns):
        return frame
    result = frame.copy()
    result["portfolio_value"] = pd.to_numeric(result["portfolio_value"], errors="coerce")
    if "strategy_label" not in result.columns:
        result = _with_strategy_labels(result)
    return (
        result.dropna(subset=["date", "portfolio_value"])
        .groupby(["date", "experiment", "strategy_label"], as_index=False)["portfolio_value"]
        .mean()
        .sort_values(["strategy_label", "date"])
    )


def _best_metric_row(metrics: pd.DataFrame, metric: str) -> pd.Series | None:
    if metrics.empty or metric not in metrics.columns:
        return None
    frame = metrics.copy()
    frame[metric] = pd.to_numeric(frame[metric], errors="coerce")
    frame = frame.dropna(subset=[metric])
    if frame.empty:
        return None
    return frame.sort_values(metric, ascending=False).iloc[0]


def _fmt_number(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "N/A"
    return f"{number:,.4f}" if abs(number) < 100 else f"{number:,.2f}"


def _fmt_pct(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "N/A"
    return f"{number:.2%}"


def _trading_summary(metrics: pd.DataFrame) -> str:
    equity = _best_metric_row(metrics, "final_equity")
    sharpe = _best_metric_row(metrics, "sharpe_ratio")
    text = []
    if equity is not None:
        text.append(f"- **Best final equity:** {equity['strategy_label']} with {_fmt_number(equity.get('final_equity'))}.")
    if sharpe is not None:
        text.append(f"- **Best risk-adjusted return:** {sharpe['strategy_label']} with Sharpe {_fmt_number(sharpe.get('sharpe_ratio'))}.")
    if "number_of_trades" in metrics.columns:
        trade_frame = metrics.copy()
        trade_frame["number_of_trades"] = pd.to_numeric(trade_frame["number_of_trades"], errors="coerce")
        active = trade_frame.sort_values("number_of_trades", ascending=False).head(1)
        if not active.empty:
            row = active.iloc[0]
            text.append(f"- **Most active strategy:** {row['strategy_label']} with {_fmt_number(row.get('number_of_trades'))} trades.")
    text.append("- Drawdown and exposure should be read together: a higher final equity is less convincing if it comes with deeper drawdowns or excessive trading.")
    return "\n".join(text)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        for column in ["symbol", "target_symbol", "peer_symbol"]:
            if column in frame.columns:
                frame[column] = frame[column].map(_normalize_symbol)
        return frame
    except Exception:
        return pd.DataFrame()


def _market_from_csv(path: Path | None) -> pd.DataFrame:
    frame = _read_csv(path) if path else pd.DataFrame()
    if frame.empty:
        return frame
    for column in ["open", "high", "low", "close", "volume"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["date"]).sort_values("date")


def _latest_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.stat().st_size > 4 and not path.name.endswith("_master.csv")]
    if not candidates:
        candidates = [path for path in directory.glob(pattern) if path.stat().st_size > 4]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)[-1] if candidates else None


def _latest_report_csv(directory: Path, pattern: str) -> pd.DataFrame:
    return _read_csv(_latest_file(directory, pattern) or Path("__missing__"))


def _load_mapping() -> pd.DataFrame:
    path = PROJECT_ROOT / "reports" / "tables" / "stock_sector_mapping.csv"
    frame = _read_mapping_csv(path)
    if frame.empty:
        try:
            frame = build_stock_sector_mapping()
        except Exception:
            frame = _read_mapping_csv(PROJECT_ROOT / "config" / "stock_sector_mapping.csv")
    if not frame.empty and "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_symbol)
        if "company_name" in frame.columns:
            bad_company = frame["company_name"].fillna("").astype(str).str.fullmatch(r"\d+")
            frame.loc[bad_company, "company_name"] = ""
        for column in ["company_name", "sector", "industry"]:
            if column not in frame.columns:
                frame[column] = "UNKNOWN" if column != "company_name" else frame["symbol"]
        frame["company_name"] = frame["company_name"].fillna("").astype(str)
        frame.loc[frame["company_name"].str.strip().eq(""), "company_name"] = frame.loc[frame["company_name"].str.strip().eq(""), "symbol"]
    return frame


def _read_mapping_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame()
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    return frame


def _target_meta(mapping: pd.DataFrame, symbol: str, company: str) -> dict[str, str]:
    if not mapping.empty and "symbol" in mapping.columns:
        row = mapping[mapping["symbol"].map(_normalize_symbol) == _normalize_symbol(symbol)]
        if not row.empty:
            record = row.iloc[0].to_dict()
            return {
                "company_name": str(record.get("company_name", company or symbol) or company or symbol),
                "sector": str(record.get("sector", "UNKNOWN") or "UNKNOWN"),
                "industry": str(record.get("industry", "UNKNOWN") or "UNKNOWN"),
            }
    return {"company_name": company or symbol, "sector": "UNKNOWN", "industry": "UNKNOWN"}


def _peer_plan(mapping: pd.DataFrame, symbol: str, sector: str) -> pd.DataFrame:
    if mapping.empty or "sector" not in mapping.columns:
        return pd.DataFrame([{"symbol": symbol, "company_name": _company_from_mapping(symbol), "sector": sector or "UNKNOWN"}])
    peers = mapping[mapping["sector"].astype(str) == str(sector)].copy()
    if peers.empty:
        peers = mapping[mapping["symbol"].map(_normalize_symbol) == _normalize_symbol(symbol)].copy()
    return peers[["symbol", "company_name", "sector"]].drop_duplicates().sort_values("symbol").reset_index(drop=True)


def _role_peer_plan(peer_plan: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if peer_plan.empty:
        return pd.DataFrame(columns=["role", "symbol", "company_name", "sector"])
    frame = peer_plan.copy()
    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["role"] = np.where(frame["symbol"] == _normalize_symbol(symbol), "experiment_target", "training_peer")
    columns = ["role"] + [column for column in ["symbol", "company_name", "sector", "industry"] if column in frame.columns]
    return frame[columns].sort_values(["role", "symbol"]).reset_index(drop=True)


def _company_from_mapping(symbol: str, mapping: pd.DataFrame | None = None) -> str:
    mapping = mapping if mapping is not None and not mapping.empty else _read_mapping_csv(PROJECT_ROOT / "config" / "stock_sector_mapping.csv")
    if not mapping.empty and {"symbol", "company_name"}.issubset(mapping.columns):
        row = mapping[mapping["symbol"].map(_normalize_symbol) == _normalize_symbol(symbol)]
        if not row.empty:
            company = str(row["company_name"].iloc[0]).strip()
            if company and not company.isdigit() and company != _normalize_symbol(symbol):
                return company
    alias = _company_from_alias(symbol)
    return alias or _normalize_symbol(symbol)


def _company_from_alias(symbol: str) -> str:
    path = PROJECT_ROOT / "config" / "stock_aliases.json"
    if not path.exists():
        return ""
    try:
        aliases = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    value = str(aliases.get(_normalize_symbol(symbol), "")).strip()
    return value if value and not value.isdigit() else ""


def _available_symbols(default_symbol: str) -> list[str]:
    symbols = [default_symbol]
    if STOCK_OUTPUT_ROOT.exists():
        symbols.extend(path.name for path in STOCK_OUTPUT_ROOT.iterdir() if path.is_dir() and path.name.isdigit())
    return sorted(set(_normalize_symbol(symbol) for symbol in symbols if symbol))


def _combined_signals(bundle: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames = []
    sentiment = bundle["sentiment_daily"]
    impact = bundle["impact_daily"]
    if not sentiment.empty:
        frames.append(sentiment[["date"] + [col for col in ["sector_sentiment_score", "marketwide_sentiment_score", "target_news_count"] if col in sentiment.columns]])
    if not impact.empty:
        frames.append(impact[["date"] + [col for col in ["sector_impact_score", "marketwide_impact_score", "target_news_count"] if col in impact.columns]])
    if not frames:
        return pd.DataFrame()
    result = frames[0]
    for frame in frames[1:]:
        result = result.merge(frame, on="date", how="outer", suffixes=("", "_impact"))
    if "target_news_count_impact" in result.columns and "target_news_count" not in result.columns:
        result["target_news_count"] = result["target_news_count_impact"]
    return result.sort_values("date")


def _daily_news_counts(bundle: dict[str, pd.DataFrame]) -> pd.DataFrame:
    for frame, column in [(bundle["impact_daily"], "target_news_count"), (bundle["sentiment_daily"], "target_news_count")]:
        if not frame.empty and column in frame.columns:
            return frame[["date", column]].rename(columns={column: "news_count"}).copy()
    market = bundle["market"]
    if market.empty:
        return pd.DataFrame()
    event_col = _first_present(market, ["event_count", "news_count"])
    if event_col:
        return market[["date", event_col]].rename(columns={event_col: "news_count"}).copy()
    return pd.DataFrame()


def _density_window(bundle: dict[str, pd.DataFrame]) -> tuple[str, str]:
    for frame in [bundle["impact_daily"], bundle["sentiment_daily"]]:
        start = _first_value([frame], "high_density_eval_start", "")
        end = _first_value([frame], "high_density_eval_end", "")
        if start and end:
            return start, end
    return "", ""


def _first_value(frames: list[pd.DataFrame], column: str, default: str) -> str:
    for frame in frames:
        if not frame.empty and column in frame.columns and frame[column].notna().any():
            value = str(frame[column].dropna().iloc[0])
            if value:
                return value
    return default


def _first_present(frame: pd.DataFrame, columns: list[str]) -> str:
    for column in columns:
        if column in frame.columns:
            return column
    return ""


def _build_html_report(symbol: str, bundle: dict[str, pd.DataFrame]) -> str:
    metrics = _filter_scope_experiments(_with_strategy_labels(bundle["market_metrics"]))
    curves = _mean_portfolio_curve(_filter_scope_experiments(_with_strategy_labels(bundle["market_curves"])))
    logs = _filter_scope_experiments(_with_strategy_labels(bundle["market_logs"]))
    signals = _combined_signals(bundle)
    market = bundle["market"]
    news_daily = _daily_news_counts(bundle)
    figures: list[str] = []

    if not market.empty:
        figures.append(
            _figure_html(
                px.line(market, x="date", y="close", title="Close Price", labels={"date": "Trading date", "close": "Close price"}),
                "Close price gives the market backdrop for the target stock.",
            )
        )
        if "volume" in market.columns:
            figures.append(
                _figure_html(
                    px.bar(market, x="date", y="volume", title="Daily Volume", labels={"date": "Trading date", "volume": "Trading volume"}),
                    "Volume is market activity context and is not a text signal.",
                )
            )
    if not news_daily.empty:
        figures.append(
            _figure_html(
                px.bar(news_daily, x="date", y="news_count", title="Daily News Count", labels={"date": "Trading date", "news_count": "News rows"}),
                "News count shows whether the evaluation window has enough information density.",
            )
        )
        tmp = news_daily.sort_values("date").copy()
        tmp["cumulative_news_pct"] = tmp["news_count"].cumsum() / max(tmp["news_count"].sum(), 1)
        fig = px.line(tmp, x="date", y="cumulative_news_pct", title="Cumulative News Percentage", labels={"date": "Trading date", "cumulative_news_pct": "Cumulative news share"})
        fig.add_hline(y=0.8, line_dash="dash", line_color=PALETTE["rose"])
        figures.append(_figure_html(fig, "The 80% threshold marks the recent high-information-density region."))
    if not signals.empty:
        signal_cols = _filter_scope_signal_columns([col for col in ["sector_sentiment_score", "marketwide_sentiment_score", "sector_impact_score", "marketwide_impact_score"] if col in signals.columns])
        if signal_cols:
            figures.append(
                _figure_html(
                    px.line(signals, x="date", y=signal_cols, title="Daily NLP Signals", labels={"date": "Trading date", "value": "NLP signal score", "variable": "Signal type"}),
                    "Sentiment captures text tone; market-impact captures learned peer market reaction.",
                )
            )
    if not curves.empty:
        figures.append(
            _figure_html(
                px.line(curves, x="date", y="portfolio_value", color="strategy_label", color_discrete_map=STRATEGY_COLORS, title="Portfolio Value Curve", labels={"date": "Trading date", "portfolio_value": "Portfolio value", "strategy_label": "Experiment group"}),
                "Portfolio curves compare no-NLP, peer-sentiment NLP, and market-impact NLP strategies under the same test window.",
            )
        )
        drawdown = curves[["date", "experiment", "strategy_label", "portfolio_value"]].copy()
        drawdown["portfolio_value"] = pd.to_numeric(drawdown["portfolio_value"], errors="coerce")
        drawdown["drawdown"] = drawdown.groupby("experiment")["portfolio_value"].transform(lambda s: s / s.cummax() - 1)
        figures.append(
            _figure_html(
                px.line(drawdown, x="date", y="drawdown", color="strategy_label", color_discrete_map=STRATEGY_COLORS, title="Portfolio Drawdown by Strategy", labels={"date": "Trading date", "drawdown": "Drawdown from previous portfolio peak", "strategy_label": "Experiment group"}),
                "Drawdown shows downside risk. More negative values indicate deeper losses from each strategy's own prior peak.",
            )
        )
    for metric, title, ylabel in [
        ("final_equity", "Final Equity by Strategy", "Final equity"),
        ("cumulative_return", "Cumulative Return by Strategy", "Cumulative return"),
        ("sharpe_ratio", "Sharpe Ratio by Strategy", "Sharpe ratio"),
        ("max_drawdown", "Max Drawdown by Strategy", "Max drawdown"),
    ]:
        if not metrics.empty and metric in metrics.columns:
            plot_metrics = metrics[pd.to_numeric(metrics[metric], errors="coerce").notna()].copy()
            if plot_metrics.empty:
                continue
            figures.append(
                _figure_html(
                    px.bar(plot_metrics, x="strategy_label", y=metric, color="strategy_label", color_discrete_map=STRATEGY_COLORS, title=title, labels={"strategy_label": "Experiment group", metric: ylabel}),
                    f"{title} summarizes trading performance across experiment groups.",
                )
            )
    if not logs.empty:
        action_counts = logs.groupby(["strategy_label", "action"], dropna=False).size().reset_index(name="count")
        figures.append(
            _figure_html(
                px.bar(action_counts, x="strategy_label", y="count", color="action", barmode="group", title="Action Distribution by Strategy", labels={"strategy_label": "Experiment group", "count": "Number of actions", "action": "Trading action"}),
                "Action distribution shows whether NLP changed trading frequency or caused overtrading.",
            )
        )

    best = _best_metric_row(metrics, "final_equity")
    best_text = (
        f"<p><strong>Best final-equity strategy:</strong> {best['strategy_label']} "
        f"with final equity {_fmt_number(best.get('final_equity'))}, return {_fmt_pct(best.get('cumulative_return'))}, "
        f"Sharpe {_fmt_number(best.get('sharpe_ratio'))}.</p>"
        if best is not None
        else "<p>No valid final-equity metric found.</p>"
    )
    effect_html = bundle["market_effect"].to_html(index=False) if not bundle["market_effect"].empty else "<p>No market-impact effect summary found.</p>"
    metrics_html = metrics.to_html(index=False) if not metrics.empty else "<p>No metrics found.</p>"
    validity_html = bundle["signal_validity"].to_html(index=False) if not bundle["signal_validity"].empty else "<p>No signal validity table found.</p>"
    checks_html = bundle["market_reliability"].to_html(index=False) if not bundle["market_reliability"].empty else "<p>No reliability table found.</p>"
    figure_html = "\n".join(figures)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{symbol} Peer NLP Report</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>body{{font-family:Arial,sans-serif;color:{PALETTE['ink']};background:white;margin:40px}}h1,h2,h3{{color:{PALETTE['ink']}}}table{{border-collapse:collapse;width:100%;margin:12px 0;font-size:13px}}td,th{{border:1px solid #ddd;padding:6px}}.note,.caption{{color:{PALETTE['lavender']}}}.chart{{margin:26px 0}}</style></head>
<body>
<h1>{symbol} Peer Sentiment + Market-Impact NLP Report</h1>
<p class="note">Target stock is held out from peer NLP training. Sentiment measures text tone; market-impact predicts peer future-return-labelled market reaction.</p>
<h2>Experiment Overview</h2>
<p>Target stock → peer corpus excluding target → peer sentiment and market-impact NLP scoring → lagged signals enter DQN → compare no NLP, sentiment NLP, and market-impact NLP.</p>
{best_text}
<h2>Data, News Flow, NLP Signals, DQN, and Trading Behaviour</h2>
{figure_html}
<h2>Key Effects</h2>{effect_html}
<h2>DQN Ablation Metrics</h2>{metrics_html}
<h2>Signal Validity</h2>{validity_html}
<h2>Reliability Checks</h2>{checks_html}
<h2>Key Findings</h2>
<p>The improved experiment should be interpreted only with reliability checks: target exclusion, peer corpus sufficiency, high-density window length, signal coverage, non-flat portfolio curves, and positive test-trade count.</p>
</body></html>"""


def _figure_html(fig: go.Figure, caption: str) -> str:
    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color=PALETTE["ink"], size=12),
        title=dict(font=dict(size=18), x=0.02),
        margin=dict(l=76, r=32, t=70, b=78),
        height=420,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(gridcolor="#eadfd6", showline=True, linecolor=PALETTE["mist"], ticks="outside", nticks=8, automargin=True)
    fig.update_yaxes(gridcolor="#eadfd6", showline=True, linecolor=PALETTE["mist"], ticks="outside", nticks=7, automargin=True)
    return f"<div class='chart'>{fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})}<p class='caption'>{caption}</p></div>"


def _bundle_outputs(symbol: str) -> bytes:
    import io

    buffer = io.BytesIO()
    paths = [
        stock_results_dir(symbol) / "peer_nlp_daily_sentiment.csv",
        stock_results_dir(symbol) / "peer_market_impact_daily_signal.csv",
        stock_results_dir(symbol) / "market_impact_ablation_metrics.csv",
        stock_results_dir(symbol) / "market_impact_portfolio_curves.csv",
        stock_results_dir(symbol) / "market_impact_effect_summary.csv",
        stock_reports_dir(symbol) / "signal_validity_summary.csv",
        stock_reports_dir(symbol) / "market_impact_reliability_check.csv",
        SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_summary.csv",
    ]
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        for path in paths:
            if path.exists() and path.stat().st_size > 4:
                archive.write(path, arcname=path.name)
    return buffer.getvalue()


def _parse_date(value: str) -> date:
    return pd.to_datetime(value, errors="coerce").date()


def _normalize_symbol(symbol: str) -> str:
    text = str(symbol).strip()
    extracted = pd.Series([text]).str.extract(r"(\d{6})", expand=False).iloc[0]
    if pd.notna(extracted):
        return str(extracted)
    digits = re.sub(r"\D", "", text)
    if digits and len(digits) <= 6:
        return digits.zfill(6)
    return text


if __name__ == "__main__":
    main()
