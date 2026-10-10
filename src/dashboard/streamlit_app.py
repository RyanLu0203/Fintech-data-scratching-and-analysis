"""Interactive control-center dashboard for the NLP-RL trading platform."""

from __future__ import annotations

from datetime import datetime
import hashlib
import html
import importlib
import inspect
import json
import shutil
import sys
import traceback
import zipfile
from pathlib import Path

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pandas.errors import EmptyDataError, ParserError

from src.config.paths import OUTPUTS_ROOT, STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR, normalize_symbol_for_path, stock_data_dir, stock_reports_dir, stock_results_dir
from src.config.settings import PROJECT_ROOT, settings
from src.data_ingestion.cache import build_master_csv, resolve_cached_csv
from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.feasibility_audit import run_feasibility_audit
from src.evaluation.information_density import generate_information_density_outputs
from src.evaluation.market_impact_ablation import build_market_impact_cross_stock_summary
from src.evaluation.model_upgrade_diagnostics import MODEL_UPGRADE_FILENAMES, discover_model_upgrade_diagnostics
from src.evaluation.peer_nlp_ablation import build_peer_nlp_cross_stock_summary, write_peer_nlp_integrity_report
from src.evaluation.sector_peer_bootstrap import ensure_sector_peer_data
from src.features.money_flow import compute_daily_net_flow


st.set_page_config(page_title="Peer NLP Trading Experiment", layout="wide")
from src.dashboard.coursework_view import render_coursework_reader

if not st.sidebar.checkbox("Advanced research controls", value=False):
    render_coursework_reader()
    st.stop()
st.warning("Research workspace · Legacy corpus and grid paths are diagnostic. Use scripts/run_coursework.py for chronological formal evaluation.")
st.title("Peer NLP Transfer Trading Experiment")
st.caption("支持 held-out target stock + peer sentiment NLP / market-impact NLP + DQN cross analysis。")


PALETTE = {
    "ink": "#243040",
    "muted": "#687084",
    "blue": "#2563eb",
    "teal": "#0f9f8f",
    "amber": "#d97706",
    "rose": "#e11d48",
    "purple": "#7c3aed",
    "green": "#16a34a",
    "slate": "#e5e9f0",
    "soft": "#f7f9fc",
    "white": "#ffffff",
    "grid": "rgba(104, 112, 132, 0.22)",
}
SERIES_COLORS = [PALETTE["blue"], PALETTE["teal"], PALETTE["amber"], PALETTE["rose"], PALETTE["purple"], PALETTE["green"]]
EXPERIMENT_COLORS = {
    "buy_and_hold": PALETTE["ink"],
    "dqn_without_nlp": PALETTE["blue"],
    "dqn_with_nlp": PALETTE["rose"],
    "dqn_with_sector_peer_nlp": PALETTE["teal"],
    "dqn_with_marketwide_peer_nlp": PALETTE["amber"],
    "dqn_with_sector_sentiment_nlp": PALETTE["teal"],
    "dqn_with_marketwide_sentiment_nlp": PALETTE["amber"],
    "dqn_with_sector_impact_nlp": PALETTE["purple"],
    "dqn_with_marketwide_impact_nlp": PALETTE["rose"],
}
EXPERIMENT_DISPLAY_NAMES = {
    "buy_and_hold": "Buy-and-hold benchmark",
    "dqn_without_nlp": "No NLP DQN",
    "dqn_with_sector_peer_nlp": "Sector Sentiment",
    "dqn_with_marketwide_peer_nlp": "Marketwide Sentiment",
    "dqn_with_sector_sentiment_nlp": "Sector Sentiment",
    "dqn_with_marketwide_sentiment_nlp": "Marketwide Sentiment",
    "dqn_with_sector_impact_nlp": "Sector Impact",
    "dqn_with_marketwide_impact_nlp": "Marketwide Impact",
}
CORE_EXPERIMENT_METRICS = [
    "final_equity",
    "cumulative_return",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "max_drawdown",
    "number_of_trades",
    "win_rate",
]
CONSUMER_ELECTRONICS_EXAMPLE_SYMBOLS = ["002475", "002241", "300433", "300136", "601138", "601231"]
CONSUMER_ELECTRONICS_EXAMPLE_LABEL = "Consumer electronics example: Luxshare / GoerTek / Lens / Sunway / FII / USI"
PLOT_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "doubleClick": False,
    "responsive": True,
}
_PLOTLY_KEY_COUNTS: dict[str, int] = {}
OFFICIAL_STOCK_RESULT_FILES = [
    "peer_nlp_daily_sentiment.csv",
    "peer_nlp_item_sentiment.csv",
    "peer_nlp_ablation_metrics.csv",
    "peer_nlp_ablation_metrics_by_seed.csv",
    "peer_nlp_portfolio_curves.csv",
    "peer_nlp_drawdown_curves.csv",
    "peer_nlp_trading_logs.csv",
    "peer_nlp_training_rewards_all_seeds.csv",
    "peer_nlp_effect_summary.csv",
    "peer_market_impact_daily_signal.csv",
    "peer_market_impact_item_signal.csv",
    "market_impact_ablation_metrics.csv",
    "market_impact_ablation_metrics_by_seed.csv",
    "market_impact_portfolio_curves.csv",
    "market_impact_drawdown_curves.csv",
    "market_impact_trading_logs.csv",
    "market_impact_training_rewards_all_seeds.csv",
    "market_impact_effect_summary.csv",
]
OFFICIAL_STOCK_REPORT_FILES = [
    "peer_nlp_experiment_window.csv",
    "peer_nlp_information_density_split.csv",
    "peer_nlp_integrity_check.csv",
    "peer_nlp_leakage_diagnostics.csv",
    "peer_nlp_report_section.md",
    "peer_nlp_state_vector_compliance.csv",
    "peer_nlp_group_state_diagnostics.csv",
    "peer_nlp_train_eval_windows.csv",
    "market_impact_state_vector_compliance.csv",
    "market_impact_group_state_diagnostics.csv",
    "market_impact_leakage_diagnostics.csv",
    "market_impact_train_eval_windows.csv",
    "market_impact_reliability_check.csv",
    "market_impact_report_section.md",
    "market_impact_experiment_window.csv",
]
MODEL_UPGRADE_DIR = OUTPUTS_ROOT / "model_upgrade"
OFFICIAL_SYSTEM_FILES = [
    SYSTEM_OUTPUT_DIR / "latest_peer_nlp_run_status.json",
    SYSTEM_OUTPUT_DIR / "latest_peer_nlp_run_log.csv",
    SYSTEM_OUTPUT_DIR / "latest_peer_nlp_peer_corpus_log.csv",
    SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv",
    SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_diagnostics.csv",
    SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_discussion.md",
    SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_summary.csv",
    SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_diagnostics.csv",
    SYSTEM_OUTPUT_DIR / "market_impact_cross_stock_discussion.md",
    PROJECT_ROOT / "reports" / "tables" / "peer_nlp_effect_summary.csv",
    PROJECT_ROOT / "reports" / "tables" / "peer_nlp_integrity_check.csv",
    PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv",
    PROJECT_ROOT / "reports" / "tables" / "market_impact_effect_summary.csv",
    PROJECT_ROOT / "reports" / "tables" / "market_impact_corpus_summary.csv",
    PROJECT_ROOT / "reports" / "peer_nlp_integrity_check.md",
]


def apply_dashboard_theme() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: {PALETTE["white"]};
            color: {PALETTE["ink"]};
        }}
        [data-testid="stSidebar"] {{
            background: {PALETTE["white"]};
            border-right: 1px solid rgba(63, 49, 87, 0.08);
        }}
        h1, h2, h3, h4, h5, h6 {{
            color: {PALETTE["ink"]};
            letter-spacing: 0;
        }}
        .stCaption, .stMarkdown, .stText, label, p, li, span {{
            color: {PALETTE["ink"]};
        }}
        .stButton > button, .stDownloadButton > button {{
            background: transparent;
            color: {PALETTE["blue"]};
            border: 1px solid rgba(37, 99, 235, 0.34);
            border-radius: 8px;
            box-shadow: none;
        }}
        .stButton > button:hover, .stDownloadButton > button:hover {{
            background: rgba(37, 99, 235, 0.08);
            border-color: {PALETTE["blue"]};
            color: {PALETTE["blue"]};
        }}
        button[kind="primary"] {{
            background: transparent !important;
            color: {PALETTE["rose"]} !important;
            border-color: rgba(225, 29, 72, 0.42) !important;
        }}
        button[kind="primary"]:hover {{
            background: rgba(225, 29, 72, 0.08) !important;
            border-color: {PALETTE["rose"]} !important;
            color: {PALETTE["rose"]} !important;
        }}
        [data-baseweb="tab-list"] {{
            gap: 0.35rem;
        }}
        [data-baseweb="tab"] {{
            color: {PALETTE["muted"]};
            border-radius: 8px 8px 0 0;
            padding: 0.5rem 0.8rem;
        }}
        [aria-selected="true"][data-baseweb="tab"] {{
            color: {PALETTE["rose"]};
            border-bottom-color: {PALETTE["rose"]} !important;
            font-weight: 600;
        }}
        [data-testid="stMetricValue"] {{
            color: {PALETTE["ink"]};
        }}
        [data-testid="stDataFrame"] {{
            border: 1px solid rgba(63, 49, 87, 0.08);
            border-radius: 10px;
        }}
        .experiment-status-card {{
            border: 1px solid rgba(36, 48, 64, 0.14);
            border-left: 5px solid var(--status-color);
            border-radius: 8px;
            padding: 1rem 1.1rem;
            background: {PALETTE["soft"]};
            margin: 0.75rem 0 1rem;
        }}
        .experiment-status-card strong {{
            color: var(--status-color);
        }}
        .experiment-status-card ul {{
            margin-bottom: 0;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


apply_dashboard_theme()


def _base_layout(title: str | None = None, height: int = 320) -> dict[str, object]:
    return {
        "title": {"text": title or "", "font": {"color": PALETTE["ink"], "size": 18}},
        "paper_bgcolor": PALETTE["white"],
        "plot_bgcolor": PALETTE["white"],
        "font": {"color": PALETTE["ink"]},
        "height": height,
        "margin": {"l": 48, "r": 18, "t": 24 if title else 10, "b": 40},
        "hovermode": "x unified",
        "dragmode": False,
        "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0.0},
        "xaxis": {
            "showgrid": True,
            "gridcolor": PALETTE["grid"],
            "zeroline": False,
            "fixedrange": True,
            "title": "",
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": PALETTE["grid"],
            "zeroline": False,
            "fixedrange": True,
            "title": "",
        },
    }


def _reset_plotly_key_counter() -> None:
    _PLOTLY_KEY_COUNTS.clear()


def _render_plot(fig: go.Figure, key: str | None = None) -> None:
    chart_key = key or _auto_plotly_key(fig)
    try:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=chart_key)
    except TypeError:
        st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG, key=chart_key)


def _auto_plotly_key(fig: go.Figure) -> str:
    caller = inspect.stack()[2]
    title = ""
    try:
        title = str(fig.layout.title.text or "")
    except Exception:
        title = ""
    trace_names = "|".join(str(getattr(trace, "name", "") or "") for trace in fig.data)
    raw = f"{caller.function}:{caller.lineno}:{title}:{trace_names}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    count = _PLOTLY_KEY_COUNTS.get(digest, 0) + 1
    _PLOTLY_KEY_COUNTS[digest] = count
    return f"plotly_{digest}_{count}"


def _display_experiment_name(experiment: object) -> str:
    return EXPERIMENT_DISPLAY_NAMES.get(str(experiment), str(experiment))


def _display_experiment_color_map() -> dict[str, str]:
    return {_display_experiment_name(key): value for key, value in EXPERIMENT_COLORS.items()}


def _official_metric_table(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    display = metrics.copy()
    if "experiment" in display.columns:
        display.insert(0, "strategy_label", display["experiment"].map(_display_experiment_name).fillna(display["experiment"].astype(str)))
    columns = [col for col in ["strategy_label", "experiment", "seed_count", *CORE_EXPERIMENT_METRICS] if col in display.columns]
    return display[columns] if columns else display


def _target_company_command(symbol: str, bundle: dict[str, object]) -> str:
    company = str(bundle.get("company_name", "") or "").strip()
    if not company:
        company = normalize_symbol_for_path(symbol)
    return (
        "No latest model-upgrade results found for selected target_symbol. Run:\n"
        f"python scripts/run_model_upgrade_grid.py --target-symbol {normalize_symbol_for_path(symbol)} "
        f"--target-company {company} --medium-test"
    )


def _target_model_upgrade_frames(symbol: str) -> tuple[object, dict[str, pd.DataFrame]]:
    bundle = discover_model_upgrade_diagnostics()
    code = normalize_symbol_for_path(symbol)

    def target_filter(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty or "target_symbol" not in frame.columns:
            return pd.DataFrame()
        out = frame.copy()
        return out[out["target_symbol"].astype(str).apply(normalize_symbol_for_path) == code].copy()

    frames = {filename: target_filter(bundle.get(filename)) for filename in MODEL_UPGRADE_FILENAMES}
    return bundle, frames


def _has_model_upgrade_rows(frames: dict[str, pd.DataFrame]) -> bool:
    for filename in [
        "model_upgrade_summary.csv",
        "seed_level_metrics.csv",
        "portfolio_curves.csv",
        "action_distribution_diagnostics.csv",
        "env_dynamics_diagnostics.csv",
    ]:
        if not frames.get(filename, pd.DataFrame()).empty:
            return True
    return False


def _upgrade_filter_controls(frame: pd.DataFrame, prefix: str, *, include_experiment: bool = True) -> dict[str, str]:
    if frame.empty:
        return {}
    columns = st.columns(4)
    filters: dict[str, str] = {}
    controls = [
        ("run_mode", ["medium_test", "quick_test", "full_grid"]),
        ("model_variant", None),
        ("reward_variant", None),
        ("state_feature_mode", None),
        ("action_mode", None),
        ("seed", ["all"]),
    ]
    if include_experiment:
        controls.append(("experiment", ["all"]))
    for index, (column, preferred) in enumerate(controls):
        if column not in frame.columns:
            continue
        options = _diagnostic_options(frame, column, preferred or [])
        if preferred and "all" in preferred:
            options = ["all"] + [item for item in options if item != "all"]
        if not options:
            continue
        filters[column] = columns[index % len(columns)].selectbox(
            column,
            options,
            index=0,
            key=f"{prefix}_{column}",
        )
    return filters


def _apply_upgrade_filters(frame: pd.DataFrame, filters: dict[str, str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for column, value in filters.items():
        if not value or value == "all" or column not in out.columns:
            continue
        out = out[out[column].astype(str) == str(value)]
    return out


def _upgrade_metric_table(seed_metrics: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    metric_cols = ["final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown", "number_of_trades", "win_rate"]
    if not seed_metrics.empty and "experiment" in seed_metrics.columns:
        table = seed_metrics.copy()
        for column in metric_cols:
            if column in table.columns:
                table[column] = pd.to_numeric(table[column], errors="coerce")
        agg_cols = [column for column in metric_cols if column in table.columns]
        grouped = table.groupby("experiment", as_index=False)[agg_cols].mean()
        grouped.insert(1, "seed_count", table.groupby("experiment")["seed"].nunique().reindex(grouped["experiment"]).to_numpy() if "seed" in table.columns else len(table))
        grouped.insert(0, "strategy_label", grouped["experiment"].map(_display_experiment_name).fillna(grouped["experiment"].astype(str)))
        return grouped
    if not summary.empty and "experiment" in summary.columns:
        table = summary.copy()
        rename_map = {
            "mean_final_equity": "final_equity",
            "mean_cumulative_return": "cumulative_return",
            "mean_sharpe_ratio": "sharpe_ratio",
            "mean_max_drawdown": "max_drawdown",
            "mean_number_of_trades": "number_of_trades",
            "mean_win_rate": "win_rate",
        }
        table = table.rename(columns=rename_map)
        cols = [col for col in ["experiment", "seed_count", *metric_cols] if col in table.columns]
        table = table[cols].copy()
        table.insert(0, "strategy_label", table["experiment"].map(_display_experiment_name).fillna(table["experiment"].astype(str)))
        return table
    return pd.DataFrame()


def _render_upgrade_metric_comparison(seed_metrics: pd.DataFrame, summary: pd.DataFrame, title: str) -> None:
    table = _upgrade_metric_table(seed_metrics, summary)
    if table.empty:
        st.info("No model-upgrade metric rows are available for the selected filters.")
        return
    st.dataframe(table, use_container_width=True, hide_index=True)
    metric_cols = [col for col in ["final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown", "number_of_trades", "win_rate"] if col in table.columns]
    if metric_cols:
        chart = table.set_index("strategy_label")[metric_cols].apply(pd.to_numeric, errors="coerce")
        render_multi_series(chart, title=title, kind="bar", color_map=_display_experiment_color_map(), height=380)


def _portfolio_curve_source(portfolio: pd.DataFrame, env: pd.DataFrame) -> pd.DataFrame:
    if not portfolio.empty and {"date", "experiment", "portfolio_value"}.issubset(portfolio.columns):
        curves = portfolio.copy()
        curves["portfolio_value"] = pd.to_numeric(curves["portfolio_value"], errors="coerce")
        return curves
    if not env.empty and "experiment" in env.columns:
        value_col = _portfolio_after_column(env)
        if value_col:
            curves = env.copy()
            curves["portfolio_value"] = pd.to_numeric(curves[value_col], errors="coerce")
            return curves
    return pd.DataFrame()


def _render_upgrade_portfolio_curves(portfolio: pd.DataFrame, env: pd.DataFrame, title: str) -> None:
    curves = _portfolio_curve_source(portfolio, env)
    if curves.empty or "experiment" not in curves.columns:
        st.info("No chart-ready model-upgrade portfolio curves are available.")
        return
    curves = curves.copy()
    if "date" in curves.columns:
        curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
        index_col = "date"
    elif "step" in curves.columns:
        curves["step"] = pd.to_numeric(curves["step"], errors="coerce")
        index_col = "step"
    else:
        curves = curves.reset_index()
        index_col = "index"
    curves["series"] = curves["experiment"].astype(str).map(_display_experiment_name).fillna(curves["experiment"].astype(str))
    curve_frame = curves.pivot_table(index=index_col, columns="series", values="portfolio_value", aggfunc="mean")
    render_multi_series(curve_frame, title=title, kind="line", color_map=_display_experiment_color_map(), height=360)


def _render_upgrade_action_distribution(actions: pd.DataFrame, env: pd.DataFrame, title: str) -> None:
    if not env.empty and "experiment" in env.columns:
        label_col = "mapped_action_label" if "mapped_action_label" in env.columns else "action"
        if label_col in env.columns:
            action_frame = env.copy()
            action_frame["experiment"] = action_frame["experiment"].astype(str).map(_display_experiment_name).fillna(action_frame["experiment"].astype(str))
            action_frame[label_col] = action_frame[label_col].astype(str)
            counts = action_frame.groupby([label_col, "experiment"]).size().unstack(fill_value=0)
            render_multi_series(counts, title=title, kind="bar", color_map=_display_experiment_color_map(), height=320)
            return
    if not actions.empty and {"experiment", "hold_count", "buy_count", "sell_count"}.issubset(actions.columns):
        counts = actions.copy()
        for column in ["hold_count", "buy_count", "sell_count"]:
            counts[column] = pd.to_numeric(counts[column], errors="coerce").fillna(0)
        counts = counts.groupby("experiment")[["buy_count", "sell_count", "hold_count"]].sum().rename(index=_display_experiment_name).T
        render_multi_series(counts, title=title, kind="bar", color_map=_display_experiment_color_map(), height=320)
        return
    st.info("No model-upgrade action distribution rows are available.")


def rerun_dashboard() -> None:
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def render_series_line(data: pd.DataFrame, x: str, y: str, title: str | None = None, color: str | None = None, height: int = 320) -> None:
    chart = data[[x, y]].copy().dropna(subset=[x, y])
    chart[y] = pd.to_numeric(chart[y], errors="coerce")
    try:
        chart[x] = pd.to_datetime(chart[x])
    except Exception:
        pass
    chart = chart.dropna(subset=[x, y])
    if chart.empty:
        st.info("No chart-ready rows are available yet.")
        return
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart[x],
            y=chart[y],
            mode="lines",
            line={"color": color or PALETTE["ink"], "width": 2.4},
            name=y,
        )
    )
    fig.update_layout(**_base_layout(title=title, height=height))
    fig.update_yaxes(rangemode="tozero", zeroline=True, zerolinecolor=PALETTE["grid"])
    _render_plot(fig)


def render_series_bar(
    data: pd.DataFrame,
    x: str,
    y: str,
    title: str | None = None,
    color: str | None = None,
    color_by_sign: bool = False,
    height: int = 320,
) -> None:
    chart = data[[x, y]].copy().dropna(subset=[x, y])
    chart[y] = pd.to_numeric(chart[y], errors="coerce")
    try:
        chart[x] = pd.to_datetime(chart[x])
    except Exception:
        pass
    chart = chart.dropna(subset=[x, y])
    if chart.empty:
        st.info("No chart-ready rows are available yet.")
        return
    marker_color = color or PALETTE["blue"]
    if color_by_sign:
        marker_color = [PALETTE["teal"] if value >= 0 else PALETTE["rose"] for value in chart[y].tolist()]
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=chart[x],
            y=chart[y],
            marker={"color": marker_color, "line": {"width": 0}},
            name=y,
        )
    )
    fig.update_layout(**_base_layout(title=title, height=height))
    fig.update_yaxes(rangemode="normal" if color_by_sign else "tozero", zeroline=True, zerolinecolor=PALETTE["grid"])
    _render_plot(fig)


def render_multi_series(
    frame: pd.DataFrame,
    title: str | None = None,
    kind: str = "line",
    color_map: dict[str, str] | None = None,
    height: int = 340,
) -> None:
    chart = frame.copy().dropna(how="all")
    if chart.empty:
        st.info("No chart-ready rows are available yet.")
        return
    fig = go.Figure()
    for index, column in enumerate(chart.columns):
        series = chart[column].dropna()
        if series.empty:
            continue
        color = (color_map or {}).get(column, SERIES_COLORS[index % len(SERIES_COLORS)])
        if kind == "bar":
            fig.add_trace(
                go.Bar(
                    x=series.index,
                    y=series.values,
                    name=str(column),
                    marker={"color": color},
                )
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=series.index,
                    y=series.values,
                    mode="lines",
                    name=str(column),
                    line={"color": color, "width": 2.4},
                )
            )
    fig.update_layout(**_base_layout(title=title, height=height))
    if kind == "bar":
        fig.update_layout(barmode="group")
    _render_plot(fig)


def render_scatter(
    data: pd.DataFrame,
    x: str,
    y: str,
    category: str | None = None,
    title: str | None = None,
    height: int = 340,
) -> None:
    cols = [x, y] + ([category] if category else [])
    chart = data[cols].copy().dropna(subset=[x, y])
    if chart.empty:
        st.info("No chart-ready rows are available yet.")
        return
    fig = go.Figure()
    if category and category in chart.columns:
        for index, (name, part) in enumerate(chart.groupby(category)):
            fig.add_trace(
                go.Scatter(
                    x=part[x],
                    y=part[y],
                    mode="markers+text",
                    text=[str(name)] * len(part),
                    textposition="top center",
                    name=str(name),
                    marker={"size": 10, "color": SERIES_COLORS[index % len(SERIES_COLORS)]},
                )
            )
    else:
        fig.add_trace(
            go.Scatter(
                x=chart[x],
                y=chart[y],
                mode="markers",
                marker={"size": 10, "color": PALETTE["blue"]},
                name=y,
            )
        )
    fig.update_layout(**_base_layout(title=title, height=height))
    _render_plot(fig)


def get_pipeline_runner():
    for module_name in [
        "src.rl.train",
        "src.evaluation.ablation",
        "src.nlp.peer_sentiment",
        "src.nlp.market_impact",
        "src.evaluation.peer_nlp_ablation",
        "src.evaluation.market_impact_ablation",
        "main",
    ]:
        module = importlib.import_module(module_name)
        importlib.reload(module)
    rl_train_module = importlib.import_module("src.rl.train")
    if "state_scaler" not in inspect.signature(rl_train_module.evaluate_agent).parameters:
        st.error("Dashboard runtime is using a stale DQN module. Restart Streamlit so src.rl.train.evaluate_agent reloads with state_scaler support.")
        st.stop()
    main_module = importlib.import_module("main")
    return main_module.run_pipeline_for_symbol


def density_from_peer_daily(peer_daily: pd.DataFrame) -> pd.DataFrame:
    if peer_daily.empty or "date" not in peer_daily.columns:
        return pd.DataFrame()
    daily = peer_daily.copy()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily = daily.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if daily.empty:
        return pd.DataFrame()
    count_col = "target_news_count" if "target_news_count" in daily.columns else ""
    available_col = "target_news_available" if "target_news_available" in daily.columns else ""
    result = pd.DataFrame({"date": daily["date"]})
    result["daily_news_count"] = pd.to_numeric(daily.get(count_col, 0), errors="coerce").fillna(0).clip(lower=0) if count_col else 0
    if available_col:
        result["news_available"] = pd.to_numeric(daily[available_col], errors="coerce").fillna(0).astype(int)
    else:
        result["news_available"] = (result["daily_news_count"] > 0).astype(int)
    total = float(result["daily_news_count"].sum())
    result["cumulative_news_count"] = result["daily_news_count"].cumsum()
    result["cumulative_news_pct"] = result["cumulative_news_count"] / total if total > 0 else 0.0
    result["recent_cumulative_news_pct"] = result["cumulative_news_pct"]
    if "high_density_eval_start" in daily.columns and daily["high_density_eval_start"].notna().any():
        start = pd.to_datetime(daily["high_density_eval_start"].dropna().iloc[0], errors="coerce")
        result["is_high_density_window"] = result["date"] >= start if pd.notna(start) else False
    return result


def load_stock_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    mapping_path = PROJECT_ROOT / "config" / "stock_sector_mapping.csv"
    if mapping_path.exists():
        try:
            mapping = pd.read_csv(mapping_path, dtype=str)
            if {"symbol", "company_name"}.issubset(mapping.columns):
                for _, row in mapping.iterrows():
                    code = normalize_symbol_for_path(row.get("symbol", ""))
                    name = str(row.get("company_name", "") or "").strip()
                    if code and name and not _looks_like_symbol_name(name, code):
                        aliases[code] = name
        except Exception:
            aliases = {}
    path = PROJECT_ROOT / "config" / "stock_aliases.json"
    if not path.exists():
        return aliases
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return aliases
    aliases.update({normalize_symbol_for_path(key): str(value).strip() for key, value in payload.items()})
    return aliases


STOCK_ALIASES = load_stock_aliases()


def latest_stock_dirs() -> list[Path]:
    return sorted(path for path in STOCK_OUTPUT_ROOT.glob("*") if path.is_dir() and normalize_symbol_for_path(path.name).isdigit())


def available_local_symbols() -> list[str]:
    return [path.name for path in latest_stock_dirs()]


def configured_target_symbols() -> list[str]:
    mapping = safe_read_csv(PROJECT_ROOT / "config" / "stock_sector_mapping.csv", dtype=str)
    if mapping.empty or "symbol" not in mapping.columns:
        return available_local_symbols()
    targets = mapping[mapping.get("is_target_candidate", "0").astype(str).isin(["1", "True", "true"])].copy()
    symbols = [normalize_symbol_for_path(symbol) for symbol in targets["symbol"].dropna().astype(str)]
    return [symbol for symbol in symbols if symbol]


def configured_symbols() -> list[str]:
    mapping = safe_read_csv(PROJECT_ROOT / "config" / "stock_sector_mapping.csv", dtype=str)
    if mapping.empty or "symbol" not in mapping.columns:
        return []
    symbols = [normalize_symbol_for_path(symbol) for symbol in mapping["symbol"].dropna().astype(str)]
    return [symbol for symbol in symbols if symbol]


def _looks_like_symbol_name(value: str, symbol: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    normalized_symbol = normalize_symbol_for_path(symbol)
    stripped_symbol = normalized_symbol.lstrip("0") or normalized_symbol
    variants = {
        normalized_symbol.upper(),
        stripped_symbol.upper(),
        f"{normalized_symbol}.SZ",
        f"{normalized_symbol}.SS",
        f"SZ{normalized_symbol}",
        f"SH{normalized_symbol}",
    }
    upper_text = text.upper()
    if upper_text in variants:
        return True
    return bool(text.isdigit() and (text == stripped_symbol or text.zfill(6) == normalized_symbol))


def sanitize_company_name(symbol: str, candidate: str) -> str:
    value = str(candidate or "").strip()
    return "" if _looks_like_symbol_name(value, symbol) else value


def safe_read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except (EmptyDataError, ParserError):
        return pd.DataFrame()


def _first_value(frame: pd.DataFrame, column: str, default: object = "-") -> object:
    if frame.empty or column not in frame.columns:
        return default
    values = frame[column].dropna()
    if values.empty:
        return default
    return values.iloc[0]


def load_stock_sector_mapping() -> pd.DataFrame:
    """Load stock classification with manual config as the authority.

    Generated report tables can be stale after users edit the configured stock
    universe.  The dashboard should therefore resolve sector/company metadata
    from config first, while still falling back to generated reports for
    symbols that only exist in local outputs.
    """
    frames: list[pd.DataFrame] = []
    for path in [
        PROJECT_ROOT / "config" / "stock_sector_mapping.csv",
        PROJECT_ROOT / "reports" / "tables" / "stock_sector_mapping.csv",
    ]:
        frame = safe_read_csv(path, dtype=str)
        if frame.empty or "symbol" not in frame.columns:
            continue
        frame = frame.copy()
        frame["symbol"] = frame["symbol"].astype(str).apply(normalize_symbol_for_path)
        frame["mapping_source_file"] = str(path.relative_to(PROJECT_ROOT))
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=["symbol", "company_name", "sector", "industry", "sector_source", "mapping_source_file"])
    mapping = pd.concat(frames, ignore_index=True, sort=False)
    mapping = mapping[mapping["symbol"].astype(str) != ""].drop_duplicates(subset=["symbol"], keep="first")
    for column in ["company_name", "sector", "industry", "sector_source", "mapping_source_file"]:
        if column not in mapping.columns:
            mapping[column] = ""
    return mapping


def stock_classification(symbol: str) -> dict[str, object]:
    code = normalize_symbol_for_path(symbol)
    mapping = load_stock_sector_mapping()
    row = pd.DataFrame()
    if not mapping.empty:
        row = mapping[mapping["symbol"].astype(str).apply(normalize_symbol_for_path) == code].copy()
    company = _first_value(row, "company_name", "") if not row.empty else ""
    if _looks_like_symbol_name(str(company), code):
        company = STOCK_ALIASES.get(code, "")
    if not company:
        company = discover_company_name(code)[0]
    sector = str(_first_value(row, "sector", "UNKNOWN") if not row.empty else "UNKNOWN").strip() or "UNKNOWN"
    industry = str(_first_value(row, "industry", "UNKNOWN") if not row.empty else "UNKNOWN").strip() or "UNKNOWN"
    return {
        "symbol": code,
        "company_name": company or "-",
        "sector": sector,
        "industry": industry,
        "sector_source": _first_value(row, "sector_source", "missing") if not row.empty else "missing",
        "mapping_source_file": _first_value(row, "mapping_source_file", "missing") if not row.empty else "missing",
    }


def format_symbol_with_sector(symbol: str) -> str:
    info = stock_classification(symbol)
    return f"{info['symbol']} - {info['company_name']} - {info['sector']}"


def same_sector_symbols(symbol: str, *, include_target: bool = False) -> list[str]:
    code = normalize_symbol_for_path(symbol)
    info = stock_classification(code)
    sector = str(info.get("sector", "UNKNOWN") or "UNKNOWN")
    if sector.upper() == "UNKNOWN":
        return []
    mapping = load_stock_sector_mapping()
    if mapping.empty:
        return []
    rows = mapping[mapping["sector"].astype(str) == sector].copy()
    symbols = [normalize_symbol_for_path(item) for item in rows["symbol"].dropna().astype(str)]
    symbols = [item for item in symbols if item and (include_target or item != code)]
    return sorted(dict.fromkeys(symbols))


def render_target_classification(symbol: str, bundle: dict[str, object] | None = None) -> dict[str, object]:
    info = stock_classification(symbol)
    code = str(info["symbol"])
    data = bundle.get("data", pd.DataFrame()) if bundle else pd.DataFrame()
    local_data_available = isinstance(data, pd.DataFrame) and not data.empty
    if not local_data_available:
        local_data_available = stock_data_dir(code).exists() and any(stock_data_dir(code).glob("*_finance_text*.csv"))
    row = {
        "symbol": code,
        "company_name": info["company_name"],
        "sector": info["sector"],
        "industry": info["industry"],
        "sector_source": info["sector_source"],
        "mapping_source_file": info["mapping_source_file"],
        "local_data_available": bool(local_data_available),
    }
    st.markdown("#### Target Stock Classification")
    st.dataframe(pd.DataFrame([row]), use_container_width=True, hide_index=True)
    if str(info["sector"]).upper() in {"", "UNKNOWN", "NAN"}:
        st.warning("This stock has no usable sector classification. Add it to `config/stock_sector_mapping.csv` before sector-peer NLP can be reliable.")
    return row


def is_usable_daily_net_flow(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return False
    flow_col = "net_flow_cny_million" if "net_flow_cny_million" in frame.columns else "net_flow" if "net_flow" in frame.columns else ""
    if not flow_col:
        return False
    values = pd.to_numeric(frame[flow_col], errors="coerce").fillna(0.0)
    return bool(values.abs().sum() > 0)


def news_concentration_warning(sentiment: pd.DataFrame) -> str:
    if sentiment.empty or not {"date", "news_count"}.issubset(sentiment.columns):
        return ""
    frame = sentiment[["date", "news_count"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["news_count"] = pd.to_numeric(frame["news_count"], errors="coerce").fillna(0)
    frame = frame.dropna(subset=["date"])
    total = float(frame["news_count"].sum())
    if total <= 0 or frame.empty:
        return ""
    cutoff = frame["date"].max() - pd.Timedelta(days=30)
    latest_share = float(frame.loc[frame["date"] >= cutoff, "news_count"].sum() / total)
    if latest_share >= 0.6:
        return (
            f"最近 30 天占全部 news/event rows 的 {latest_share:.1%}。"
            "这通常说明搜索源有明显近端偏置；建议重新运行 workflow 以应用 date-balanced NLP cap，"
            "或降低 News cap / 增加公告类数据源后再做正式对比。"
        )
    return ""


def parse_symbol_list(raw_text: str) -> list[str]:
    tokens = []
    for chunk in str(raw_text or "").replace("\n", ",").replace(";", ",").split(","):
        symbol = normalize_symbol_for_path(chunk.strip())
        if symbol and symbol not in tokens:
            tokens.append(symbol)
    return tokens


def build_symbol_plan(primary_symbol: str, run_cross_analysis: bool, cross_symbols_text: str) -> list[str]:
    symbols = [normalize_symbol_for_path(primary_symbol)]
    if run_cross_analysis:
        for symbol in parse_symbol_list(cross_symbols_text):
            if symbol not in symbols:
                symbols.append(symbol)
    return symbols


def combine_cross_symbol_inputs(selected_symbols: list[str], manual_symbols_text: str) -> str:
    symbols: list[str] = []
    for symbol in selected_symbols + parse_symbol_list(manual_symbols_text):
        code = normalize_symbol_for_path(symbol)
        if code and code not in symbols:
            symbols.append(code)
    return ", ".join(symbols)


def truthy_flag(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def discover_company_name(symbol: str) -> tuple[str, str]:
    code = normalize_symbol_for_path(symbol)
    if code in STOCK_ALIASES:
        return STOCK_ALIASES[code], "config/stock_aliases.json"

    data_dir = stock_data_dir(code)
    for path in sorted(data_dir.glob("*_finance_text*.csv"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            sample = safe_read_csv(path, usecols=lambda col: col in {"company_name"}, nrows=20)
        except Exception:
            continue
        if "company_name" not in sample.columns:
            continue
        names = sample["company_name"].dropna().astype(str).str.strip()
        names = names[(names != "") & (~names.apply(lambda value: _looks_like_symbol_name(value, code)))]
        if not names.empty:
            return str(names.iloc[0]), path.name
    return "", "manual input required"


def resolve_company_name(symbol: str, typed_company: str) -> tuple[str, str]:
    code = normalize_symbol_for_path(symbol)
    manual = str(typed_company or "").strip()
    configured = STOCK_ALIASES.get(code, "")
    if _looks_like_symbol_name(manual, code):
        manual = ""
    if configured and manual and manual != configured and manual in set(STOCK_ALIASES.values()):
        return configured, "config/stock_aliases.json corrected stale manual input"
    if configured and manual == configured:
        return configured, "config stock mapping"
    if manual:
        return manual, "manual"
    if configured:
        return configured, "config stock mapping"
    return discover_company_name(symbol)


def build_company_resolution_table(primary_symbol: str, resolved_primary_company: str, run_cross_analysis: bool, cross_symbols_text: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    primary_code = normalize_symbol_for_path(primary_symbol)
    for symbol in build_symbol_plan(primary_symbol, run_cross_analysis, cross_symbols_text):
        symbol = normalize_symbol_for_path(symbol)
        info = stock_classification(symbol)
        is_target = symbol == primary_code
        if is_target:
            company, source = resolve_company_name(symbol, resolved_primary_company)
        else:
            company, source = resolve_company_name(symbol, "")
        rows.append(
            {
                "role": "experiment_target" if is_target else "sector_peer_training_candidate",
                "symbol": symbol,
                "company_name": company or info["company_name"],
                "sector": info["sector"],
                "industry": info["industry"],
                "company_source": source,
                "sector_source": info["sector_source"],
                "target_excluded_from_training": not is_target,
            }
        )
    return pd.DataFrame(rows)


def cache_preview(symbol: str, start_date: str, end_date: str) -> dict[str, object]:
    preview: dict[str, object] = {"status": "missing", "message": "No local cache covers this range yet."}
    master = build_master_csv(symbol)
    if master is not None and not master.empty and "date" in master.columns:
        dates = pd.to_datetime(master["date"], errors="coerce").dropna()
        if not dates.empty:
            preview["master_start"] = str(dates.min().date())
            preview["master_end"] = str(dates.max().date())
    resolved = resolve_cached_csv(symbol, start_date, end_date, build_master=True)
    if resolved is not None:
        preview["status"] = "covered"
        preview["message"] = f"Local cache already covers this request via `{resolved.source}`."
        preview["resolved_path"] = str(resolved.path)
        preview["rows"] = int(resolved.rows)
    return preview


def latest_non_master_csv(symbol: str) -> Path | None:
    files = [path for path in stock_data_dir(symbol).glob("*_finance_text_*.csv") if not path.name.endswith("_master.csv")]
    if files:
        return sorted(files, key=lambda item: item.stat().st_mtime)[-1]
    master = stock_data_dir(symbol) / f"{normalize_symbol_for_path(symbol)}_finance_text_master.csv"
    return master if master.exists() else None


def latest_matching_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    files = sorted((item for item in directory.glob(pattern) if item.stat().st_size > 4), key=lambda item: item.stat().st_mtime)
    return files[-1] if files else None


def remove_if_exists(path: Path) -> None:
    """Remove a generated dashboard/peer-experiment artifact if it exists."""
    try:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    except OSError:
        pass


def cleanup_previous_dashboard_experiment(target_symbols: list[str]) -> None:
    """Clear stale official peer-NLP outputs before a new dashboard experiment.

    Raw market/news caches are intentionally preserved. The cleanup only removes
    generated peer-NLP result/report artifacts that can contaminate the next run.
    """
    for key in [
        "workflow_runs",
        "workflow_symbols",
        "workflow_failures",
        "cross_payload",
        "market_cross_payload",
        "workflow_phase_logs",
        "workflow_status_rows",
        "training_status_rows",
        "workflow_export_bundle",
    ]:
        st.session_state.pop(key, None)

    remove_if_exists(SYSTEM_OUTPUT_DIR / "dashboard_exports")
    for path in OFFICIAL_SYSTEM_FILES:
        remove_if_exists(path)

    for symbol in target_symbols:
        code = normalize_symbol_for_path(symbol)
        for filename in OFFICIAL_STOCK_RESULT_FILES:
            remove_if_exists(stock_results_dir(code) / filename)
        for filename in OFFICIAL_STOCK_REPORT_FILES:
            remove_if_exists(stock_reports_dir(code) / filename)


def load_stock_bundle(symbol: str) -> dict[str, object]:
    code = normalize_symbol_for_path(symbol)
    selected_csv = latest_non_master_csv(code)
    if selected_csv is None or not selected_csv.exists():
        return {"symbol": code, "available": False, "reason": "No integrated CSV found yet for this stock."}

    data = safe_read_csv(selected_csv)
    if data.empty:
        return {"symbol": code, "available": False, "reason": f"Integrated CSV is empty or unreadable: {selected_csv.name}"}
    if "date" in data.columns:
        data["date"] = pd.to_datetime(data["date"], errors="coerce")
        data = data.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    reports_dir = stock_reports_dir(code)
    results_dir = stock_results_dir(code)
    stem = selected_csv.stem

    def read_csv_with_fallback(path: Path, pattern: str) -> pd.DataFrame:
        if path.exists():
            direct = safe_read_csv(path)
            if not direct.empty:
                return direct
        fallback = latest_matching_file(path.parent, pattern)
        return safe_read_csv(fallback) if fallback and fallback.exists() else pd.DataFrame()

    sentiment = read_csv_with_fallback(reports_dir / f"{stem}_daily_sentiment.csv", "*_daily_sentiment.csv")
    daily_net_flow = read_csv_with_fallback(reports_dir / f"{stem}_daily_net_flow.csv", "*_daily_net_flow.csv")
    signal_diag = read_csv_with_fallback(reports_dir / f"{stem}_signal_diagnostics.csv", "*_signal_diagnostics.csv")
    diagnostics = read_csv_with_fallback(reports_dir / f"{stem}_diagnostics.csv", "*_diagnostics.csv")
    nlp_eval = read_csv_with_fallback(reports_dir / f"{stem}_nlp_evaluation.csv", "*_nlp_evaluation.csv")
    state_compliance = read_csv_with_fallback(reports_dir / f"{stem}_state_vector_compliance.csv", "*_state_vector_compliance.csv")
    leakage_diag = read_csv_with_fallback(reports_dir / f"{stem}_leakage_diagnostics.csv", "*_leakage_diagnostics.csv")
    metrics = safe_read_csv(results_dir / "ablation_metrics.csv")
    seed_metrics = safe_read_csv(results_dir / "ablation_metrics_by_seed.csv")
    curves = safe_read_csv(results_dir / "portfolio_curves.csv")
    drawdowns = safe_read_csv(results_dir / "drawdown_curves.csv")
    logs = safe_read_csv(results_dir / "trading_logs.csv")
    training_rewards = safe_read_csv(results_dir / "training_rewards_all_seeds.csv")
    density_split = safe_read_csv(reports_dir / "information_density_split.csv")
    if density_split.empty:
        density_split = safe_read_csv(reports_dir / "peer_nlp_information_density_split.csv")
    daily_density = safe_read_csv(reports_dir / "daily_news_density.csv")
    window_summary = safe_read_csv(reports_dir / "experiment_window_summary.csv")
    high_density_metrics = safe_read_csv(results_dir / "high_density_ablation_metrics.csv")
    high_density_curves = safe_read_csv(results_dir / "high_density_portfolio_curves.csv")
    high_density_logs = safe_read_csv(results_dir / "high_density_trading_logs.csv")
    peer_daily = safe_read_csv(results_dir / "peer_nlp_daily_sentiment.csv")
    peer_metrics = safe_read_csv(results_dir / "peer_nlp_ablation_metrics.csv")
    peer_seed_metrics = safe_read_csv(results_dir / "peer_nlp_ablation_metrics_by_seed.csv")
    peer_curves = safe_read_csv(results_dir / "peer_nlp_portfolio_curves.csv")
    peer_drawdowns = safe_read_csv(results_dir / "peer_nlp_drawdown_curves.csv")
    peer_logs = safe_read_csv(results_dir / "peer_nlp_trading_logs.csv")
    peer_training_rewards = safe_read_csv(results_dir / "peer_nlp_training_rewards_all_seeds.csv")
    peer_effect = safe_read_csv(results_dir / "peer_nlp_effect_summary.csv")
    peer_integrity = safe_read_csv(reports_dir / "peer_nlp_integrity_check.csv")
    peer_state_diagnostics = safe_read_csv(reports_dir / "peer_nlp_group_state_diagnostics.csv")
    market_impact_daily = safe_read_csv(results_dir / "peer_market_impact_daily_signal.csv")
    market_impact_metrics = safe_read_csv(results_dir / "market_impact_ablation_metrics.csv")
    market_impact_seed_metrics = safe_read_csv(results_dir / "market_impact_ablation_metrics_by_seed.csv")
    market_impact_curves = safe_read_csv(results_dir / "market_impact_portfolio_curves.csv")
    market_impact_drawdowns = safe_read_csv(results_dir / "market_impact_drawdown_curves.csv")
    market_impact_logs = safe_read_csv(results_dir / "market_impact_trading_logs.csv")
    market_impact_effect = safe_read_csv(results_dir / "market_impact_effect_summary.csv")
    market_impact_reliability = safe_read_csv(reports_dir / "market_impact_reliability_check.csv")
    market_impact_state_diagnostics = safe_read_csv(reports_dir / "market_impact_group_state_diagnostics.csv")
    if daily_density.empty:
        daily_density = density_from_peer_daily(peer_daily)
    behavior_summary = safe_read_csv(reports_dir / "model_behavior_visual_summary.csv")
    trade_outcomes = safe_read_csv(reports_dir / "trade_outcome_win_rate.csv")
    summary_json = latest_matching_file(reports_dir, "*_analysis_summary.json")
    report_md = latest_matching_file(reports_dir, "*_report_draft.md")

    if daily_net_flow.empty or not is_usable_daily_net_flow(daily_net_flow):
        daily_net_flow = compute_daily_net_flow(data)
    if "date" in daily_net_flow.columns:
        daily_net_flow["date"] = pd.to_datetime(daily_net_flow["date"], errors="coerce")
    for column in ["net_flow_cny_million", "net_flow", "traded_value"]:
        if column in daily_net_flow.columns:
            daily_net_flow[column] = pd.to_numeric(daily_net_flow[column], errors="coerce")

    raw_company = ""
    if "company_name" in data.columns and data["company_name"].notna().any():
        raw_company = str(data["company_name"].dropna().iloc[0]).strip()
    company_name = sanitize_company_name(code, raw_company) or discover_company_name(code)[0]
    return {
        "symbol": code,
        "company_name": company_name,
        "available": True,
        "selected_csv": selected_csv,
        "data": data,
        "sentiment": sentiment,
        "signal_diag": signal_diag,
        "diagnostics": diagnostics,
        "nlp_eval": nlp_eval,
        "state_compliance": state_compliance,
        "leakage_diag": leakage_diag,
        "metrics": metrics,
        "seed_metrics": seed_metrics,
        "curves": curves,
        "drawdowns": drawdowns,
        "logs": logs,
        "training_rewards": training_rewards,
        "density_split": density_split,
        "daily_density": daily_density,
        "window_summary": window_summary,
        "high_density_metrics": high_density_metrics,
        "high_density_curves": high_density_curves,
        "high_density_logs": high_density_logs,
        "official_current_experiment": "peer_sector_nlp_transfer",
        "peer_daily": peer_daily,
        "peer_metrics": peer_metrics,
        "peer_seed_metrics": peer_seed_metrics,
        "peer_curves": peer_curves,
        "peer_drawdowns": peer_drawdowns,
        "peer_logs": peer_logs,
        "peer_training_rewards": peer_training_rewards,
        "peer_effect": peer_effect,
        "peer_integrity": peer_integrity,
        "peer_state_diagnostics": peer_state_diagnostics,
        "market_impact_daily": market_impact_daily,
        "market_impact_metrics": market_impact_metrics,
        "market_impact_seed_metrics": market_impact_seed_metrics,
        "market_impact_curves": market_impact_curves,
        "market_impact_drawdowns": market_impact_drawdowns,
        "market_impact_logs": market_impact_logs,
        "market_impact_effect": market_impact_effect,
        "market_impact_reliability": market_impact_reliability,
        "market_impact_state_diagnostics": market_impact_state_diagnostics,
        "behavior_summary": behavior_summary,
        "trade_outcomes": trade_outcomes,
        "daily_net_flow": daily_net_flow,
        "summary_json": summary_json,
        "report_md": report_md,
    }


def render_stock_outputs(bundle: dict[str, object]) -> None:
    if not bundle.get("available"):
        st.warning(str(bundle.get("reason", "No outputs available.")))
        return

    data = bundle["data"]
    legacy_sentiment = bundle["sentiment"]
    legacy_metrics = bundle["metrics"]
    signal_diag = bundle["signal_diag"]
    diagnostics = bundle["diagnostics"]
    legacy_seed_metrics = bundle["seed_metrics"]
    legacy_curves = bundle["curves"]
    legacy_drawdowns = bundle["drawdowns"]
    legacy_logs = bundle["logs"]
    daily_net_flow = bundle["daily_net_flow"]
    peer_daily = bundle.get("peer_daily", pd.DataFrame())
    peer_metrics = bundle.get("peer_metrics", pd.DataFrame())
    market_impact_metrics = bundle.get("market_impact_metrics", pd.DataFrame())
    peer_seed_metrics = bundle.get("peer_seed_metrics", pd.DataFrame())
    peer_curves = bundle.get("peer_curves", pd.DataFrame())
    peer_drawdowns = bundle.get("peer_drawdowns", pd.DataFrame())
    peer_logs = bundle.get("peer_logs", pd.DataFrame())
    peer_effect = bundle.get("peer_effect", pd.DataFrame())
    peer_integrity = bundle.get("peer_integrity", pd.DataFrame())

    official_available = isinstance(market_impact_metrics, pd.DataFrame) and not market_impact_metrics.empty
    sentiment = peer_daily if isinstance(peer_daily, pd.DataFrame) else pd.DataFrame()
    metrics = peer_metrics if isinstance(peer_metrics, pd.DataFrame) else pd.DataFrame()
    seed_metrics = peer_seed_metrics if isinstance(peer_seed_metrics, pd.DataFrame) else pd.DataFrame()
    curves = peer_curves if isinstance(peer_curves, pd.DataFrame) else pd.DataFrame()
    drawdowns = peer_drawdowns if isinstance(peer_drawdowns, pd.DataFrame) else pd.DataFrame()
    logs = peer_logs if isinstance(peer_logs, pd.DataFrame) else pd.DataFrame()

    ticker = bundle["symbol"]
    if "symbol" in data.columns and data["symbol"].notna().any():
        ticker = str(data["symbol"].dropna().iloc[0])
    company_name = str(bundle.get("company_name", "")).strip()

    event_count = data["event_count"] if "event_count" in data.columns else pd.Series([0] * len(data), index=data.index)
    news_rows = int(pd.to_numeric(event_count, errors="coerce").fillna(0).sum())
    market_rows = int(data["close"].notna().sum()) if "close" in data.columns else len(data)
    if not official_available:
        st.warning(
            "Official sector sentiment/impact outputs are missing for this stock. "
            "Legacy stock-level NLP is not displayed as the main result; use the workflow controls to generate market_impact_* outputs."
        )
    else:
        st.success("Official current experiment loaded: sector sentiment/impact DQN comparison. Legacy stock-level NLP is excluded from the main view.")

    score_col = "marketwide_sentiment_score" if not sentiment.empty and "marketwide_sentiment_score" in sentiment.columns else "daily_sentiment_score" if not sentiment.empty and "daily_sentiment_score" in sentiment.columns else "sentiment_score"
    sentiment_coverage = 0.0
    if not sentiment.empty and "target_news_available" in sentiment.columns:
        sentiment_coverage = float(pd.to_numeric(sentiment["target_news_available"], errors="coerce").fillna(0).mean())
    elif not sentiment.empty and "news_count" in sentiment.columns:
        sentiment_coverage = float((pd.to_numeric(sentiment["news_count"], errors="coerce").fillna(0) > 0).mean())

    header_cols = st.columns(5)
    header_cols[0].metric("Ticker", ticker)
    header_cols[1].metric("Company", company_name or "-")
    header_cols[2].metric("Market rows", market_rows)
    header_cols[3].metric("News/event rows", news_rows)
    header_cols[4].metric("Sentiment coverage", f"{sentiment_coverage:.1%}")

    if official_available and isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty:
        effect_row = peer_effect.iloc[0]
        cols = st.columns(5)
        cols[0].metric("Official experiment", "Peer NLP")
        cols[1].metric("Sector corpus", str(sentiment.get("sector_corpus_status", pd.Series([""])).dropna().iloc[0]) if "sector_corpus_status" in sentiment.columns and sentiment["sector_corpus_status"].notna().any() else "-")
        cols[2].metric("Marketwide corpus", str(sentiment.get("marketwide_corpus_status", pd.Series([""])).dropna().iloc[0]) if "marketwide_corpus_status" in sentiment.columns and sentiment["marketwide_corpus_status"].notna().any() else "-")
        cols[3].metric("Sector effect", str(effect_row.get("sector_effect_label", "-")))
        cols[4].metric("Reliability", str(effect_row.get("reliability_status", "-")))

    view_tabs = st.tabs(["Price & Signals", "RL & Portfolio", "Peer NLP Effect", "Reliability", "Text Analysis"])
    with view_tabs[0]:
        st.subheader("Close Price")
        if "close" in data.columns:
            render_series_line(data, "date", "close", color=PALETTE["ink"], height=340)
        else:
            st.info("Close price is not available in the integrated CSV.")

        st.subheader("Daily Net Inflow / Outflow")
        if daily_net_flow.empty:
            st.info("No market data available to compute daily net flow.")
        else:
            st.caption("优先使用现有净流入字段；没有时使用 OHLCV/turnover proxy，仅作解释变量，不直接当成可预测信号。")
            flow_col = "net_flow_cny_million" if "net_flow_cny_million" in daily_net_flow.columns else "net_flow"
            render_series_bar(daily_net_flow, "date", flow_col, color_by_sign=True, height=340)

        st.subheader("Daily Sentiment Trend")
        if sentiment.empty or score_col not in sentiment.columns:
            st.info("No daily sentiment file found yet.")
        else:
            sentiment["date"] = pd.to_datetime(sentiment["date"], errors="coerce")
            if official_available and {"sector_sentiment_score", "marketwide_sentiment_score"}.issubset(sentiment.columns):
                frame = sentiment.set_index("date")[["sector_sentiment_score", "marketwide_sentiment_score"]]
                render_multi_series(frame, kind="line", color_map={
                    "sector_sentiment_score": PALETTE["teal"],
                    "marketwide_sentiment_score": PALETTE["rose"],
                }, height=340)
                st.caption("Peer-trained sentiment is scored on the target stock's own high-density news, then lagged before DQN decisions.")
            else:
                render_series_line(sentiment, "date", score_col, color=PALETTE["rose"], height=340)

        st.subheader("Daily News Count")
        news_count_col = "target_news_count" if official_available and "target_news_count" in sentiment.columns else "news_count"
        if sentiment.empty or news_count_col not in sentiment.columns:
            st.info("No news-count series found yet.")
        else:
            warning = news_concentration_warning(sentiment)
            if warning:
                st.warning(warning)
            render_series_bar(sentiment, "date", news_count_col, color=PALETTE["teal"], height=340)

    with view_tabs[1]:
        st.subheader("Portfolio Value Curve")
        if curves.empty:
            st.info("No portfolio curves found yet.")
        else:
            curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
            curve_frame = curves.pivot_table(index="date", columns="experiment", values="portfolio_value", aggfunc="mean")
            render_multi_series(curve_frame, kind="line", color_map=EXPERIMENT_COLORS, height=360)
            st.caption("Official comparison: no-NLP DQN vs sector-peer NLP DQN vs marketwide-peer NLP DQN over the same target high-density test window.")

        st.subheader("Drawdown Curves")
        if drawdowns.empty:
            st.info("No drawdown curves found yet.")
        else:
            drawdowns["date"] = pd.to_datetime(drawdowns["date"], errors="coerce")
            drawdown_frame = drawdowns.pivot_table(index="date", columns="experiment", values="drawdown", aggfunc="mean")
            render_multi_series(drawdown_frame, kind="line", color_map=EXPERIMENT_COLORS, height=360)

        st.subheader("Ablation Metrics")
        if metrics.empty:
            st.info("No ablation metrics found yet.")
        else:
            st.dataframe(metrics, use_container_width=True)
            metric_panels = st.tabs(["Risk/Return", "Trades", "Exposure", "Seeds"])
            with metric_panels[0]:
                cols = [col for col in ["final_equity", "cumulative_return", "annualized_return", "sharpe_ratio", "max_drawdown"] if col in metrics.columns]
                if cols:
                    render_multi_series(metrics.set_index("experiment")[cols], kind="bar", height=360)
            with metric_panels[1]:
                cols = [col for col in ["number_of_trades", "trade_frequency", "profit_factor"] if col in metrics.columns]
                if cols:
                    render_multi_series(metrics.set_index("experiment")[cols], kind="bar", height=360)
            with metric_panels[2]:
                cols = [col for col in ["exposure_ratio", "win_rate"] if col in metrics.columns]
                if cols:
                    render_multi_series(metrics.set_index("experiment")[cols], kind="bar", height=360)
            with metric_panels[3]:
                if seed_metrics.empty:
                    st.info("No per-seed metrics found yet.")
                else:
                    st.dataframe(seed_metrics, use_container_width=True)

        if not legacy_metrics.empty:
            with st.expander("Legacy stock-level NLP outputs", expanded=False):
                st.warning("Deprecated robustness output only. Do not use this as the current official conclusion.")
                st.dataframe(legacy_metrics, use_container_width=True)

        st.subheader("Buy / Sell / Hold Actions")
        if logs.empty:
            st.info("No trading logs found yet.")
        else:
            action_counts = logs["action"].value_counts().rename_axis("action").reset_index(name="count")
            render_series_bar(action_counts, "action", "count", color=PALETTE["blue"], height=300)
            st.dataframe(logs.tail(50), use_container_width=True)

    with view_tabs[2]:
        st.subheader("Peer NLP Effect Summary")
        if isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty:
            st.dataframe(peer_effect, use_container_width=True)
            effect_cols = [
                col
                for col in [
                    "sector_final_equity_effect",
                    "marketwide_final_equity_effect",
                    "sector_sharpe_effect",
                    "marketwide_sharpe_effect",
                    "sector_vs_marketwide_final_equity_effect",
                ]
                if col in peer_effect.columns
            ]
            if effect_cols:
                render_multi_series(peer_effect.set_index("target_symbol")[effect_cols], kind="bar", height=340)
                st.caption("Positive bars mean the peer NLP strategy beat the no-NLP baseline on that metric; reliability must still pass the checks below.")
        else:
            st.info("No peer NLP effect summary found. Run the official peer NLP workflow first.")

    with view_tabs[3]:
        st.subheader("Peer NLP Integrity / Reliability")
        if isinstance(peer_integrity, pd.DataFrame) and not peer_integrity.empty:
            st.dataframe(peer_integrity, use_container_width=True)
        else:
            st.info("No peer NLP integrity check found yet.")

        st.subheader("Signal Diagnostics")
        if signal_diag.empty:
            st.info("No signal diagnostics file found yet.")
        else:
            st.dataframe(signal_diag, use_container_width=True)
            corr = signal_diag[signal_diag["metric"].astype(str).str.contains("corr", case=False, na=False)].copy()
            if not corr.empty:
                corr["value"] = pd.to_numeric(corr["value"], errors="coerce")
                render_series_bar(corr, "metric", "value", color_by_sign=True, height=320)

        st.subheader("Run Diagnostics")
        if diagnostics.empty:
            st.info("No diagnostics table found yet.")
        else:
            st.dataframe(diagnostics, use_container_width=True)

    with view_tabs[4]:
        report_md = bundle.get("report_md")
        summary_json = bundle.get("summary_json")
        if report_md and Path(report_md).exists():
            st.markdown(Path(report_md).read_text(encoding="utf-8"))
        else:
            st.info("No report draft markdown found yet.")
        if summary_json and Path(summary_json).exists():
            with st.expander("Analysis Summary JSON"):
                st.json(json.loads(Path(summary_json).read_text(encoding="utf-8")))


def render_cross_stock_outputs(summary_df: pd.DataFrame, discussion_path: Path | None) -> None:
    st.subheader("Cross-Stock Comparison")
    if summary_df.empty:
        st.info("Cross-stock summary is empty. 至少需要两只成功跑完的股票才会更有比较意义。")
        return

    if "target_symbol" in summary_df.columns:
        st.success("Official peer-sector NLP cross-stock summary loaded.")
        st.dataframe(summary_df, use_container_width=True)
        symbol_col = "target_symbol"
        peer_tabs = st.tabs(["Final Equity Effect", "Sharpe Effect", "Sector vs Marketwide", "Coverage", "Reliability", "Discussion"])
        with peer_tabs[0]:
            cols = [col for col in ["sector_final_equity_effect", "marketwide_final_equity_effect"] if col in summary_df.columns]
            if cols:
                render_multi_series(summary_df.set_index(symbol_col)[cols], kind="bar", color_map={
                    "sector_final_equity_effect": PALETTE["teal"],
                    "marketwide_final_equity_effect": PALETTE["rose"],
                }, height=360)
                st.caption("Positive values mean the peer NLP DQN beat the no-NLP DQN in final equity.")
        with peer_tabs[1]:
            cols = [col for col in ["sector_sharpe_effect", "marketwide_sharpe_effect"] if col in summary_df.columns]
            if cols:
                render_multi_series(summary_df.set_index(symbol_col)[cols], kind="bar", color_map={
                    "sector_sharpe_effect": PALETTE["teal"],
                    "marketwide_sharpe_effect": PALETTE["rose"],
                }, height=360)
                st.caption("Sharpe effect compares risk-adjusted performance against the no-NLP baseline.")
        with peer_tabs[2]:
            cols = [col for col in ["sector_vs_marketwide_effect", "sector_vs_marketwide_sharpe_effect"] if col in summary_df.columns]
            if cols:
                render_multi_series(summary_df.set_index(symbol_col)[cols], kind="bar", height=360)
                st.caption("This directly compares sector-peer transfer with marketwide-peer transfer.")
        with peer_tabs[3]:
            if "target_sentiment_coverage" in summary_df.columns:
                render_series_bar(summary_df, symbol_col, "target_sentiment_coverage", color=PALETTE["teal"], height=320)
                st.caption("Coverage counts days where target-stock news exists; no-news days are missing signal, not neutral sentiment.")
            if {"target_sentiment_coverage", "sector_final_equity_effect"}.issubset(summary_df.columns):
                render_scatter(summary_df, "target_sentiment_coverage", "sector_final_equity_effect", category=symbol_col, height=340)
        with peer_tabs[4]:
            cols = [col for col in ["target_symbol", "target_sector", "sector_effect_label", "marketwide_effect_label", "reliability_status", "reason_if_not_reliable"] if col in summary_df.columns]
            st.dataframe(summary_df[cols], use_container_width=True)
        with peer_tabs[5]:
            if discussion_path and discussion_path.exists():
                st.markdown(discussion_path.read_text(encoding="utf-8"))
            else:
                st.info("No peer NLP cross-stock discussion markdown found yet.")
        return

    status_text = ", ".join(sorted(set(summary_df.get("comparability_status", pd.Series(["UNKNOWN"])).dropna().astype(str))))
    common_start = summary_df.get("common_start_date", pd.Series([""])).dropna().astype(str).iloc[0] if "common_start_date" in summary_df.columns else ""
    common_end = summary_df.get("common_end_date", pd.Series([""])).dropna().astype(str).iloc[0] if "common_end_date" in summary_df.columns else ""
    overlap = summary_df.get("common_overlap_trading_days", pd.Series([""])).dropna().astype(str).iloc[0] if "common_overlap_trading_days" in summary_df.columns else ""
    if "NOT_RELIABLE" in status_text:
        st.warning(f"Cross-stock reliability: {status_text}. Common window {common_start} to {common_end}, overlap trading days: {overlap}.")
    else:
        st.info(f"Cross-stock reliability: {status_text}. Common window {common_start} to {common_end}, overlap trading days: {overlap}.")

    st.dataframe(summary_df, use_container_width=True)
    cross_tabs = st.tabs(["Final Equity", "Cumulative Return", "Sharpe", "NLP Effect", "Sentiment Coverage", "Discussion"])
    with cross_tabs[0]:
        cols = [col for col in ["buy_and_hold_final_equity", "dqn_without_nlp_final_equity", "dqn_with_nlp_final_equity"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "buy_and_hold_final_equity": PALETTE["ink"],
                "dqn_without_nlp_final_equity": PALETTE["blue"],
                "dqn_with_nlp_final_equity": PALETTE["rose"],
            }, height=360)
    with cross_tabs[1]:
        cols = [col for col in ["buy_and_hold_cumulative_return", "dqn_without_nlp_cumulative_return", "dqn_with_nlp_cumulative_return"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "buy_and_hold_cumulative_return": PALETTE["ink"],
                "dqn_without_nlp_cumulative_return": PALETTE["blue"],
                "dqn_with_nlp_cumulative_return": PALETTE["rose"],
            }, height=360)
    with cross_tabs[2]:
        cols = [col for col in ["buy_and_hold_sharpe", "dqn_without_nlp_sharpe", "dqn_with_nlp_sharpe"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "buy_and_hold_sharpe": PALETTE["ink"],
                "dqn_without_nlp_sharpe": PALETTE["blue"],
                "dqn_with_nlp_sharpe": PALETTE["rose"],
            }, height=360)
    with cross_tabs[3]:
        cols = [col for col in ["nlp_final_equity_effect", "nlp_sharpe_effect"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "nlp_final_equity_effect": PALETTE["rose"],
                "nlp_sharpe_effect": PALETTE["teal"],
            }, height=360)
    with cross_tabs[4]:
        if "sentiment_coverage_ratio" in summary_df.columns:
            render_series_bar(summary_df, "symbol", "sentiment_coverage_ratio", color=PALETTE["teal"], height=320)
        if {"sentiment_coverage_ratio", "nlp_final_equity_effect"}.issubset(summary_df.columns):
            render_scatter(summary_df, "sentiment_coverage_ratio", "nlp_final_equity_effect", category="symbol", height=340)
    with cross_tabs[5]:
        if discussion_path and discussion_path.exists():
            st.markdown(discussion_path.read_text(encoding="utf-8"))
        else:
            st.info("No cross-stock discussion markdown found yet.")


def status_value(complete: bool, partial: bool = False, warning: bool = False) -> str:
    if complete and warning:
        return "Warning"
    if complete:
        return "Complete"
    if partial:
        return "Partial"
    return "Missing"


def render_status_badges(rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    if frame.empty:
        st.info("No status rows available.")
        return
    st.dataframe(frame, use_container_width=True, hide_index=True)


def latest_report_file(pattern: str) -> Path | None:
    files = sorted((path for path in (PROJECT_ROOT / "reports").glob(pattern) if path.exists()), key=lambda item: item.stat().st_mtime)
    return files[-1] if files else None


def status_from_file(path: Path) -> str:
    return "Complete" if path.exists() and path.stat().st_size > 4 else "Missing"


def output_file_index() -> pd.DataFrame:
    roots = [PROJECT_ROOT / "reports", PROJECT_ROOT / "outputs"]
    rows = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            suffix = path.suffix.lower()
            if suffix in {".csv", ".md", ".json", ".png", ".svg", ".zip"}:
                rows.append(
                    {
                        "path": str(path.relative_to(PROJECT_ROOT)),
                        "type": suffix.lstrip("."),
                        "size_kb": round(path.stat().st_size / 1024, 1),
                        "modified": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
                    }
                )
    return pd.DataFrame(rows)


def stock_status_rows(symbol: str, bundle: dict[str, object]) -> list[dict[str, object]]:
    data = bundle.get("data", pd.DataFrame())
    sentiment = bundle.get("peer_daily", pd.DataFrame())
    nlp_eval = bundle.get("nlp_eval", pd.DataFrame())
    dates = pd.to_datetime(data.get("date", pd.Series(dtype=str)), errors="coerce") if isinstance(data, pd.DataFrame) else pd.Series(dtype="datetime64[ns]")
    finbert_status = "Missing"
    if isinstance(nlp_eval, pd.DataFrame) and not nlp_eval.empty:
        finbert_values = nlp_eval.get("finbert_status", pd.Series(dtype=str)).dropna().astype(str)
        if not finbert_values.empty:
            finbert_status = "Complete" if "ok" in set(finbert_values) else "Warning"
    gold_eval = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "nlp_gold_label_evaluation.csv")
    gold_status = "Missing"
    if not gold_eval.empty:
        eval_types = set(gold_eval.get("evaluation_type", pd.Series(dtype=str)).dropna().astype(str))
        gold_status = "Complete" if "gold_label_eval" in eval_types else "Partial"
    peer_metrics = bundle.get("peer_metrics", pd.DataFrame())
    peer_effect = bundle.get("peer_effect", pd.DataFrame())
    peer_integrity = bundle.get("peer_integrity", pd.DataFrame())
    market_impact_metrics = bundle.get("market_impact_metrics", pd.DataFrame())
    market_impact_effect = bundle.get("market_impact_effect", pd.DataFrame())
    cross_summary = safe_read_csv(SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv")
    density_split = bundle.get("density_split", pd.DataFrame())
    window_summary = bundle.get("window_summary", pd.DataFrame())
    density_status = "Missing"
    density_detail = "Run scripts/generate_information_density_outputs.py"
    if isinstance(density_split, pd.DataFrame) and not density_split.empty:
        density_value = str(density_split.get("density_status", pd.Series([""])).dropna().astype(str).iloc[0])
        density_status = "Complete" if density_value == "OK" else "Warning"
        density_detail = density_value
    if isinstance(window_summary, pd.DataFrame) and not window_summary.empty:
        usage = str(window_summary.get("recommended_usage", pd.Series([""])).dropna().astype(str).iloc[0])
        density_detail = f"{density_detail}; usage={usage}"
    cross_detail = ""
    if cross_summary.empty:
        cross_status = "Missing"
    else:
        cross_detail = str(cross_summary.get("reliability_status", pd.Series(["READY_WITH_WARNINGS"])).dropna().astype(str).iloc[0])
        cross_status = "Complete" if cross_detail == "READY_FOR_SUBMISSION" else "Warning"
    return [
        {"item": "Selected stock", "status": status_value(bool(bundle.get("available"))), "detail": symbol},
        {
            "item": "Date range",
            "status": status_value(dates.notna().any()),
            "detail": f"{dates.min().date()} to {dates.max().date()}" if dates.notna().any() else "-",
        },
        {"item": "Data availability", "status": status_value(isinstance(data, pd.DataFrame) and not data.empty), "detail": f"{len(data) if isinstance(data, pd.DataFrame) else 0} rows"},
        {"item": "Official sector impact outputs", "status": status_value(isinstance(market_impact_metrics, pd.DataFrame) and not market_impact_metrics.empty), "detail": "outputs/stocks/<symbol>/results/market_impact_ablation_metrics.csv"},
        {"item": "Official impact effect summary", "status": status_value(isinstance(market_impact_effect, pd.DataFrame) and not market_impact_effect.empty), "detail": "market_impact_effect_summary.csv"},
        {"item": "Peer sentiment diagnostics", "status": status_value(isinstance(peer_metrics, pd.DataFrame) and not peer_metrics.empty), "detail": "peer_nlp_ablation_metrics.csv"},
        {"item": "Peer NLP effect diagnostics", "status": status_value(isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty), "detail": "peer_nlp_effect_summary.csv"},
        {"item": "Peer NLP integrity", "status": status_value(isinstance(peer_integrity, pd.DataFrame) and not peer_integrity.empty), "detail": "peer_nlp_integrity_check.csv"},
        {"item": "Information density status", "status": density_status, "detail": density_detail},
        {"item": "FinBERT status", "status": finbert_status, "detail": "ok=used; skipped=fallback sentiment"},
        {"item": "Gold-label evaluation", "status": gold_status, "detail": "reports/tables/nlp_gold_label_evaluation.csv"},
        {"item": "Walk-forward status", "status": status_from_file(PROJECT_ROOT / "reports" / "tables" / "walk_forward_results.csv"), "detail": "honest split diagnostics unless rolling retrain is run"},
        {"item": "SQLite demo", "status": status_from_file(PROJECT_ROOT / "reports" / "sqlite_demo.md"), "detail": "optional storage evidence"},
        {"item": "Peer cross-stock analysis", "status": cross_status, "detail": cross_detail or "outputs/system/peer_nlp_cross_stock_summary.csv"},
        {"item": "Final report", "status": status_from_file(PROJECT_ROOT / "reports" / "final_report.md"), "detail": "reports/final_report.md"},
        {"item": "Presentation materials", "status": status_from_file(PROJECT_ROOT / "reports" / "presentation_outline.md"), "detail": "outline/demo/Q&A"},
        {"item": "GitHub readiness", "status": status_from_file(PROJECT_ROOT / "reports" / "github_submission_checklist.md"), "detail": ".git not required for review; checklist explains commands"},
    ]


def render_overview_page(symbol: str, bundle: dict[str, object]) -> None:
    st.subheader("Overview")
    mapping = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "stock_sector_mapping.csv")
    if mapping.empty:
        mapping = safe_read_csv(PROJECT_ROOT / "config" / "stock_sector_mapping.csv", dtype=str)
    sector = "UNKNOWN"
    industry = "UNKNOWN"
    company = bundle.get("company_name", "")
    if not mapping.empty and "symbol" in mapping.columns:
        row = mapping[mapping["symbol"].astype(str).apply(normalize_symbol_for_path) == normalize_symbol_for_path(symbol)]
        if not row.empty:
            sector = str(row.get("sector", pd.Series(["UNKNOWN"])).iloc[0] or "UNKNOWN")
            industry = str(row.get("industry", pd.Series(["UNKNOWN"])).iloc[0] or "UNKNOWN")
            company = str(row.get("company_name", pd.Series([company])).iloc[0] or company)
    st.info(
        f"Current official experiment: sector sentiment/impact DQN comparison. Target `{symbol}` "
        f"({company or '-'}) belongs to sector `{sector}`, industry `{industry}`."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "group": "Group 0",
                    "experiment": "dqn_without_nlp",
                    "description": "DQN uses only lagged market/technical state: price, MA50, MA200, RSI, MACD, position, cash.",
                },
                {
                    "group": "Group 1",
                    "experiment": "dqn_with_sector_sentiment_nlp",
                    "description": "Sentiment NLP is trained on same-sector peer news, excluding the target stock, then scores target news.",
                },
                {
                    "group": "Group 2",
                    "experiment": "dqn_with_sector_impact_nlp",
                    "description": "Impact NLP is trained on same-sector peer news labelled by peer post-news returns, excluding target labels.",
                },
                {
                    "group": "Optional add-on",
                    "experiment": "dqn_with_marketwide_sentiment_nlp / dqn_with_marketwide_impact_nlp",
                    "description": "Only when selected: adds cross-sector marketwide peer sentiment and impact groups under the same DQN settings.",
                },
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )
    render_status_badges(stock_status_rows(symbol, bundle))
    data = bundle.get("data", pd.DataFrame())
    sentiment = bundle.get("peer_daily", pd.DataFrame())
    metrics = bundle.get("market_impact_metrics", pd.DataFrame())
    cols = st.columns(4)
    cols[0].metric("Detected stocks", len(available_local_symbols()))
    cols[1].metric("Rows", len(data) if isinstance(data, pd.DataFrame) else 0)
    coverage = 0.0
    if isinstance(sentiment, pd.DataFrame) and not sentiment.empty and "target_news_available" in sentiment.columns:
        coverage = float(pd.to_numeric(sentiment["target_news_available"], errors="coerce").fillna(0).mean())
    cols[2].metric("Sentiment coverage", f"{coverage:.1%}")
    best = "-"
    if isinstance(metrics, pd.DataFrame) and not metrics.empty and {"experiment", "final_equity"}.issubset(metrics.columns):
        best = str(metrics.sort_values("final_equity", ascending=False)["experiment"].iloc[0])
    cols[3].metric("Best strategy", best)
    corpus_summary = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv")
    target_corpus = pd.DataFrame()
    if isinstance(corpus_summary, pd.DataFrame) and not corpus_summary.empty and {"target_symbol", "corpus_type"}.issubset(corpus_summary.columns):
        target_corpus = corpus_summary[
            corpus_summary["target_symbol"].astype(str).apply(normalize_symbol_for_path) == normalize_symbol_for_path(symbol)
        ].copy()
    if not target_corpus.empty:
        sector_row = target_corpus[target_corpus["corpus_type"].astype(str) == "sector_peer"]
        market_row = target_corpus[target_corpus["corpus_type"].astype(str) == "marketwide_peer"]
        status_cols = st.columns(4)
        status_cols[0].metric("Sector corpus", _first_value(sector_row, "corpus_status", "-"))
        status_cols[1].metric("Sector peers", _first_value(sector_row, "number_of_peer_stocks", "-"))
        status_cols[2].metric("Marketwide corpus", _first_value(market_row, "corpus_status", "-"))
        status_cols[3].metric("Marketwide peers", _first_value(market_row, "number_of_peer_stocks", "-"))
    elif isinstance(sentiment, pd.DataFrame) and not sentiment.empty:
        status_cols = st.columns(4)
        status_cols[0].metric("Sector corpus", str(sentiment.get("sector_corpus_status", pd.Series(["-"])).dropna().iloc[0]) if "sector_corpus_status" in sentiment.columns and sentiment["sector_corpus_status"].notna().any() else "-")
        status_cols[1].metric("Sector peers", str(pd.to_numeric(sentiment.get("sector_peer_stock_count", 0), errors="coerce").fillna(0).max()) if "sector_peer_stock_count" in sentiment.columns else "-")
        status_cols[2].metric("Marketwide corpus", str(sentiment.get("marketwide_corpus_status", pd.Series(["-"])).dropna().iloc[0]) if "marketwide_corpus_status" in sentiment.columns and sentiment["marketwide_corpus_status"].notna().any() else "-")
        status_cols[3].metric("Marketwide peers", str(pd.to_numeric(sentiment.get("marketwide_peer_stock_count", 0), errors="coerce").fillna(0).max()) if "marketwide_peer_stock_count" in sentiment.columns else "-")


def render_single_stock_result_page(bundle: dict[str, object]) -> None:
    render_stock_outputs(bundle)
    st.subheader("Additional Result Tables")
    for label, key in [
        ("NLP evaluation", "nlp_eval"),
        ("State vector compliance", "state_compliance"),
        ("No-lookahead diagnostics", "leakage_diag"),
        ("Metrics by seed", "seed_metrics"),
    ]:
        frame = bundle.get(key, pd.DataFrame())
        with st.expander(label, expanded=False):
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                st.dataframe(frame, use_container_width=True)
            else:
                st.warning(f"{label} file is missing or empty.")


def render_information_density_page(bundle: dict[str, object]) -> None:
    st.subheader("Information Density")
    split = bundle.get("density_split", pd.DataFrame())
    daily = bundle.get("daily_density", pd.DataFrame())
    window = bundle.get("window_summary", pd.DataFrame())
    if isinstance(split, pd.DataFrame) and not split.empty:
        row = split.iloc[0]
        cols = st.columns(4)
        cols[0].metric("Density status", str(row.get("density_status", "")))
        cols[1].metric("80% cutoff", str(row.get("density_cutoff_date", "")))
        cols[2].metric("High-density days", str(row.get("high_density_trading_days", "")))
        coverage = pd.to_numeric(pd.Series([row.get("high_density_coverage_ratio")]), errors="coerce").iloc[0]
        cols[3].metric("High-density coverage", f"{coverage:.1%}" if pd.notna(coverage) else "-")
        st.dataframe(split, use_container_width=True)
    else:
        st.warning("Information-density split is missing. Run `scripts/generate_information_density_outputs.py` to generate cached review files.")

    if isinstance(window, pd.DataFrame) and not window.empty:
        st.markdown("#### Experiment windows")
        st.dataframe(window, use_container_width=True)
        usage = ", ".join(sorted(set(window.get("recommended_usage", pd.Series(dtype=str)).dropna().astype(str))))
        if "MAIN_EXPERIMENT" not in usage:
            st.warning(f"Recommended usage: `{usage or 'unknown'}`. Treat NLP evidence cautiously.")

    if isinstance(daily, pd.DataFrame) and not daily.empty:
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        chart_tabs = st.tabs(["Daily Count", "Cumulative %", "Coverage"])
        with chart_tabs[0]:
            render_series_bar(daily, "date", "daily_news_count", title="Daily news count", color=PALETTE["teal"], height=340)
        with chart_tabs[1]:
            pct_col = "recent_cumulative_news_pct" if "recent_cumulative_news_pct" in daily.columns else "cumulative_news_pct"
            render_series_line(daily, "date", pct_col, title="Cumulative news percentage", color=PALETTE["rose"], height=340)
        with chart_tabs[2]:
            if "news_available" in daily.columns:
                daily["rolling_coverage_20d"] = pd.to_numeric(daily["news_available"], errors="coerce").fillna(0).rolling(20, min_periods=1).mean()
                render_series_line(daily, "date", "rolling_coverage_20d", title="20-day news coverage", color=PALETTE["blue"], height=340)
        st.dataframe(daily.tail(80), use_container_width=True)
    else:
        st.info("Daily news-density table is missing.")


def render_nlp_evaluation_page(bundle: dict[str, object]) -> None:
    st.subheader("Peer NLP Scoring")
    st.warning("Official logic: target stock news is scored by NLP models trained from peer corpora. The target stock is excluded from its own NLP training corpus.")
    peer_daily = bundle.get("peer_daily", pd.DataFrame())
    peer_effect = bundle.get("peer_effect", pd.DataFrame())
    corpus = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv")
    mapping = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "stock_sector_mapping.csv")
    if isinstance(peer_daily, pd.DataFrame) and not peer_daily.empty:
        st.markdown("#### Target daily peer sentiment")
        st.dataframe(peer_daily, use_container_width=True)
        peer_daily["date"] = pd.to_datetime(peer_daily["date"], errors="coerce")
        score_cols = [col for col in ["sector_sentiment_score", "marketwide_sentiment_score"] if col in peer_daily.columns]
        if score_cols:
            render_multi_series(peer_daily.set_index("date")[score_cols], kind="line", height=340)
    else:
        st.warning("Official peer daily sentiment is missing. Run the official peer NLP workflow first.")
    if not corpus.empty:
        st.markdown("#### Peer corpus summary")
        st.dataframe(corpus, use_container_width=True)
    if not mapping.empty:
        st.markdown("#### Stock-sector mapping")
        st.dataframe(mapping, use_container_width=True)
    if isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty:
        st.markdown("#### Peer NLP effect label")
        st.dataframe(peer_effect, use_container_width=True)

    st.markdown("#### Legacy / gold-label diagnostics")
    st.info("The following tables are diagnostics only. They are not the official current experiment unless explicitly marked peer NLP.")
    stock_eval = bundle.get("nlp_eval", pd.DataFrame())
    gold_eval = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "nlp_gold_label_evaluation.csv")
    final_comp = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "nlp_model_comparison_final.csv")
    if isinstance(stock_eval, pd.DataFrame) and not stock_eval.empty:
        st.markdown("#### Stock-level NLP evaluation")
        st.dataframe(stock_eval, use_container_width=True)
        finbert_status = ", ".join(sorted(set(stock_eval.get("finbert_status", pd.Series(dtype=str)).dropna().astype(str))))
        main_method = ", ".join(sorted(set(stock_eval.get("main_experiment_method", pd.Series(dtype=str)).dropna().astype(str))))
        st.info(f"FinBERT status: `{finbert_status or 'unknown'}`. Sentiment method used in RL state: `{main_method or 'unknown'}`.")
    else:
        st.warning("Stock-level NLP evaluation file is missing.")

    st.markdown("#### Gold-label / final comparison")
    if not final_comp.empty:
        st.dataframe(final_comp, use_container_width=True)
        f1_cols = [col for col in ["macro_f1", "weighted_f1", "f1"] if col in final_comp.columns]
        if f1_cols and "method" in final_comp.columns:
            render_multi_series(final_comp.set_index("method")[f1_cols], kind="bar", height=320)
    elif not gold_eval.empty:
        st.dataframe(gold_eval, use_container_width=True)
    else:
        st.warning("Gold-label evaluation is missing. Fill `reports/tables/nlp_gold_label_template.csv` and rerun the final submission generator.")

    confusion = latest_report_file("tables/*confusion*.csv")
    if confusion and confusion.exists():
        st.markdown("#### Confusion matrix")
        st.dataframe(safe_read_csv(confusion), use_container_width=True)
    else:
        st.info("No confusion matrix file found yet.")


def _nlp_improvement_text(metrics: pd.DataFrame) -> str:
    if metrics.empty or "experiment" not in metrics.columns or "final_equity" not in metrics.columns:
        return "Inconclusive: ablation metrics are missing."
    rows = metrics.set_index("experiment")
    if {"dqn_with_nlp", "dqn_without_nlp"}.issubset(rows.index):
        equity_delta = pd.to_numeric(pd.Series([rows.loc["dqn_with_nlp", "final_equity"]]), errors="coerce").iloc[0] - pd.to_numeric(pd.Series([rows.loc["dqn_without_nlp", "final_equity"]]), errors="coerce").iloc[0]
        sharpe_delta = 0
        if "sharpe_ratio" in rows.columns:
            sharpe_delta = pd.to_numeric(pd.Series([rows.loc["dqn_with_nlp", "sharpe_ratio"]]), errors="coerce").iloc[0] - pd.to_numeric(pd.Series([rows.loc["dqn_without_nlp", "sharpe_ratio"]]), errors="coerce").iloc[0]
        if equity_delta > 0 and sharpe_delta > 0:
            return "Yes for this stock: NLP improved both final equity and Sharpe."
        if equity_delta < 0 and sharpe_delta < 0:
            return "No for this stock: NLP hurt both final equity and Sharpe."
        return "Mixed for this stock: NLP helped one metric but not all."
    return "Inconclusive: DQN with/without NLP rows are missing."


def render_rl_ablation_page(bundle: dict[str, object]) -> None:
    st.subheader("RL and Ablation")
    metrics = bundle.get("peer_metrics", pd.DataFrame())
    rewards = bundle.get("peer_training_rewards", pd.DataFrame())
    curves = bundle.get("peer_curves", pd.DataFrame())
    if isinstance(metrics, pd.DataFrame) and not metrics.empty:
        st.info("Main view: official peer-sector NLP transfer. Legacy stock-level NLP is excluded from this section.")
        st.markdown("#### Official peer NLP ablation")
        st.caption("The target stock is held out from NLP training. DQN trains on the earlier market-learning window and tests in the target high-density window.")
        st.dataframe(_official_metric_table(metrics), use_container_width=True, hide_index=True)
        cols = [col for col in ["final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown"] if col in metrics.columns]
        if cols:
            render_multi_series(metrics.set_index("experiment")[cols], kind="bar", height=360)
        if isinstance(curves, pd.DataFrame) and not curves.empty:
            curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
            curve_frame = curves.pivot_table(index="date", columns="experiment", values="portfolio_value", aggfunc="mean")
            render_multi_series(curve_frame, title="Peer NLP portfolio curves", kind="line", color_map=EXPERIMENT_COLORS, height=340)
    else:
        st.warning("Official peer NLP outputs are missing. Run the workflow with the peer NLP experiment enabled.")

    legacy_metrics = bundle.get("metrics", pd.DataFrame())
    if isinstance(legacy_metrics, pd.DataFrame) and not legacy_metrics.empty:
        with st.expander("Legacy stock-level NLP robustness outputs", expanded=False):
            st.warning("Deprecated: these are not official current results.")
            st.dataframe(legacy_metrics, use_container_width=True)
    if isinstance(rewards, pd.DataFrame) and not rewards.empty:
        st.markdown("#### Training rewards")
        rewards["episode"] = pd.to_numeric(rewards["episode"], errors="coerce")
        reward_frame = rewards.pivot_table(index="episode", columns="experiment", values="total_reward", aggfunc="mean")
        render_multi_series(reward_frame, kind="line", height=340)
        if "loss" in rewards.columns:
            st.markdown("#### DQN loss")
            loss_frame = rewards.pivot_table(index="episode", columns="experiment", values="loss", aggfunc="mean")
            render_multi_series(loss_frame, kind="line", height=340)
    else:
        st.info("Training reward/loss curves are missing.")
    seed_metrics = bundle.get("peer_seed_metrics", pd.DataFrame())
    if isinstance(seed_metrics, pd.DataFrame) and not seed_metrics.empty:
        st.markdown("#### Multi-seed results")
        st.dataframe(seed_metrics, use_container_width=True)


def _display_behavior_image(symbol: str, filename: str, caption: str) -> None:
    path = stock_reports_dir(symbol) / filename
    if path.exists() and path.stat().st_size > 4:
        st.image(str(path), caption=caption, use_container_width=True)
    else:
        st.warning(f"`{filename}` is missing. Run `scripts/generate_trading_visualizations.py` to refresh cached visualizations.")


def render_model_behavior_visualization_page(symbol: str, bundle: dict[str, object]) -> None:
    st.subheader("Model Behavior & Trading Visualization")
    st.caption("All charts use cached results and the selected evaluation window where available. Actions come from DQN logs whose state features are lagged before decisions.")
    logs = bundle.get("peer_logs", pd.DataFrame())
    if not isinstance(logs, pd.DataFrame) or logs.empty:
        logs = bundle.get("high_density_logs", pd.DataFrame())
    if not isinstance(logs, pd.DataFrame) or logs.empty:
        logs = bundle.get("logs", pd.DataFrame())
    if isinstance(logs, pd.DataFrame) and not logs.empty and "experiment" in logs.columns:
        logs = logs.copy()
        logs["experiment"] = logs["experiment"].astype(str).replace({"dqn_with_nlp": "dqn_with_basic_nlp"})
        strategies = sorted(set(logs["experiment"].dropna().astype(str)))
        selected_strategy = st.selectbox(
            "Strategy for action overlays",
            strategies,
            index=0,
            format_func=lambda item: {
                "dqn_without_nlp": "DQN without NLP",
                "dqn_with_sector_peer_nlp": "DQN with Sector-Peer NLP",
                "dqn_with_marketwide_peer_nlp": "DQN with Marketwide-Peer NLP",
                "dqn_with_basic_nlp": "Legacy Basic NLP",
                "dqn_with_enhanced_nlp": "Legacy Enhanced NLP",
                "predict_then_trade": "Predict-then-Trade",
            }.get(item, item),
        )
        market = bundle.get("data", pd.DataFrame())
        if isinstance(market, pd.DataFrame) and not market.empty and "close" in market.columns:
            market = market.copy()
            market["date"] = pd.to_datetime(market["date"], errors="coerce")
            action_rows = logs[logs["experiment"] == selected_strategy].copy()
            action_rows["date"] = pd.to_datetime(action_rows["date"], errors="coerce")
            if "seed" in action_rows.columns and action_rows["seed"].notna().any():
                seed = str(action_rows["seed"].dropna().astype(str).iloc[0])
                action_rows = action_rows[action_rows["seed"].astype(str) == seed]
            joined = action_rows.merge(market[["date", "close"]], on="date", how="left")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=market["date"], y=market["close"], mode="lines", name="Close", line={"color": PALETTE["ink"]}))
            for action, marker, color in [("Buy", "triangle-up", "green"), ("Sell", "triangle-down", "red"), ("Hold", "circle", "lightgrey")]:
                part = joined[joined["action"].astype(str) == action]
                if action == "Hold" and len(part) > 250:
                    continue
                fig.add_trace(go.Scatter(x=part["date"], y=part["close"], mode="markers", name=action, marker={"symbol": marker, "size": 9 if action != "Hold" else 5, "color": color, "opacity": 0.45 if action == "Hold" else 0.9}))
            fig.update_layout(**_base_layout(title="Price with Trading Actions", height=420))
            _render_plot(fig)
    else:
        st.warning("Trading logs are missing, so action overlays cannot be drawn.")

    tabs = st.tabs(["Saved Charts", "Behavior Tables", "Prediction Outputs"])
    with tabs[0]:
        for filename, caption in [
            ("price_with_trading_actions.png", "Close price with Buy/Sell/Hold action markers."),
            ("portfolio_value_comparison.png", "Portfolio value comparison over the same cached evaluation window."),
            ("action_distribution.png", "Buy/Sell/Hold counts and percentages, with conservative/overtrading warnings in the summary table."),
            ("trade_outcome_win_rate.png", "Trade-level reward/outcome visualization when trading logs contain trade actions."),
            ("drawdown_curve_comparison.png", "Drawdown comparison for available strategies."),
            ("sentiment_action_overlay.png", "Price/actions above daily sentiment and news count."),
        ]:
            with st.expander(caption, expanded=filename in {"price_with_trading_actions.png", "portfolio_value_comparison.png"}):
                _display_behavior_image(symbol, filename, caption)
    with tabs[1]:
        behavior = bundle.get("behavior_summary", pd.DataFrame())
        trades = bundle.get("trade_outcomes", pd.DataFrame())
        if isinstance(behavior, pd.DataFrame) and not behavior.empty:
            st.markdown("#### Action behavior warnings")
            st.dataframe(behavior, use_container_width=True)
        else:
            st.info("No action behavior summary table found yet.")
        if isinstance(trades, pd.DataFrame) and not trades.empty:
            st.markdown("#### Trade outcome / win-rate metrics")
            st.dataframe(trades, use_container_width=True)
        else:
            st.info("No trade outcome table found yet.")
    with tabs[2]:
        _display_behavior_image(symbol, "prediction_vs_actual_direction.png", "Prediction vs actual direction if predict-then-trade outputs exist.")
        st.caption("If this chart is missing, cached predict-then-trade prediction files were not found; this is a warning, not a dashboard error.")


def render_cross_stock_review_page() -> None:
    st.subheader("Cross-Stock Comparison")
    peer_summary = safe_read_csv(SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv")
    peer_diagnostics = safe_read_csv(SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_diagnostics.csv")
    peer_discussion = SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_discussion.md"
    if not peer_summary.empty:
        render_cross_stock_outputs(peer_summary, peer_discussion if peer_discussion.exists() else None)
        if not peer_diagnostics.empty:
            st.markdown("#### Peer NLP diagnostics")
            st.dataframe(peer_diagnostics, use_container_width=True)
        return

    summary = safe_read_csv(SYSTEM_OUTPUT_DIR / "cross_stock_summary.csv")
    diagnostics = safe_read_csv(SYSTEM_OUTPUT_DIR / "cross_stock_diagnostics.csv")
    high_summary = safe_read_csv(SYSTEM_OUTPUT_DIR / "cross_stock_high_density_summary.csv")
    high_diagnostics = safe_read_csv(SYSTEM_OUTPUT_DIR / "cross_stock_high_density_diagnostics.csv")
    high_discussion = SYSTEM_OUTPUT_DIR / "cross_stock_high_density_discussion.md"
    if not high_summary.empty:
        st.markdown("#### Coverage-controlled high-density comparison")
        st.dataframe(high_summary, use_container_width=True)
        cols = [col for col in ["nlp_basic_final_equity_effect", "nlp_enhanced_final_equity_effect", "nlp_basic_sharpe_effect"] if col in high_summary.columns]
        if cols:
            render_multi_series(high_summary.set_index("symbol")[cols], kind="bar", height=360)
        if {"high_density_coverage_ratio", "nlp_basic_final_equity_effect"}.issubset(high_summary.columns):
            render_scatter(high_summary, "high_density_coverage_ratio", "nlp_basic_final_equity_effect", category="symbol", title="Coverage vs NLP final equity effect", height=340)
        if not high_diagnostics.empty:
            with st.expander("High-density diagnostics", expanded=False):
                st.dataframe(high_diagnostics, use_container_width=True)
        if high_discussion.exists():
            with st.expander("High-density discussion", expanded=False):
                st.markdown(high_discussion.read_text(encoding="utf-8"))
    discussion = SYSTEM_OUTPUT_DIR / "cross_stock_discussion.md"
    if summary.empty:
        if high_summary.empty:
            st.warning("Cross-stock summary not found. Run optional cross-stock robustness analysis first.")
        return
    st.markdown("#### Full-period/common-window robustness check")
    render_cross_stock_outputs(summary, discussion if discussion.exists() else None)
    if not diagnostics.empty:
        st.markdown("#### Cross-stock diagnostics")
        st.dataframe(diagnostics, use_container_width=True)
    st.warning("The content below is legacy stock-level/full-period robustness. It is not the latest official peer NLP result.")
    if {"symbol", "market_regime", "conclusion_label"}.issubset(summary.columns):
        compact = summary[["symbol", "market_regime", "conclusion_label", "comparability_status"]].copy()
        compact["best_strategy"] = summary[[col for col in ["buy_and_hold_final_equity", "dqn_without_nlp_final_equity", "dqn_with_nlp_final_equity"] if col in summary.columns]].idxmax(axis=1).str.replace("_final_equity", "", regex=False)
        st.dataframe(compact, use_container_width=True)


def render_feasibility_page() -> None:
    st.subheader("Result Reliability and Data Quality")
    report_dir = SYSTEM_OUTPUT_DIR / "reports"
    for label, path in [
        ("Feasibility audit", report_dir / "feasibility_audit.csv"),
        ("Missing file report", report_dir / "missing_file_report.csv"),
        ("Data quality diagnostics", report_dir / "data_quality_diagnostics.csv"),
        ("Cross-stock feasibility summary", report_dir / "cross_stock_feasibility_summary.csv"),
        ("Cross-stock diagnostics", SYSTEM_OUTPUT_DIR / "cross_stock_diagnostics.csv"),
        ("Peer NLP cross-stock diagnostics", SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_diagnostics.csv"),
        ("Peer NLP integrity", PROJECT_ROOT / "reports" / "tables" / "peer_nlp_integrity_check.csv"),
        ("Peer NLP corpus summary", PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv"),
        ("Stock sector mapping", PROJECT_ROOT / "reports" / "tables" / "stock_sector_mapping.csv"),
        ("Information-density diagnostics", PROJECT_ROOT / "reports" / "tables" / "information_density_diagnostics.csv"),
        ("Experiment window summary", PROJECT_ROOT / "reports" / "tables" / "experiment_window_summary.csv"),
        ("High-density cross-stock diagnostics", SYSTEM_OUTPUT_DIR / "cross_stock_high_density_diagnostics.csv"),
    ]:
        st.markdown(f"#### {label}")
        frame = safe_read_csv(path)
        if frame.empty:
            st.warning(f"{path.relative_to(PROJECT_ROOT)} is missing or empty.")
        else:
            st.dataframe(frame, use_container_width=True)
    md = report_dir / "feasibility_audit.md"
    if md.exists():
        with st.expander("Feasibility audit markdown", expanded=False):
            st.markdown(md.read_text(encoding="utf-8"))


def render_submission_readiness_page() -> None:
    st.subheader("Report and Submission Readiness")
    checks = [
        ("final_report.md", PROJECT_ROOT / "reports" / "final_report.md"),
        ("final_report_figure_index.csv", PROJECT_ROOT / "reports" / "final_report_figure_index.csv"),
        ("final_report_table_index.csv", PROJECT_ROOT / "reports" / "final_report_table_index.csv"),
        ("presentation_outline.md", PROJECT_ROOT / "reports" / "presentation_outline.md"),
        ("live_demo_script.md", PROJECT_ROOT / "reports" / "live_demo_script.md"),
        ("github_submission_checklist.md", PROJECT_ROOT / "reports" / "github_submission_checklist.md"),
        ("README.md", PROJECT_ROOT / "README.md"),
        ("requirements.txt", PROJECT_ROOT / "requirements.txt"),
        ("peer_nlp_cross_stock_summary.csv", SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv"),
        ("peer_nlp_effect_summary.csv", PROJECT_ROOT / "reports" / "tables" / "peer_nlp_effect_summary.csv"),
        ("peer_nlp_integrity_check.csv", PROJECT_ROOT / "reports" / "tables" / "peer_nlp_integrity_check.csv"),
        ("nlp_gold_label_evaluation.csv", PROJECT_ROOT / "reports" / "tables" / "nlp_gold_label_evaluation.csv"),
        ("sqlite_demo.md", PROJECT_ROOT / "reports" / "sqlite_demo.md"),
    ]
    rows = [{"deliverable": name, "status": status_from_file(path), "path": str(path.relative_to(PROJECT_ROOT))} for name, path in checks]
    render_status_badges(rows)
    missing = [row for row in rows if row["status"] != "Complete"]
    if missing:
        st.warning("Missing deliverables remain.")
        st.dataframe(pd.DataFrame(missing), use_container_width=True, hide_index=True)
    else:
        st.success("All tracked final-submission files are present.")
    file_index = output_file_index()
    st.markdown("#### Output file index")
    st.dataframe(file_index, use_container_width=True)


def render_file_explorer_page() -> None:
    st.subheader("File Browser / Output Explorer")
    index = output_file_index()
    if index.empty:
        st.warning("No report/output files found.")
        return
    filtered = index.copy()
    st.dataframe(filtered, use_container_width=True)
    selected = st.selectbox("Preview file", filtered["path"].tolist())
    path = PROJECT_ROOT / selected
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            st.dataframe(safe_read_csv(path), use_container_width=True)
        elif suffix == ".md":
            st.markdown(path.read_text(encoding="utf-8"))
        elif suffix == ".json":
            st.json(json.loads(path.read_text(encoding="utf-8")))
        elif suffix in {".png", ".svg"}:
            st.image(str(path))
        else:
            st.info(f"Preview is not supported for `{suffix}`.")
    except Exception as exc:
        st.warning(f"Could not preview {selected}: {exc}")


def render_single_stock_basic_page(symbol: str, bundle: dict[str, object]) -> None:
    """Focused single-stock view: cached data health, close price, and net flow."""
    if not bundle.get("available"):
        st.warning(str(bundle.get("reason", "No outputs available.")))
        return

    data = bundle.get("data", pd.DataFrame())
    if not isinstance(data, pd.DataFrame) or data.empty:
        st.warning("No integrated market/news CSV is available for this stock.")
        return

    classification = render_target_classification(symbol, bundle)
    code = normalize_symbol_for_path(symbol)
    company_name = str(bundle.get("company_name", "") or "").strip() or str(classification.get("company_name", "-"))
    sector = str(classification.get("sector", "-"))
    industry = str(classification.get("industry", "-"))
    data = data.copy()
    data["date"] = pd.to_datetime(data.get("date"), errors="coerce")
    market_rows = int(pd.to_numeric(data.get("close", pd.Series(dtype=float)), errors="coerce").notna().sum())
    event_count = pd.to_numeric(data.get("event_count", pd.Series([0] * len(data), index=data.index)), errors="coerce").fillna(0)
    news_rows = int(event_count.sum())
    data_start = data["date"].min()
    data_end = data["date"].max()

    selected_csv = bundle.get("selected_csv")
    summary = pd.DataFrame(
        [
            {
                "symbol": code,
                "ticker": str(data.get("symbol", pd.Series([code])).dropna().iloc[0]) if "symbol" in data.columns and data["symbol"].notna().any() else code,
                "company_name": company_name,
                "sector": sector,
                "industry": industry,
                "market_rows": market_rows,
                "news_event_rows": news_rows,
                "local_data_start": data_start.date().isoformat() if pd.notna(data_start) else "-",
                "local_data_end": data_end.date().isoformat() if pd.notna(data_end) else "-",
                "selected_csv": str(Path(selected_csv).relative_to(PROJECT_ROOT)) if selected_csv and Path(selected_csv).exists() else "-",
            }
        ]
    )
    st.subheader("Single-Stock Basic Data")
    st.caption("这里只展示该 target 股票本身的基础缓存、价格走势和资金流入流出 proxy，用来支撑后续 peer cross analysis。")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    metric_cols = st.columns(5)
    metric_cols[0].metric("Company", company_name)
    metric_cols[1].metric("Sector", sector)
    metric_cols[2].metric("Market rows", market_rows)
    metric_cols[3].metric("News/event rows", news_rows)
    metric_cols[4].metric("Date range", f"{summary['local_data_start'].iloc[0]} -> {summary['local_data_end'].iloc[0]}")

    st.markdown("#### Close Price")
    if "close" in data.columns:
        render_series_line(data, "date", "close", title=f"{code} close price", color=PALETTE["ink"], height=340)
        st.caption("这张图只反映价格路径，用来确认研究区间、上市/停牌空窗和整体市场方向。它不直接等同于新闻情绪。")
    else:
        st.warning("`close` column is missing from the integrated CSV.")

    st.markdown("#### Daily Net Inflow / Outflow")
    daily_net_flow = bundle.get("daily_net_flow", pd.DataFrame())
    if not isinstance(daily_net_flow, pd.DataFrame) or daily_net_flow.empty:
        daily_net_flow = compute_daily_net_flow(data)
    if isinstance(daily_net_flow, pd.DataFrame) and not daily_net_flow.empty:
        daily_net_flow = daily_net_flow.copy()
        daily_net_flow["date"] = pd.to_datetime(daily_net_flow.get("date"), errors="coerce")
        flow_col = "net_flow_cny_million" if "net_flow_cny_million" in daily_net_flow.columns else "net_flow" if "net_flow" in daily_net_flow.columns else None
        if flow_col:
            daily_net_flow[flow_col] = pd.to_numeric(daily_net_flow[flow_col], errors="coerce")
            non_zero = daily_net_flow[flow_col].fillna(0).abs().sum()
            if non_zero == 0:
                st.warning("当前缓存没有可用的真实资金流字段，OHLCV proxy 也为 0；这通常是数据源没有返回净流入/成交额字段。")
            render_series_bar(daily_net_flow, "date", flow_col, title="Daily net inflow / outflow", color_by_sign=True, height=340)
            st.caption("正值表示净流入 proxy，负值表示净流出 proxy。若数据源没有真实资金流，本项目只用 OHLCV/turnover proxy 作解释，不把它当作 DQN 的预测特征。")
        else:
            st.warning("No net-flow compatible column was found.")
    else:
        st.warning("No rows are available to draw net inflow/outflow.")


def render_peer_cross_analysis_for_symbol(symbol: str, bundle: dict[str, object]) -> None:
    """Latest model-upgrade summary page for one held-out target stock."""
    code = normalize_symbol_for_path(symbol)
    st.subheader("Peer Cross Analysis")
    st.caption("Latest Model-Upgrade DQN Groups: no-NLP vs sentiment/impact NLP groups using the model-upgrade diagnostics.")
    classification = render_target_classification(symbol, bundle)
    st.caption(
        f"Target `{classification['symbol']}` belongs to sector `{classification['sector']}` "
        f"and industry `{classification['industry']}`. The target-stock exclusion research principle remains unchanged."
    )
    _, frames = _target_model_upgrade_frames(code)
    if not _has_model_upgrade_rows(frames):
        st.info(_target_company_command(code, bundle))
        return

    source = frames["seed_level_metrics.csv"] if not frames["seed_level_metrics.csv"].empty else frames["env_dynamics_diagnostics.csv"]
    filters = _upgrade_filter_controls(source, "peer_upgrade", include_experiment=True)
    summary = _apply_upgrade_filters(frames["model_upgrade_summary.csv"], filters)
    seed_metrics = _apply_upgrade_filters(frames["seed_level_metrics.csv"], filters)
    portfolio = _apply_upgrade_filters(frames["portfolio_curves.csv"], filters)
    actions = _apply_upgrade_filters(frames["action_distribution_diagnostics.csv"], filters)
    env = _apply_upgrade_filters(frames["env_dynamics_diagnostics.csv"], filters)

    experiment_table = pd.DataFrame(
        [
            {"experiment": "dqn_without_nlp", "label": "No NLP DQN", "meaning": "Model-upgrade baseline using market/trading state without NLP signal."},
            {"experiment": "dqn_with_sector_sentiment_nlp", "label": "Sector Sentiment", "meaning": "Peer-trained sector sentiment signal added to the DQN state."},
            {"experiment": "dqn_with_sector_impact_nlp", "label": "Sector Impact", "meaning": "Peer-trained sector market-impact signal added to the DQN state."},
            {"experiment": "dqn_with_marketwide_sentiment_nlp", "label": "Marketwide Sentiment", "meaning": "Cross-sector marketwide sentiment signal added when available."},
            {"experiment": "dqn_with_marketwide_impact_nlp", "label": "Marketwide Impact", "meaning": "Cross-sector marketwide impact signal added when available."},
        ]
    )
    st.dataframe(experiment_table, use_container_width=True, hide_index=True)

    st.markdown("#### Latest Model-Upgrade DQN Groups")
    _render_upgrade_metric_comparison(seed_metrics, summary, title="Latest model-upgrade DQN groups")
    st.info("Buy-and-hold benchmark is shown only when a compatible model-upgrade benchmark curve is available.")

    st.markdown("#### Model-Upgrade Portfolio Curves")
    _render_upgrade_portfolio_curves(portfolio, env, title="Latest Model-Upgrade Portfolio Curves")

    st.markdown("#### Model-Upgrade Buy / Sell / Hold Counts")
    _render_upgrade_action_distribution(actions, env, title="Latest Model-Upgrade Buy / Sell / Hold counts")

    st.markdown("#### Model-Upgrade NLP Effect Summary")
    nlp_effect = _apply_upgrade_filters(frames["nlp_incremental_effect.csv"], filters)
    if nlp_effect.empty:
        st.info("NLP incremental effect diagnostics are not available. Run paired NLP effect analysis before claiming NLP improvement.")
    else:
        st.dataframe(nlp_effect, use_container_width=True, hide_index=True)


def render_market_impact_analysis_for_symbol(symbol: str, bundle: dict[str, object]) -> None:
    """Latest model-upgrade sentiment/impact comparison for the selected target stock."""
    code = normalize_symbol_for_path(symbol)
    st.subheader("Latest Model-Upgrade Sentiment / Impact Comparison")
    st.caption("Compares no-NLP, sentiment NLP, and market-impact NLP DQN groups using the latest model-upgrade diagnostics.")
    _, frames = _target_model_upgrade_frames(code)
    if not _has_model_upgrade_rows(frames):
        st.info(_target_company_command(code, bundle))
        return

    source = frames["seed_level_metrics.csv"] if not frames["seed_level_metrics.csv"].empty else frames["env_dynamics_diagnostics.csv"]
    filters = _upgrade_filter_controls(source, "impact_upgrade", include_experiment=True)
    summary = _apply_upgrade_filters(frames["model_upgrade_summary.csv"], filters)
    seed_metrics = _apply_upgrade_filters(frames["seed_level_metrics.csv"], filters)
    portfolio = _apply_upgrade_filters(frames["portfolio_curves.csv"], filters)
    actions = _apply_upgrade_filters(frames["action_distribution_diagnostics.csv"], filters)
    env = _apply_upgrade_filters(frames["env_dynamics_diagnostics.csv"], filters)
    q_values = _apply_upgrade_filters(frames["q_value_diagnostics.csv"], filters)
    reward_compare = _apply_upgrade_filters(frames["reward_variant_comparison.csv"], filters)
    state_features = _apply_upgrade_filters(frames["state_feature_diagnostics.csv"], filters)

    st.markdown("#### Latest sentiment / impact DQN comparison")
    _render_upgrade_metric_comparison(seed_metrics, summary, title="Latest Model-Upgrade Sentiment / Impact Comparison")

    st.markdown("#### Latest Model-Upgrade Portfolio Curves")
    _render_upgrade_portfolio_curves(portfolio, env, title="Latest Model-Upgrade Portfolio Curves")

    st.markdown("#### Latest Model-Upgrade Action Distribution")
    _render_upgrade_action_distribution(actions, env, title="Latest Model-Upgrade Action Distribution")

    st.markdown("#### Q-value diagnostics")
    if q_values.empty:
        st.warning("Q-value diagnostics not found. Q-value decision view is unavailable.")
    else:
        q_plot = q_values.copy()
        q_plot["selected_q_value"] = pd.to_numeric(q_plot.get("selected_q_value"), errors="coerce")
        q_plot["q_value_margin"] = pd.to_numeric(q_plot.get("q_value_margin"), errors="coerce")
        q_summary = q_plot.groupby("experiment")[["selected_q_value", "q_value_margin"]].agg(["mean", "std", "min", "max"])
        q_summary.columns = ["_".join(col).strip("_") for col in q_summary.columns]
        q_summary = q_summary.reset_index()
        q_summary["experiment"] = q_summary["experiment"].astype(str).map(_display_experiment_name).fillna(q_summary["experiment"].astype(str))
        st.dataframe(q_summary, use_container_width=True, hide_index=True)
        if "raw_action_id" in q_plot.columns:
            raw_counts = q_plot.groupby(["raw_action_id", "experiment"]).size().unstack(fill_value=0)
            raw_counts = raw_counts.rename(columns=_display_experiment_name)
            render_multi_series(raw_counts, title="Selected raw action distribution", kind="bar", color_map=_display_experiment_color_map(), height=300)

    st.markdown("#### State feature diagnostics")
    if state_features.empty:
        st.info("state_feature_diagnostics.csv is missing for the selected model-upgrade rows.")
    else:
        st.caption("The latest model-upgrade experiment uses normalized_plus features instead of the old official raw 8D state interface when configured that way.")
        st.dataframe(state_features.head(500), use_container_width=True, hide_index=True)

    st.markdown("#### Reward variant comparison")
    if reward_compare.empty:
        st.info("reward_variant_comparison.csv is missing for the selected model-upgrade rows.")
    else:
        st.dataframe(reward_compare, use_container_width=True, hide_index=True)
        metric_cols = [col for col in ["mean_final_equity", "mean_sharpe_ratio", "mean_max_drawdown", "mean_number_of_trades", "mean_exposure_ratio"] if col in reward_compare.columns]
        if "reward_variant" in reward_compare.columns and metric_cols:
            chart = reward_compare.groupby("reward_variant")[metric_cols].mean(numeric_only=True)
            render_multi_series(chart, title="one_day_return vs three_day_return", kind="bar", height=320)


def _diagnostic_sort(frame: pd.DataFrame) -> pd.DataFrame:
    """Sort trade diagnostics by date when available, then by step."""
    if frame.empty:
        return frame
    out = frame.copy()
    sort_cols: list[str] = []
    helper_cols: list[str] = []
    if "date" in out.columns and out["date"].notna().any():
        helper = "_diagnostic_sort_date"
        out[helper] = pd.to_datetime(out["date"], errors="coerce")
        sort_cols.append(helper)
        helper_cols.append(helper)
    if "step" in out.columns and out["step"].notna().any():
        helper = "_diagnostic_sort_step"
        out[helper] = pd.to_numeric(out["step"], errors="coerce")
        sort_cols.append(helper)
        helper_cols.append(helper)
    if sort_cols:
        out = out.sort_values(sort_cols, kind="mergesort")
    return out.drop(columns=helper_cols, errors="ignore")


def _diagnostic_x_column(frame: pd.DataFrame) -> str:
    if "date" in frame.columns and frame["date"].notna().any():
        return "date"
    if "step" in frame.columns:
        return "step"
    return frame.index.name or "index"


def _diagnostic_options(frame: pd.DataFrame, column: str, preferred: list[str] | None = None) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    values = [str(value) for value in frame[column].dropna().unique().tolist()]
    preferred = preferred or []
    ordered = [value for value in preferred if value in values]
    ordered.extend(sorted(value for value in values if value not in ordered))
    return ordered


def _safe_metric_value(value: object, *, percent: bool = False, money: bool = False) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "N/A"
    if percent:
        return f"{float(number):.2%}"
    if money:
        return f"{float(number):,.2f}"
    return f"{float(number):.4f}"


def _portfolio_after_column(frame: pd.DataFrame) -> str | None:
    for column in ["portfolio_after", "portfolio_after_trade", "portfolio_value"]:
        if column in frame.columns:
            return column
    return None


def _filter_diagnostic_frame(frame: pd.DataFrame, selections: dict[str, str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for column, value in selections.items():
        if not value or value == "all" or column not in out.columns:
            continue
        out = out[out[column].astype(str) == str(value)]
    return out


def _merge_q_value_diagnostics(env_frame: pd.DataFrame, q_values: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    if env_frame.empty or q_values.empty:
        return env_frame.copy(), 0, 0
    required_keys = ["run_id", "period", "episode", "step"]
    if not all(key in env_frame.columns and key in q_values.columns for key in required_keys):
        return env_frame.copy(), 0, len(env_frame)
    q_cols = required_keys + [
        col
        for col in [
            "raw_action_id",
            "mapped_action_label",
            "q_hold_or_action0",
            "q_action1",
            "q_action2",
            "q_action3",
            "q_action4",
            "selected_q_value",
            "q_value_margin",
        ]
        if col in q_values.columns
    ]
    q_subset = q_values[q_cols].drop_duplicates(required_keys, keep="last")
    merged = env_frame.merge(q_subset, on=required_keys, how="left", suffixes=("", "_q"))
    dropped_rows = max(0, len(env_frame) - len(merged))
    missing_q_matches = int(merged["selected_q_value"].isna().sum()) if "selected_q_value" in merged.columns else len(merged)
    return merged, dropped_rows, missing_q_matches


def _trade_hover_columns(frame: pd.DataFrame) -> list[str]:
    requested = [
        "date",
        "step",
        "raw_action_id",
        "mapped_action_label",
        "action_mode",
        "target_exposure",
        "current_exposure_before",
        "current_exposure_after",
        "price",
        "cash_before",
        "cash_after",
        "shares_before",
        "shares_after",
        "portfolio_before",
        "portfolio_after",
        "reward",
        "transaction_cost",
    ]
    return [column for column in requested if column in frame.columns]


def _add_action_markers(fig: go.Figure, frame: pd.DataFrame, *, x_col: str, y_col: str) -> None:
    if frame.empty or x_col not in frame.columns or y_col not in frame.columns:
        return
    label_col = "mapped_action_label" if "mapped_action_label" in frame.columns else "action"
    hover_cols = _trade_hover_columns(frame)
    action_styles = {
        "Buy": {"color": PALETTE["green"], "symbol": "triangle-up", "size": 10, "opacity": 0.95},
        "Sell": {"color": PALETTE["rose"], "symbol": "triangle-down", "size": 10, "opacity": 0.95},
        "Hold": {"color": PALETTE["muted"], "symbol": "circle", "size": 6, "opacity": 0.35},
    }
    for label, style in action_styles.items():
        part = frame[frame[label_col].astype(str).str.lower() == label.lower()].copy()
        if part.empty:
            continue
        customdata = part[hover_cols].astype(str).to_numpy() if hover_cols else None
        hovertemplate = "<br>".join([f"{col}: %{{customdata[{idx}]}}" for idx, col in enumerate(hover_cols)])
        if hovertemplate:
            hovertemplate += "<extra></extra>"
        fig.add_trace(
            go.Scatter(
                x=part[x_col],
                y=pd.to_numeric(part[y_col], errors="coerce"),
                mode="markers",
                name=label,
                marker={
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                    "line": {"width": 0.5, "color": PALETTE["white"]},
                },
                customdata=customdata,
                hovertemplate=hovertemplate or None,
            )
        )


def _render_trade_summary_cards(frame: pd.DataFrame, benchmark_curve: pd.DataFrame) -> None:
    sorted_frame = _diagnostic_sort(frame)
    label_col = "mapped_action_label" if "mapped_action_label" in sorted_frame.columns else "action"
    labels = sorted_frame[label_col].astype(str) if label_col in sorted_frame.columns else pd.Series(dtype=str)
    exposure = pd.to_numeric(sorted_frame.get("current_exposure_after"), errors="coerce") if "current_exposure_after" in sorted_frame.columns else pd.Series(dtype=float)
    portfolio_col = _portfolio_after_column(sorted_frame)
    portfolio = pd.to_numeric(sorted_frame[portfolio_col], errors="coerce") if portfolio_col else pd.Series(dtype=float)
    reward = pd.to_numeric(sorted_frame.get("reward"), errors="coerce") if "reward" in sorted_frame.columns else pd.Series(dtype=float)
    transaction_cost = pd.to_numeric(sorted_frame.get("transaction_cost"), errors="coerce") if "transaction_cost" in sorted_frame.columns else pd.Series(dtype=float)
    start_value = pd.to_numeric(sorted_frame.get("portfolio_before"), errors="coerce").dropna()
    start_value = start_value.iloc[0] if not start_value.empty else (portfolio.dropna().iloc[0] if not portfolio.dropna().empty else np.nan)
    final_value = portfolio.dropna().iloc[-1] if not portfolio.dropna().empty else np.nan
    total_return = (final_value / start_value - 1) if pd.notna(start_value) and start_value else np.nan
    unique_portfolio_values = int(portfolio.round(6).nunique()) if not portfolio.dropna().empty else "N/A"
    hold_ratio = float((labels.str.lower() == "hold").mean()) if not labels.empty else np.nan
    q_margin = pd.to_numeric(sorted_frame.get("q_value_margin"), errors="coerce") if "q_value_margin" in sorted_frame.columns else pd.Series(dtype=float)
    benchmark_col = _portfolio_after_column(benchmark_curve) if not benchmark_curve.empty else None
    benchmark_portfolio = pd.to_numeric(benchmark_curve[benchmark_col], errors="coerce").dropna() if benchmark_col else pd.Series(dtype=float)
    benchmark_final = benchmark_portfolio.iloc[-1] if not benchmark_portfolio.empty else np.nan
    benchmark_diff = final_value - benchmark_final if pd.notna(final_value) and pd.notna(benchmark_final) else np.nan

    metrics = [
        ("Buy count", int((labels.str.lower() == "buy").sum()) if not labels.empty else "N/A"),
        ("Sell count", int((labels.str.lower() == "sell").sum()) if not labels.empty else "N/A"),
        ("Hold count", int((labels.str.lower() == "hold").sum()) if not labels.empty else "N/A"),
        ("Average exposure", _safe_metric_value(exposure.mean(), percent=True)),
        ("Maximum exposure", _safe_metric_value(exposure.max(), percent=True)),
        ("Minimum exposure", _safe_metric_value(exposure.min(), percent=True)),
        ("Final portfolio value", _safe_metric_value(final_value, money=True)),
        ("Total return", _safe_metric_value(total_return, percent=True)),
        ("Average reward", _safe_metric_value(reward.mean())),
        ("Total transaction cost", _safe_metric_value(transaction_cost.sum(), money=True)),
        ("Unique portfolio values", unique_portfolio_values),
        ("Hold ratio", _safe_metric_value(hold_ratio, percent=True)),
        ("Average Q-value margin", _safe_metric_value(q_margin.mean())),
        ("DQN final value", _safe_metric_value(final_value, money=True)),
        ("Buy-and-hold final value", _safe_metric_value(benchmark_final, money=True)),
        ("DQN minus benchmark", _safe_metric_value(benchmark_diff, money=True)),
    ]
    for offset in range(0, len(metrics), 5):
        cols = st.columns(5)
        for col, (label, value) in zip(cols, metrics[offset : offset + 5]):
            col.metric(label, value)


def _render_portfolio_actions_chart(frame: pd.DataFrame, portfolio_curves: pd.DataFrame) -> None:
    value_col = _portfolio_after_column(frame)
    if not value_col:
        st.warning("Portfolio value columns are missing for the selected run.")
        return
    sorted_frame = _diagnostic_sort(frame)
    x_col = _diagnostic_x_column(sorted_frame)
    if x_col not in sorted_frame.columns:
        sorted_frame = sorted_frame.reset_index().rename(columns={"index": "index"})
        x_col = "index"
    fig = go.Figure()
    hover_cols = [col for col in ["date", "step", "portfolio_before", "portfolio_after", "reward", "transaction_cost"] if col in sorted_frame.columns]
    fig.add_trace(
        go.Scatter(
            x=sorted_frame[x_col],
            y=pd.to_numeric(sorted_frame[value_col], errors="coerce"),
            mode="lines",
            name=value_col,
            line={"color": PALETTE["ink"], "width": 2.6},
            customdata=sorted_frame[hover_cols].astype(str).to_numpy() if hover_cols else None,
            hovertemplate=("<br>".join([f"{col}: %{{customdata[{idx}]}}" for idx, col in enumerate(hover_cols)]) + "<extra></extra>") if hover_cols else None,
        )
    )
    _add_action_markers(fig, sorted_frame, x_col=x_col, y_col=value_col)

    benchmark_available = False
    if not portfolio_curves.empty:
        benchmark_col = _portfolio_after_column(portfolio_curves)
        if benchmark_col:
            benchmark_rows = _diagnostic_sort(portfolio_curves)
            benchmark_x = _diagnostic_x_column(benchmark_rows)
            if benchmark_x in benchmark_rows.columns:
                fig.add_trace(
                    go.Scatter(
                        x=benchmark_rows[benchmark_x],
                        y=pd.to_numeric(benchmark_rows[benchmark_col], errors="coerce"),
                        mode="lines",
                        name="buy_and_hold",
                        line={"color": PALETTE["muted"], "width": 2, "dash": "dash"},
                    )
                )
                benchmark_available = True
    fig.update_layout(**_base_layout(title="Portfolio Value with DQN Trade Actions", height=430))
    _render_plot(fig)
    if not benchmark_available:
        st.info("Benchmark curve not available for the selected run.")


def _render_exposure_actions_chart(frame: pd.DataFrame) -> None:
    sorted_frame = _diagnostic_sort(frame)
    x_col = _diagnostic_x_column(sorted_frame)
    if x_col not in sorted_frame.columns:
        sorted_frame = sorted_frame.reset_index().rename(columns={"index": "index"})
        x_col = "index"
    exposure_cols = [col for col in ["target_exposure", "current_exposure_before", "current_exposure_after", "position_ratio_after"] if col in sorted_frame.columns]
    if not exposure_cols:
        st.warning("Exposure columns are missing for the selected run.")
        return
    fig = go.Figure()
    colors = {
        "current_exposure_before": PALETTE["muted"],
        "current_exposure_after": PALETTE["teal"],
        "target_exposure": PALETTE["purple"],
        "position_ratio_after": PALETTE["blue"],
    }
    for col in exposure_cols:
        fig.add_trace(
            go.Scatter(
                x=sorted_frame[x_col],
                y=pd.to_numeric(sorted_frame[col], errors="coerce"),
                mode="lines",
                name=col,
                line={"color": colors.get(col, PALETTE["blue"]), "width": 2},
            )
        )
    marker_y = "current_exposure_after" if "current_exposure_after" in sorted_frame.columns else exposure_cols[0]
    _add_action_markers(fig, sorted_frame, x_col=x_col, y_col=marker_y)
    fig.update_layout(**_base_layout(title="Target Exposure and Actual Exposure Over Time", height=410))
    fig.update_yaxes(tickformat=".0%")
    _render_plot(fig)


def _render_q_value_decision_chart(frame: pd.DataFrame, action_mode: str) -> None:
    if frame.empty or "selected_q_value" not in frame.columns:
        st.warning("Q-value diagnostics not found. Q-value decision view is unavailable.")
        return
    sorted_frame = _diagnostic_sort(frame)
    x_col = _diagnostic_x_column(sorted_frame)
    if x_col not in sorted_frame.columns:
        sorted_frame = sorted_frame.reset_index().rename(columns={"index": "index"})
        x_col = "index"
    q_cols = ["q_hold_or_action0", "q_action1", "q_action2"]
    if action_mode == "target_position":
        q_cols.extend(["q_action3", "q_action4"])
    q_cols = [col for col in q_cols if col in sorted_frame.columns]
    if not q_cols:
        st.warning("Q-value action columns are unavailable for the selected run.")
        return
    fig = go.Figure()
    for index, col in enumerate(q_cols):
        fig.add_trace(
            go.Scatter(
                x=sorted_frame[x_col],
                y=pd.to_numeric(sorted_frame[col], errors="coerce"),
                mode="lines",
                name=col,
                line={"color": SERIES_COLORS[index % len(SERIES_COLORS)], "width": 1.5},
                opacity=0.75,
            )
        )
    hover_cols = [col for col in ["date", "step", "raw_action_id", "mapped_action_label", "selected_q_value", "q_value_margin"] if col in sorted_frame.columns]
    fig.add_trace(
        go.Scatter(
            x=sorted_frame[x_col],
            y=pd.to_numeric(sorted_frame["selected_q_value"], errors="coerce"),
            mode="lines+markers",
            name="selected_q_value",
            line={"color": "#000000", "width": 4},
            marker={"color": "#000000", "size": 5},
            customdata=sorted_frame[hover_cols].astype(str).to_numpy() if hover_cols else None,
            hovertemplate=("<br>".join([f"{col}: %{{customdata[{idx}]}}" for idx, col in enumerate(hover_cols)]) + "<extra></extra>") if hover_cols else None,
        )
    )
    if "q_value_margin" in sorted_frame.columns:
        fig.add_trace(
            go.Scatter(
                x=sorted_frame[x_col],
                y=pd.to_numeric(sorted_frame["q_value_margin"], errors="coerce"),
                mode="lines",
                name="q_value_margin",
                line={"color": PALETTE["muted"], "width": 2, "dash": "dot"},
            )
        )
    fig.update_layout(**_base_layout(title="Q-Values Behind Each DQN Decision", height=430))
    _render_plot(fig)
    st.caption(
        "In target_position mode, action0 to action4 correspond to target exposure 0%, 25%, 50%, 75%, 100%. "
        "The selected action is the action with the highest Q-value. q_value_margin measures the gap between the best and second-best action."
    )


def _render_reward_breakdown_chart(reward_diagnostics: pd.DataFrame, selections: dict[str, str]) -> None:
    if reward_diagnostics.empty:
        st.warning("Reward diagnostics not found. Run the model-upgrade diagnostic script with reward logging enabled.")
        return
    reward_frame = _filter_diagnostic_frame(reward_diagnostics, selections)
    reward_frame = _diagnostic_sort(reward_frame)
    reward_cols = [
        col
        for col in [
            "portfolio_return",
            "benchmark_return",
            "base_reward",
            "final_reward",
            "transaction_cost",
            "flat_penalty_applied",
            "trade_bonus_applied",
            "reward",
        ]
        if col in reward_frame.columns
    ]
    if reward_frame.empty or not reward_cols:
        st.warning("Reward diagnostics do not contain chart-ready reward columns for the selected run.")
        return
    x_col = _diagnostic_x_column(reward_frame)
    if x_col not in reward_frame.columns:
        reward_frame = reward_frame.reset_index().rename(columns={"index": "index"})
        x_col = "index"
    fig = go.Figure()
    for index, col in enumerate(reward_cols):
        fig.add_trace(
            go.Scatter(
                x=reward_frame[x_col],
                y=pd.to_numeric(reward_frame[col], errors="coerce"),
                mode="lines",
                name=col,
                line={"color": SERIES_COLORS[index % len(SERIES_COLORS)], "width": 2},
            )
        )
    fig.update_layout(**_base_layout(title="Reward Breakdown by Trading Step", height=410))
    _render_plot(fig)


def _render_trade_table(frame: pd.DataFrame, selections: dict[str, str]) -> None:
    with st.expander("Detailed Trade Log", expanded=False):
        action_filter = st.selectbox("Trade table action filter", ["all", "Buy", "Sell", "Hold"], index=0, key="trade_behavior_table_action")
        table = _diagnostic_sort(frame)
        label_col = "mapped_action_label" if "mapped_action_label" in table.columns else "action"
        if action_filter != "all" and label_col in table.columns:
            table = table[table[label_col].astype(str).str.lower() == action_filter.lower()]
        display_cols = [
            col
            for col in [
                "target_symbol",
                "target_company",
                "run_id",
                "run_mode",
                "experiment",
                "model_variant",
                "reward_variant",
                "state_feature_mode",
                "seed",
                "period",
                "episode",
                "date",
                "step",
                "raw_action_id",
                "mapped_action_label",
                "action",
                "action_mode",
                "target_exposure",
                "current_exposure_before",
                "current_exposure_after",
                "price",
                "cash_before",
                "cash_after",
                "shares_before",
                "shares_after",
                "portfolio_before",
                "portfolio_after",
                "portfolio_after_trade",
                "portfolio_value",
                "reward",
                "transaction_cost",
            ]
            if col in table.columns
        ]
        st.dataframe(table[display_cols] if display_cols else table, use_container_width=True, hide_index=True)
        run_id = selections.get("run_id") or "selected_run"
        st.download_button(
            "Download filtered trade log as CSV",
            data=table.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"trade_log_{run_id}_{action_filter}.csv",
            mime="text/csv",
            use_container_width=True,
        )


def render_trade_by_trade_dqn_behavior(
    env_dynamics: pd.DataFrame,
    q_values: pd.DataFrame,
    reward_diagnostics: pd.DataFrame,
    seed_metrics: pd.DataFrame,
    actions: pd.DataFrame,
    portfolio_curves: pd.DataFrame,
    source_bundle: object | None = None,
    selected_target_symbol: str = "",
) -> None:
    st.markdown("### Trade-by-Trade DQN Behavior")
    if env_dynamics.empty:
        st.info("Trade behavior diagnostics not found. Run scripts/run_model_upgrade_grid.py --medium-test first.")
        return

    base = env_dynamics.copy()
    selector_cols = st.columns(4)
    run_modes = _diagnostic_options(base, "run_mode", ["medium_test", "quick_test", "full_grid"])
    selected_run_mode = selector_cols[0].selectbox("Trade run_mode", run_modes, index=0, key="trade_behavior_run_mode") if run_modes else ""
    experiments = _diagnostic_options(_filter_diagnostic_frame(base, {"run_mode": selected_run_mode}), "experiment")
    selected_experiment = selector_cols[1].selectbox("Trade experiment", experiments, index=0, key="trade_behavior_experiment") if experiments else ""
    models = _diagnostic_options(_filter_diagnostic_frame(base, {"run_mode": selected_run_mode, "experiment": selected_experiment}), "model_variant")
    selected_model = selector_cols[2].selectbox("Trade model_variant", models, index=0, key="trade_behavior_model") if models else ""
    rewards = _diagnostic_options(_filter_diagnostic_frame(base, {"run_mode": selected_run_mode, "experiment": selected_experiment, "model_variant": selected_model}), "reward_variant")
    selected_reward = selector_cols[3].selectbox("Trade reward_variant", rewards, index=0, key="trade_behavior_reward") if rewards else ""

    selector_cols = st.columns(5)
    partial = _filter_diagnostic_frame(
        base,
        {
            "run_mode": selected_run_mode,
            "experiment": selected_experiment,
            "model_variant": selected_model,
            "reward_variant": selected_reward,
        },
    )
    states = _diagnostic_options(partial, "state_feature_mode")
    selected_state = selector_cols[0].selectbox("Trade state_feature_mode", states, index=0, key="trade_behavior_state") if states else ""
    action_modes = _diagnostic_options(partial, "action_mode")
    selected_action_mode = selector_cols[1].selectbox("Trade action_mode", action_modes, index=0, key="trade_behavior_action_mode") if action_modes else ""
    seeds = _diagnostic_options(partial, "seed")
    selected_seed = selector_cols[2].selectbox("Trade seed", seeds, index=0, key="trade_behavior_seed") if seeds else ""
    periods = ["all"] + _diagnostic_options(partial, "period")
    selected_period = selector_cols[3].selectbox("Trade period", periods, index=0, key="trade_behavior_period")
    chart_modes = ["Portfolio + Actions", "Exposure + Actions", "Q-values", "Reward Breakdown", "Trade Table"]
    selected_chart_mode = selector_cols[4].selectbox("Trade chart mode", chart_modes, index=0, key="trade_behavior_chart_mode")

    selections = {
        "run_mode": selected_run_mode,
        "experiment": selected_experiment,
        "model_variant": selected_model,
        "reward_variant": selected_reward,
        "state_feature_mode": selected_state,
        "action_mode": selected_action_mode,
        "seed": selected_seed,
        "period": selected_period,
    }
    selected_env = _filter_diagnostic_frame(base, selections)
    if selected_env.empty:
        st.warning("No trade diagnostics match the selected filters.")
        return
    selected_env = _diagnostic_sort(selected_env)
    run_ids = _diagnostic_options(selected_env, "run_id")
    selections["run_id"] = run_ids[0] if run_ids else ""

    selected_q = _filter_diagnostic_frame(q_values, selections) if not q_values.empty else pd.DataFrame()
    merged, dropped_rows, missing_q_matches = _merge_q_value_diagnostics(selected_env, selected_q)
    if dropped_rows:
        st.warning(f"Warning: {dropped_rows} rows were dropped during diagnostic merge.")
    if q_values.empty:
        st.warning("Q-value diagnostics not found. Q-value decision view is unavailable.")

    benchmark_selections = {key: value for key, value in selections.items() if key not in {"experiment", "period", "run_id"}}
    benchmark_curve = _filter_diagnostic_frame(portfolio_curves, benchmark_selections)
    if not benchmark_curve.empty and "experiment" in benchmark_curve.columns:
        benchmark_curve = benchmark_curve[benchmark_curve["experiment"].astype(str) == "buy_and_hold"].copy()

    _render_trade_summary_cards(merged, benchmark_curve)

    if selected_chart_mode == "Portfolio + Actions":
        _render_portfolio_actions_chart(selected_env, benchmark_curve)
    elif selected_chart_mode == "Exposure + Actions":
        _render_exposure_actions_chart(selected_env)
    elif selected_chart_mode == "Q-values":
        _render_q_value_decision_chart(merged, selected_action_mode)
    elif selected_chart_mode == "Reward Breakdown":
        _render_reward_breakdown_chart(reward_diagnostics, selections)
    else:
        _render_trade_table(selected_env, selections)

    with st.expander("Trade Behavior Data Source Debug", expanded=False):
        loaded_rows = []
        if source_bundle is not None:
            for item in getattr(source_bundle, "loaded_files", []):
                loaded_rows.append(
                    {
                        "file_name": item.filename,
                        "path": item.path,
                        "row_count": item.row_count,
                        "detected_target_symbol_values": ", ".join(item.target_symbols),
                    }
                )
            checked = getattr(source_bundle, "checked_paths", [])
            st.markdown("Checked paths")
            st.dataframe(pd.DataFrame({"path": checked}), use_container_width=True, hide_index=True)
        st.markdown("Files loaded")
        st.dataframe(pd.DataFrame(loaded_rows), use_container_width=True, hide_index=True)
        debug_rows = [
            {"item": "selected_target_symbol", "value": selected_target_symbol},
            {"item": "selected_run_id_count", "value": selected_env["run_id"].nunique() if "run_id" in selected_env.columns else 0},
            {"item": "env_dynamics_diagnostics.csv_found", "value": not env_dynamics.empty},
            {"item": "q_value_diagnostics.csv_found", "value": not q_values.empty},
            {"item": "reward_diagnostics.csv_found", "value": not reward_diagnostics.empty},
            {"item": "merge_row_drops", "value": dropped_rows},
            {"item": "q_value_missing_matches", "value": missing_q_matches},
        ]
        required_env_cols = ["run_id", "target_symbol", "experiment", "seed", "period", "step", "mapped_action_label"]
        missing_required = [col for col in required_env_cols if col not in env_dynamics.columns]
        debug_rows.append({"item": "missing_required_env_columns", "value": ", ".join(missing_required) if missing_required else ""})
        st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True)


def render_model_upgrade_diagnostics(symbol: str, stock_bundle: dict[str, object] | None = None) -> None:
    st.subheader("Model Upgrade Diagnostics")
    diagnostics_bundle = discover_model_upgrade_diagnostics()
    if not diagnostics_bundle.found_any:
        st.info("No model upgrade diagnostics found in outputs/model_upgrade or per-stock output folders.")
        with st.expander("Trade Behavior Data Source Debug", expanded=False):
            st.dataframe(pd.DataFrame({"checked_path": diagnostics_bundle.checked_paths}), use_container_width=True, hide_index=True)
        return

    detected_targets = diagnostics_bundle.target_symbols
    current_symbol = normalize_symbol_for_path(symbol)
    if current_symbol not in detected_targets:
        st.info(_target_company_command(current_symbol, stock_bundle or {}))
        with st.expander("Trade Behavior Data Source Debug", expanded=False):
            st.markdown("Checked paths")
            st.dataframe(pd.DataFrame({"checked_path": diagnostics_bundle.checked_paths}), use_container_width=True, hide_index=True)
            st.markdown("Files loaded")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "file_name": item.filename,
                            "path": item.path,
                            "row_count": item.row_count,
                            "detected_target_symbol_values": ", ".join(item.target_symbols),
                        }
                        for item in diagnostics_bundle.loaded_files
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        return
    target_index = detected_targets.index(current_symbol) if current_symbol in detected_targets else 0
    selected_target = st.selectbox("model-upgrade target_symbol", detected_targets, index=target_index, key="model_upgrade_target_symbol")

    def _target_filter(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty or "target_symbol" not in frame.columns:
            return frame.copy()
        return frame[frame["target_symbol"].astype(str).apply(normalize_symbol_for_path) == selected_target].copy()

    summary = _target_filter(diagnostics_bundle.get("model_upgrade_summary.csv"))
    seed_metrics = _target_filter(diagnostics_bundle.get("seed_level_metrics.csv"))
    actions = _target_filter(diagnostics_bundle.get("action_distribution_diagnostics.csv"))
    env_dynamics = _target_filter(diagnostics_bundle.get("env_dynamics_diagnostics.csv"))
    exploration = _target_filter(diagnostics_bundle.get("exploration_diagnostics.csv"))
    portfolio = _target_filter(diagnostics_bundle.get("portfolio_curves.csv"))
    q_values = _target_filter(diagnostics_bundle.get("q_value_diagnostics.csv"))
    reward_diagnostics = _target_filter(diagnostics_bundle.get("reward_diagnostics.csv"))
    reward_compare = _target_filter(diagnostics_bundle.get("reward_variant_comparison.csv"))
    state_features = _target_filter(diagnostics_bundle.get("state_feature_diagnostics.csv"))

    if seed_metrics.empty and summary.empty:
        st.info("No model upgrade seed/summary rows found for this target; trade diagnostics may still be available below.")

    selector_cols = st.columns(6)
    filter_source = seed_metrics if not seed_metrics.empty else env_dynamics
    run_options = sorted(filter_source["run_mode"].dropna().astype(str).unique().tolist()) if "run_mode" in filter_source.columns else []
    action_mode_options = sorted(filter_source["action_mode"].dropna().astype(str).unique().tolist()) if "action_mode" in filter_source.columns else []
    model_options = sorted(filter_source["model_variant"].dropna().astype(str).unique().tolist()) if "model_variant" in filter_source.columns else []
    reward_options = sorted(filter_source["reward_variant"].dropna().astype(str).unique().tolist()) if "reward_variant" in filter_source.columns else []
    state_options = sorted(filter_source["state_feature_mode"].dropna().astype(str).unique().tolist()) if "state_feature_mode" in filter_source.columns else []
    selected_run_mode = selector_cols[0].selectbox("run_mode", run_options, index=0) if run_options else ""
    selected_action_mode = selector_cols[1].selectbox("action_mode", action_mode_options, index=0) if action_mode_options else ""
    selected_model = selector_cols[2].selectbox("model_variant", model_options, index=0) if model_options else ""
    selected_reward = selector_cols[3].selectbox("reward_variant", reward_options, index=0) if reward_options else ""
    selected_state = selector_cols[4].selectbox("state_feature_mode", state_options, index=0) if state_options else ""
    episode_options = sorted(filter_source["episodes"].dropna().astype(str).unique().tolist()) if "episodes" in filter_source.columns else []
    selected_episodes = selector_cols[5].selectbox("episodes", episode_options, index=0) if episode_options else ""

    if selected_run_mode == "quick_test":
        st.warning("quick-test = code validation only")
    elif selected_run_mode == "medium_test":
        st.warning("MEDIUM TEST - PRELIMINARY, NOT FINAL")
    elif selected_run_mode == "full_grid":
        st.info("full-grid = formal comparison")

    def _filter(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        out = frame.copy()
        for column, value in [
            ("run_mode", selected_run_mode),
            ("action_mode", selected_action_mode),
            ("model_variant", selected_model),
            ("reward_variant", selected_reward),
            ("state_feature_mode", selected_state),
            ("episodes", selected_episodes),
        ]:
            if column in out.columns and value:
                out = out[out[column].astype(str) == str(value)]
        return out

    filtered_seed = _filter(seed_metrics)
    filtered_summary = _filter(summary)
    filtered_actions = _filter(actions)
    filtered_env = _filter(env_dynamics)
    filtered_exploration = _filter(exploration)
    filtered_portfolio = _filter(portfolio)
    filtered_q_values = _filter(q_values)

    st.markdown("#### Seed-level final equity")
    _render_box_or_table(filtered_seed, metric="final_equity")
    st.markdown("#### Seed-level Sharpe ratio")
    _render_box_or_table(filtered_seed, metric="sharpe_ratio")

    if not filtered_seed.empty:
        meta_cols = [col for col in ["run_mode", "episodes", "action_mode", "model_variant", "reward_variant", "state_feature_mode", "experiment", "seed", "action_buy_count", "action_sell_count", "action_hold_count", "average_exposure", "unique_portfolio_values"] if col in filtered_seed.columns]
        st.markdown("#### Seed behavior summary")
        st.dataframe(filtered_seed[meta_cols].head(200), use_container_width=True, hide_index=True)

    if not filtered_actions.empty and {"experiment", "hold_ratio", "buy_ratio", "sell_ratio"}.issubset(filtered_actions.columns):
        action_plot = filtered_actions.copy()
        for column in ["hold_count", "buy_count", "sell_count", "total_actions"]:
            action_plot[column] = pd.to_numeric(action_plot.get(column, 0), errors="coerce").fillna(0)
        action_counts = action_plot.groupby("experiment")[["hold_count", "buy_count", "sell_count", "total_actions"]].sum()
        denominator = action_counts["total_actions"].replace(0, np.nan)
        action_frame = pd.DataFrame(
            {
                "hold_ratio": action_counts["hold_count"] / denominator,
                "buy_ratio": action_counts["buy_count"] / denominator,
                "sell_ratio": action_counts["sell_count"] / denominator,
            }
        ).rename(index=_display_experiment_name)
        render_multi_series(action_frame, title="Average action distribution by experiment", kind="bar", height=340)
        if "raw_action_id" in filtered_actions.columns:
            raw = filtered_actions.copy()
            raw["raw_action_id"] = raw["raw_action_id"].astype(str)
            raw["total_actions"] = pd.to_numeric(raw.get("total_actions", 0), errors="coerce")
            raw_frame = raw.groupby("raw_action_id")["total_actions"].sum().to_frame("count")
            render_multi_series(raw_frame, title="Raw target-position action distribution", kind="bar", color_map={"count": PALETTE["blue"]}, height=280)
            st.dataframe(filtered_actions.head(200), use_container_width=True, hide_index=True)

    if not filtered_seed.empty and {"experiment", "exposure_ratio"}.issubset(filtered_seed.columns):
        exposure = filtered_seed.copy()
        exposure["exposure_ratio"] = pd.to_numeric(exposure["exposure_ratio"], errors="coerce")
        exposure_column = "average_exposure" if "average_exposure" in exposure.columns else "exposure_ratio"
        exposure[exposure_column] = pd.to_numeric(exposure[exposure_column], errors="coerce")
        exposure_frame = exposure.groupby("experiment")[exposure_column].mean().to_frame("average_exposure").rename(index=_display_experiment_name)
        render_multi_series(exposure_frame, title="Average exposure by strategy", kind="bar", color_map={"average_exposure": PALETTE["teal"]}, height=300)

    if not filtered_portfolio.empty and {"date", "portfolio_value", "experiment"}.issubset(filtered_portfolio.columns):
        curve = filtered_portfolio.copy()
        curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
        curve["portfolio_value"] = pd.to_numeric(curve["portfolio_value"], errors="coerce")
        curve["series"] = curve["experiment"].astype(str).map(_display_experiment_name).fillna(curve["experiment"].astype(str))
        if "seed" in curve.columns:
            curve = curve.groupby(["date", "series"], as_index=False)["portfolio_value"].mean()
        curve_frame = curve.pivot_table(index="date", columns="series", values="portfolio_value", aggfunc="mean")
        render_multi_series(curve_frame, title="Portfolio curves by selected upgrade setting", kind="line", height=340)

    if not filtered_env.empty and {"date", "current_exposure_after", "experiment"}.issubset(filtered_env.columns):
        exposure_curve = filtered_env.copy()
        exposure_curve["date"] = pd.to_datetime(exposure_curve["date"], errors="coerce")
        exposure_curve["current_exposure_after"] = pd.to_numeric(exposure_curve["current_exposure_after"], errors="coerce")
        exposure_curve["series"] = exposure_curve["experiment"].astype(str).map(_display_experiment_name).fillna(exposure_curve["experiment"].astype(str))
        exposure_frame = exposure_curve.pivot_table(index="date", columns="series", values="current_exposure_after", aggfunc="mean")
        render_multi_series(exposure_frame, title="Exposure curve", kind="line", height=300)

    if not filtered_exploration.empty:
        st.markdown("#### Exploration diagnostics")
        if {"episode", "epsilon"}.issubset(filtered_exploration.columns):
            exploration_plot = filtered_exploration.copy()
            exploration_plot["episode"] = pd.to_numeric(exploration_plot["episode"], errors="coerce")
            exploration_plot["epsilon"] = pd.to_numeric(exploration_plot["epsilon"], errors="coerce")
            epsilon_frame = exploration_plot.pivot_table(index="episode", columns="experiment", values="epsilon", aggfunc="mean").rename(columns=_display_experiment_name)
            render_multi_series(epsilon_frame, title="Epsilon schedule", kind="line", height=260)
        st.dataframe(filtered_exploration.head(300), use_container_width=True, hide_index=True)

    if not filtered_env.empty:
        if "reward" in filtered_env.columns:
            reward_frame = filtered_env.copy()
            reward_frame["reward"] = pd.to_numeric(reward_frame["reward"], errors="coerce")
            reward_summary = reward_frame.groupby("experiment")["reward"].agg(["count", "mean", "std", "min", "max"]).reset_index()
            reward_summary["experiment"] = reward_summary["experiment"].astype(str).map(_display_experiment_name).fillna(reward_summary["experiment"].astype(str))
            st.markdown("#### Reward distribution")
            st.dataframe(reward_summary, use_container_width=True, hide_index=True)
        st.markdown("#### Environment dynamics")
        st.dataframe(filtered_env.head(500), use_container_width=True, hide_index=True)

    if not filtered_q_values.empty:
        st.markdown("#### Q-value diagnostics")
        q_plot = filtered_q_values.copy()
        q_plot["selected_q_value"] = pd.to_numeric(q_plot.get("selected_q_value"), errors="coerce")
        q_plot["q_value_margin"] = pd.to_numeric(q_plot.get("q_value_margin"), errors="coerce")
        q_summary = q_plot.groupby("experiment")[["selected_q_value", "q_value_margin"]].agg(["mean", "std", "min", "max"])
        q_summary.columns = ["_".join(col).strip("_") for col in q_summary.columns]
        q_summary = q_summary.reset_index()
        q_summary["experiment"] = q_summary["experiment"].astype(str).map(_display_experiment_name).fillna(q_summary["experiment"].astype(str))
        st.dataframe(q_summary, use_container_width=True, hide_index=True)

    render_trade_by_trade_dqn_behavior(
        env_dynamics=env_dynamics,
        q_values=q_values,
        reward_diagnostics=reward_diagnostics,
        seed_metrics=seed_metrics,
        actions=actions,
        portfolio_curves=portfolio,
        source_bundle=diagnostics_bundle,
        selected_target_symbol=selected_target,
    )

    st.markdown("#### model_upgrade_summary.csv")
    if filtered_summary.empty:
        st.info("model_upgrade_summary.csv is missing or has no rows for this target.")
    else:
        st.dataframe(filtered_summary, use_container_width=True, hide_index=True)
    if not reward_compare.empty:
        with st.expander("reward_variant_comparison.csv", expanded=False):
            st.dataframe(_filter(reward_compare), use_container_width=True, hide_index=True)
    if not state_features.empty:
        with st.expander("state_feature_diagnostics.csv", expanded=False):
            st.dataframe(state_features.head(500), use_container_width=True, hide_index=True)

    st.markdown("#### Diagnostic README / Report")
    readme_path = MODEL_UPGRADE_DIR / "README_diagnostics.md"
    compact_path = MODEL_UPGRADE_DIR / "CHATGPT_DIAGNOSTIC_SUMMARY.md"
    report_dir = MODEL_UPGRADE_DIR / "diagnostic_reports"
    if not readme_path.exists():
        st.info("Diagnostic README not found. Run model-upgrade grid or generate_model_upgrade_diagnostic_readme.py first.")
    else:
        readme_text = readme_path.read_text(encoding="utf-8")
        st.text_area("README_diagnostics.md preview", readme_text[:12000], height=260)
        st.download_button("Download README_diagnostics.md", readme_text.encode("utf-8"), file_name="README_diagnostics.md", mime="text/markdown", use_container_width=True)
    if compact_path.exists():
        compact_text = compact_path.read_text(encoding="utf-8")
        st.text_area("CHATGPT_DIAGNOSTIC_SUMMARY.md preview", compact_text[:8000], height=220)
        st.download_button("Download CHATGPT_DIAGNOSTIC_SUMMARY.md", compact_text.encode("utf-8"), file_name="CHATGPT_DIAGNOSTIC_SUMMARY.md", mime="text/markdown", use_container_width=True)
    if report_dir.exists():
        reports = sorted(report_dir.glob("*.md"), reverse=True)
        if reports:
            labels = [path.name for path in reports]
            selected_report = st.selectbox("Timestamped diagnostic report", labels, index=0)
            report_path = reports[labels.index(selected_report)]
            report_text = report_path.read_text(encoding="utf-8")
            st.download_button("Download timestamped diagnostic report", report_text.encode("utf-8"), file_name=report_path.name, mime="text/markdown", use_container_width=True)


def _render_box_or_table(frame: pd.DataFrame, metric: str) -> None:
    if frame.empty or metric not in frame.columns or "experiment" not in frame.columns:
        st.info("No chart-ready rows are available yet.")
        return
    chart = frame.copy()
    chart[metric] = pd.to_numeric(chart[metric], errors="coerce")
    chart = chart.dropna(subset=[metric])
    if chart.empty:
        st.info("No chart-ready rows are available yet.")
        return
    fig = go.Figure()
    for index, (experiment, part) in enumerate(chart.groupby("experiment")):
        label = _display_experiment_name(experiment)
        fig.add_trace(
            go.Box(
                y=part[metric],
                name=label,
                marker={"color": EXPERIMENT_COLORS.get(str(experiment), SERIES_COLORS[index % len(SERIES_COLORS)])},
                boxmean=True,
            )
        )
    fig.update_layout(**_base_layout(title=metric, height=340))
    _render_plot(fig)


def render_result_review_dashboard(symbol: str) -> None:
    _reset_plotly_key_counter()
    bundle = load_stock_bundle(symbol)
    tabs = st.tabs(["Single Stock Basics", "Scraping Density", "Peer Cross Analysis", "Market Impact Analysis", "Model Upgrade Diagnostics"])
    with tabs[0]:
        render_single_stock_basic_page(symbol, bundle)
    with tabs[1]:
        render_information_density_page(bundle)
    with tabs[2]:
        render_peer_cross_analysis_for_symbol(symbol, bundle)
    with tabs[3]:
        render_market_impact_analysis_for_symbol(symbol, bundle)
    with tabs[4]:
        render_model_upgrade_diagnostics(symbol, bundle)


def _figure_to_html(fig: go.Figure, *, include_plotlyjs: bool = False) -> str:
    return fig.to_html(
        full_html=False,
        include_plotlyjs="cdn" if include_plotlyjs else False,
        config=PLOT_CONFIG,
    )


def _add_html_line_chart(parts: list[str], frame: pd.DataFrame, x: str, y: str, title: str, *, color: str, include_plotlyjs: bool = False) -> bool:
    if frame.empty or x not in frame.columns or y not in frame.columns:
        return False
    chart = frame[[x, y]].copy()
    chart[x] = pd.to_datetime(chart[x], errors="coerce")
    chart[y] = pd.to_numeric(chart[y], errors="coerce")
    chart = chart.dropna(subset=[x, y])
    if chart.empty:
        return False
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=chart[x], y=chart[y], mode="lines", line={"color": color, "width": 2.4}, name=y))
    fig.update_layout(**_base_layout(title=title, height=340))
    parts.append(_figure_to_html(fig, include_plotlyjs=include_plotlyjs))
    return True


def _add_html_bar_chart(parts: list[str], frame: pd.DataFrame, x: str, y: str, title: str, *, color: str, include_plotlyjs: bool = False) -> bool:
    if frame.empty or x not in frame.columns or y not in frame.columns:
        return False
    chart = frame[[x, y]].copy()
    chart[y] = pd.to_numeric(chart[y], errors="coerce")
    if x == "date":
        chart[x] = pd.to_datetime(chart[x], errors="coerce")
    chart = chart.dropna(subset=[x, y])
    if chart.empty:
        return False
    fig = go.Figure()
    fig.add_trace(go.Bar(x=chart[x], y=chart[y], marker={"color": color}, name=y))
    fig.update_layout(**_base_layout(title=title, height=340))
    parts.append(_figure_to_html(fig, include_plotlyjs=include_plotlyjs))
    return True


def create_visual_report_html(
    target_symbol: str,
    completed_symbols: list[str],
    failures: list[dict[str, str]],
    cross_payload: dict[str, object] | None,
    output_dir: Path,
    *,
    market_impact_add_on: bool = False,
) -> Path:
    """Create a portable HTML report with the official experiment tables and charts."""
    code = normalize_symbol_for_path(target_symbol)
    bundle = load_stock_bundle(code)
    info = stock_classification(code)
    html_parts: list[str] = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Peer NLP Experiment Report</title>",
        f"""
        <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 32px; color: {PALETTE["ink"]}; background: white; }}
        h1, h2, h3 {{ color: {PALETTE["ink"]}; }}
        table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; }}
        th, td {{ border: 1px solid #e8e1df; padding: 8px 10px; text-align: left; }}
        th {{ background: #f7f3f1; }}
        .note {{ background: #f4f0fb; padding: 14px 16px; border-radius: 8px; margin: 12px 0 20px; }}
        </style>
        """,
        "</head><body>",
        "<h1>Peer NLP Transfer Trading Experiment</h1>",
        (
            "<div class='note'>"
            "Official logic: the target stock is held out from NLP training. "
            "Sector sentiment and sector impact are the default experiment. "
            "When enabled, marketwide peer benchmarks contribute two additional DQN groups."
            "</div>"
        ),
        "<h2>Experiment Target</h2>",
        "<table><tbody>"
        f"<tr><th>Target symbol</th><td>{html.escape(code)}</td></tr>"
        f"<tr><th>Company</th><td>{html.escape(str(info.get('company_name', '-')))}</td></tr>"
        f"<tr><th>Sector</th><td>{html.escape(str(info.get('sector', '-')))}</td></tr>"
        f"<tr><th>Industry</th><td>{html.escape(str(info.get('industry', '-')))}</td></tr>"
        f"<tr><th>Completed symbols</th><td>{html.escape(', '.join(completed_symbols) if completed_symbols else 'none')}</td></tr>"
        f"<tr><th>Failed symbols</th><td>{len(failures)}</td></tr>"
        f"<tr><th>Marketwide peer add-on</th><td>{'enabled' if market_impact_add_on else 'disabled'}</td></tr>"
        "</tbody></table>",
    ]

    data = bundle.get("data", pd.DataFrame())
    density = bundle.get("daily_density", pd.DataFrame())
    net_flow = bundle.get("daily_net_flow", pd.DataFrame())
    peer_daily = bundle.get("peer_daily", pd.DataFrame())
    metrics = bundle.get("market_impact_metrics", pd.DataFrame())
    curves = bundle.get("market_impact_curves", pd.DataFrame())
    effect = bundle.get("market_impact_effect", pd.DataFrame())
    peer_metrics = bundle.get("peer_metrics", pd.DataFrame())

    html_parts.append("<h2>Single Stock Basics</h2>")
    first_chart = True
    if isinstance(data, pd.DataFrame):
        first_chart = not _add_html_line_chart(html_parts, data, "date", "close", "Close price", color=PALETTE["ink"], include_plotlyjs=first_chart)
    if isinstance(net_flow, pd.DataFrame):
        flow_col = "net_flow_cny_million" if "net_flow_cny_million" in net_flow.columns else "net_flow" if "net_flow" in net_flow.columns else ""
        if flow_col:
            first_chart = not _add_html_bar_chart(html_parts, net_flow, "date", flow_col, "Daily net inflow / outflow", color=PALETTE["blue"], include_plotlyjs=first_chart)

    html_parts.append("<h2>Scraping Density</h2>")
    if isinstance(density, pd.DataFrame) and "news_count" in density.columns:
        first_chart = not _add_html_bar_chart(html_parts, density, "date", "news_count", "Daily news density", color=PALETTE["teal"], include_plotlyjs=first_chart)
    elif isinstance(peer_daily, pd.DataFrame) and "target_news_count" in peer_daily.columns:
        first_chart = not _add_html_bar_chart(html_parts, peer_daily, "date", "target_news_count", "Target news count in evaluation window", color=PALETTE["teal"], include_plotlyjs=first_chart)

    html_parts.append("<h2>Peer Cross Analysis</h2>")
    if isinstance(peer_daily, pd.DataFrame) and not peer_daily.empty:
        score_cols = [col for col in ["sector_sentiment_score", "marketwide_sentiment_score"] if col in peer_daily.columns]
        if score_cols:
            sent = peer_daily.copy()
            sent["date"] = pd.to_datetime(sent["date"], errors="coerce")
            fig = go.Figure()
            for idx, col in enumerate(score_cols):
                fig.add_trace(go.Scatter(x=sent["date"], y=pd.to_numeric(sent[col], errors="coerce"), mode="lines", name=col, line={"color": SERIES_COLORS[idx + 2], "width": 2.4}))
            fig.update_layout(**_base_layout(title="Peer-trained sentiment scores", height=340))
            html_parts.append(_figure_to_html(fig, include_plotlyjs=first_chart))
            first_chart = False
    if isinstance(curves, pd.DataFrame) and not curves.empty and {"date", "experiment", "portfolio_value"}.issubset(curves.columns):
        curve_frame = curves.copy()
        curve_frame["date"] = pd.to_datetime(curve_frame["date"], errors="coerce")
        pivot = curve_frame.pivot_table(index="date", columns="experiment", values="portfolio_value", aggfunc="mean")
        fig = go.Figure()
        for idx, column in enumerate(pivot.columns):
            fig.add_trace(go.Scatter(x=pivot.index, y=pivot[column], mode="lines", name=str(column), line={"color": EXPERIMENT_COLORS.get(str(column), SERIES_COLORS[idx % len(SERIES_COLORS)]), "width": 2.4}))
        fig.update_layout(**_base_layout(title="Official sector sentiment/impact portfolio comparison", height=360))
        html_parts.append(_figure_to_html(fig, include_plotlyjs=first_chart))
        first_chart = False
    if isinstance(metrics, pd.DataFrame) and not metrics.empty:
        html_parts.append("<h3>Official sector sentiment/impact ablation metrics</h3>")
        html_parts.append(metrics.to_html(index=False, escape=True))
    if isinstance(effect, pd.DataFrame) and not effect.empty:
        html_parts.append("<h3>Official market-impact effect summary</h3>")
        html_parts.append(effect.to_html(index=False, escape=True))
    if isinstance(peer_metrics, pd.DataFrame) and not peer_metrics.empty:
        html_parts.append("<h3>Peer sentiment diagnostic metrics</h3>")
        html_parts.append(peer_metrics.to_html(index=False, escape=True))
    if cross_payload:
        cross_summary = safe_read_csv(Path(str(cross_payload.get("summary_csv", ""))))
        if not cross_summary.empty:
            html_parts.append("<h3>Cross-stock summary generated by this run</h3>")
            html_parts.append(cross_summary.to_html(index=False, escape=True))
    if failures:
        html_parts.append("<h2>Failures</h2><ul>")
        for failure in failures:
            html_parts.append(f"<li><b>{html.escape(str(failure.get('symbol', '-')))}</b>: {html.escape(str(failure.get('error', '-')))}</li>")
        html_parts.append("</ul>")
    html_parts.append("</body></html>")

    path = output_dir / "peer_nlp_experiment_visual_report.html"
    path.write_text("\n".join(html_parts), encoding="utf-8")
    return path


def create_export_bundle(
    run_rows: list[dict[str, object]],
    completed_symbols: list[str],
    failures: list[dict[str, str]],
    cross_payload: dict[str, object] | None,
    *,
    target_symbol: str | None = None,
    market_cross_payload: dict[str, object] | None = None,
    market_impact_add_on: bool = False,
) -> dict[str, Path]:
    export_root = SYSTEM_OUTPUT_DIR / "dashboard_exports"
    export_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bundle_dir = export_root / f"dashboard_run_{timestamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    run_summary_csv = bundle_dir / "run_summary.csv"
    pd.DataFrame(run_rows).to_csv(run_summary_csv, index=False, encoding="utf-8-sig")

    failure_json = bundle_dir / "failed_runs.json"
    failure_json.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")

    markdown_lines = [
        "# Dashboard Run Summary",
        "",
        f"- Generated at: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        f"- Completed stocks: `{', '.join(completed_symbols) if completed_symbols else 'none'}`",
        f"- Failed stocks: `{len(failures)}`",
        f"- Marketwide peer add-on: `{'enabled' if market_impact_add_on else 'disabled'}`",
        f"- DQN group count: `{'5' if market_impact_add_on else '3'}`",
        "",
        "## Completed Runs",
        "",
    ]
    for row in run_rows:
        markdown_lines.extend(
            [
                f"### {row.get('symbol', '-')}",
                f"- Company: `{row.get('company_name', '-')}`",
                f"- Status: `{row.get('status', '-')}`",
                f"- Marketwide peer add-on: `{row.get('market_impact_add_on', market_impact_add_on)}`",
                f"- DQN groups: `{row.get('dqn_groups', '5' if market_impact_add_on else '3')}`",
                f"- Input CSV: `{row.get('input_csv', '-')}`",
                f"- Reports: `{row.get('reports_dir', '-')}`",
                f"- Results: `{row.get('results_dir', '-')}`",
                "",
            ]
        )

    if failures:
        markdown_lines.extend(["## Failures", ""])
        for failure in failures:
            markdown_lines.append(f"- `{failure['symbol']}`: {failure['error']}")
        markdown_lines.append("")

    cross_summary_path: Path | None = None
    cross_discussion_path: Path | None = None
    if cross_payload:
        cross_summary_path = Path(str(cross_payload["summary_csv"]))
        cross_discussion_path = Path(str(cross_payload["discussion_md"]))
        markdown_lines.extend(
            [
                "## Peer Cross Analysis",
                "",
                f"- Summary CSV: `{cross_summary_path}`",
                f"- Discussion Markdown: `{cross_discussion_path}`",
                "",
            ]
        )

    market_cross_summary_path: Path | None = None
    market_cross_discussion_path: Path | None = None
    if market_cross_payload:
        market_cross_summary_path = Path(str(market_cross_payload["summary_csv"]))
        market_cross_discussion_path = Path(str(market_cross_payload["discussion_md"]))
        markdown_lines.extend(
            [
                "## Market-Impact Cross Analysis",
                "",
                f"- Summary CSV: `{market_cross_summary_path}`",
                f"- Discussion Markdown: `{market_cross_discussion_path}`",
                "",
            ]
        )

    summary_md = bundle_dir / "dashboard_run_summary.md"
    summary_md.write_text("\n".join(markdown_lines), encoding="utf-8")
    target_for_report = target_symbol or (completed_symbols[0] if completed_symbols else "")
    visual_html_path = (
        create_visual_report_html(
            target_for_report,
            completed_symbols,
            failures,
            cross_payload,
            bundle_dir,
            market_impact_add_on=market_impact_add_on,
        )
        if target_for_report
        else None
    )

    zip_path = bundle_dir / "dashboard_run_bundle.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(run_summary_csv, arcname=run_summary_csv.name)
        archive.write(summary_md, arcname=summary_md.name)
        archive.write(failure_json, arcname=failure_json.name)
        if visual_html_path and visual_html_path.exists():
            archive.write(visual_html_path, arcname=visual_html_path.name)
        if cross_summary_path and cross_summary_path.exists():
            archive.write(cross_summary_path, arcname=f"cross_stock/{cross_summary_path.name}")
        if cross_discussion_path and cross_discussion_path.exists():
            archive.write(cross_discussion_path, arcname=f"cross_stock/{cross_discussion_path.name}")
        if market_cross_summary_path and market_cross_summary_path.exists():
            archive.write(market_cross_summary_path, arcname=f"cross_stock/{market_cross_summary_path.name}")
        if market_cross_discussion_path and market_cross_discussion_path.exists():
            archive.write(market_cross_discussion_path, arcname=f"cross_stock/{market_cross_discussion_path.name}")
        for symbol in completed_symbols:
            bundle = load_stock_bundle(symbol)
            for key in ["selected_csv"]:
                value = bundle.get(key)
                if value and Path(value).exists():
                    path = Path(value)
                    archive.write(path, arcname=f"{symbol}/{path.name}")
            for path in [
                stock_results_dir(symbol) / "peer_nlp_daily_sentiment.csv",
                stock_results_dir(symbol) / "peer_nlp_ablation_metrics.csv",
                stock_results_dir(symbol) / "peer_nlp_ablation_metrics_by_seed.csv",
                stock_results_dir(symbol) / "peer_nlp_portfolio_curves.csv",
                stock_results_dir(symbol) / "peer_nlp_drawdown_curves.csv",
                stock_results_dir(symbol) / "peer_nlp_trading_logs.csv",
                stock_results_dir(symbol) / "peer_nlp_effect_summary.csv",
                stock_reports_dir(symbol) / "peer_nlp_integrity_check.csv",
                stock_reports_dir(symbol) / "peer_nlp_report_section.md",
                stock_reports_dir(symbol) / "peer_nlp_information_density_split.csv",
                stock_reports_dir(symbol) / "peer_nlp_train_eval_windows.csv",
                stock_reports_dir(symbol) / "peer_nlp_group_state_diagnostics.csv",
                stock_results_dir(symbol) / "peer_market_impact_daily_signal.csv",
                stock_results_dir(symbol) / "market_impact_ablation_metrics.csv",
                stock_results_dir(symbol) / "market_impact_ablation_metrics_by_seed.csv",
                stock_results_dir(symbol) / "market_impact_portfolio_curves.csv",
                stock_results_dir(symbol) / "market_impact_drawdown_curves.csv",
                stock_results_dir(symbol) / "market_impact_trading_logs.csv",
                stock_results_dir(symbol) / "market_impact_effect_summary.csv",
                stock_reports_dir(symbol) / "market_impact_reliability_check.csv",
                stock_reports_dir(symbol) / "market_impact_report_section.md",
                stock_reports_dir(symbol) / "market_impact_train_eval_windows.csv",
            ]:
                if path.exists():
                    archive.write(path, arcname=f"{symbol}/{path.name}")

    result = {"bundle_dir": bundle_dir, "summary_md": summary_md, "summary_csv": run_summary_csv, "zip_path": zip_path}
    if visual_html_path:
        result["visual_html"] = visual_html_path
    return result


def workflow_phase_label(stage: object, role: object = "") -> str:
    stage_text = str(stage or "").strip().lower()
    role_text = str(role or "").strip().lower()
    if role_text == "training_peer":
        if stage_text in {"cache_ready", "waiting_for_fetch", "fetch_result", "cache_missing"} or "fetch" in stage_text:
            return "2. Peer 数据准备"
        if "peer_nlp" in stage_text or "corpus" in stage_text or "training" in stage_text:
            return "3. Peer 语料/模型"
        return "2. Peer 数据准备"
    if stage_text in {"waiting", "starting", "preflight", "ready_local", "local_cross"} or "feasibility" in stage_text:
        return "0. 运行前检查"
    if stage_text in {"ingestion", "cache"}:
        return "1. Target 数据"
    if stage_text.startswith("sector_peer") or stage_text in {"cache_ready", "waiting_for_fetch", "fetch_result", "cache_missing"}:
        return "2. Peer 数据准备"
    if stage_text in {
        "peer_nlp",
        "peer_nlp_training_corpus",
        "peer_nlp_peer_processing",
        "peer_nlp_peer_processed",
        "peer_nlp_peer_skipped",
        "peer_nlp_model_training",
        "peer_nlp_target_data",
        "peer_nlp_target_scoring",
        "peer_nlp_sentiment_saved",
        "sentiment_baseline",
        "waiting_for_peer_training",
    }:
        return "3. Peer 情绪信号"
    if stage_text in {"market_impact_nlp", "impact_corpus", "impact_labels", "impact_target_data", "impact_scoring", "impact_saved"}:
        return "4. Market-impact 信号"
    if stage_text in {
        "dqn_features",
        "dqn_split",
        "dqn_pretrain",
        "dqn_without_nlp",
        "dqn_peer_sentiment",
        "dqn_market_impact",
        "rl",
    }:
        return "5. DQN 训练/评估"
    if stage_text in {"signals", "metrics", "save_outputs", "figures", "dashboard_cache", "cross_stock", "experiment_summary", "peer_summary", "done"}:
        return "6. 指标/导出"
    if stage_text == "error":
        return "错误"
    return "运行中"


def render_live_status_table(status_rows: list[dict[str, object]], container) -> None:
    if not status_rows:
        return
    frame = pd.DataFrame(status_rows)
    if "phase" not in frame.columns:
        frame["phase"] = frame.apply(lambda row: workflow_phase_label(row.get("stage", ""), row.get("role", "")), axis=1)
    preferred = ["phase", "role", "symbol", "company_name", "status", "stage", "last_update", "message"]
    columns = [col for col in preferred if col in frame.columns]
    container.dataframe(frame[columns], use_container_width=True, hide_index=True)


def _status_progress(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 1.0
    complete_states = {"ready_local", "ready", "fetched", "processed", "completed", "skipped"}
    done = sum(1 for row in rows if str(row.get("status", "")).lower() in complete_states)
    return min(max(done / len(rows), 0.0), 1.0)


def _curve_is_flat(frame: pd.DataFrame, experiment: str) -> bool:
    if frame.empty or "experiment" not in frame.columns or "portfolio_value" not in frame.columns:
        return True
    values = pd.to_numeric(frame.loc[frame["experiment"].astype(str) == experiment, "portfolio_value"], errors="coerce").dropna()
    return values.empty or values.nunique() <= 1


def _trade_count(frame: pd.DataFrame, experiment: str) -> int:
    if frame.empty or "experiment" not in frame.columns or "action" not in frame.columns:
        return 0
    actions = frame.loc[frame["experiment"].astype(str) == experiment, "action"].astype(str)
    return int(actions.isin(["Buy", "Sell"]).sum())


def experiment_anomaly_rows(symbols: list[str], *, include_market_impact: bool) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for symbol in symbols:
        code = normalize_symbol_for_path(symbol)
        bundle = load_stock_bundle(code)
        peer_effect = bundle.get("peer_effect", pd.DataFrame())
        peer_curves = bundle.get("peer_curves", pd.DataFrame())
        peer_logs = bundle.get("peer_logs", pd.DataFrame())
        peer_diag = bundle.get("peer_state_diagnostics", pd.DataFrame())
        reliability = ""
        if isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty:
            reliability = str(peer_effect.iloc[0].get("reliability_status", ""))
        if reliability and reliability != "READY_FOR_SUBMISSION":
            reason = str(peer_effect.iloc[0].get("reason_if_not_reliable", "")) if isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty else ""
            rows.append({"symbol": code, "experiment": "Peer sentiment NLP", "severity": "warning", "issue": reliability, "evidence": reason or "Peer NLP reliability did not pass."})
        for experiment in ["dqn_with_sector_peer_nlp", "dqn_with_marketwide_peer_nlp"]:
            if _curve_is_flat(peer_curves, experiment) or _trade_count(peer_logs, experiment) == 0:
                evidence = f"portfolio_curve_flat={_curve_is_flat(peer_curves, experiment)}; buy/sell trades={_trade_count(peer_logs, experiment)}"
                if isinstance(peer_diag, pd.DataFrame) and not peer_diag.empty and {"experiment", "period", "state_column", "nonzero_count"}.issubset(peer_diag.columns):
                    match = peer_diag[
                        (peer_diag["experiment"].astype(str) == experiment)
                        & (peer_diag["period"].astype(str) == "train")
                        & (
                            peer_diag["state_column"].astype(str).eq("nlp_signal_score")
                            | peer_diag["state_column"].astype(str).str.contains("sentiment_score", na=False)
                        )
                    ]
                    if not match.empty:
                        evidence += f"; train_nonzero_signal={int(pd.to_numeric(match['nonzero_count'], errors='coerce').fillna(0).sum())}"
                rows.append({"symbol": code, "experiment": experiment, "severity": "warning", "issue": "NLP DQN did not trade reliably", "evidence": evidence})

        if include_market_impact:
            market_files = {
                "daily_signal": stock_results_dir(code) / "peer_market_impact_daily_signal.csv",
                "metrics": stock_results_dir(code) / "market_impact_ablation_metrics.csv",
                "curves": stock_results_dir(code) / "market_impact_portfolio_curves.csv",
                "logs": stock_results_dir(code) / "market_impact_trading_logs.csv",
            }
            missing = [name for name, path in market_files.items() if not path.exists() or path.stat().st_size <= 4]
            if missing:
                rows.append({"symbol": code, "experiment": "Market-impact NLP", "severity": "error", "issue": "Incomplete market-impact outputs", "evidence": ", ".join(missing)})
            market_effect = bundle.get("market_impact_effect", pd.DataFrame())
            if isinstance(market_effect, pd.DataFrame) and not market_effect.empty:
                status = str(market_effect.iloc[0].get("reliability_status", ""))
                if status and status != "READY_FOR_PRESENTATION":
                    rows.append({"symbol": code, "experiment": "Market-impact NLP", "severity": "warning", "issue": status, "evidence": str(market_effect.iloc[0].get("reason_if_not_reliable", ""))})
    return rows


def render_experiment_completion_notice(
    run_rows: list[dict[str, object]],
    failures: list[dict[str, str]],
    completed: list[str],
    *,
    include_market_impact: bool,
) -> None:
    if not run_rows:
        return
    running = [row for row in run_rows if str(row.get("status", "")).lower() == "running"]
    if running:
        return
    anomalies = experiment_anomaly_rows(completed, include_market_impact=include_market_impact) if completed else []
    status_color = PALETTE["rose"] if failures or any(row["severity"] == "error" for row in anomalies) else PALETTE["amber"] if anomalies else PALETTE["green"]
    title = "实验已结束"
    if failures:
        subtitle = f"{len(completed)} completed, {len(failures)} failed."
    elif anomalies:
        subtitle = f"{len(completed)} completed, 0 failed, but {len(anomalies)} reliability warning(s) need review."
    else:
        subtitle = f"{len(completed)} completed, 0 failed, no blocking anomaly detected."
    issue_items = "".join(f"<li><strong>{html.escape(row['experiment'])}</strong> [{html.escape(row['symbol'])}]: {html.escape(row['issue'])}. {html.escape(row['evidence'])}</li>" for row in anomalies[:6])
    failure_items = "".join(f"<li><strong>{html.escape(str(item.get('symbol', '-')))}</strong>: {html.escape(str(item.get('error', '-')))}</li>" for item in failures[:6])
    details = issue_items + failure_items
    if not details:
        details = "<li>All tracked artifacts are present and reliability checks did not raise a dashboard-level warning.</li>"
    st.markdown(
        f"""
        <div class="experiment-status-card" style="--status-color: {status_color};">
            <strong>{title}</strong>
            <div>{html.escape(subtitle)}</div>
            <ul>{details}</ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def experiment_workflow_rows(marketwide_add_on: bool) -> pd.DataFrame:
    rows = [
        {
            "阶段": "0. 运行前检查",
            "输入": "目标股票、日期区间、本地缓存",
            "动作": "确认目标股票在所选区间内是否可运行。",
            "输出": "正式运行前的 ready / warning",
        },
        {
            "阶段": "1. Target 数据",
            "输入": "仅 held-out 目标股票",
            "动作": "复用或更新目标股票的行情和新闻数据。",
            "输出": "Target CSV；目标新闻不会用于训练自己的 NLP 模型",
        },
        {
            "阶段": "2. Peer 数据准备",
            "输入": "同板块 peers；可选跨板块 marketwide peers",
            "动作": "自动识别 sector peers；勾选 marketwide 时检查/补齐跨板块 peers。",
            "输出": "训练语料候选股票；target 已剔除",
        },
        {
            "阶段": "3. Peer 情绪信号",
            "输入": "Peer 新闻文本",
            "动作": "训练 sector sentiment scorer；勾选后额外训练 marketwide sentiment scorer。",
            "输出": "peer_nlp_daily_sentiment.csv；sentiment DQN 输入",
        },
        {
            "阶段": "4. Market-impact 信号",
            "输入": "Peer 新闻 + peer 未来收益",
            "动作": "用固定 horizon/threshold 给 peer 新闻贴 impact 标签；默认 sector impact，勾选后额外 marketwide impact。",
            "输出": "peer_market_impact_daily_signal.csv；impact DQN 输入",
        },
    ]
    if marketwide_add_on:
        rows.append(
            {
                "阶段": "5. DQN 训练/评估",
                "输入": "行情特征 + sentiment / impact 信号",
                "动作": "五组 DQN 对比：no-NLP、sector sentiment、sector impact、marketwide sentiment、marketwide impact。",
                "输出": "market_impact_ablation_metrics.csv 和 portfolio curves；buy_and_hold 仅作 benchmark",
            }
        )
    else:
        rows.append(
            {
                "阶段": "5. DQN 训练/评估",
                "输入": "行情特征 + sector sentiment / sector impact 信号",
                "动作": "sector-only 三组 DQN 对比：no-NLP、sector sentiment、sector impact。",
                "输出": "market_impact_ablation_metrics.csv 和 portfolio curves；buy_and_hold 仅作 benchmark",
            }
        )
    rows.append(
        {
            "阶段": "6. 指标/导出",
            "输入": "已完成的实验输出",
            "动作": "生成可靠性检查、summary 和 dashboard 下载包。",
            "输出": "Cross-stock summary、报告片段、export zip",
        }
    )
    return pd.DataFrame(rows)


def render_experiment_workflow_overview(
    *,
    primary_symbol: str,
    resolved_company: str,
    target_info: dict[str, object],
    training_peer_preview: pd.DataFrame,
    market_impact_add_on: bool,
    market_impact_horizon_days: int,
    market_impact_pos_threshold: float,
    market_impact_neg_threshold: float,
) -> None:
    st.markdown("## Experiment Workflow")
    summary_cols = st.columns(4)
    summary_cols[0].metric("目标股票", primary_symbol)
    summary_cols[1].metric("公司", resolved_company or "-")
    summary_cols[2].metric("板块", str(target_info.get("sector", "UNKNOWN")))
    summary_cols[3].metric("训练 peers", int(len(training_peer_preview)))

    st.caption("Sector sentiment + sector impact 是默认正式实验。Marketwide sentiment/impact 是可选 benchmark；target 始终是唯一 held-out 股票，不进入 peer corpus。")
    st.dataframe(experiment_workflow_rows(market_impact_add_on), use_container_width=True, hide_index=True)

    if market_impact_add_on:
        with st.expander("固定 Market-impact 标签规则", expanded=False):
            label_rows = pd.DataFrame(
                [
                    {"参数": "Impact horizon days", "值": market_impact_horizon_days, "含义": "新闻后向未来看多少个交易日的收益。"},
                    {"参数": "Positive return threshold", "值": market_impact_pos_threshold, "含义": "未来收益达到或超过该值，标为 bullish impact。"},
                    {"参数": "Negative return threshold", "值": market_impact_neg_threshold, "含义": "未来收益达到或低于该值，标为 bearish impact。"},
                ]
            )
            st.dataframe(label_rows, use_container_width=True, hide_index=True)


def build_training_peer_status_rows(peer_symbols: list[str], start_date: object, end_date: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in peer_symbols:
        company = resolve_company_name(symbol, "")[0] or stock_classification(symbol).get("company_name", "")
        preview = cache_preview(symbol, str(start_date), str(end_date))
        is_ready = preview.get("status") == "covered"
        rows.append(
            {
                "role": "training_peer",
                "symbol": symbol,
                "company_name": company,
                "status": "ready_local" if is_ready else "pending_fetch",
                "stage": "cache_ready" if is_ready else "waiting_for_fetch",
                "last_update": datetime.now().strftime("%H:%M:%S"),
                "message": str(preview.get("message", "")),
            }
        )
    return rows


def update_training_peer_row(rows: list[dict[str, object]], symbol: str, *, status: str, stage: str, message: str) -> None:
    code = normalize_symbol_for_path(symbol)
    for row in rows:
        if normalize_symbol_for_path(str(row.get("symbol", ""))) == code:
            row["status"] = status
            row["stage"] = stage
            row["last_update"] = datetime.now().strftime("%H:%M:%S")
            row["message"] = message
            return


def update_all_training_peer_rows(
    rows: list[dict[str, object]],
    *,
    status: str,
    stage: str,
    message: str,
    only_statuses: set[str] | None = None,
) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    for row in rows:
        current = str(row.get("status", "")).lower()
        if only_statuses and current not in only_statuses:
            continue
        row["status"] = status
        row["stage"] = stage
        row["last_update"] = now
        row["message"] = message


def finalize_training_peer_status_rows(rows: list[dict[str, object]], start_date: object, end_date: object, fetch_log: pd.DataFrame) -> None:
    fetch_status: dict[str, tuple[str, str]] = {}
    if isinstance(fetch_log, pd.DataFrame) and not fetch_log.empty and "symbol" in fetch_log.columns:
        for _, item in fetch_log.iterrows():
            symbol = normalize_symbol_for_path(str(item.get("symbol", "")))
            status = str(item.get("status", ""))
            message = str(item.get("error", item.get("reason", item.get("csv_path", ""))))
            fetch_status[symbol] = (status, message)
    for row in rows:
        symbol = normalize_symbol_for_path(str(row.get("symbol", "")))
        preview = cache_preview(symbol, str(start_date), str(end_date))
        if preview.get("status") == "covered":
            status = "fetched" if fetch_status.get(symbol, ("", ""))[0] == "fetched" else "ready_local"
            update_training_peer_row(rows, symbol, status=status, stage="cache_ready", message=str(preview.get("message", "Ready.")))
        elif symbol in fetch_status:
            status, message = fetch_status[symbol]
            update_training_peer_row(rows, symbol, status=status or "missing", stage="fetch_result", message=message)
        else:
            update_training_peer_row(rows, symbol, status="missing", stage="cache_missing", message="No local cache covers this peer for the requested range.")


def run_dashboard_preflight(symbols: list[str], start_date: object, end_date: object, allow_fetch: bool) -> pd.DataFrame:
    try:
        outputs = run_feasibility_audit(
            symbols=symbols,
            start_date=str(start_date),
            end_date=str(end_date),
            mode="dry_run",
            allow_fetch_missing_data=allow_fetch,
            output_dir=SYSTEM_OUTPUT_DIR / "reports",
        )
    except Exception as exc:
        st.warning(f"Preflight audit failed, continuing with normal workflow. Error: {exc}")
        return pd.DataFrame()
    audit = outputs.get("audit", pd.DataFrame())
    summary = outputs.get("summary", pd.DataFrame())
    if not summary.empty:
        status_value = summary["cross_stock_feasibility_status"].iloc[0]
        if status_value == "READY_FOR_CROSS_STOCK_ANALYSIS":
            st.success("Preflight passed: the target stock has enough local/requested-range data to start the selected experiment.")
        else:
            st.warning(f"Preflight warning: {status_value}. The audit table is saved under outputs/system/reports but hidden from the dashboard run view.")
    if not audit.empty and "available_history_starts_after_requested_start" in audit.columns:
        gap_rows = audit.loc[audit["available_history_starts_after_requested_start"].apply(truthy_flag)]
        for _, row in gap_rows.head(5).iterrows():
            st.warning(
                f"{row.get('symbol', '')}: requested start `{start_date}` is earlier than the first local/available "
                f"trading date `{row.get('local_data_start', 'unknown')}`. "
                "This usually means the stock listed later or the local cache does not cover the requested early period."
            )
    return audit if isinstance(audit, pd.DataFrame) else pd.DataFrame()


existing_stock_dirs = latest_stock_dirs()
default_symbol = existing_stock_dirs[-1].name if existing_stock_dirs else settings.default_symbol
default_company, default_company_source = resolve_company_name(default_symbol, "")

st.sidebar.header("Peer NLP Experiment")
st.sidebar.caption("当前 dashboard 固定运行 sector-only sentiment/impact 实验；marketwide groups 是可选 benchmark。")
if "dashboard_target_symbol" not in st.session_state:
    st.session_state["dashboard_target_symbol"] = default_symbol
symbol_input = st.sidebar.text_input("Experiment target symbol", key="dashboard_target_symbol")
primary_symbol = normalize_symbol_for_path(symbol_input)
auto_company, auto_company_source = resolve_company_name(primary_symbol, "")
if "dashboard_target_company" not in st.session_state:
    st.session_state["dashboard_target_company"] = auto_company or default_company
if st.session_state.get("_dashboard_company_synced_for_symbol") != primary_symbol:
    st.session_state["dashboard_target_company"] = auto_company
    st.session_state["_dashboard_company_synced_for_symbol"] = primary_symbol
typed_company = st.sidebar.text_input("Target company name", key="dashboard_target_company")
resolved_company, company_source = resolve_company_name(primary_symbol, typed_company)
st.sidebar.caption(f"Company used for news search: {resolved_company or 'manual input required'} ({company_source})")

default_start = pd.Timestamp(settings.default_start_date).date()
default_end = pd.Timestamp(settings.default_end_date).date()
start_date = st.sidebar.date_input("Start date", value=default_start)
end_date = st.sidebar.date_input("End date", value=default_end)
sources = st.sidebar.text_input("Data source priority", value="tencent")
news_count = int(st.sidebar.number_input("News cap", min_value=0, value=5000, step=100))
episodes = int(st.sidebar.number_input("DQN episodes", min_value=1, value=200, step=10))

add_marketwide_peer_groups = st.sidebar.checkbox(
    "Add marketwide peer benchmark groups",
    value=False,
    help=(
        "Default sector-only experiment runs no-NLP, sector sentiment, and sector impact. "
        "When selected, the run additionally includes marketwide sentiment and marketwide impact benchmark groups."
    ),
)
run_market_impact_nlp = True
experiment_label = "sector + marketwide peer NLP benchmark" if add_marketwide_peer_groups else "sector-only peer sentiment/impact NLP"
dqn_group_count = 5 if add_marketwide_peer_groups else 3
market_impact_horizon_days = int(settings.market_impact_horizon_days)
market_impact_pos_threshold = float(settings.market_impact_pos_threshold)
market_impact_neg_threshold = float(settings.market_impact_neg_threshold)

run_peer_nlp_experiment = True
run_legacy_stock_level_nlp = False
allow_fetch_missing_sector_peers = True
run_high_density_ablation = False
run_ingestion_flag = True
reuse_existing_csv = True
require_news = False
use_sqlite = True
cross_enabled = True
run_cross_analysis = True
run_cross_pipeline = True
run_cross_existing_only = False
run_cross_preflight_only = False
supplement_sector_peers = True
st.sidebar.caption(f"Fixed run options: target data cache is reused/updated automatically; sector peers are supplemented before the {experiment_label} experiment; market-impact label parameters are fixed; SQLite persistence is enabled and legacy stock-level NLP is disabled.")
st.sidebar.caption(f"DQN comparison groups: {dqn_group_count} DQN groups; buy-and-hold remains a benchmark only.")

target_sector_peers = same_sector_symbols(primary_symbol, include_target=False)
target_sector_peers = [symbol for symbol in target_sector_peers if normalize_symbol_for_path(symbol) != primary_symbol]
cross_symbols_text = ", ".join(target_sector_peers)
company_resolution_table = build_company_resolution_table(primary_symbol, resolved_company, cross_enabled, cross_symbols_text)
target_info = stock_classification(primary_symbol)
target_preview = pd.DataFrame(
    [
        {
            "target": primary_symbol,
            "company": resolved_company,
            "sector": target_info["sector"],
        }
    ]
)
training_peer_preview = company_resolution_table[
    company_resolution_table["role"].astype(str) == "sector_peer_training_candidate"
].copy()
training_peer_preview = training_peer_preview[
    training_peer_preview["symbol"].astype(str).apply(normalize_symbol_for_path) != primary_symbol
]
st.sidebar.markdown("#### Auto target/peer plan")
st.sidebar.caption("Target is resolved first; only same-sector non-target stocks are used as NLP training peers.")
st.sidebar.dataframe(target_preview, use_container_width=True, hide_index=True)
if training_peer_preview.empty:
    st.sidebar.warning("No configured same-sector training peers were found for this target.")
else:
    st.sidebar.caption(f"Training peers: {len(training_peer_preview)} same-sector stocks; target excluded.")
    st.sidebar.dataframe(
        training_peer_preview[["symbol", "company_name", "sector"]],
        use_container_width=True,
        hide_index=True,
    )
with st.sidebar.expander("Resolved target and training peer plan", expanded=False):
    target_rows = company_resolution_table[company_resolution_table["role"].astype(str) == "experiment_target"].copy()
    peer_rows = training_peer_preview.copy()
    st.markdown("**Experiment target**")
    st.dataframe(
        target_rows[["symbol", "company_name", "sector", "industry", "sector_source"]],
        use_container_width=True,
        hide_index=True,
    )
    st.markdown("**Training peers (target excluded)**")
    st.dataframe(
        peer_rows[["symbol", "company_name", "sector", "industry", "sector_source", "target_excluded_from_training"]],
        use_container_width=True,
        hide_index=True,
    )

preview = cache_preview(primary_symbol, str(start_date), str(end_date))
if preview["status"] == "covered":
    st.sidebar.success(preview["message"])
else:
    st.sidebar.warning(preview["message"])
if "master_start" in preview and "master_end" in preview:
    st.sidebar.caption(f"Local cached timeline: {preview['master_start']} -> {preview['master_end']}")

render_experiment_workflow_overview(
    primary_symbol=primary_symbol,
    resolved_company=resolved_company,
    target_info=target_info,
    training_peer_preview=training_peer_preview,
    market_impact_add_on=add_marketwide_peer_groups,
    market_impact_horizon_days=market_impact_horizon_days,
    market_impact_pos_threshold=market_impact_pos_threshold,
    market_impact_neg_threshold=market_impact_neg_threshold,
)

run_clicked = st.sidebar.button(f"Run target {experiment_label} experiment", type="primary", use_container_width=True)

if run_clicked:
    selected_symbols_for_cross = [primary_symbol]
    cleanup_previous_dashboard_experiment(selected_symbols_for_cross)

    if run_cross_preflight_only:
        audit = run_dashboard_preflight(selected_symbols_for_cross, start_date, end_date, allow_fetch=run_ingestion_flag)
        run_rows = []
        status_rows = []
        for symbol in selected_symbols_for_cross:
            audit_row = audit.loc[audit["symbol"].astype(str) == symbol].iloc[0].to_dict() if not audit.empty and symbol in set(audit["symbol"].astype(str)) else {}
            status_value = str(audit_row.get("pipeline_feasibility_status", "unknown"))
            warning_text = str(audit_row.get("warnings", "Preflight completed."))
            company = resolved_company if symbol == primary_symbol else resolve_company_name(symbol, "")[0]
            run_rows.append(
                {
                    "symbol": symbol,
                    "company_name": company,
                    "status": status_value,
                    "input_csv": "-",
                    "reports_dir": str(stock_reports_dir(symbol)),
                    "results_dir": str(stock_results_dir(symbol)),
                    "market_impact_add_on": add_marketwide_peer_groups,
                    "dqn_groups": dqn_group_count,
                }
            )
            status_rows.append(
                {
                    "role": "experiment_target",
                    "symbol": symbol,
                    "company_name": company,
                    "status": "preflight_only",
                    "stage": status_value,
                    "last_update": datetime.now().strftime("%H:%M:%S"),
                    "message": warning_text,
                }
            )
        st.session_state["workflow_runs"] = run_rows
        st.session_state["workflow_symbols"] = []
        st.session_state["workflow_failures"] = []
        st.session_state["cross_payload"] = None
        st.session_state["workflow_phase_logs"] = []
        st.session_state["workflow_status_rows"] = status_rows
        st.session_state["workflow_export_bundle"] = create_export_bundle(
            run_rows,
            [],
            [],
            None,
            target_symbol=primary_symbol,
            market_impact_add_on=add_marketwide_peer_groups,
        )
        rerun_dashboard()

    if run_cross_existing_only:
        audit = run_dashboard_preflight(selected_symbols_for_cross, start_date, end_date, allow_fetch=False)
        if audit.empty:
            valid_symbols = selected_symbols_for_cross
            skipped_symbols: dict[str, str] = {}
        else:
            valid_symbols = []
            skipped_symbols = {}
            for _, item in audit.iterrows():
                symbol = str(item.get("symbol", ""))
                status_value = str(item.get("pipeline_feasibility_status", ""))
                is_start_gap = truthy_flag(item.get("unfillable_start_gap_suspected", False))
                has_local_outputs = status_value == "READY_LOCAL"
                if has_local_outputs and not is_start_gap:
                    valid_symbols.append(symbol)
                else:
                    skipped_symbols[symbol] = str(item.get("warnings", "Local outputs are not sufficient for existing-data-only comparison."))

        cross_payload: dict[str, object] | None = None
        failures = [{"symbol": symbol, "error": reason, "traceback": ""} for symbol, reason in skipped_symbols.items()]
        if len(valid_symbols) >= 2:
            cross_payload = build_peer_nlp_cross_stock_summary(selected_symbols=valid_symbols)
            write_peer_nlp_integrity_report(selected_symbols=valid_symbols)
        elif len(valid_symbols) < 2:
            st.warning("Existing-data-only cross analysis needs at least two locally ready stocks.")

        run_rows = []
        status_rows = []
        for symbol in selected_symbols_for_cross:
            company = resolved_company if symbol == primary_symbol else resolve_company_name(symbol, "")[0]
            is_valid = symbol in valid_symbols
            run_rows.append(
                {
                    "symbol": symbol,
                    "company_name": company,
                    "status": "included_local_cross" if is_valid else "skipped",
                    "input_csv": str(latest_non_master_csv(symbol) or "-"),
                    "reports_dir": str(stock_reports_dir(symbol)),
                    "results_dir": str(stock_results_dir(symbol)),
                    "market_impact_add_on": add_marketwide_peer_groups,
                    "dqn_groups": dqn_group_count,
                }
            )
            status_rows.append(
                {
                    "role": "experiment_target",
                    "symbol": symbol,
                    "company_name": company,
                    "status": "completed" if is_valid else "skipped",
                    "stage": "local_cross",
                    "last_update": datetime.now().strftime("%H:%M:%S"),
                    "message": "Included from existing local outputs" if is_valid else skipped_symbols.get(symbol, "Skipped by preflight."),
                }
            )

        st.session_state["workflow_runs"] = run_rows
        st.session_state["workflow_symbols"] = valid_symbols
        st.session_state["workflow_failures"] = failures
        st.session_state["cross_payload"] = cross_payload
        st.session_state["workflow_phase_logs"] = []
        st.session_state["workflow_status_rows"] = status_rows
        st.session_state["workflow_export_bundle"] = create_export_bundle(
            run_rows,
            valid_symbols,
            failures,
            cross_payload,
            target_symbol=primary_symbol,
            market_impact_add_on=add_marketwide_peer_groups,
        )
        rerun_dashboard()

    pipeline_runner = get_pipeline_runner()
    symbols_to_run = [primary_symbol]
    preflight_audit = run_dashboard_preflight(symbols_to_run, start_date, end_date, allow_fetch=run_ingestion_flag)
    blocked_symbols: dict[str, str] = {}
    if not preflight_audit.empty:
        for _, item in preflight_audit.iterrows():
            symbol = str(item.get("symbol", ""))
            if truthy_flag(item.get("unfillable_start_gap_suspected", False)):
                blocked_symbols[symbol] = (
                    f"研究开始日期早于这只股票的可用/上市交易历史 "
                    f"({item.get('local_data_start', 'unknown')} to {item.get('local_data_end', 'unknown')})，"
                    "请缩短研究区间或从 cross-stock 对比里排除它。"
                )
            elif truthy_flag(item.get("available_history_starts_after_requested_start", False)) and not run_ingestion_flag:
                blocked_symbols[symbol] = (
                    f"本地缓存从 {item.get('local_data_start', 'unknown')} 才开始，早于该日期的研究区间没有本地数据；"
                    "当前关闭了 Fetch/update data，所以不会尝试补齐。"
                )
            elif str(item.get("pipeline_feasibility_status", "")) == "FAILED":
                blocked_symbols[symbol] = str(item.get("warnings", "Preflight marked this stock as failed."))

    training_status_rows: list[dict[str, object]] = []
    training_progress = None
    training_status_box = None
    if run_cross_pipeline and supplement_sector_peers:
        st.info("Checking and supplementing same-sector training peer data before the held-out target experiment.")
        st.markdown("#### Training Peer Set Progress")
        st.caption("这些股票只作为 NLP training corpus，不作为本次 experiment target；目标股票会在 NLP 训练集中被排除。")
        training_status_rows = build_training_peer_status_rows(target_sector_peers, start_date, end_date)
        training_progress = st.progress(_status_progress(training_status_rows))
        training_status_box = st.empty()
        render_live_status_table(training_status_rows, training_status_box)

        def on_peer_bootstrap(stage: str, message: str) -> None:
            mentioned_symbol = normalize_symbol_for_path(message)
            if mentioned_symbol and any(normalize_symbol_for_path(str(row.get("symbol", ""))) == mentioned_symbol for row in training_status_rows):
                if stage == "sector_peer_fetch":
                    update_training_peer_row(training_status_rows, mentioned_symbol, status="fetching", stage=stage, message=message)
                elif stage == "sector_peer_fetch_failed":
                    update_training_peer_row(training_status_rows, mentioned_symbol, status="failed", stage=stage, message=message)
            else:
                for row in training_status_rows:
                    if row.get("status") == "pending_fetch":
                        row["stage"] = stage
                        row["last_update"] = datetime.now().strftime("%H:%M:%S")
                        row["message"] = message
            training_progress.progress(_status_progress(training_status_rows))
            render_live_status_table(training_status_rows, training_status_box)

        peer_bootstrap = ensure_sector_peer_data(
            target_symbols=symbols_to_run,
            start_date=str(start_date),
            end_date=str(end_date),
            sources=sources,
            news_count=news_count,
            allow_fetch=allow_fetch_missing_sector_peers,
            fetch_all_configured_peers=True,
            status_callback=on_peer_bootstrap,
        )
        readiness = peer_bootstrap.get("readiness", pd.DataFrame())
        finalize_training_peer_status_rows(training_status_rows, start_date, end_date, peer_bootstrap.get("fetch_log", pd.DataFrame()))
        training_progress.progress(_status_progress(training_status_rows))
        render_live_status_table(training_status_rows, training_status_box)
        st.session_state["training_status_rows"] = training_status_rows
        if isinstance(readiness, pd.DataFrame) and not readiness.empty:
            first = readiness.iloc[0].to_dict()
            st.success(
                f"Sector peer readiness: {first.get('sector', 'UNKNOWN')} -> "
                f"{first.get('status', 'UNKNOWN')}; local peers {first.get('local_stock_count_before_or_after', '-')}/"
                f"{first.get('required_total_stocks', '-')}"
            )
    else:
        st.session_state["training_status_rows"] = []

    progress = st.progress(0.0)
    status = st.empty()
    phase_box = st.empty()
    live_status_box = st.empty()
    run_rows: list[dict[str, object]] = []
    completed_symbols: list[str] = []
    failures: list[dict[str, str]] = []
    phase_logs: list[dict[str, str]] = []
    status_rows: list[dict[str, object]] = []

    for symbol in symbols_to_run:
        company = resolved_company if symbol == primary_symbol else resolve_company_name(symbol, "")[0]
        status_rows.append(
            {
                "role": "experiment_target",
                "symbol": symbol,
                "company_name": company,
                "status": "pending",
                "stage": "waiting",
                "last_update": datetime.now().strftime("%H:%M:%S"),
                "message": "Waiting to start",
            }
        )
    render_live_status_table(status_rows, live_status_box)

    for index, symbol in enumerate(symbols_to_run, start=1):
        company = resolved_company if symbol == primary_symbol else resolve_company_name(symbol, "")[0]
        if symbol in blocked_symbols:
            row = next(item for item in status_rows if item["symbol"] == symbol)
            row["status"] = "skipped"
            row["stage"] = "preflight"
            row["last_update"] = datetime.now().strftime("%H:%M:%S")
            row["message"] = blocked_symbols[symbol]
            failures.append({"symbol": symbol, "error": blocked_symbols[symbol], "traceback": ""})
            run_rows.append({"symbol": symbol, "company_name": company, "status": "skipped", "input_csv": "-", "reports_dir": "-", "results_dir": "-", "market_impact_add_on": add_marketwide_peer_groups, "dqn_groups": dqn_group_count})
            progress.progress(index / max(len(symbols_to_run), 1))
            render_live_status_table(status_rows, live_status_box)
            continue
        status.info(f"Running {experiment_label} experiment for `{symbol}` ({company or 'company name not provided'}) [{index}/{len(symbols_to_run)}]")
        row = next(item for item in status_rows if item["symbol"] == symbol)
        row["status"] = "running"
        row["stage"] = "starting"
        row["last_update"] = datetime.now().strftime("%H:%M:%S")
        row["message"] = f"Starting {experiment_label} experiment {index}/{len(symbols_to_run)}"
        render_live_status_table(status_rows, live_status_box)
        try:
            def on_status(stage: str, message: str, *, _symbol: str = symbol) -> None:
                phase_logs.append(
                    {
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                        "symbol": _symbol,
                        "stage": stage,
                        "message": message,
                    }
                )
                current_row = next(item for item in status_rows if item["symbol"] == _symbol)
                training_stage_names = {
                    "peer_nlp",
                    "peer_nlp_training_corpus",
                    "peer_nlp_model_training",
                    "peer_nlp_peer_processing",
                    "peer_nlp_peer_processed",
                    "peer_nlp_peer_skipped",
                }
                if stage in training_stage_names:
                    current_row["status"] = "waiting"
                    current_row["stage"] = "waiting_for_peer_training"
                    current_row["message"] = "Waiting while the training peer corpus is processed before target scoring/DQN."
                else:
                    current_row["status"] = "running"
                    current_row["stage"] = stage
                    current_row["message"] = message
                current_row["last_update"] = datetime.now().strftime("%H:%M:%S")
                render_live_status_table(status_rows, live_status_box)
                phase_box.dataframe(pd.DataFrame(phase_logs[-12:]), use_container_width=True, hide_index=True)
                lower_message = message.lower()
                if training_status_rows and stage in {"peer_nlp", "peer_nlp_training_corpus", "peer_nlp_model_training"}:
                    if stage == "peer_nlp_training_corpus" or "running official" in lower_message:
                        update_all_training_peer_rows(
                            training_status_rows,
                            status="processing_corpus",
                            stage="peer_nlp_training_corpus",
                            message="Peer news is being assembled and used to train/score the held-out target sentiment.",
                            only_statuses={"ready_local", "fetched", "ready"},
                        )
                    elif stage == "peer_nlp_model_training":
                        update_all_training_peer_rows(
                            training_status_rows,
                            status="training_nlp",
                            stage=stage,
                            message=message,
                            only_statuses={"processed", "processing_corpus", "ready_local", "fetched", "ready"},
                        )
                    elif "saved official" in lower_message:
                        update_all_training_peer_rows(
                            training_status_rows,
                            status="processed",
                            stage="peer_nlp_sentiment_ready",
                            message="Peer corpus processing finished; target peer sentiment files were saved.",
                            only_statuses={"training_nlp", "processing", "processing_corpus", "ready_local", "fetched", "ready"},
                        )
                    if training_progress is not None:
                        training_progress.progress(_status_progress(training_status_rows))
                    if training_status_box is not None:
                        render_live_status_table(training_status_rows, training_status_box)
                    st.session_state["training_status_rows"] = training_status_rows
                elif training_status_rows and stage in {"peer_nlp_peer_processing", "peer_nlp_peer_processed", "peer_nlp_peer_skipped"}:
                    mentioned_symbol = normalize_symbol_for_path(message)
                    if mentioned_symbol:
                        if stage == "peer_nlp_peer_processing":
                            update_training_peer_row(training_status_rows, mentioned_symbol, status="processing", stage=stage, message=message)
                        elif stage == "peer_nlp_peer_processed":
                            update_training_peer_row(training_status_rows, mentioned_symbol, status="processed", stage=stage, message=message)
                        else:
                            update_training_peer_row(training_status_rows, mentioned_symbol, status="skipped", stage=stage, message=message)
                    if training_progress is not None:
                        training_progress.progress(_status_progress(training_status_rows))
                    if training_status_box is not None:
                        render_live_status_table(training_status_rows, training_status_box)
                    st.session_state["training_status_rows"] = training_status_rows
                elif training_status_rows and stage == "peer_nlp_sentiment_saved":
                    update_all_training_peer_rows(
                        training_status_rows,
                        status="processed",
                        stage="peer_nlp_sentiment_ready",
                        message="Peer corpus processing finished; target peer sentiment files were saved.",
                        only_statuses={"training_nlp", "processing", "processing_corpus", "ready_local", "fetched", "ready"},
                    )
                    if training_progress is not None:
                        training_progress.progress(_status_progress(training_status_rows))
                    if training_status_box is not None:
                        render_live_status_table(training_status_rows, training_status_box)
                    st.session_state["training_status_rows"] = training_status_rows
                elif training_status_rows and stage in {"signals", "rl"}:
                    update_all_training_peer_rows(
                        training_status_rows,
                        status="processed",
                        stage="peer_nlp_sentiment_ready",
                        message="Training peer corpus has already been processed and is feeding the experiment target.",
                        only_statuses={"training_nlp", "processing_corpus", "ready_local", "fetched", "ready"},
                    )
                    if training_progress is not None:
                        training_progress.progress(_status_progress(training_status_rows))
                    if training_status_box is not None:
                        render_live_status_table(training_status_rows, training_status_box)
                    st.session_state["training_status_rows"] = training_status_rows

            summary = pipeline_runner(
                symbol=symbol,
                company_name=company,
                start_date=str(start_date),
                end_date=str(end_date),
                sources=sources,
                news_count=news_count,
                run_ingestion_flag=run_ingestion_flag,
                run_nlp_flag=True,
                run_rl_flag=True,
                run_ablation_flag=True,
                episodes=episodes,
                use_sqlite=use_sqlite,
                reuse_existing_csv=reuse_existing_csv,
                require_news=require_news,
                build_cross_stock_outputs=False,
                status_callback=on_status,
                run_high_density_ablation=run_high_density_ablation,
                run_peer_nlp_experiment=run_peer_nlp_experiment,
                run_legacy_stock_level_nlp=run_legacy_stock_level_nlp,
                allow_fetch_missing_sector_peers=allow_fetch_missing_sector_peers,
                include_marketwide_peer=add_marketwide_peer_groups,
                run_market_impact_nlp=run_market_impact_nlp,
                market_impact_horizon_days=market_impact_horizon_days,
                market_impact_pos_threshold=market_impact_pos_threshold,
                market_impact_neg_threshold=market_impact_neg_threshold,
            )
            completed_symbols.append(symbol)
            run_rows.append(
                {
                    "symbol": symbol,
                    "company_name": company,
                    "status": "completed",
                    "input_csv": summary["input_csv"],
                    "reports_dir": summary["reports_dir"],
                    "results_dir": summary["results_dir"],
                    "market_impact_add_on": add_marketwide_peer_groups,
                    "dqn_groups": dqn_group_count,
                }
            )
            row["status"] = "completed"
            row["stage"] = "done"
            row["last_update"] = datetime.now().strftime("%H:%M:%S")
            row["message"] = "Completed successfully"
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc), "traceback": traceback.format_exc(limit=8)})
            run_rows.append({"symbol": symbol, "company_name": company, "status": "failed", "input_csv": "-", "reports_dir": "-", "results_dir": "-", "market_impact_add_on": add_marketwide_peer_groups, "dqn_groups": dqn_group_count})
            row["status"] = "failed"
            row["stage"] = "error"
            row["last_update"] = datetime.now().strftime("%H:%M:%S")
            row["message"] = str(exc)
        progress.progress(index / max(len(symbols_to_run), 1))
        render_live_status_table(status_rows, live_status_box)

    cross_payload: dict[str, object] | None = None
    market_cross_payload: dict[str, object] | None = None
    if run_cross_analysis and completed_symbols:
        status.info("Building target experiment comparison summary from the completed output.")
        for row in status_rows:
            if row["symbol"] in completed_symbols:
                row["stage"] = "experiment_summary"
                row["message"] = "Waiting for experiment summary generation"
                row["last_update"] = datetime.now().strftime("%H:%M:%S")
        render_live_status_table(status_rows, live_status_box)
        cross_payload = build_peer_nlp_cross_stock_summary(selected_symbols=completed_symbols)
        write_peer_nlp_integrity_report(selected_symbols=completed_symbols)
        if run_market_impact_nlp:
            market_cross_payload = build_market_impact_cross_stock_summary(selected_symbols=completed_symbols)
        for row in status_rows:
            if row["symbol"] in completed_symbols:
                row["stage"] = "done"
                row["message"] = "Included in target experiment summary"
                row["last_update"] = datetime.now().strftime("%H:%M:%S")
        render_live_status_table(status_rows, live_status_box)
    elif run_cross_analysis and not completed_symbols:
        status.warning("No target stock completed successfully, so no experiment summary was generated.")
    else:
        status.success("Workflow completed.")

    st.session_state["workflow_runs"] = run_rows
    st.session_state["workflow_symbols"] = completed_symbols
    st.session_state["workflow_failures"] = failures
    st.session_state["cross_payload"] = cross_payload
    st.session_state["market_cross_payload"] = market_cross_payload
    st.session_state["workflow_phase_logs"] = phase_logs
    st.session_state["workflow_status_rows"] = status_rows
    st.session_state["workflow_export_bundle"] = create_export_bundle(
        run_rows,
        completed_symbols,
        failures,
        cross_payload,
        target_symbol=primary_symbol,
        market_cross_payload=market_cross_payload,
        market_impact_add_on=add_marketwide_peer_groups,
    )

st.subheader("Target Experiment Run Status")
workflow_runs = st.session_state.get("workflow_runs", [])
workflow_failures = st.session_state.get("workflow_failures", [])
completed_symbols = st.session_state.get("workflow_symbols", [])
cross_payload = st.session_state.get("cross_payload")
market_cross_payload = st.session_state.get("market_cross_payload")
workflow_phase_logs = st.session_state.get("workflow_phase_logs", [])
workflow_status_rows = st.session_state.get("workflow_status_rows", [])
training_status_rows = st.session_state.get("training_status_rows", [])
workflow_export_bundle = st.session_state.get("workflow_export_bundle")
workflow_has_official_market_impact = bool(workflow_runs)

render_experiment_completion_notice(
    workflow_runs,
    workflow_failures,
    completed_symbols,
    include_market_impact=workflow_has_official_market_impact,
)

if training_status_rows:
    st.markdown("#### Training Peer Set Progress")
    st.caption("训练组只用于构建 sector-peer NLP corpus；它们不是本次 held-out experiment set。")
    st.progress(_status_progress(training_status_rows))
    render_live_status_table(training_status_rows, st)

if workflow_status_rows:
    st.markdown("#### Experiment Set Progress")
    st.caption("左侧输入的股票是唯一 held-out experiment set。这里显示目标股票从 ingestion、peer NLP 到 DQN/RL 的运行状态。")
    render_live_status_table(workflow_status_rows, st)

if workflow_runs:
    st.dataframe(pd.DataFrame(workflow_runs), use_container_width=True)
    if workflow_phase_logs:
        with st.expander("Step-by-step experiment log", expanded=False):
            st.dataframe(pd.DataFrame(workflow_phase_logs), use_container_width=True, hide_index=True)
    if workflow_failures:
        with st.expander("Failed runs"):
            for failure in workflow_failures:
                st.error(f"{failure['symbol']}: {failure['error']}")
                st.code(failure["traceback"])
    if market_cross_payload:
        with st.expander("Market-impact summary outputs", expanded=False):
            market_summary = safe_read_csv(Path(str(market_cross_payload.get("summary_csv", ""))))
            if market_summary.empty:
                st.info("Market-impact summary was generated, but no valid rows are available yet.")
            else:
                st.dataframe(market_summary, use_container_width=True, hide_index=True)
    if workflow_export_bundle:
        st.markdown("#### Download Experiment Outputs")
        st.caption("下载包只包含本次 target 实验的描述性结果、官方 peer_nlp / market_impact 表格和可视化 HTML；不会打包旧 stock-level NLP 结果。")
        export_cols = st.columns(3)
        summary_md_path = Path(str(workflow_export_bundle["summary_md"]))
        zip_path = Path(str(workflow_export_bundle["zip_path"]))
        export_cols[0].download_button(
            "Download run summary markdown",
            data=summary_md_path.read_bytes(),
            file_name=summary_md_path.name,
            mime="text/markdown",
            use_container_width=True,
        )
        export_cols[1].download_button(
            "Download run bundle zip",
            data=zip_path.read_bytes(),
            file_name=zip_path.name,
            mime="application/zip",
            use_container_width=True,
        )
        visual_path = workflow_export_bundle.get("visual_html")
        if visual_path:
            visual_html_path = Path(str(visual_path))
            if visual_html_path.exists():
                export_cols[2].download_button(
                    "Download visual report HTML",
                    data=visual_html_path.read_bytes(),
                    file_name=visual_html_path.name,
                    mime="text/html",
                    use_container_width=True,
                )
else:
    st.info("还没有运行新的 target peer experiment。下方会只浏览当前输入 target 的现有 peer_nlp outputs。")

available_symbols_for_view = completed_symbols or [primary_symbol]
if available_symbols_for_view:
    st.markdown("## Selected Stock Dashboard")
    review_cols = st.columns([3, 1])
    inspect_symbol = review_cols[0].selectbox(
        "Select target stock",
        available_symbols_for_view,
        index=max(len(available_symbols_for_view) - 1, 0),
        format_func=format_symbol_with_sector,
    )
    if review_cols[1].button("Refresh / reload", use_container_width=True):
        rerun_dashboard()
    render_result_review_dashboard(inspect_symbol)
else:
    st.warning("No stock outputs are available yet.")

if False and cross_payload:
    summary_df = cross_payload.get("summary", pd.DataFrame())
    discussion_md = cross_payload.get("discussion_md")
    render_cross_stock_outputs(summary_df, Path(discussion_md) if discussion_md else None)
elif False and run_cross_analysis:
    existing_summary = SYSTEM_OUTPUT_DIR / "cross_stock_summary.csv"
    existing_discussion = SYSTEM_OUTPUT_DIR / "cross_stock_discussion.md"
    if existing_summary.exists():
        render_cross_stock_outputs(pd.read_csv(existing_summary), existing_discussion if existing_discussion.exists() else None)
