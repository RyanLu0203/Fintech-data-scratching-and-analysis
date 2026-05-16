"""Interactive control-center dashboard for the NLP-RL trading platform."""

from __future__ import annotations

from datetime import datetime
import html
import importlib
import json
import shutil
import sys
import traceback
import zipfile
from pathlib import Path

PROJECT_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_IMPORTS))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pandas.errors import EmptyDataError, ParserError

from src.config.paths import STOCK_OUTPUT_ROOT, SYSTEM_OUTPUT_DIR, normalize_symbol_for_path, stock_data_dir, stock_reports_dir, stock_results_dir
from src.config.settings import PROJECT_ROOT, settings
from src.data_ingestion.cache import build_master_csv, resolve_cached_csv
from src.evaluation.cross_stock import build_cross_stock_summary
from src.evaluation.feasibility_audit import run_feasibility_audit
from src.evaluation.information_density import generate_information_density_outputs
from src.evaluation.market_impact_ablation import build_market_impact_cross_stock_summary
from src.evaluation.peer_nlp_ablation import build_peer_nlp_cross_stock_summary, write_peer_nlp_integrity_report
from src.evaluation.sector_peer_bootstrap import ensure_sector_peer_data
from src.features.money_flow import compute_daily_net_flow


st.set_page_config(page_title="Peer NLP Trading Experiment", layout="wide")
st.title("Peer NLP Transfer Trading Experiment")
st.caption("支持 held-out target stock + peer sentiment NLP / market-impact NLP + DQN cross analysis。")


PALETTE = {
    "ink": "#3f3157",
    "plum": "#604771",
    "lavender": "#846992",
    "rose": "#995a70",
    "mauve": "#d5bfc2",
    "cream": "#e7d5be",
    "white": "#ffffff",
    "grid": "rgba(231, 213, 190, 0.55)",
}
SERIES_COLORS = [PALETTE["ink"], PALETTE["plum"], PALETTE["lavender"], PALETTE["rose"]]
EXPERIMENT_COLORS = {
    "buy_and_hold": PALETTE["ink"],
    "dqn_without_nlp": PALETTE["plum"],
    "dqn_with_nlp": PALETTE["rose"],
    "dqn_with_sector_peer_nlp": PALETTE["lavender"],
    "dqn_with_marketwide_peer_nlp": PALETTE["rose"],
}
CONSUMER_ELECTRONICS_EXAMPLE_SYMBOLS = ["002475", "002241", "300433", "300136", "601138", "601231"]
CONSUMER_ELECTRONICS_EXAMPLE_LABEL = "Consumer electronics example: Luxshare / GoerTek / Lens / Sunway / FII / USI"
PLOT_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "doubleClick": False,
    "responsive": True,
}
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
    "peer_nlp_train_eval_windows.csv",
    "market_impact_state_vector_compliance.csv",
    "market_impact_group_state_diagnostics.csv",
    "market_impact_leakage_diagnostics.csv",
    "market_impact_train_eval_windows.csv",
    "market_impact_reliability_check.csv",
    "market_impact_report_section.md",
]
OFFICIAL_SYSTEM_FILES = [
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
            background: {PALETTE["ink"]};
            color: white;
            border: 1px solid {PALETTE["ink"]};
            border-radius: 8px;
        }}
        .stButton > button:hover, .stDownloadButton > button:hover {{
            background: {PALETTE["plum"]};
            border-color: {PALETTE["plum"]};
            color: white;
        }}
        button[kind="primary"] {{
            background: {PALETTE["rose"]} !important;
            border-color: {PALETTE["rose"]} !important;
        }}
        button[kind="primary"]:hover {{
            background: {PALETTE["plum"]} !important;
            border-color: {PALETTE["plum"]} !important;
        }}
        [data-baseweb="tab-list"] {{
            gap: 0.35rem;
        }}
        [data-baseweb="tab"] {{
            color: {PALETTE["plum"]};
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


def _render_plot(fig: go.Figure) -> None:
    st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)


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
    marker_color = color or PALETTE["plum"]
    if color_by_sign:
        marker_color = [PALETTE["lavender"] if value >= 0 else PALETTE["rose"] for value in chart[y].tolist()]
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
                marker={"size": 10, "color": PALETTE["plum"]},
                name=y,
            )
        )
    fig.update_layout(**_base_layout(title=title, height=height))
    _render_plot(fig)


def get_pipeline_runner():
    for module_name in [
        "src.nlp.peer_sentiment",
        "src.evaluation.peer_nlp_ablation",
        "main",
    ]:
        module = importlib.import_module(module_name)
        importlib.reload(module)
    main_module = importlib.import_module("main")
    return main_module.run_pipeline_for_symbol


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
    peer_seed_metrics = bundle.get("peer_seed_metrics", pd.DataFrame())
    peer_curves = bundle.get("peer_curves", pd.DataFrame())
    peer_drawdowns = bundle.get("peer_drawdowns", pd.DataFrame())
    peer_logs = bundle.get("peer_logs", pd.DataFrame())
    peer_effect = bundle.get("peer_effect", pd.DataFrame())
    peer_integrity = bundle.get("peer_integrity", pd.DataFrame())

    official_available = isinstance(peer_metrics, pd.DataFrame) and not peer_metrics.empty
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
            "Official peer-sector NLP outputs are missing for this stock. "
            "Legacy stock-level NLP is not displayed as the main result; use the workflow controls to generate peer_nlp_* outputs."
        )
    else:
        st.success("Official current experiment loaded: Peer-Sector NLP Transfer. Legacy stock-level NLP is excluded from the main view.")

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
                    "sector_sentiment_score": PALETTE["lavender"],
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
            render_series_bar(sentiment, "date", news_count_col, color=PALETTE["lavender"], height=340)

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
            render_series_bar(action_counts, "action", "count", color=PALETTE["plum"], height=300)
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
                    "sector_final_equity_effect": PALETTE["lavender"],
                    "marketwide_final_equity_effect": PALETTE["rose"],
                }, height=360)
                st.caption("Positive values mean the peer NLP DQN beat the no-NLP DQN in final equity.")
        with peer_tabs[1]:
            cols = [col for col in ["sector_sharpe_effect", "marketwide_sharpe_effect"] if col in summary_df.columns]
            if cols:
                render_multi_series(summary_df.set_index(symbol_col)[cols], kind="bar", color_map={
                    "sector_sharpe_effect": PALETTE["lavender"],
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
                render_series_bar(summary_df, symbol_col, "target_sentiment_coverage", color=PALETTE["lavender"], height=320)
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
                "dqn_without_nlp_final_equity": PALETTE["plum"],
                "dqn_with_nlp_final_equity": PALETTE["rose"],
            }, height=360)
    with cross_tabs[1]:
        cols = [col for col in ["buy_and_hold_cumulative_return", "dqn_without_nlp_cumulative_return", "dqn_with_nlp_cumulative_return"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "buy_and_hold_cumulative_return": PALETTE["ink"],
                "dqn_without_nlp_cumulative_return": PALETTE["plum"],
                "dqn_with_nlp_cumulative_return": PALETTE["rose"],
            }, height=360)
    with cross_tabs[2]:
        cols = [col for col in ["buy_and_hold_sharpe", "dqn_without_nlp_sharpe", "dqn_with_nlp_sharpe"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "buy_and_hold_sharpe": PALETTE["ink"],
                "dqn_without_nlp_sharpe": PALETTE["plum"],
                "dqn_with_nlp_sharpe": PALETTE["rose"],
            }, height=360)
    with cross_tabs[3]:
        cols = [col for col in ["nlp_final_equity_effect", "nlp_sharpe_effect"] if col in summary_df.columns]
        if cols:
            render_multi_series(summary_df.set_index("symbol")[cols], kind="bar", color_map={
                "nlp_final_equity_effect": PALETTE["rose"],
                "nlp_sharpe_effect": PALETTE["lavender"],
            }, height=360)
    with cross_tabs[4]:
        if "sentiment_coverage_ratio" in summary_df.columns:
            render_series_bar(summary_df, "symbol", "sentiment_coverage_ratio", color=PALETTE["lavender"], height=320)
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
        {"item": "Official peer NLP outputs", "status": status_value(isinstance(peer_metrics, pd.DataFrame) and not peer_metrics.empty), "detail": "outputs/stocks/<symbol>/results/peer_nlp_ablation_metrics.csv"},
        {"item": "Peer NLP effect summary", "status": status_value(isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty), "detail": "peer_nlp_effect_summary.csv"},
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
        f"Current official experiment: Peer-Sector NLP Transfer. Target `{symbol}` "
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
                    "experiment": "dqn_with_sector_peer_nlp",
                    "description": "NLP is trained on same-sector peer news, excluding the target stock, then scores target news.",
                },
                {
                    "group": "Group 2",
                    "experiment": "dqn_with_marketwide_peer_nlp",
                    "description": "NLP is trained on all available A-share peer news, excluding the target stock, then scores target news.",
                },
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )
    render_status_badges(stock_status_rows(symbol, bundle))
    data = bundle.get("data", pd.DataFrame())
    sentiment = bundle.get("peer_daily", pd.DataFrame())
    metrics = bundle.get("peer_metrics", pd.DataFrame())
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
            render_series_bar(daily, "date", "daily_news_count", title="Daily news count", color=PALETTE["lavender"], height=340)
        with chart_tabs[1]:
            pct_col = "recent_cumulative_news_pct" if "recent_cumulative_news_pct" in daily.columns else "cumulative_news_pct"
            render_series_line(daily, "date", pct_col, title="Cumulative news percentage", color=PALETTE["rose"], height=340)
        with chart_tabs[2]:
            if "news_available" in daily.columns:
                daily["rolling_coverage_20d"] = pd.to_numeric(daily["news_available"], errors="coerce").fillna(0).rolling(20, min_periods=1).mean()
                render_series_line(daily, "date", "rolling_coverage_20d", title="20-day news coverage", color=PALETTE["plum"], height=340)
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
        st.dataframe(metrics, use_container_width=True)
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
    """Official current experiment page for one held-out target stock."""
    code = normalize_symbol_for_path(symbol)
    st.subheader("Peer Cross Analysis")
    st.caption("官方当前实验：目标股票不参与自己的 NLP 训练；只比较 no-NLP、sector-peer NLP、marketwide-peer NLP 三组。")
    classification = render_target_classification(symbol, bundle)
    st.caption(
        f"Target `{classification['symbol']}` belongs to sector `{classification['sector']}` "
        f"and industry `{classification['industry']}`. Sector-peer corpus must exclude this target stock."
    )

    experiment_table = pd.DataFrame(
        [
            {
                "group": "Group 0",
                "experiment": "dqn_without_nlp",
                "state_feature": "[price, MA50, MA200, RSI, MACD, position, cash]",
                "meaning": "只用滞后一日的市场/技术状态，是检验 NLP 是否有增益的基准 DQN。",
            },
            {
                "group": "Group 1",
                "experiment": "dqn_with_sector_peer_nlp",
                "state_feature": "Group 0 + sector_sentiment_score",
                "meaning": "用同板块其他股票新闻训练 NLP，排除目标股票，再给目标股票新闻打分。",
            },
            {
                "group": "Group 2",
                "experiment": "dqn_with_marketwide_peer_nlp",
                "state_feature": "Group 0 + marketwide_sentiment_score",
                "meaning": "用全市场其他 A 股新闻训练 NLP，排除目标股票，再给目标股票新闻打分。",
            },
        ]
    )
    st.dataframe(experiment_table, use_container_width=True, hide_index=True)

    corpus = safe_read_csv(PROJECT_ROOT / "reports" / "tables" / "peer_nlp_corpus_summary.csv", dtype=str)
    target_corpus = pd.DataFrame()
    if not corpus.empty and "target_symbol" in corpus.columns:
        target_corpus = corpus[corpus["target_symbol"].astype(str).apply(normalize_symbol_for_path) == code].copy()
    if not target_corpus.empty:
        st.markdown("#### Peer NLP Corpus")
        display_cols = [
            col
            for col in [
                "target_symbol",
                "target_company_name",
                "target_sector",
                "corpus_type",
                "number_of_peer_stocks",
                "total_news_count",
                "date_start",
                "date_end",
                "corpus_status",
                "high_density_only",
                "included_symbols",
                "excluded_symbols",
            ]
            if col in target_corpus.columns
        ]
        st.dataframe(target_corpus[display_cols], use_container_width=True, hide_index=True)
        sector_row = target_corpus[target_corpus["corpus_type"].astype(str) == "sector_peer"]
        market_row = target_corpus[target_corpus["corpus_type"].astype(str) == "marketwide_peer"]
        corpus_cols = st.columns(4)
        corpus_cols[0].metric("Sector corpus", _first_value(sector_row, "corpus_status", "-"))
        corpus_cols[1].metric("Sector peers", _first_value(sector_row, "number_of_peer_stocks", "-"))
        corpus_cols[2].metric("Marketwide corpus", _first_value(market_row, "corpus_status", "-"))
        corpus_cols[3].metric("Marketwide peers", _first_value(market_row, "number_of_peer_stocks", "-"))
    else:
        st.warning("No peer corpus summary found for this target. Run the peer cross workflow or supplement sector peers first.")

    peer_daily = bundle.get("peer_daily", pd.DataFrame())
    if isinstance(peer_daily, pd.DataFrame) and not peer_daily.empty:
        st.markdown("#### Peer-Trained Sentiment on Target News")
        peer_daily = peer_daily.copy()
        peer_daily["date"] = pd.to_datetime(peer_daily.get("date"), errors="coerce")
        score_cols = [col for col in ["sector_sentiment_score", "marketwide_sentiment_score"] if col in peer_daily.columns]
        if score_cols:
            sentiment_frame = peer_daily.set_index("date")[score_cols].apply(pd.to_numeric, errors="coerce")
            render_multi_series(
                sentiment_frame,
                title="Sector-peer vs marketwide-peer sentiment scores",
                kind="line",
                color_map={"sector_sentiment_score": PALETTE["lavender"], "marketwide_sentiment_score": PALETTE["rose"]},
                height=340,
            )
            st.caption("这张图表示 peer-trained NLP 对目标股票近端新闻的每日情绪打分；没有新闻的交易日不被当作真实中性新闻。")
        if "target_news_count" in peer_daily.columns:
            peer_daily["target_news_count"] = pd.to_numeric(peer_daily["target_news_count"], errors="coerce").fillna(0)
            render_series_bar(peer_daily, "date", "target_news_count", title="Target stock daily news count", color=PALETTE["plum"], height=300)
            st.caption("这张图显示目标股票在测试窗口内实际可被 NLP 打分的新闻密度。")
    else:
        st.warning("`peer_nlp_daily_sentiment.csv` is missing for this stock.")

    metrics = bundle.get("peer_metrics", pd.DataFrame())
    if isinstance(metrics, pd.DataFrame) and not metrics.empty:
        st.markdown("#### Official Peer NLP Ablation Metrics")
        metrics = metrics.copy()
        st.dataframe(metrics, use_container_width=True, hide_index=True)
        metric_cols = [col for col in ["final_equity", "cumulative_return", "sharpe_ratio", "max_drawdown"] if col in metrics.columns]
        if "experiment" in metrics.columns and metric_cols:
            chart_metrics = metrics.set_index("experiment")[metric_cols].apply(pd.to_numeric, errors="coerce")
            render_multi_series(chart_metrics, title="Ablation metrics by official strategy", kind="bar", color_map=EXPERIMENT_COLORS, height=360)
            st.caption("同一目标股票、同一训练/测试窗口、同一 DQN 设置下，比较是否加入 peer-trained NLP sentiment 后表现改善。")
    else:
        st.warning("`peer_nlp_ablation_metrics.csv` is missing for this stock.")

    curves = bundle.get("peer_curves", pd.DataFrame())
    if isinstance(curves, pd.DataFrame) and not curves.empty and {"date", "experiment", "portfolio_value"}.issubset(curves.columns):
        curves = curves.copy()
        curves["date"] = pd.to_datetime(curves["date"], errors="coerce")
        curves["portfolio_value"] = pd.to_numeric(curves["portfolio_value"], errors="coerce")
        curve_frame = curves.pivot_table(index="date", columns="experiment", values="portfolio_value", aggfunc="mean")
        render_multi_series(curve_frame, title="Portfolio value curves", kind="line", color_map=EXPERIMENT_COLORS, height=360)
        st.caption("这张图是三种策略的测试期资产曲线；如果曲线完全平坦或交易次数为 0，结果应标记为不可靠。")
    else:
        st.warning("`peer_nlp_portfolio_curves.csv` is missing or incomplete for this stock.")

    logs = bundle.get("peer_logs", pd.DataFrame())
    if isinstance(logs, pd.DataFrame) and not logs.empty and {"experiment", "action"}.issubset(logs.columns):
        st.markdown("#### Action Distribution")
        counts = logs.copy()
        counts["experiment"] = counts["experiment"].astype(str)
        counts["action"] = counts["action"].astype(str)
        action_frame = counts.groupby(["action", "experiment"]).size().unstack(fill_value=0)
        render_multi_series(action_frame, title="Buy / Sell / Hold counts", kind="bar", color_map=EXPERIMENT_COLORS, height=320)
        st.caption("这张图检查 DQN 是否真的产生交易行为。若几乎全是 Hold，NLP 效果通常不应强解释。")

    peer_effect = bundle.get("peer_effect", pd.DataFrame())
    if isinstance(peer_effect, pd.DataFrame) and not peer_effect.empty:
        st.markdown("#### NLP Effect Summary for This Target")
        st.dataframe(peer_effect, use_container_width=True, hide_index=True)

    cross_summary = safe_read_csv(SYSTEM_OUTPUT_DIR / "peer_nlp_cross_stock_summary.csv")
    if not cross_summary.empty and "target_symbol" in cross_summary.columns:
        target_summary = cross_summary[cross_summary["target_symbol"].astype(str).apply(normalize_symbol_for_path) == code].copy()
        if not target_summary.empty:
            st.markdown("#### Cross-Stock Summary Row for This Target")
            st.dataframe(target_summary, use_container_width=True, hide_index=True)
            effect_cols = [col for col in ["sector_final_equity_effect", "marketwide_final_equity_effect", "sector_sharpe_effect", "marketwide_sharpe_effect"] if col in target_summary.columns]
            if effect_cols:
                effects = target_summary.set_index("target_symbol")[effect_cols].apply(pd.to_numeric, errors="coerce")
                render_multi_series(effects, title="Peer NLP effects for selected target", kind="bar", height=320)
                st.caption("正值代表该 peer NLP 组相对 no-NLP DQN 更好；负值代表表现变差。")

    integrity = bundle.get("peer_integrity", pd.DataFrame())
    if isinstance(integrity, pd.DataFrame) and not integrity.empty:
        with st.expander("Peer NLP integrity checks", expanded=False):
            st.dataframe(integrity, use_container_width=True, hide_index=True)


def render_result_review_dashboard(symbol: str) -> None:
    bundle = load_stock_bundle(symbol)
    tabs = st.tabs(["Single Stock Basics", "Scraping Density", "Peer Cross Analysis"])
    with tabs[0]:
        render_single_stock_basic_page(symbol, bundle)
    with tabs[1]:
        render_information_density_page(bundle)
    with tabs[2]:
        render_peer_cross_analysis_for_symbol(symbol, bundle)


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


def create_visual_report_html(target_symbol: str, completed_symbols: list[str], failures: list[dict[str, str]], cross_payload: dict[str, object] | None, output_dir: Path) -> Path:
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
            "Sector-peer NLP uses same-sector peers, and marketwide-peer NLP uses all other available A-share peers."
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
        "</tbody></table>",
    ]

    data = bundle.get("data", pd.DataFrame())
    density = bundle.get("daily_density", pd.DataFrame())
    net_flow = bundle.get("daily_net_flow", pd.DataFrame())
    peer_daily = bundle.get("peer_daily", pd.DataFrame())
    metrics = bundle.get("peer_metrics", pd.DataFrame())
    curves = bundle.get("peer_curves", pd.DataFrame())
    effect = bundle.get("peer_effect", pd.DataFrame())

    html_parts.append("<h2>Single Stock Basics</h2>")
    first_chart = True
    if isinstance(data, pd.DataFrame):
        first_chart = not _add_html_line_chart(html_parts, data, "date", "close", "Close price", color=PALETTE["ink"], include_plotlyjs=first_chart)
    if isinstance(net_flow, pd.DataFrame):
        flow_col = "net_flow_cny_million" if "net_flow_cny_million" in net_flow.columns else "net_flow" if "net_flow" in net_flow.columns else ""
        if flow_col:
            first_chart = not _add_html_bar_chart(html_parts, net_flow, "date", flow_col, "Daily net inflow / outflow", color=PALETTE["plum"], include_plotlyjs=first_chart)

    html_parts.append("<h2>Scraping Density</h2>")
    if isinstance(density, pd.DataFrame) and "news_count" in density.columns:
        first_chart = not _add_html_bar_chart(html_parts, density, "date", "news_count", "Daily news density", color=PALETTE["lavender"], include_plotlyjs=first_chart)
    elif isinstance(peer_daily, pd.DataFrame) and "target_news_count" in peer_daily.columns:
        first_chart = not _add_html_bar_chart(html_parts, peer_daily, "date", "target_news_count", "Target news count in evaluation window", color=PALETTE["lavender"], include_plotlyjs=first_chart)

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
        fig.update_layout(**_base_layout(title="Portfolio value comparison", height=360))
        html_parts.append(_figure_to_html(fig, include_plotlyjs=first_chart))
        first_chart = False
    if isinstance(metrics, pd.DataFrame) and not metrics.empty:
        html_parts.append("<h3>Peer NLP ablation metrics</h3>")
        html_parts.append(metrics.to_html(index=False, escape=True))
    if isinstance(effect, pd.DataFrame) and not effect.empty:
        html_parts.append("<h3>Peer NLP effect summary</h3>")
        html_parts.append(effect.to_html(index=False, escape=True))
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
    visual_html_path = create_visual_report_html(target_for_report, completed_symbols, failures, cross_payload, bundle_dir) if target_for_report else None

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


def render_live_status_table(status_rows: list[dict[str, object]], container) -> None:
    if not status_rows:
        return
    frame = pd.DataFrame(status_rows)
    preferred = ["role", "symbol", "company_name", "status", "stage", "last_update", "message"]
    columns = [col for col in preferred if col in frame.columns]
    container.dataframe(frame[columns], use_container_width=True, hide_index=True)


def _status_progress(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 1.0
    complete_states = {"ready_local", "ready", "fetched", "processed", "completed", "skipped"}
    done = sum(1 for row in rows if str(row.get("status", "")).lower() in complete_states)
    return min(max(done / len(rows), 0.0), 1.0)


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
            st.success("Preflight passed: the target stock has enough local/requested-range data to start the peer experiment.")
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
st.sidebar.caption("当前 dashboard 支持 held-out peer sentiment transfer 和 peer market-impact transfer 实验。")
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

experiment_mode = st.sidebar.radio(
    "Experiment type",
    options=["peer_sentiment", "market_impact"],
    format_func=lambda value: {
        "peer_sentiment": "Peer sentiment NLP",
        "market_impact": "Market-impact NLP",
    }.get(value, value),
    horizontal=False,
)
run_market_impact_nlp = experiment_mode == "market_impact"
experiment_label = "market-impact NLP" if run_market_impact_nlp else "peer sentiment NLP"
market_impact_horizon_days = 3
market_impact_pos_threshold = 0.015
market_impact_neg_threshold = -0.015
if run_market_impact_nlp:
    with st.sidebar.expander("Market-impact label settings", expanded=False):
        market_impact_horizon_days = int(st.number_input("Impact horizon days", min_value=1, max_value=20, value=3, step=1))
        market_impact_pos_threshold = float(st.number_input("Positive return threshold", min_value=0.0, value=0.015, step=0.005, format="%.3f"))
        market_impact_neg_threshold = float(st.number_input("Negative return threshold", max_value=0.0, value=-0.015, step=0.005, format="%.3f"))

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
st.sidebar.caption(f"Fixed run options: target data cache is reused/updated automatically; same-sector peers are supplemented before the {experiment_label} experiment; SQLite persistence is enabled and legacy stock-level NLP is disabled.")

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
        st.session_state["workflow_export_bundle"] = create_export_bundle(run_rows, [], [], None, target_symbol=primary_symbol)
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
        st.session_state["workflow_export_bundle"] = create_export_bundle(run_rows, valid_symbols, failures, cross_payload, target_symbol=primary_symbol)
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
            run_rows.append({"symbol": symbol, "company_name": company, "status": "skipped", "input_csv": "-", "reports_dir": "-", "results_dir": "-"})
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
                }
            )
            row["status"] = "completed"
            row["stage"] = "done"
            row["last_update"] = datetime.now().strftime("%H:%M:%S")
            row["message"] = "Completed successfully"
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc), "traceback": traceback.format_exc(limit=8)})
            run_rows.append({"symbol": symbol, "company_name": company, "status": "failed", "input_csv": "-", "reports_dir": "-", "results_dir": "-"})
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
    st.session_state["workflow_export_bundle"] = create_export_bundle(run_rows, completed_symbols, failures, cross_payload, target_symbol=primary_symbol, market_cross_payload=market_cross_payload)

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
        with st.expander("Step-by-step peer experiment log", expanded=False):
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
