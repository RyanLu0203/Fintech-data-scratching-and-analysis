"""Generate markdown, tables, and figures for the final report."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ARCHITECTURE_MERMAID = """flowchart TD
    A[Raw OHLCV + financial news] --> B[Data ingestion]
    B --> C[CSV artifacts / optional SQLite]
    C --> D[NLP sentiment pipeline]
    D --> E[Daily sentiment signal]
    C --> F[Technical indicators]
    E --> G[RL state vector]
    F --> G
    G --> H[DQN trading engine]
    H --> I[Trading logs + portfolio curve]
    I --> J[Evaluation, diagnostics, and ablation]
    J --> K[Notebook report + Streamlit dashboard]
"""


def generate_report_artifacts(
    reports_dir: Path,
    input_csv: Path,
    market_data: pd.DataFrame,
    daily_sentiment: pd.DataFrame,
    nlp_evaluation: pd.DataFrame,
    state_compliance: pd.DataFrame,
    ablation_metrics: pd.DataFrame,
    portfolio_curves: pd.DataFrame,
    trading_logs: pd.DataFrame,
    ablation_seed_metrics: pd.DataFrame | None = None,
    walk_forward_table: pd.DataFrame | None = None,
    daily_net_flow: pd.DataFrame | None = None,
    drawdown_curves: pd.DataFrame | None = None,
    signal_diagnostics_table: pd.DataFrame | None = None,
    diagnostics_table: pd.DataFrame | None = None,
    leakage_table: pd.DataFrame | None = None,
) -> dict[str, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    stem = input_csv.stem
    paths: dict[str, Path] = {}

    paths["architecture_mermaid"] = reports_dir / "system_architecture.mmd"
    paths["architecture_mermaid"].write_text(ARCHITECTURE_MERMAID, encoding="utf-8")

    paths["nlp_model_comparison"] = reports_dir / f"{stem}_nlp_model_comparison.csv"
    nlp_evaluation.to_csv(paths["nlp_model_comparison"], index=False, encoding="utf-8-sig")

    paths["state_vector_compliance"] = reports_dir / f"{stem}_state_vector_compliance.csv"
    state_compliance.to_csv(paths["state_vector_compliance"], index=False, encoding="utf-8-sig")

    if walk_forward_table is not None:
        paths["walk_forward_splits"] = reports_dir / f"{stem}_walk_forward_splits.csv"
        walk_forward_table.to_csv(paths["walk_forward_splits"], index=False, encoding="utf-8-sig")
    if diagnostics_table is not None:
        paths["diagnostics_csv"] = reports_dir / f"{stem}_diagnostics.csv"
        diagnostics_table.to_csv(paths["diagnostics_csv"], index=False, encoding="utf-8-sig")
    if leakage_table is not None:
        paths["leakage_csv"] = reports_dir / f"{stem}_leakage_diagnostics.csv"
        leakage_table.to_csv(paths["leakage_csv"], index=False, encoding="utf-8-sig")

    paths.update(
        _generate_figures(
            reports_dir=reports_dir,
            stem=stem,
            market_data=market_data,
            daily_sentiment=daily_sentiment,
            portfolio_curves=portfolio_curves,
            trading_logs=trading_logs,
            daily_net_flow=daily_net_flow,
            ablation_metrics=ablation_metrics,
            drawdown_curves=drawdown_curves,
            signal_diagnostics_table=signal_diagnostics_table,
        )
    )

    report = _build_report_markdown(
        input_csv=input_csv,
        market_data=market_data,
        daily_sentiment=daily_sentiment,
        nlp_evaluation=nlp_evaluation,
        state_compliance=state_compliance,
        ablation_metrics=ablation_metrics,
        portfolio_curves=portfolio_curves,
        trading_logs=trading_logs,
        ablation_seed_metrics=ablation_seed_metrics,
        walk_forward_table=walk_forward_table,
        daily_net_flow=daily_net_flow,
        drawdown_curves=drawdown_curves,
        signal_diagnostics_table=signal_diagnostics_table,
        diagnostics_table=diagnostics_table,
        leakage_table=leakage_table,
    )
    paths["report_draft"] = reports_dir / f"{stem}_report_draft.md"
    paths["report_draft"].write_text(report, encoding="utf-8")
    return paths


def _generate_figures(
    reports_dir: Path,
    stem: str,
    market_data: pd.DataFrame,
    daily_sentiment: pd.DataFrame,
    portfolio_curves: pd.DataFrame,
    trading_logs: pd.DataFrame,
    daily_net_flow: pd.DataFrame | None = None,
    ablation_metrics: pd.DataFrame | None = None,
    drawdown_curves: pd.DataFrame | None = None,
    signal_diagnostics_table: pd.DataFrame | None = None,
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    market = market_data.copy()
    if "date" in market.columns:
        market["date"] = pd.to_datetime(market["date"], errors="coerce")
        market = market.sort_values("date")

    if not market.empty and "close" in market.columns:
        paths["close_price_figure"] = reports_dir / f"{stem}_close_price.svg"
        _write_line_svg(paths["close_price_figure"], market["close"], "Close Price")

    if daily_net_flow is not None and not daily_net_flow.empty:
        value_col = "net_flow_cny_million" if "net_flow_cny_million" in daily_net_flow.columns else "net_flow"
        paths["daily_net_flow_figure"] = reports_dir / f"{stem}_daily_net_flow.svg"
        _write_signed_bar_svg(paths["daily_net_flow_figure"], daily_net_flow[value_col], "Daily Net Inflow / Outflow Proxy")

    if not daily_sentiment.empty:
        score_col = "daily_sentiment_score" if "daily_sentiment_score" in daily_sentiment.columns else "sentiment_score"
        paths["sentiment_trend_figure"] = reports_dir / f"{stem}_sentiment_trend.svg"
        _write_line_svg(paths["sentiment_trend_figure"], daily_sentiment[score_col], "Daily Sentiment Score")
        if "news_count" in daily_sentiment.columns:
            paths["news_count_figure"] = reports_dir / f"{stem}_daily_news_count.svg"
            _write_bar_svg(paths["news_count_figure"], daily_sentiment["news_count"], "Daily News Count", labels=daily_sentiment.get("date"))

    aggregated_curves = _aggregate_curves(portfolio_curves)
    if not aggregated_curves.empty:
        paths["portfolio_curve_figure"] = reports_dir / f"{stem}_portfolio_curves.svg"
        _write_multi_line_svg(paths["portfolio_curve_figure"], aggregated_curves, "portfolio_value", "experiment", "Portfolio Curves")

    if drawdown_curves is not None and not drawdown_curves.empty:
        aggregated_drawdowns = _aggregate_curves(drawdown_curves, value_col="drawdown")
        paths["drawdown_curve_figure"] = reports_dir / f"{stem}_drawdown_curves.svg"
        _write_multi_line_svg(paths["drawdown_curve_figure"], aggregated_drawdowns, "drawdown", "experiment", "Drawdown Curves")

    if ablation_metrics is not None and not ablation_metrics.empty:
        if {"final_equity", "experiment"}.issubset(ablation_metrics.columns):
            paths["final_portfolio_value_figure"] = reports_dir / f"{stem}_final_portfolio_values.svg"
            _write_metric_bar_svg(
                paths["final_portfolio_value_figure"],
                ablation_metrics,
                metric="final_equity",
                title="Final Portfolio Value",
            )
        if {"annualized_volatility", "annualized_return", "experiment"}.issubset(ablation_metrics.columns):
            paths["risk_return_figure"] = reports_dir / f"{stem}_risk_return_scatter.svg"
            _write_xy_svg(
                paths["risk_return_figure"],
                ablation_metrics,
                x_col="annualized_volatility",
                y_col="annualized_return",
                label_col="experiment",
                title="Risk vs Return",
            )

    if not trading_logs.empty:
        paths["action_distribution_figure"] = reports_dir / f"{stem}_action_distribution.svg"
        _write_bar_svg(paths["action_distribution_figure"], trading_logs["action"].astype(str).value_counts(), "Action Distribution")

        paths["trading_activity_figure"] = reports_dir / f"{stem}_trading_activity.svg"
        action_counts = (
            trading_logs.loc[trading_logs["action"].astype(str) != "0"]
            .assign(date=pd.to_datetime(trading_logs["date"], errors="coerce"))
            .dropna(subset=["date"])
            .groupby("date")
            .size()
        )
        _write_bar_svg(paths["trading_activity_figure"], action_counts, "Trading Activity by Day", labels=action_counts.index if not action_counts.empty else None)

    if signal_diagnostics_table is not None and not signal_diagnostics_table.empty:
        corr = signal_diagnostics_table[
            signal_diagnostics_table["metric"].astype(str).str.contains("corr", case=False, na=False)
        ].copy()
        if not corr.empty:
            corr["value"] = pd.to_numeric(corr["value"], errors="coerce")
            corr = corr.dropna(subset=["value"])
            paths["signal_return_correlation_figure"] = reports_dir / f"{stem}_signal_return_correlations.svg"
            _write_metric_bar_svg(
                paths["signal_return_correlation_figure"],
                corr.rename(columns={"metric": "experiment", "value": "metric_value"}),
                metric="metric_value",
                title="Signal / Return Correlations",
                experiment_col="experiment",
            )

    return paths


def _aggregate_curves(frame: pd.DataFrame, value_col: str = "portfolio_value") -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns or "experiment" not in frame.columns or value_col not in frame.columns:
        return pd.DataFrame()
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna(subset=["date", value_col])
    group_cols = ["date", "experiment"]
    return data.groupby(group_cols, as_index=False)[value_col].mean().sort_values(["experiment", "date"])


def _write_line_svg(path: Path, values: pd.Series, title: str) -> None:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if series.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    points = _scale_points(series.tolist(), 760, 300, 20, 50)
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    svg = _svg_shell(title, f'<polyline fill="none" stroke="#2563eb" stroke-width="3" points="{polyline}" />')
    path.write_text(svg, encoding="utf-8")


def _write_multi_line_svg(path: Path, frame: pd.DataFrame, value_col: str, group_col: str, title: str) -> None:
    colors = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#f59e0b"]
    parts = []
    all_values = pd.to_numeric(frame[value_col], errors="coerce").dropna()
    if all_values.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    min_v, max_v = float(all_values.min()), float(all_values.max())
    for index, (name, group) in enumerate(frame.groupby(group_col)):
        ordered = group.sort_values("date") if "date" in group.columns else group
        values = pd.to_numeric(ordered[value_col], errors="coerce").dropna().tolist()
        points = _scale_points(values, 760, 300, 20, 50, min_v=min_v, max_v=max_v)
        polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        color = colors[index % len(colors)]
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{polyline}" />')
        parts.append(f'<text x="30" y="{380 + index * 18}" font-size="13" fill="{color}">{name}</text>')
    path.write_text(_svg_shell(title, "\n".join(parts)), encoding="utf-8")


def _write_bar_svg(path: Path, counts: pd.Series, title: str, labels: pd.Series | pd.Index | None = None) -> None:
    counts = pd.to_numeric(pd.Series(counts), errors="coerce").fillna(0.0)
    if counts.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    max_v = max(float(counts.max()), 1.0)
    bar_width = 680 / max(len(counts), 1)
    parts = []
    label_values = list(labels) if labels is not None and len(labels) == len(counts) else list(counts.index)
    for index, value in enumerate(counts.tolist()):
        height = 280 * float(value) / max_v
        x = 60 + index * bar_width
        y = 340 - height
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width * 0.7:.1f}" height="{height:.1f}" fill="#2563eb" />')
        if len(counts) <= 18:
            label = str(label_values[index])[:16]
            parts.append(f'<text x="{x:.1f}" y="365" font-size="11">{label}</text>')
    path.write_text(_svg_shell(title, "\n".join(parts)), encoding="utf-8")


def _write_signed_bar_svg(path: Path, values: pd.Series, title: str) -> None:
    series = pd.to_numeric(values, errors="coerce").fillna(0.0)
    if series.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    max_abs = max(float(series.abs().max()), 1e-9)
    bar_width = 760 / max(len(series), 1)
    zero_y = 200
    parts = ['<line x1="40" y1="200" x2="800" y2="200" stroke="#6b7280" stroke-dasharray="4 4"/>']
    for index, value in enumerate(series.tolist()):
        height = 140 * abs(float(value)) / max_abs
        x = 40 + index * bar_width
        y = zero_y - height if value >= 0 else zero_y
        color = "#16a34a" if value >= 0 else "#dc2626"
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width * 0.75:.1f}" height="{height:.1f}" fill="{color}" />')
    parts.append('<text x="45" y="385" font-size="12" fill="#374151">green = estimated inflow, red = estimated outflow</text>')
    path.write_text(_svg_shell(title, "\n".join(parts)), encoding="utf-8")


def _write_metric_bar_svg(
    path: Path,
    frame: pd.DataFrame,
    metric: str,
    title: str,
    experiment_col: str = "experiment",
) -> None:
    data = frame[[experiment_col, metric]].copy()
    data[metric] = pd.to_numeric(data[metric], errors="coerce")
    data = data.dropna(subset=[metric])
    if data.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    series = data.set_index(experiment_col)[metric]
    _write_bar_svg(path, series, title)


def _write_xy_svg(path: Path, frame: pd.DataFrame, x_col: str, y_col: str, label_col: str, title: str) -> None:
    data = frame[[x_col, y_col, label_col]].copy()
    data[x_col] = pd.to_numeric(data[x_col], errors="coerce")
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce")
    data = data.dropna(subset=[x_col, y_col])
    if data.empty:
        path.write_text(_empty_svg(title), encoding="utf-8")
        return
    x_min, x_max = float(data[x_col].min()), float(data[x_col].max())
    y_min, y_max = float(data[y_col].min()), float(data[y_col].max())
    x_span = max(x_max - x_min, 1e-9)
    y_span = max(y_max - y_min, 1e-9)
    colors = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#f59e0b"]
    parts = []
    for index, row in data.reset_index(drop=True).iterrows():
        x = 60 + ((float(row[x_col]) - x_min) / x_span) * 700
        y = 340 - ((float(row[y_col]) - y_min) / y_span) * 260
        color = colors[index % len(colors)]
        label = str(row[label_col])
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="7" fill="{color}" />')
        parts.append(f'<text x="{x + 10:.1f}" y="{y + 4:.1f}" font-size="12">{label}</text>')
    parts.append('<text x="340" y="390" font-size="12">x = annualized volatility, y = annualized return</text>')
    path.write_text(_svg_shell(title, "\n".join(parts)), encoding="utf-8")


def _scale_points(
    values: list[float],
    width: int,
    height: int,
    x0: int,
    y0: int,
    min_v: float | None = None,
    max_v: float | None = None,
) -> list[tuple[float, float]]:
    if not values:
        return []
    min_value = float(min(values) if min_v is None else min_v)
    max_value = float(max(values) if max_v is None else max_v)
    span = max(max_value - min_value, 1e-9)
    denom = max(len(values) - 1, 1)
    return [
        (x0 + index * width / denom, y0 + height - ((float(value) - min_value) / span) * height)
        for index, value in enumerate(values)
    ]


def _svg_shell(title: str, body: str) -> str:
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="840" height="420" viewBox="0 0 840 420">
<rect width="840" height="420" fill="white"/>
<text x="30" y="30" font-size="20" font-family="Arial, sans-serif">{title}</text>
<line x1="40" y1="350" x2="800" y2="350" stroke="#d1d5db"/>
<line x1="40" y1="50" x2="40" y2="350" stroke="#d1d5db"/>
{body}
</svg>"""


def _empty_svg(title: str) -> str:
    return _svg_shell(title, '<text x="80" y="200" font-size="16">No data available</text>')


def _build_report_markdown(
    input_csv: Path,
    market_data: pd.DataFrame,
    daily_sentiment: pd.DataFrame,
    nlp_evaluation: pd.DataFrame,
    state_compliance: pd.DataFrame,
    ablation_metrics: pd.DataFrame,
    portfolio_curves: pd.DataFrame,
    trading_logs: pd.DataFrame,
    ablation_seed_metrics: pd.DataFrame | None,
    walk_forward_table: pd.DataFrame | None,
    daily_net_flow: pd.DataFrame | None,
    drawdown_curves: pd.DataFrame | None,
    signal_diagnostics_table: pd.DataFrame | None,
    diagnostics_table: pd.DataFrame | None,
    leakage_table: pd.DataFrame | None,
) -> str:
    market_regime = _market_regime(market_data)
    best = ablation_metrics.sort_values("final_equity", ascending=False).iloc[0] if not ablation_metrics.empty and "final_equity" in ablation_metrics.columns else None
    sentiment_cov = _metric_value(signal_diagnostics_table, "sentiment_coverage")
    sentiment_corr = _metric_value(signal_diagnostics_table, "sentiment_next_day_return_corr")
    flow_same = _metric_value(signal_diagnostics_table, "net_flow_same_day_return_corr")
    flow_next = _metric_value(signal_diagnostics_table, "net_flow_next_day_return_corr")
    nlp_effect = _nlp_effect_text(ablation_metrics)
    finbert_status = _first_value(nlp_evaluation, "finbert_status")
    main_sentiment_method = _first_value(nlp_evaluation, "main_experiment_method")
    fallback_used = _first_value(nlp_evaluation, "sentiment_fallback_used")

    lines = [
        "# NLP-Driven Reinforcement Learning Trading Platform Report Draft",
        "",
        "## Data Source",
        f"- Integrated CSV: `{input_csv}`",
        f"- Market rows: `{len(market_data)}`",
        f"- Daily sentiment rows: `{len(daily_sentiment)}`",
        f"- Daily net-flow rows: `{0 if daily_net_flow is None else len(daily_net_flow)}`",
        f"- Portfolio curve rows: `{len(portfolio_curves)}`",
        f"- Drawdown curve rows: `{0 if drawdown_curves is None else len(drawdown_curves)}`",
        f"- Trading log rows: `{len(trading_logs)}`",
        "",
        "## Market Regime Analysis",
        f"- Estimated regime: `{market_regime}`.",
        f"- Close-price path suggests the test window should be interpreted in the context of a `{market_regime}` market rather than assuming a universally stable trend.",
        "",
        "## Sentiment Coverage Analysis",
        f"- Sentiment coverage: `{_fmt_pct(sentiment_cov)}`.",
        f"- Main sentiment / return relationship on next day: `{_fmt_num(sentiment_corr)}`.",
        "- Low coverage means NLP can easily look weaker simply because many trading days have no textual signal and are correctly set to zero.",
        "",
        "## Signal-Return Correlation Analysis",
        f"- `net_flow_same_day_return_corr`: `{_fmt_num(flow_same)}`.",
        f"- `net_flow_next_day_return_corr`: `{_fmt_num(flow_next)}`.",
        "- A high same-day correlation for `net_flow_proxy` is expected when the proxy is derived from same-day price/volume movement. Treat it as descriptive, not predictive.",
        "",
        "## NLP Model Comparison",
        f"- Main RL sentiment method: `{main_sentiment_method or 'unknown'}`.",
        f"- FinBERT status: `{finbert_status or 'unknown'}`.",
        f"- Fallback sentiment used: `{fallback_used if fallback_used != '' else 'unknown'}`.",
        "- Pseudo-label F1 and gold-label F1 must be interpreted separately; see final report tables for gold-label evidence when available.",
        _table(nlp_evaluation) if not nlp_evaluation.empty else "No labelled NLP evaluation data was available.",
        "",
        "## State Vector Compliance",
        _table(state_compliance),
        "",
        "## Diagnostics",
        _table(diagnostics_table) if diagnostics_table is not None and not diagnostics_table.empty else "Diagnostics table was not generated.",
        "",
        "## Leakage Diagnostics",
        _table(leakage_table) if leakage_table is not None and not leakage_table.empty else "Leakage diagnostics table was not generated.",
        "",
        "## Ablation Metrics",
        _table(ablation_metrics) if not ablation_metrics.empty else "Ablation metrics were not generated.",
        "",
    ]
    if ablation_seed_metrics is not None and not ablation_seed_metrics.empty:
        lines.extend(["## Multi-Seed Results", _table(ablation_seed_metrics), ""])
    if walk_forward_table is not None and not walk_forward_table.empty:
        lines.extend(["## Walk-Forward Splits", _table(walk_forward_table), ""])
    if best is not None:
        lines.extend(
            [
                "## Ablation Result",
                f"- Best experiment by final equity: `{best['experiment']}`.",
                f"- Final equity: `{_fmt_num(best.get('final_equity'))}`.",
                f"- Sharpe ratio: `{_fmt_num(best.get('sharpe_ratio'))}`.",
                f"- Max drawdown: `{_fmt_pct(best.get('max_drawdown'))}`.",
                "",
            ]
        )
    lines.extend(
        [
            "## Risk-Return Interpretation",
            "- Compare annualized return together with annualized volatility, Sharpe, Sortino, and Calmar rather than reading final equity alone.",
            "- Max Drawdown is shown as a positive percentage for readability: larger means deeper loss from peak to trough.",
            "",
            "## Trading Behavior Interpretation",
            f"- Total logged actions: `{len(trading_logs)}`.",
            "- Action distribution and trading activity charts help distinguish a genuinely selective policy from one that simply over-trades.",
            "",
            "## Whether NLP Improved Performance",
            f"- Conclusion for this stock: {nlp_effect}",
            "- The comparison is only meaningful because the no-NLP and with-NLP DQN runs now share the same split, architecture, reward function, transaction cost, and seed list.",
            "",
            "## Limitations",
            "- Sentiment does not universally improve DQN trading performance. Effects depend on market regime, sentiment coverage, signal quality, and conservative time alignment.",
            "- FinBERT may be skipped in constrained environments; when that happens the pipeline falls back to another standardized sentiment method and logs it explicitly.",
            "- Net-flow proxy is retained for explanation and diagnostics, not as a leakage-prone predictive state feature.",
        ]
    )
    return "\n".join(lines)


def _metric_value(table: pd.DataFrame | None, metric: str) -> float | None:
    if table is None or table.empty or "metric" not in table.columns or "value" not in table.columns:
        return None
    match = table.loc[table["metric"].astype(str) == metric, "value"]
    if match.empty:
        return None
    value = pd.to_numeric(match, errors="coerce").dropna()
    return None if value.empty else float(value.iloc[0])


def _first_value(table: pd.DataFrame | None, column: str) -> str:
    if table is None or table.empty or column not in table.columns:
        return ""
    values = table[column].dropna().astype(str)
    return "" if values.empty else values.iloc[0]


def _market_regime(frame: pd.DataFrame) -> str:
    if frame.empty or "close" not in frame.columns:
        return "unknown"
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if len(close) < 20:
        return "unknown"
    returns = close.pct_change().dropna()
    total_return = close.iloc[-1] / close.iloc[0] - 1
    vol = returns.std() * np.sqrt(252) if not returns.empty else np.nan
    if pd.notna(vol) and vol > 0.45:
        return "volatile"
    if total_return > 0.15:
        return "bullish"
    if total_return < -0.15:
        return "bearish"
    return "sideways"


def _nlp_effect_text(ablation_metrics: pd.DataFrame) -> str:
    if ablation_metrics.empty or "experiment" not in ablation_metrics.columns:
        return "No ablation result available."
    indexed = ablation_metrics.set_index("experiment")
    if not {"dqn_with_nlp", "dqn_without_nlp"}.issubset(indexed.index):
        return "Incomplete DQN comparison."
    with_nlp = indexed.loc["dqn_with_nlp"]
    without_nlp = indexed.loc["dqn_without_nlp"]
    equity_effect = pd.to_numeric(with_nlp.get("final_equity"), errors="coerce") - pd.to_numeric(without_nlp.get("final_equity"), errors="coerce")
    sharpe_effect = pd.to_numeric(with_nlp.get("sharpe_ratio"), errors="coerce") - pd.to_numeric(without_nlp.get("sharpe_ratio"), errors="coerce")
    if pd.notna(equity_effect) and pd.notna(sharpe_effect) and equity_effect > 0 and sharpe_effect > 0:
        return "NLP improved this stock in both final equity and Sharpe ratio."
    if pd.notna(equity_effect) and pd.notna(sharpe_effect) and equity_effect < 0 and sharpe_effect < 0:
        return "NLP hurt this stock in both final equity and Sharpe ratio."
    return "Mixed effect: NLP helped some dimensions but not all."


def _fmt_num(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "N/A" if pd.isna(numeric) else f"{float(numeric):.4f}"


def _fmt_pct(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "N/A" if pd.isna(numeric) else f"{float(numeric) * 100:.2f}%"


def _table(frame: pd.DataFrame | None) -> str:
    if frame is None or frame.empty:
        return "No data available."
    data = frame.copy()
    for column in data.columns:
        if pd.api.types.is_numeric_dtype(data[column]):
            data[column] = data[column].map(lambda x: "N/A" if pd.isna(x) else round(float(x), 6))
    try:
        return data.to_markdown(index=False)
    except ImportError:
        columns = [str(col) for col in data.columns]
        header = "| " + " | ".join(columns) + " |"
        divider = "| " + " | ".join(["---"] * len(columns)) + " |"
        rows = [
            "| " + " | ".join("N/A" if pd.isna(value) else str(value) for value in row) + " |"
            for row in data.itertuples(index=False, name=None)
        ]
        return "\n".join([header, divider, *rows])
