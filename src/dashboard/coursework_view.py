"""Read-only coursework reader. Opening this module never fits or fetches data."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
NAMES = {
    "dqn_without_nlp": "Market only",
    "dqn_with_sector_sentiment_nlp": "Sector sentiment",
    "dqn_with_sector_impact_nlp": "Sector impact",
    "buy_and_hold": "Buy-and-Hold",
}
COLORS = {
    "dqn_without_nlp": "#17324D",
    "dqn_with_sector_sentiment_nlp": "#2A7F7A",
    "dqn_with_sector_impact_nlp": "#B7791F",
    "buy_and_hold": "#65758B",
}
DASHES = {
    "dqn_without_nlp": "solid",
    "dqn_with_sector_sentiment_nlp": "dash",
    "dqn_with_sector_impact_nlp": "dot",
    "buy_and_hold": "dashdot",
}


def load_run(path: Path) -> tuple[dict, dict[str, pd.DataFrame]]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("mode") not in {
        "smoke",
        "full",
    }:
        raise ValueError("Unsupported run schema or evidence mode")
    if (
        manifest.get("status") != "complete"
        or manifest.get("accounting_version") != "previous_close_to_current_close_v2"
    ):
        raise ValueError("Run is incomplete or uses historical accounting")
    directory = (
        (ROOT / manifest["run_directory"]).resolve()
        if "run_directory" in manifest
        else path.parent.resolve()
    )
    if not directory.is_relative_to(ROOT.resolve()):
        raise ValueError("Run directory must be within the project")
    frames = {}
    for name, relative in manifest["files"].items():
        source = (directory / relative).resolve()
        if not source.is_relative_to(directory) or not source.is_file():
            raise ValueError(f"Missing run file: {name}")
        if hashlib.sha256(source.read_bytes()).hexdigest() != manifest[
            "file_sha256"
        ].get(name):
            raise ValueError(f"Run file changed after completion: {name}")
        if source.suffix == ".csv":
            frames[name] = pd.read_csv(
                source, dtype={"symbol": str, "target_symbol": str}
            )
    return manifest, frames


def run_choices() -> dict[str, Path | None]:
    showcase = ROOT / "outputs/showcase/manifest.json"
    choices = {
        "Formal showcase"
        if showcase.exists()
        else "Formal showcase — awaiting real-data run": showcase
        if showcase.exists()
        else None
    }
    paths = (
        list((ROOT / "outputs/runs").glob("*/manifest.json"))
        + list((ROOT / "outputs/demo").glob("*/manifest.json"))
        + list((ROOT / "outputs/real").glob("*/manifest.json"))
    )
    for path in sorted(paths):
        try:
            manifest = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if manifest.get("status") == "complete":
            choices[f"{manifest['mode'].upper()} · {manifest['run_id']}"] = path
    return choices


def render_coursework_reader() -> None:
    st.title("From Financial News to Trading Decisions")
    st.caption(
        "NLP-Driven Reinforcement Learning Trading Platform · A group project for Data Analysis and FinTech Fundamentals"
    )
    page = st.sidebar.radio(
        "Explore the project",
        ["Overview", "News to Decision", "Experiments", "Methods & Project"],
    )
    choices = run_choices()
    version = st.sidebar.selectbox("Result version", list(choices))
    manifest, frames = None, {}
    if choices[version]:
        try:
            manifest, frames = load_run(choices[version])
            if version == "Formal showcase" and manifest.get("mode") != "full":
                raise ValueError("Formal showcase must reference a Full run")
        except (OSError, ValueError, KeyError) as exc:
            st.error(f"This result version cannot be opened: {exc}")
            return
    if manifest and manifest["mode"] == "smoke":
        st.warning(
            "SYNTHETIC SMOKE RUN · Checks the pipeline on generated fixtures. The numbers below provide no evidence of market performance or NLP value."
        )
    elif manifest:
        st.info(
            "Historical disclosure-title re-evaluation · Availability is delayed after the public notice date; prices use a frozen adjusted snapshot. Assess every fold and seed before drawing conclusions."
        )
    else:
        st.info(
            "Formal results await source-complete data and a 200-episode run. Completed Smoke runs can be selected explicitly in Result version."
        )
    if page == "Overview":
        st.header("Does financial news add information to DQN trading?")
        st.write(
            "We connect market data, peer-trained text signals, a handwritten DQN and a common trading ledger. The experiment compares market-only, sentiment and return-labelled signals under the same conditions."
        )
        for column, (title, body) in zip(
            st.columns(4),
            [
                (
                    "01 · Data",
                    "Separate market sessions from news. Record when each story became available.",
                ),
                (
                    "02 · Signals",
                    "Fit sentiment and impact models on non-target peers before RL training.",
                ),
                (
                    "03 · Decisions",
                    "Use a fixed network, action space, seeds and transaction costs.",
                ),
                (
                    "04 · Evidence",
                    "Read paired results, actual trades and limitations together.",
                ),
            ],
        ):
            column.subheader(title)
            column.write(body)
        st.subheader("Two problems shaped the project")
        left, right = st.columns(2)
        left.markdown(
            "**Uneven news coverage**\n\nPeer-trained signals address limited target training text. Availability and label maturity still require separate checks."
        )
        right.markdown(
            "**Hold-heavy policies**\n\nState scaling and action exploration helped diagnose behavior. More trades alone do not establish better returns."
        )
        st.subheader("A short demonstration")
        st.write(
            "1. Select a completed result version. 2. Follow a story in News to Decision. 3. Compare the same fold in Experiments. 4. Review Methods & Project."
        )
        if manifest:
            st.json(
                {
                    "target": manifest["config"]["target_symbol"],
                    "folds": len(manifest["folds"]),
                    "episodes": manifest["config"]["episodes"],
                    "seeds": manifest["config"]["seeds"],
                    "evidence": manifest["evidence_status"],
                }
            )
            for limitation in manifest.get("limitations", []):
                st.caption(limitation)
        st.caption(
            "The original May 2026 diagnostic files remain in the repository. They use earlier accounting and are excluded from this reader's result comparison."
        )
    elif page == "News to Decision":
        render_trace(manifest, frames)
    elif page == "Experiments":
        render_experiments(manifest, frames)
    else:
        st.header("Methods & Project")
        section = st.selectbox(
            "Read about",
            [
                "real_experiments",
                "finbert_evaluation",
                "methodology",
                "project_journey",
                "data",
                "reproduction",
                "team",
            ],
        )
        path = ROOT / "docs" / f"{section}.md"
        if path.is_file():
            st.markdown(path.read_text(encoding="utf-8"))
        st.caption(
            "Advanced controls expose the original research workspace. The default reader only opens frozen local files."
        )


def render_trace(manifest: dict | None, frames: dict) -> None:
    st.header("News to Decision")
    st.write(
        "Trace available news through model signals, the decision state and executed trade. This view records a sequence; it does not attribute a trade causally to one story."
    )
    if not manifest:
        st.warning(
            "Missing: a completed run with news_trace.csv, daily_signals.csv and trading_logs.csv."
        )
        return
    fold = st.selectbox("Time fold", [row["fold"] for row in manifest["folds"]])
    trades = frames["trades"]
    trades = trades[trades["fold"] == fold]
    news = frames["trace"]
    news = news[
        (news["fold"] == fold)
        & pd.to_datetime(news["decision_date"]).isin(pd.to_datetime(trades["date"]))
    ].reset_index(drop=True)
    if news.empty:
        st.warning(
            "No recorded news in this test fold. Market sessions remain in the experiment."
        )
        return
    index = st.selectbox(
        "News sample",
        list(news.index),
        format_func=lambda i: (
            f"{news.loc[i, 'decision_date'][:10]} · {news.loc[i, 'text'][:70]}"
        ),
    )
    story = news.loc[index]
    st.subheader(story["text"])
    st.caption(
        f"Source: {story['source']} · Available: {story['available_at']} · First decision session: {story['decision_date'][:10]}"
    )
    st.table(
        pd.DataFrame(
            {
                "Signal": ["Sentiment", "Impact"],
                "Score P(positive) − P(negative)": [
                    story["sentiment_score"],
                    story["impact_score"],
                ],
            }
        )
    )
    experiment = st.selectbox(
        "Comparison group", list(NAMES)[:3], format_func=NAMES.get
    )
    seed = st.selectbox("Seed", manifest["config"]["seeds"])
    day = trades[
        (trades["experiment"] == experiment)
        & (trades["seed"] == seed)
        & (pd.to_datetime(trades["date"]) == pd.to_datetime(story["decision_date"]))
    ]
    if day.empty:
        st.warning(
            "No matching decision in this run; no other stock or date is substituted."
        )
        return
    state_columns = [name for name in day if name.startswith("state_")]
    if state_columns:
        st.subheader("State presented to DQN (training-window normalization)")
        st.table(day[state_columns].T.rename(columns={day.index[0]: "Value"}))
    st.subheader("Action and ledger")
    fields = [
        "raw_action_id",
        "mapped_action_label",
        "executed_action",
        "price",
        "executed_trade_value",
        "transaction_cost",
        "cash",
        "shares",
        "valuation_price",
        "portfolio_value",
        "reward",
    ]
    labels = {
        "raw_action_id": "Decision action ID",
        "mapped_action_label": "Intended action",
        "executed_action": "Executed action",
        "price": "Execution price (CNY)",
        "executed_trade_value": "Traded value (CNY)",
        "transaction_cost": "Fee (CNY)",
        "cash": "Cash (CNY)",
        "shares": "Shares held",
        "valuation_price": "Close price (CNY)",
        "portfolio_value": "NAV at close (CNY)",
        "reward": "Net portfolio return",
    }
    display = []
    for name in fields:
        if name in day:
            value = day.iloc[0][name]
            if name == "reward":
                value = f"{value:.4%}"
            elif name == "shares":
                value = f"{value:,.3f}"
            elif name not in {
                "raw_action_id",
                "mapped_action_label",
                "executed_action",
            }:
                value = f"{value:,.2f}"
            display.append({"Ledger item": labels[name], "Value": str(value)})
    st.table(pd.DataFrame(display).set_index("Ledger item"))
    st.caption(
        "Q-value margin is a network diagnostic, not a probability of profit. Missing news is distinct from a neutral score."
    )


def render_experiments(manifest: dict | None, frames: dict) -> None:
    st.header("Experiments")
    st.write(
        "Compare the same stock, time fold, costs, architecture and seed budget. The text slot is zero in the market-only control; retained 12-slot explorations also clear every derived text channel."
    )
    if not manifest:
        st.warning(
            "Missing: a completed source-bound Full run. Historical diagnostic returns are not a substitute."
        )
        return
    fold = st.selectbox("Time fold", [row["fold"] for row in manifest["folds"]])
    config = manifest["config"]
    fold_meta = next(row for row in manifest["folds"] if row["fold"] == fold)
    st.caption(
        f"{config['target_symbol']} · {fold_meta['test_start']} to {fold_meta['test_end']} · Cost {config['transaction_cost']:.2%} per side · {len(config['seeds'])} DQN seed(s) · Mean NAV across seeds; B&H once per fold"
    )
    data = frames["trades"]
    data = data[data["fold"] == fold].copy()
    fig = go.Figure()
    for experiment in NAMES:
        curve = (
            data[data["experiment"] == experiment]
            .groupby("date")["portfolio_value"]
            .mean()
            .sort_index()
        )
        if curve.empty:
            continue
        dates = pd.to_datetime(curve.index)
        fig.add_trace(
            go.Scatter(
                x=[dates.min() - pd.Timedelta(seconds=1), *dates],
                y=[1.0, *(curve / config["initial_cash"])],
                name=NAMES[experiment],
                mode="lines",
                line={
                    "color": COLORS[experiment],
                    "dash": DASHES[experiment],
                    "width": 2,
                },
            )
        )
    fig.update_layout(
        title="Portfolio NAV by strategy",
        xaxis_title="Test session",
        yaxis_title="NAV (initial cash = 1.00)",
        template="plotly_white",
        legend={"orientation": "h", "y": 1.12},
        height=430,
        margin={"l": 35, "r": 15, "t": 90, "b": 35},
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Source: this run's trading_logs.csv. Includes initial cash, all daily marks and entry/exit costs; holdings are marked at final close without forced liquidation."
    )
    metrics = frames["metrics"]
    metrics = metrics[metrics["fold"] == fold]
    summary = (
        metrics.groupby("experiment")[
            [
                "cumulative_return",
                "sharpe_ratio",
                "max_drawdown",
                "number_of_trades",
                "average_exposure",
            ]
        ]
        .mean()
        .rename(index=NAMES)
    )
    formats = {
        "cumulative_return": ".2%",
        "sharpe_ratio": ".2f",
        "max_drawdown": ".2%",
        "number_of_trades": ".1f",
        "average_exposure": ".2%",
    }
    for name, spec in formats.items():
        summary[name] = summary[name].map(
            lambda value: "N/A" if pd.isna(value) else format(value, spec)
        )
    summary = summary.rename(
        columns={
            "cumulative_return": "Net return",
            "sharpe_ratio": "Sharpe",
            "max_drawdown": "Max drawdown",
            "number_of_trades": "Executed trades",
            "average_exposure": "Mean exposure",
        }
    )
    st.dataframe(summary, use_container_width=True)
    st.caption(
        "Table: mean per-seed metrics, with initial NAV included in risk measures. These differ from metrics of the plotted mean-NAV curve. Seeds describe optimization variation, not independent market samples."
    )
    st.subheader("Paired NLP differences")
    paired = frames["paired"]
    paired = paired[paired["fold"] == fold]
    paired_view = paired[
        [
            "experiment",
            "seed",
            "delta_cumulative_return",
            "delta_sharpe_ratio",
            "delta_max_drawdown",
        ]
    ].copy()
    paired_view["experiment"] = paired_view["experiment"].map(NAMES)
    for name in ["delta_cumulative_return", "delta_max_drawdown"]:
        paired_view[name] = paired_view[name].map(
            lambda value: "N/A" if pd.isna(value) else f"{100 * value:+.2f}"
        )
    paired_view["delta_sharpe_ratio"] = paired_view["delta_sharpe_ratio"].map(
        lambda value: "N/A" if pd.isna(value) else f"{value:+.2f}"
    )
    paired_view = paired_view.rename(
        columns={
            "experiment": "Comparison",
            "seed": "Seed",
            "delta_cumulative_return": "ΔReturn (pp)",
            "delta_sharpe_ratio": "ΔSharpe",
            "delta_max_drawdown": "ΔMDD (pp)",
        }
    )
    st.dataframe(paired_view, use_container_width=True, hide_index=True)
    st.caption(
        "NLP minus market-only, within fold and seed. Negative ΔMDD means a smaller drawdown. Undefined Sharpe remains N/A."
    )
    st.download_button(
        "Download paired differences",
        frames["paired"].to_csv(index=False),
        file_name="paired_differences.csv",
        mime="text/csv",
    )
    with st.expander("Run provenance"):
        st.json(manifest)
