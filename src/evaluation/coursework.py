"""Small, fixed walk-forward comparison using the existing handwritten DQN."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.evaluation.ablation import _metrics_from_log
from src.evaluation.model_upgrade import _prepare_single_frame_pair
from src.evaluation.market_impact_ablation import UNIFIED_DQN_STATE_COLUMNS
from src.features.technical_indicators import add_trading_features
from src.nlp.chronological import (
    fit_frozen_peer_models,
    normalize_news,
    score_frozen_target,
    session_time,
)
from src.rl.dqn_agent import DQNConfig
from src.rl.train import evaluate_agent, train_dqn
from src.rl.trading_env import FinancialTradingEnv

GROUPS = {
    "dqn_without_nlp": None,
    "dqn_with_sector_sentiment_nlp": "sector_sentiment_score",
    "dqn_with_sector_impact_nlp": "sector_impact_score",
}


@dataclass(frozen=True)
class CourseworkConfig:
    target_symbol: str = "002475"
    mode: str = "full"
    episodes: int = 200
    seeds: tuple[int, ...] = (42, 123, 2024)
    nlp_days: int = 126
    train_days: int = 252
    test_days: int = 63
    folds: int = 2
    initial_cash: float = 1_000_000.0
    transaction_cost: float = 0.001


def validate_market(market: pd.DataFrame) -> pd.DataFrame:
    required = {"symbol", "date", "open", "high", "low", "close", "volume"}
    if not required.issubset(market):
        raise ValueError(f"Market requires {sorted(required)}")
    frame = market.copy()
    frame["symbol"] = (
        frame["symbol"].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(6)
    )
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    if (
        frame.empty
        or frame[["symbol", "date"]].isna().any().any()
        or frame.duplicated(["symbol", "date"]).any()
    ):
        raise ValueError("Market must be non-empty, with unique symbol/session keys")
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if (
            not np.isfinite(frame[column]).all()
            or (frame[column] < (0 if column == "volume" else 1e-12)).any()
        ):
            raise ValueError(f"Invalid market values: {column}")
    if (frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any() or (
        frame["low"] > frame[["open", "close", "high"]].min(axis=1)
    ).any():
        raise ValueError("OHLC ranges are inconsistent")
    return frame.sort_values(["symbol", "date"]).reset_index(drop=True)


def frame_hash(frame: pd.DataFrame) -> str:
    return hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()


def input_profile(
    market: pd.DataFrame, target: pd.DataFrame, peers: pd.DataFrame
) -> dict:
    return {
        "market_grain": "one symbol per session",
        "market_rows": len(market),
        "sessions": market["date"].nunique(),
        "market_start": str(market["date"].min().date()),
        "market_end": str(market["date"].max().date()),
        "target_news_rows": len(target),
        "peer_news_rows": len(peers),
        "peer_symbols": sorted(peers["symbol"].unique().tolist()),
        "target_news_sources": target["source"].value_counts().to_dict(),
        "peer_news_by_symbol": peers["symbol"].value_counts().to_dict(),
        "target_monthly_titles": target["available_at"]
        .dt.tz_convert("Asia/Shanghai")
        .dt.strftime("%Y-%m")
        .value_counts()
        .sort_index()
        .to_dict(),
        "availability_policies": target["availability_policy"].value_counts().to_dict()
        if "availability_policy" in target
        else {"unspecified": len(target)},
        "target_news_first_available": str(target["available_at"].min()),
        "target_news_last_available": str(target["available_at"].max()),
        "nulls": {
            "market": int(market.isna().sum().sum()),
            "target_news": int(target.isna().sum().sum()),
            "peer_news": int(peers.isna().sum().sum()),
        },
    }


def run_coursework(
    market: pd.DataFrame,
    target_news: pd.DataFrame,
    peer_news: pd.DataFrame,
    peer_market: pd.DataFrame,
    config: CourseworkConfig,
    output_root: Path,
) -> Path:
    if config.mode not in {"smoke", "full"}:
        raise ValueError("mode must be smoke or full")
    if (
        min(
            config.nlp_days,
            config.train_days,
            config.test_days,
            config.folds,
            config.episodes,
        )
        <= 0
        or config.nlp_days < 14
        or config.train_days < 3
    ):
        raise ValueError(
            "Positive budgets required; NLP prefix >=14 and RL train >=3 sessions"
        )
    if not config.seeds or len(set(config.seeds)) != len(config.seeds):
        raise ValueError("Seeds must be non-empty and unique")
    if config.mode == "full" and config.episodes < 200:
        raise ValueError("Full runs require at least 200 episodes")
    market, peer_market = validate_market(market), validate_market(peer_market)
    if set(market["symbol"]) != {config.target_symbol}:
        raise ValueError("Target market contains a different symbol")
    target, peers = normalize_news(target_news), normalize_news(peer_news)
    if set(target["symbol"]) != {config.target_symbol}:
        raise ValueError("Target news contains a different symbol")
    peers = peers[peers["symbol"] != config.target_symbol].copy()
    if config.mode == "full":
        for frame in [market, peer_market, target, peers]:
            if (
                "source" not in frame
                or frame["source"].isna().any()
                or frame["source"]
                .astype(str)
                .str.contains(
                    r"synthetic|fixture|程序生成|generated", case=False, regex=True
                )
                .any()
            ):
                raise ValueError(
                    "Full requires recorded real sources; synthetic/proxy input is prohibited"
                )
    mapping = pd.read_csv(
        Path(__file__).resolve().parents[2] / "config/stock_sector_mapping.csv",
        dtype={"symbol": str},
    )
    sectors = mapping.set_index("symbol")["sector"].to_dict()
    target_sector = sectors.get(config.target_symbol)
    if not target_sector or any(
        sectors.get(symbol) != target_sector for symbol in peers["symbol"].unique()
    ):
        raise ValueError(
            "Core comparison requires configured same-sector non-target peers"
        )
    dates = market["date"].reset_index(drop=True)
    needed = config.nlp_days + config.train_days + config.test_days * config.folds
    if len(dates) < needed:
        raise ValueError(f"Need {needed} sessions for fixed folds; found {len(dates)}")
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + config.mode
        + "_"
        + config.target_symbol
    )
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "status": "running",
        "mode": config.mode,
        "evidence_status": "synthetic_path_check"
        if config.mode == "smoke"
        else "historical_re_evaluation",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": asdict(config),
        "upstream_commit": "b0660a6bf849bd241f0d5fec67cc700e707222ef",
        "accounting_version": "previous_close_to_current_close_v2",
        "protocol": "NLP prefix -> RL training -> untouched test; frozen scalers and models per fold",
        "finbert_status": "not_used_in_core_weak_label_comparison; independent gold benchmark required",
        "input_hashes": {
            name: frame_hash(frame)
            for name, frame in [
                ("target_market", market),
                ("target_news", target),
                ("peer_news", peers),
                ("peer_market", peer_market),
            ]
        },
        "folds": [],
        "files": {
            "metrics": "metrics.csv",
            "paired": "paired_differences.csv",
            "trades": "trading_logs.csv",
            "training": "training_rewards.csv",
            "signals": "daily_signals.csv",
            "trace": "news_trace.csv",
            "profile": "data_quality.json",
        },
    }
    source_root = Path(__file__).resolve().parents[1]
    manifest["code_sha256"] = hashlib.sha256(
        b"".join(
            path.relative_to(source_root).as_posix().encode() + path.read_bytes()
            for path in sorted(source_root.rglob("*.py"))
        )
    ).hexdigest()
    manifest["dqn"] = {
        "variant": "vanilla_dqn",
        "state_columns": UNIFIED_DQN_STATE_COLUMNS,
        "action_mode": "discrete_trade",
        "hidden_layers": [64, 64],
        "reward_variant": "one_day_return",
        "loss": "huber",
        "gamma": 0.99,
        "learning_rate": 0.001,
        "flat_penalty_enabled": False,
        "trade_exploration_bonus_enabled": False,
        "scaling": "fit on RL training window; frozen for test",
    }
    manifest["sector_mapping_sha256"] = hashlib.sha256(
        (
            Path(__file__).resolve().parents[2] / "config/stock_sector_mapping.csv"
        ).read_bytes()
    ).hexdigest()
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (run_dir / "data_quality.json").write_text(
        json.dumps(input_profile(market, target, peers), indent=2), encoding="utf-8"
    )
    metrics, logs, training, signals, traces = [], [], [], [], []
    try:
        for fold in range(config.folds):
            prefix = config.nlp_days + fold * config.test_days
            fit_end = session_time(dates.iloc[[prefix - 1]], 15).iloc[0]
            past_target_keys = set(
                target.loc[target["available_at"] <= fit_end, "text_key"]
            )
            models, provenance, mature_labels = fit_frozen_peer_models(
                peers, peer_market, config.target_symbol, fit_end, past_target_keys
            )
            daily, trace = score_frozen_target(models, target, dates, fit_end)
            features = add_trading_features(market)
            features = features.merge(
                daily.rename(
                    columns={
                        "sentiment_score": "sector_sentiment_score",
                        "impact_score": "sector_impact_score",
                        "news_count": "target_news_count",
                    }
                ),
                on="date",
                validate="one_to_one",
            )
            train_dates = dates.iloc[prefix : prefix + config.train_days]
            test_dates = dates.iloc[
                prefix + config.train_days : prefix
                + config.train_days
                + config.test_days
            ]
            train_frame = features[features["date"].isin(train_dates)].copy()
            test_frame = features[features["date"].isin(test_dates)].copy()
            if (
                len(train_frame) != config.train_days
                or len(test_frame) != config.test_days
            ):
                raise ValueError(
                    "Feature warmup removed planned sessions; extend the NLP prefix"
                )
            fold_id = fold + 1
            label_file = f"peer_labels_fold_{fold_id}.csv"
            mature_labels.to_csv(run_dir / label_file, index=False)
            manifest["files"][f"peer_labels_fold_{fold_id}"] = label_file
            provenance.update(
                {
                    "fold": fold_id,
                    "train_start": str(train_dates.min().date()),
                    "train_end": str(train_dates.max().date()),
                    "test_start": str(test_dates.min().date()),
                    "test_end": str(test_dates.max().date()),
                    "target_news_test_rows": int(
                        trace["decision_date"].isin(test_dates).sum()
                    ),
                }
            )
            if config.mode == "full" and (
                provenance["target_news_test_rows"] == 0
                or len(provenance["peer_symbols"]) < 2
            ):
                raise ValueError(
                    "Full needs target test news and at least two independent sector peers"
                )
            manifest["folds"].append(provenance)
            daily["fold"], trace["fold"] = fold_id, fold_id
            signals.append(daily)
            traces.append(trace)
            # B&H uses the same integer-share execution, costs, marks and end
            # convention. It is evaluated once per fold, with no DQN seed.
            benchmark = FinancialTradingEnv(
                test_frame, ["price"], config.initial_cash, config.transaction_cost
            )
            done = False
            while not done:
                _, _, done, _ = benchmark.step(1 if benchmark.index == 0 else 0)
            bh = benchmark.trading_log(experiment="buy_and_hold")
            bh["fold"] = fold_id
            bh["seed"] = -1
            logs.append(bh)
            metrics.append(
                {
                    **_metrics_from_log("buy_and_hold", bh, config.initial_cash, -1),
                    "fold": fold_id,
                    "average_exposure": float(bh["current_exposure_after"].mean()),
                    "turnover": float(bh["turnover"].sum()),
                }
            )
            for seed in config.seeds:
                for experiment, signal_column in GROUPS.items():
                    frames = _prepare_single_frame_pair(
                        train_frame, test_frame, signal_column, "official_8d", []
                    )
                    dqn = DQNConfig(
                        state_dim=len(UNIFIED_DQN_STATE_COLUMNS),
                        seed=seed,
                        model_variant="vanilla_dqn",
                        action_mode="discrete_trade",
                        state_feature_mode="official_8d",
                        flat_penalty_enabled=False,
                        hold_penalty_enabled=False,
                        trade_exploration_bonus_enabled=False,
                    )
                    trained = train_dqn(
                        frames["train"],
                        UNIFIED_DQN_STATE_COLUMNS,
                        episodes=config.episodes,
                        initial_cash=config.initial_cash,
                        transaction_cost=config.transaction_cost,
                        experiment=experiment,
                        seed=seed,
                        dqn_config=dqn,
                        output_dir=run_dir / f"fold_{fold_id}" / f"seed_{seed}",
                        cache_market_rows=True,
                        progress_callback=lambda progress: (
                            print(
                                f"fold={fold_id} seed={seed} group={experiment} episode={progress.get('episode')}/{config.episodes}",
                                flush=True,
                            )
                            if config.mode == "full"
                            else None
                        ),
                    )
                    scaler_path = (
                        run_dir
                        / f"fold_{fold_id}"
                        / f"seed_{seed}"
                        / f"{experiment}_state_scaler.json"
                    )
                    scaler_path.write_text(
                        json.dumps(trained["state_scaler"], indent=2), encoding="utf-8"
                    )
                    log = evaluate_agent(
                        trained["agent"],
                        frames["test"],
                        UNIFIED_DQN_STATE_COLUMNS,
                        initial_cash=config.initial_cash,
                        transaction_cost=config.transaction_cost,
                        experiment=experiment,
                        seed=seed,
                        state_scaler=trained["state_scaler"],
                        log_q_values=True,
                    )
                    log["fold"] = fold_id
                    reward = trained["training_rewards"].copy()
                    reward["fold"] = fold_id
                    logs.append(log)
                    training.append(reward)
                    metrics.append(
                        {
                            **_metrics_from_log(
                                experiment, log, config.initial_cash, seed
                            ),
                            "fold": fold_id,
                            "average_exposure": float(
                                log["current_exposure_after"].mean()
                            ),
                            "turnover": float(log["turnover"].sum()),
                        }
                    )
                    print(
                        f"fold={fold_id} seed={seed} group={experiment} episodes={config.episodes}",
                        flush=True,
                    )
        metric_frame = pd.DataFrame(metrics)
        paired = []
        control = metric_frame[
            metric_frame["experiment"] == "dqn_without_nlp"
        ].set_index(["fold", "seed"])
        for _, row in metric_frame[
            metric_frame["experiment"].isin(list(GROUPS)[1:])
        ].iterrows():
            baseline = control.loc[(row["fold"], row["seed"])]
            paired.append(
                {
                    "target_symbol": config.target_symbol,
                    "fold": int(row["fold"]),
                    "seed": int(row["seed"]),
                    "experiment": row["experiment"],
                    **{
                        f"delta_{metric}": float(row[metric] - baseline[metric])
                        for metric in [
                            "cumulative_return",
                            "sharpe_ratio",
                            "max_drawdown",
                        ]
                    },
                }
            )
        for name, frame in [
            ("metrics", metric_frame),
            ("paired", pd.DataFrame(paired)),
            ("trades", pd.concat(logs)),
            ("training", pd.concat(training)),
            ("signals", pd.concat(signals)),
            ("trace", pd.concat(traces)),
        ]:
            frame.to_csv(run_dir / manifest["files"][name], index=False)
        manifest["status"] = "complete"
        manifest["file_sha256"] = {
            name: hashlib.sha256((run_dir / path).read_bytes()).hexdigest()
            for name, path in manifest["files"].items()
        }
        manifest["model_sha256"] = {
            path.relative_to(run_dir).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted(run_dir.rglob("*"))
            if path.is_file()
            and (path.suffix == ".pt" or path.name.endswith("_state_scaler.json"))
        }
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        raise
    finally:
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path
