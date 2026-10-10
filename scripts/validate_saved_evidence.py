"""Recompute financial and NLP evidence independently from saved ledgers."""

from pathlib import Path
import argparse
import hashlib
import json

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score

ROOT = Path(__file__).resolve().parents[1]


def validate_source() -> dict:
    directory = ROOT / "data/frozen/electronics_2023_2025_20261010"
    path = directory / "source_manifest.json"
    source = json.loads(path.read_text())
    for name, digest in source["file_sha256"].items():
        assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == digest
    for request in source["requests"]:
        assert (
            hashlib.sha256((directory / request["file"]).read_bytes()).hexdigest()
            == request["sha256"]
        )
    return {"manifest": source, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def close(actual, expected) -> None:
    np.testing.assert_allclose(actual, expected, rtol=1e-9, atol=1e-8, equal_nan=True)


def validate_run(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    assert manifest["status"] == "complete" and manifest["mode"] == "full"
    source = validate_source()
    assert manifest["source_snapshot"] == source["manifest"]
    assert manifest["source_manifest_sha256"] == source["sha256"]
    directory = path.parent
    for name, filename in manifest["files"].items():
        assert (
            hashlib.sha256((directory / filename).read_bytes()).hexdigest()
            == manifest["file_sha256"][name]
        )
    for filename, digest in manifest["model_sha256"].items():
        assert hashlib.sha256((directory / filename).read_bytes()).hexdigest() == digest
    assert len(manifest["model_sha256"]) == 36
    assert (
        sum(name.endswith("_dqn_model.pt") for name in manifest["model_sha256"]) == 18
    )
    frames = {
        name: pd.read_csv(directory / filename)
        for name, filename in manifest["files"].items()
        if filename.endswith(".csv")
    }
    config = manifest["config"]
    assert (
        config["episodes"] == 200
        and config["seeds"] == [42, 123, 2024]
        and config["folds"] == 2
    )
    assert len(frames["metrics"]) == 20 and len(frames["paired"]) == 12
    training = frames["training"]
    assert len(training) == 3600
    for _, group in training.groupby(["fold", "seed", "experiment"]):
        assert sorted(group.episode) == list(range(1, 201))
    ledger = frames["trades"]
    assert len(ledger) == 1260
    for (fold, seed, experiment), group in ledger.groupby(
        ["fold", "seed", "experiment"]
    ):
        group = group.sort_values("date")
        assert len(group) == 63 and not group.date.duplicated().any()
        assert (group.cash >= 0).all() and (group.shares >= 0).all()
        changes = group.shares.to_numpy() - group.shares_before.to_numpy()
        gross = np.abs(changes) * group.price.to_numpy()
        close(group.transaction_cost, gross * config["transaction_cost"])
        close(
            group.cash,
            group.cash_before - changes * group.price - group.transaction_cost,
        )
        close(group.portfolio_value, group.cash + group.shares * group.valuation_price)
        nav = np.r_[config["initial_cash"], group.portfolio_value.to_numpy()]
        returns = np.diff(nav) / nav[:-1]
        close(group.previous_close_value, nav[:-1])
        close(group.reward, returns)
        close(np.prod(1 + returns), nav[-1] / nav[0])
        close(group.turnover, gross / nav[:-1])
        metric = (
            frames["metrics"]
            .query("fold == @fold and seed == @seed and experiment == @experiment")
            .iloc[0]
        )
        close(metric.cumulative_return, nav[-1] / nav[0] - 1)
        close(metric.max_drawdown, abs(np.min(nav / np.maximum.accumulate(nav) - 1)))
        sharpe = (
            np.sqrt(252) * returns.mean() / returns.std(ddof=1)
            if returns.std(ddof=1) > 0
            else np.nan
        )
        close(metric.sharpe_ratio, sharpe)
        assert metric.number_of_trades == group.action.isin(["Buy", "Sell"]).sum()
        if experiment == "dqn_without_nlp":
            assert group.state_nlp_signal_score.eq(0).all()
        if experiment == "buy_and_hold":
            assert (
                seed == -1
                and group.action.eq("Buy").sum() == 1
                and group.action.eq("Sell").sum() == 0
            )
    metrics = frames["metrics"].set_index(["fold", "seed", "experiment"])
    for row in frames["paired"].itertuples():
        base = metrics.loc[(row.fold, row.seed, "dqn_without_nlp")]
        candidate = metrics.loc[(row.fold, row.seed, row.experiment)]
        for metric in ["cumulative_return", "sharpe_ratio", "max_drawdown"]:
            close(getattr(row, "delta_" + metric), candidate[metric] - base[metric])
    for fold in manifest["folds"]:
        labels = frames[f"peer_labels_fold_{fold['fold']}"]
        cutoff = pd.Timestamp(fold["nlp_fit_end"])
        assert (pd.to_datetime(labels.label_end_at, utc=True) <= cutoff).all()
        assert (pd.to_datetime(labels.available_at, utc=True) <= cutoff).all()
        assert set(labels.symbol.astype(str).str.zfill(6)) == {"002241", "601231"}
    trace = frames["trace"]
    opening = pd.to_datetime(trace.decision_date).dt.tz_localize(
        "Asia/Shanghai"
    ) + pd.Timedelta(hours=9, minutes=30)
    assert (pd.to_datetime(trace.available_at, utc=True) < opening).all()
    return {
        "run_id": manifest["run_id"],
        "models": 18,
        "ledger_rows": len(ledger),
        "training_rows": len(training),
        "status": "passed",
    }


def validate_nlp(directory: Path) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text())
    for filename, digest in manifest["files"].items():
        assert hashlib.sha256((directory / filename).read_bytes()).hexdigest() == digest
    gold = pd.read_csv(ROOT / "data/benchmarks/cfbenchmark_sentiment.csv")
    assert (
        hashlib.sha256(
            (ROOT / "data/benchmarks/cfbenchmark_sentiment.csv").read_bytes()
        ).hexdigest()
        == manifest["gold_csv_sha256"]
    )
    assert gold.split.value_counts().to_dict() == {"train": 444, "test": 149}
    assert not gold.text.str.replace(r"\s+", "", regex=True).duplicated().any()
    common = pd.read_csv(directory / "common_test_predictions.csv")
    expected = gold[gold.split == "test"]
    assert len(common) == 149 and len(gold) == 593
    merged = common.merge(
        expected[["text", "gold_label_int"]],
        on="text",
        validate="one_to_one",
        suffixes=("", "_source"),
    )
    assert (
        len(merged) == 149
        and merged.gold_label_int.eq(merged.gold_label_int_source).all()
    )
    metrics = pd.read_csv(directory / "common_test_metrics.csv").set_index("method")
    confusion = pd.read_csv(directory / "common_test_confusion.csv")
    for method in ["lexicon", "logistic_tfidf", "finbert"]:
        close(
            metrics.loc[method, "accuracy"],
            accuracy_score(common.gold_label_int, common[method]),
        )
        close(
            metrics.loc[method, "macro_f1"],
            f1_score(common.gold_label_int, common[method], average="macro"),
        )
        matrix = (
            confusion[confusion.method == method]
            .pivot(index="true_label", columns="predicted_label", values="count")
            .reindex(index=[-1, 0, 1], columns=[-1, 0, 1])
        )
        close(
            matrix.to_numpy(),
            confusion_matrix(common.gold_label_int, common[method], labels=[-1, 0, 1]),
        )
    probability_columns = [
        "finbert_negative_probability",
        "finbert_neutral_probability",
        "finbert_positive_probability",
    ]
    np.testing.assert_allclose(
        common[probability_columns].sum(axis=1), 1, rtol=0, atol=1e-6
    )
    close(
        common.finbert,
        np.array([-1, 0, 1])[common[probability_columns].to_numpy().argmax(axis=1)],
    )
    full = pd.read_csv(directory / "finbert_all_external_predictions.csv")
    assert (
        full.id.tolist() == gold.id.tolist()
        and full.gold_label_int.eq(gold.gold_label_int).all()
        and full.text.eq(gold.text).all()
        and full.split.eq(gold.split).all()
    )
    full_columns = [
        "negative_probability",
        "neutral_probability",
        "positive_probability",
    ]
    np.testing.assert_allclose(full[full_columns].sum(axis=1), 1, rtol=0, atol=1e-6)
    close(
        full.prediction,
        np.array([-1, 0, 1])[full[full_columns].to_numpy().argmax(axis=1)],
    )
    close(
        manifest["all_external_finbert"]["accuracy"],
        accuracy_score(full.gold_label_int, full.prediction),
    )
    close(
        manifest["all_external_finbert"]["macro_f1"],
        f1_score(full.gold_label_int, full.prediction, average="macro"),
    )
    assert full.truncated.sum() == manifest["runtime"]["truncated_rows"]
    return {"benchmark_rows": 593, "common_test_rows": 149, "status": "passed"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    runs = list((ROOT / "outputs/real").glob("*/manifest.json"))
    assert len(runs) == 2, "Two completed real-data targets are required"
    result = {
        "trading": [validate_run(p) for p in sorted(runs)],
        "nlp": validate_nlp(ROOT / "outputs/nlp_evaluation/cfbenchmark_20261010"),
        "status": "passed",
    }
    if args.output:
        args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
