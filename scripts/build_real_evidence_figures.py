"""Export figures from frozen results; no fitting or winner selection."""

from pathlib import Path
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "docs/assets"
plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.labelcolor": "#273444",
        "text.color": "#273444",
        "axes.edgecolor": "#65758B",
        "savefig.facecolor": "white",
    }
)

if __name__ == "__main__":
    records = {}
    for path in (ROOT / "outputs/real").glob("*/manifest.json"):
        manifest = json.loads(path.read_text())
        assert manifest["status"] == "complete"
        records[manifest["config"]["target_symbol"]] = pd.read_csv(
            path.parent / "paired_differences.csv"
        )
    fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharey=True)
    groups = ["dqn_with_sector_sentiment_nlp", "dqn_with_sector_impact_nlp"]
    colors = ["#2A7F7A", "#B7791F"]
    markers = {42: "o", 123: "^", 2024: "s"}
    for row, symbol in enumerate(["002475", "601138"]):
        for column, fold in enumerate([1, 2]):
            ax = axes[row, column]
            frame = records[symbol].query("fold == @fold")
            ax.axhline(0, color="#65758B", linewidth=1)
            for i, group in enumerate(groups):
                values = frame[frame.experiment == group]
                for j, sample in enumerate(values.itertuples()):
                    ax.scatter(
                        i + (j - 1) * 0.10,
                        sample.delta_cumulative_return * 100,
                        color=colors[i],
                        marker=markers[sample.seed],
                        s=56,
                        label=f"Seed {sample.seed}"
                        if row == column == i == 0
                        else None,
                    )
                mean = values.delta_cumulative_return.mean() * 100
                ax.plot(
                    [i - 0.22, i + 0.22], [mean, mean], color=colors[i], linewidth=3
                )
                ax.text(i, 35, f"mean {mean:+.2f}", ha="center", fontsize=9)
            ax.set_xticks([0, 1], ["Sentiment", "Impact"])
            ax.set_xlim(-0.5, 1.5)
            ax.set_ylim(-22, 39)
            ax.set_title(f"{symbol} · Fold {fold}", loc="left", fontweight="bold")
            ax.grid(axis="y", alpha=0.17)
            if column == 0:
                ax.set_ylabel("Δ cumulative return (pp)")
    fig.suptitle(
        "NLP gains vary by target, fold and seed",
        fontsize=18,
        fontweight="bold",
        x=0.08,
        ha="left",
    )
    fig.legend(
        *axes[0, 0].get_legend_handles_labels(),
        loc="lower center",
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, 0.035),
    )
    fig.text(
        0.08,
        0.012,
        "NLP minus market-only; strokes are means. Seeds measure optimization variation, not independent markets.",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0.10, 1, 0.92))
    fig.savefig(DEST / "real-paired-returns.png", dpi=170)
    plt.close(fig)

    directory = ROOT / "outputs/nlp_evaluation/cfbenchmark_20261010"
    metrics = pd.read_csv(directory / "common_test_metrics.csv").set_index("method")
    confusion = pd.read_csv(directory / "common_test_confusion.csv")
    fig, axes = plt.subplots(
        1, 2, figsize=(11, 4.8), gridspec_kw={"width_ratios": [1.25, 1]}
    )
    values = metrics.loc[
        ["lexicon", "logistic_tfidf", "finbert"], "macro_f1"
    ].to_numpy()
    axes[0].bar(
        ["Lexicon", "TF-IDF / LR", "Chinese FinBERT"],
        values,
        color=["#65758B", "#2A7F7A", "#17324D"],
        width=0.6,
    )
    axes[0].set_ylim(0, 1)
    axes[0].set_ylabel("Macro F1")
    axes[0].set_title("Same 149 held-out texts", loc="left", fontweight="bold")
    for i, value in enumerate(values):
        axes[0].text(i, value + 0.025, f"{value:.3f}", ha="center")
    axes[0].grid(axis="y", alpha=0.17)
    matrix = (
        confusion[confusion.method == "finbert"]
        .pivot(index="true_label", columns="predicted_label", values="count")
        .reindex(index=[-1, 0, 1], columns=[-1, 0, 1])
        .to_numpy()
    )
    axes[1].imshow(matrix, cmap="Blues", vmin=0, vmax=matrix.max())
    for i in range(3):
        for j in range(3):
            axes[1].text(
                j,
                i,
                str(matrix[i, j]),
                ha="center",
                va="center",
                color="white" if matrix[i, j] > matrix.max() * 0.55 else "#273444",
                fontsize=14,
            )
    axes[1].set_xticks(range(3), ["Negative", "Neutral", "Positive"])
    axes[1].set_yticks(range(3), ["Negative", "Neutral", "Positive"])
    axes[1].set_xlabel("FinBERT prediction")
    axes[1].set_ylabel("Human consensus label")
    axes[1].set_title("FinBERT confusion counts", loc="left", fontweight="bold")
    fig.suptitle(
        "External Chinese benchmark: measured FinBERT F1 is lower",
        fontsize=17,
        fontweight="bold",
        x=0.055,
        ha="left",
    )
    fig.text(
        0.055,
        0.02,
        "Frozen 75/25 split; checkpoint unchanged. Overlap with its private training corpus is unknown. Long texts are truncated at 512 tokens.",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0.10, 1, 0.90))
    fig.savefig(DEST / "finbert-benchmark.png", dpi=170)
    plt.close(fig)
