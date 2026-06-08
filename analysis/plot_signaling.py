import json
import glob
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "signaling")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "plots")
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.4,
})

COLORS = {"central": "#1f77b4", "dht": "#ff7f0e"}

def load_results() -> pd.DataFrame:
    rows = []
    for path in glob.glob(os.path.join(RESULTS_DIR, "*.json")):
        with open(path) as f:
            data = json.load(f)
        sig = data["signaling"]
        pair_count = data["pair_count"]
        for r in data.get("results", []):
            lat_ms = r["latency_ns"] / 1e6
            err = r.get("error", "")
            rows.append({
                "signaling": sig,
                "pair_count": pair_count,
                "pair_id": r["pair_id"],
                "connect_ms": lat_ms,
                "error": err,
                "file": os.path.basename(path),
            })
    df = pd.DataFrame(rows)
    return df


def plot_median_latency(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8, 5))

    for signaling in ["central", "dht"]:
        sub = df[df["signaling"] == signaling]
        counts = sorted(sub["pair_count"].unique())
        medians, p25, p75 = [], [], []
        for n in counts:
            vals = sub[sub["pair_count"] == n]["connect_ms"].values
            medians.append(np.median(vals))
            p25.append(np.percentile(vals, 25))
            p75.append(np.percentile(vals, 75))
        label = "Centralized signaling" if signaling == "central" else "DHT signaling"
        color = COLORS[signaling]
        ax.plot(counts, medians, marker="o", label=label, color=color)
        ax.fill_between(counts, p25, p75, alpha=0.2, color=color)

    ax.set_xlabel("Number of concurrent signaling pairs")
    ax.set_ylabel("Signaling latency (ms)")
    ax.set_title("Signaling latency: centralized vs DHT")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "signaling_latency.png"))
    plt.close(fig)
    print("saved signaling_latency.png")


def plot_boxplot(df: pd.DataFrame):
    pair_counts = sorted(df["pair_count"].unique())
    n_counts = len(pair_counts)
    fig, axes = plt.subplots(1, n_counts, figsize=(4 * n_counts, 5), sharey=True)
    if n_counts == 1:
        axes = [axes]

    for ax, n in zip(axes, pair_counts):
        data_central = df[(df["signaling"] == "central") & (df["pair_count"] == n)]["connect_ms"].values
        data_dht = df[(df["signaling"] == "dht") & (df["pair_count"] == n)]["connect_ms"].values
        bp = ax.boxplot(
            [d for d in [data_central, data_dht] if len(d) > 0],
            labels=[s for s, d in [("Central", data_central), ("DHT", data_dht)] if len(d) > 0],
            patch_artist=True,
        )
        colors = [COLORS["central"], COLORS["dht"]]
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
        ax.set_title(f"N={n} pairs")
        ax.set_ylabel("Connection time (ms)" if ax == axes[0] else "")

    fig.suptitle("WebRTC connection time distribution by signaling type")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "signaling_boxplot.png"))
    plt.close(fig)
    print("saved signaling_boxplot.png")

if __name__ == "__main__":
    df = load_results()
    if df.empty:
        print("no results found in", RESULTS_DIR)
        raise SystemExit(1)

    print(f"loaded {len(df)} pair results")
    plot_median_latency(df)
    plot_boxplot(df)
