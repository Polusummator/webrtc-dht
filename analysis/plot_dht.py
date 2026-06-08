import json
import glob
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "dht2")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "plots")
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.4,
})

TRANSPORT_LABEL = {
    "udp":    "UDP",
    "webrtc": "WebRTC",
}
TRANSPORT_COLOR = {
    "udp":    "#1f77b4",
    "webrtc": "#2ca02c",
}

def _label(t): return TRANSPORT_LABEL.get(t, t.upper())
def _color(t): return TRANSPORT_COLOR.get(t, None)


def load_results() -> pd.DataFrame:
    rows = []
    for path in glob.glob(os.path.join(RESULTS_DIR, "*.json")):
        with open(path) as f:
            data = json.load(f)
        rows.extend(data)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["latency_ms"] = df["duration_ns"] / 1e6
    return df


def plot_latency_vs_nodes(df: pd.DataFrame, mode: str):
    sub = df[df["mode"] == mode]
    if sub.empty:
        print(f"no {mode} data")
        return

    node_counts = sorted(sub["node_count"].unique())
    transports  = sorted(sub["transport"].unique())

    fig, ax = plt.subplots(figsize=(8, 4))
    for tr in transports:
        medians = []
        for n in node_counts:
            vals = sub[(sub["transport"] == tr) & (sub["node_count"] == n)]["latency_ms"]
            medians.append(np.percentile(vals, 50) if not vals.empty else np.nan)
        ax.plot(node_counts, medians, marker="o", label=_label(tr), color=_color(tr))

    ax.set_xlabel("Number of DHT nodes")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"DHT {mode} latency")
    ax.set_xticks(node_counts)
    ax.legend()
    fig.tight_layout()
    out = os.path.join(OUT_DIR, f"dht_{mode}_latency.png")
    fig.savefig(out)
    plt.close(fig)
    print(f"saved {os.path.basename(out)}")

if __name__ == "__main__":
    df = load_results()
    if df.empty:
        print("no results found in", RESULTS_DIR)
        raise SystemExit(1)

    print(f"loaded {len(df)} records, transports: {sorted(df['transport'].unique())}, "
          f"node counts: {sorted(df['node_count'].unique())}")

    for mode in ["store"]:
        plot_latency_vs_nodes(df, mode)
