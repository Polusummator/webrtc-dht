import json
import sys
import os
import numpy as np
import matplotlib.pyplot as plt

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "plots")
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.4,
})


def size_label(b: int) -> str:
    if b < 1048576:
        return f"{b // 1024}KB"
    return f"{b // 1048576}MB"


def load(path: str):
    with open(path) as f:
        return json.load(f)


def plot_throughput(data, out_path):
    sizes = [r["blob_size"] for r in data]
    mbps  = [r["mbps"]      for r in data]
    labels = [size_label(s) for s in sizes]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(range(len(sizes)), mbps, marker="o", color="#2ca02c", linewidth=2)
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels(labels)
    ax.set_xlabel("Payload size")
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_title("pion/webrtc DataChannel throughput")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)
    print(f"saved {os.path.basename(out_path)}")


def plot_latency(data, out_path):
    sizes  = [r["blob_size"] for r in data]
    mins   = [r["min_ms"]    for r in data]
    meds   = [r["med_ms"]    for r in data]
    p95s   = [r["p95_ms"]    for r in data]
    labels = [size_label(s)  for s in sizes]

    x = np.arange(len(sizes))
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(x, mins, marker="o", label="min",    color="#1f77b4")
    ax.plot(x, meds, marker="s", label="median", color="#ff7f0e")
    ax.plot(x, p95s, marker="^", label="P95",    color="#d62728")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Payload size")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("pion/webrtc DataChannel latency")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)
    print(f"saved {os.path.basename(out_path)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python plot_pion_raw.py <results.json>")
        raise SystemExit(1)

    data = load(sys.argv[1])
    if not data:
        print("empty results")
        raise SystemExit(1)

    print(f"loaded {len(data)} entries")
    plot_throughput(data, os.path.join(OUT_DIR, "pion_raw_throughput.png"))
    plot_latency(data,    os.path.join(OUT_DIR, "pion_raw_latency.png"))

    print("\n=== Summary ===")
    print(f"{'blob-size':>10}  {'MB/s':>8}  {'med-ms':>8}  {'p95-ms':>8}")
    for r in data:
        print(f"{size_label(r['blob_size']):>10}  {r['mbps']:8.1f}  {r['med_ms']:8.1f}  {r['p95_ms']:8.1f}")

