import json
import glob
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results", "transport")
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
TRANSPORT_LABEL_BLOB = {
    "udp":    "UDP (TCP)",
    "webrtc": "WebRTC",
}
TRANSPORT_COLOR = {
    "udp":    "#1f77b4",
    "webrtc": "#2ca02c",
}

def _label(t, blob=False):
    labels = TRANSPORT_LABEL_BLOB if blob else TRANSPORT_LABEL
    return labels.get(t, t.upper())

def _color(t):
    return TRANSPORT_COLOR.get(t, None)


def load_results() -> pd.DataFrame:
    rows = []
    for path in glob.glob(os.path.join(RESULTS_DIR, "*.json")):
        with open(path) as f:
            data = json.load(f)
        for r in data:
            rows.append(r)
    df = pd.DataFrame(rows)
    df = df[df["transport"] != "udp-tls"]
    df["latency_ms"] = df["duration_ns"] / 1e6
    if "conn_setup_ns" not in df.columns:
        df["conn_setup_ns"] = 0
    df["conn_setup_ms"] = df["conn_setup_ns"] / 1e6
    return df


def plot_ping_cdf(df: pd.DataFrame):
    ping = df[df["mode"] == "ping"]
    if ping.empty:
        print("no ping data")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    for transport, grp in ping.groupby("transport"):
        lat = np.sort(grp["latency_ms"].values)
        cdf = np.arange(1, len(lat) + 1) / len(lat)
        ax.plot(lat, cdf, label=_label(transport), color=_color(transport))

    ax.set_xlabel("Round-trip latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title("Ping latency CDF")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "transport_ping_cdf.png"))
    plt.close(fig)
    print("saved transport_ping_cdf.png")


def plot_latency_by_payload(df: pd.DataFrame, mode: str):
    sub = df[df["mode"] == mode]
    if sub.empty:
        print(f"no {mode} data")
        return

    is_blob = mode == "blob"
    payloads = sorted(sub["payload_bytes"].unique())
    transports = sorted(sub["transport"].unique())
    x = np.arange(len(payloads))
    width = 0.8 / len(transports)

    fig, ax = plt.subplots(figsize=(8, 4))
    for i, transport in enumerate(transports):
        means, errs = [], []
        for p in payloads:
            vals = sub[(sub["transport"] == transport) & (sub["payload_bytes"] == p)]["latency_ms"]
            means.append(vals.mean() if not vals.empty else np.nan)
            errs.append(vals.std() if not vals.empty else 0)
        offset = (i - len(transports) / 2 + 0.5) * width
        ax.bar(x + offset, means, width, yerr=errs,
               label=_label(transport, blob=is_blob), color=_color(transport), capsize=4)

    ax.set_xticks(x)
    ax.set_xticklabels([
        f"{p}B" if p < 1024 else f"{p//1024}KB" if p < 1048576 else f"{p//1048576}MB"
        for p in payloads
    ])
    ax.set_xlabel("Payload size")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"{mode.capitalize()} latency")
    ax.legend()
    fig.tight_layout()
    out = os.path.join(OUT_DIR, f"transport_{mode}_latency.png")
    fig.savefig(out)
    plt.close(fig)
    print(f"saved {os.path.basename(out)}")


def plot_throughput(df: pd.DataFrame, mode: str):
    sub = df[(df["mode"] == mode) & (df["transferred_bytes"] > 0)]
    if sub.empty:
        print(f"no throughput data for {mode}")
        return

    is_blob = mode == "blob"
    payloads = sorted(sub["payload_bytes"].unique())
    transports = sorted(sub["transport"].unique())

    fig, ax = plt.subplots(figsize=(8, 4))
    for transport in transports:
        mbps_vals = []
        for p in payloads:
            grp = sub[(sub["transport"] == transport) & (sub["payload_bytes"] == p)]
            if grp.empty:
                mbps_vals.append(np.nan)
            else:
                dur_s = grp["duration_ns"].values / 1e9
                bytes_ = grp["transferred_bytes"].values
                mbps = (bytes_ / dur_s) / 1e6
                mbps_vals.append(np.nanmean(mbps))
        ax.plot(payloads, mbps_vals, marker="o",
                label=_label(transport, blob=is_blob), color=_color(transport))

    ax.set_xscale("log")
    ax.set_xlabel("Payload size")
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_title(f"{mode.capitalize()} throughput")
    ax.set_xticks(payloads)
    ax.set_xticklabels([
        f"{p}B" if p < 1024 else f"{p//1024}KB" if p < 1048576 else f"{p//1048576}MB"
        for p in payloads
    ])
    ax.legend()
    fig.tight_layout()
    out = os.path.join(OUT_DIR, f"transport_{mode}_throughput.png")
    fig.savefig(out)
    plt.close(fig)
    print(f"saved {os.path.basename(out)}")


def plot_conn_setup(df: pd.DataFrame):
    setup = df[df["conn_setup_ns"] > 0].drop_duplicates(subset=["transport", "iteration"])
    if setup.empty:
        print("no conn setup data")
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    transports = sorted(setup["transport"].unique())
    means = [setup[setup["transport"] == t]["conn_setup_ms"].mean() for t in transports]
    errs  = [setup[setup["transport"] == t]["conn_setup_ms"].std()  for t in transports]
    colors = [_color(t) for t in transports]
    ax.bar([_label(t) for t in transports], means, yerr=errs, capsize=6, color=colors)
    ax.set_ylabel("Connection setup time (ms)")
    ax.set_title("Connection setup time")
    plt.xticks(rotation=15, ha="right")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "transport_conn_setup.png"))
    plt.close(fig)
    print("saved transport_conn_setup.png")

if __name__ == "__main__":
    df = load_results()
    if df.empty:
        print("no results found in", RESULTS_DIR)
        raise SystemExit(1)

    print(f"loaded {len(df)} records, transports: {sorted(df['transport'].unique())}")
    plot_ping_cdf(df)
    plot_latency_by_payload(df, "store")
    plot_latency_by_payload(df, "findvalue")
    plot_throughput(df, "store")
    plot_throughput(df, "findvalue")
    plot_latency_by_payload(df, "blob")
    plot_throughput(df, "blob")
    plot_conn_setup(df)
