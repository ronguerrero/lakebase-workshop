#!/usr/bin/env python3
"""Regenerate the Ex6 / Ex7 architecture diagrams as PNGs.

These two round-trip diagrams are kept in sync with the current labs here (the
other exercises' diagrams were produced earlier and embedded as-is). Style is
matched to those: white ground, teal for lakehouse/Delta artifacts, orange for
Lakebase, gray for infrastructure, monospace sub-labels.

    python scripts/diagrams/build_diagrams.py

Requires matplotlib. Writes:
    labs/06-delta-to-lakebase-sync/images/ex6-delta-sync.png
    labs/07-lakebase-cdf-to-scd1/images/ex7-cdf-scd1.png
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# ── palette (sampled from the existing diagrams) ───────────────────────────
BG          = "#ffffff"
TEAL_EDGE   = "#0f6e78"; TEAL_FILL   = "#e4f0f1"
ORANGE_EDGE = "#b96c13"; ORANGE_FILL = "#fbeedd"
GRAY_EDGE   = "#cdd2da"; GRAY_FILL   = "#fbfcfd"
TITLE       = "#1f2733"
SUB         = "#586673"
ARROW_GRAY  = "#586673"
ARROW_ORANGE= "#b96c13"

KIND = {
    "teal":   (TEAL_EDGE,   TEAL_FILL),
    "orange": (ORANGE_EDGE, ORANGE_FILL),
    "gray":   (GRAY_EDGE,   GRAY_FILL),
}


def box(ax, x, y, w, h, kind, title, sub=None, caption=None):
    """Rounded box centered at (x, y). Optional orange caption line at the top."""
    edge, fill = KIND[kind]
    ax.add_patch(FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle="round,pad=0,rounding_size=0.10",
        linewidth=2.2, edgecolor=edge, facecolor=fill, mutation_aspect=1))
    top = y + h / 2
    ty = y + h * 0.10
    if caption:
        ax.text(x, top - h * 0.18, caption, ha="center", va="center",
                family="monospace", fontsize=9.5, color=ORANGE_EDGE, weight="bold")
        ty = y - h * 0.02
    ax.text(x, ty, title, ha="center", va="center",
            fontsize=14, color=TITLE, weight="bold")
    if sub:
        ax.text(x, ty - h * 0.26, sub, ha="center", va="center",
                family="monospace", fontsize=10, color=SUB)


def arrow(ax, x0, x1, y, label, color):
    ax.add_patch(FancyArrowPatch(
        (x0, y), (x1, y), arrowstyle="-|>", mutation_scale=18,
        linewidth=2.2, color=color, shrinkA=0, shrinkB=0))
    ax.text((x0 + x1) / 2, y + 0.16, label, ha="center", va="bottom",
            family="monospace", fontsize=9.5, color=color)


def canvas(w_px, h_px):
    fig = plt.figure(figsize=(w_px / 128, h_px / 128), dpi=128)
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_facecolor(BG)
    fig.patch.set_facecolor(BG)
    ax.set_xlim(0, 14); ax.set_ylim(0, 14 * h_px / w_px)
    ax.axis("off")
    return fig, ax


def save(fig, rel):
    out = os.path.join(REPO, rel)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=128, facecolor=BG)
    plt.close(fig)
    print("wrote", rel)


def build_ex6():
    fig, ax = canvas(1408, 320); yc = ax.get_ylim()[1] / 2
    bw, bh = 3.5, 1.7
    box(ax, 2.1, yc, bw, bh, "teal",   "src_client_reference", "Delta · UC · PK + CDF")
    box(ax, 7.0, yc, bw, bh, "gray",   "synced-table pipeline", "serverless · SNAPSHOT / TRIGGERED / CONT.")
    box(ax, 11.9, yc, bw, bh, "orange", "client_reference", "operational Postgres",
        caption="◆ YOUR LAKEBASE BRANCH")
    arrow(ax, 3.95, 5.15, yc, "sync", ARROW_GRAY)
    arrow(ax, 8.85, 10.05, yc, "CDF", ARROW_ORANGE)
    save(fig, "labs/06-delta-to-lakebase-sync/images/ex6-delta-sync.png")


def build_ex7():
    fig, ax = canvas(1408, 336); yc = ax.get_ylim()[1] / 2
    bw, bh = 3.35, 1.95
    box(ax, 1.9, yc, bw, bh, "orange", "positions · limits", "REPLICA IDENTITY FULL",
        caption="◆ YOUR LAKEBASE BRANCH")
    box(ax, 7.0, yc, bw, bh, "teal", "lb_positions_history", "lb_limits_history · Delta")
    box(ax, 12.1, yc, bw, bh, "teal", "positions_current", "limits_current · SCD Type 1")
    arrow(ax, 3.7, 5.2, yc, "CDF", ARROW_ORANGE)
    arrow(ax, 8.8, 10.3, yc, "AUTO CDC", ARROW_GRAY)
    # small context captions under the two lakehouse boxes
    ax.text(7.0, yc - bh / 2 - 0.35, "Unity Catalog — auto-created by CDF",
            ha="center", family="monospace", fontsize=9, color=SUB)
    ax.text(12.1, yc - bh / 2 - 0.35, "Lakeflow (DLT) AUTO CDC output",
            ha="center", family="monospace", fontsize=9, color=SUB)
    save(fig, "labs/07-lakebase-cdf-to-scd1/images/ex7-cdf-scd1.png")


if __name__ == "__main__":
    build_ex6()
    build_ex7()
