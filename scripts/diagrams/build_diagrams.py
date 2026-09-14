#!/usr/bin/env python3
"""Regenerate every exercise's architecture diagram as a PNG, in one house style.

    python scripts/diagrams/build_diagrams.py            # all
    python scripts/diagrams/build_diagrams.py ex4 ex7    # just some

Style: white ground; teal = lakehouse / Delta / built artifacts, orange =
Lakebase, gray = infrastructure & external actors; monospace sub-labels; 1408px
wide (taller diagrams keep proportion). Edit the builders below and re-run — this
script is the single source of truth for the diagrams embedded in the READMEs.

Requires matplotlib. Each builder writes to labs/<ex>/images/<name>.png.
"""
import os
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# ── palette (sampled from the original embedded diagrams) ──────────────────
BG           = "#ffffff"
TEAL_EDGE    = "#0f6e78"; TEAL_FILL    = "#e4f0f1"
ORANGE_EDGE  = "#b96c13"; ORANGE_FILL  = "#fbeedd"
GRAY_EDGE    = "#cdd2da"; GRAY_FILL    = "#fbfcfd"
TITLE_COLOR  = "#1f2733"
SUB_COLOR    = "#586673"
ARROW_GRAY   = "#586673"
ARROW_ORANGE = "#b96c13"
KIND = {"teal": (TEAL_EDGE, TEAL_FILL), "orange": (ORANGE_EDGE, ORANGE_FILL),
        "gray": (GRAY_EDGE, GRAY_FILL)}


def _canvas(w_px, h_px):
    fig = plt.figure(figsize=(w_px / 128, h_px / 128), dpi=128)
    ax = fig.add_axes([0, 0, 1, 1])
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    ax.set_xlim(0, 14); ax.set_ylim(0, 14 * h_px / w_px)
    ax.axis("off")
    return fig, ax


def _rrect(ax, cx, cy, w, h, kind):
    edge, fill = KIND[kind]
    ax.add_patch(FancyBboxPatch(
        (cx - w / 2, cy - h / 2), w, h,
        boxstyle="round,pad=0,rounding_size=0.10",
        linewidth=2.2, edgecolor=edge, facecolor=fill, mutation_aspect=1))


def node(ax, cx, cy, w, h, kind, title=None, sub=None, caption=None,
         ts=14, ss=10):
    """Rounded box with optional orange caption line, bold title, mono sub-line(s)."""
    _rrect(ax, cx, cy, w, h, kind)
    subs = [] if sub is None else (sub if isinstance(sub, list) else [sub])
    if caption:
        ax.text(cx, cy + h / 2 - 0.30, caption, ha="center", va="center",
                family="monospace", fontsize=9.5, color=ORANGE_EDGE, weight="bold")
    # center title + subs in the box (a touch low when a caption sits on top)
    base = cy - (0.12 if caption else 0) + (0.14 * len(subs))
    if title:
        ax.text(cx, base, title, ha="center", va="center",
                fontsize=ts, color=TITLE_COLOR, weight="bold")
        y = base - 0.30
    else:
        y = base
    for s in subs:
        ax.text(cx, y, s, ha="center", va="center",
                family="monospace", fontsize=ss, color=SUB_COLOR)
        y -= 0.30


def arrow(ax, p0, p1, label=None, color=ARROW_GRAY, ldx=0.0, ldy=0.18,
          size=18, fs=9.5, ha="center", va="bottom"):
    ax.add_patch(FancyArrowPatch(p0, p1, arrowstyle="-|>", mutation_scale=size,
                                 linewidth=2.2, color=color, shrinkA=0, shrinkB=0))
    if label:
        mx, my = (p0[0] + p1[0]) / 2 + ldx, (p0[1] + p1[1]) / 2 + ldy
        ax.text(mx, my, label, ha=ha, va=va, family="monospace", fontsize=fs, color=color)


def grouplabel(ax, x, y, text):
    ax.text(x, y, text, ha="left", va="center", family="monospace",
            fontsize=10, color=SUB_COLOR, weight="bold")


def _save(fig, rel):
    out = os.path.join(REPO, rel)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=128, facecolor=BG); plt.close(fig)
    print("wrote", rel)


# ── Ex1 — autoscaling / scale-to-zero ──────────────────────────────────────
def build_ex1():
    fig, ax = _canvas(1408, 320); yc = ax.get_ylim()[1] / 2
    node(ax, 1.95, yc, 3.2, 1.7, "gray", "Load (N workers)", "concurrent queries")
    # Lakebase branch with little ascending bars
    _rrect(ax, 7.0, yc, 4.3, 2.4, "orange")
    ax.text(7.0, yc + 0.85, "◆ YOUR BRANCH · compute endpoint", ha="center",
            family="monospace", fontsize=9.5, color=ORANGE_EDGE, weight="bold")
    for i, bh in enumerate([0.35, 0.55, 0.78, 1.02]):
        ax.add_patch(plt.Rectangle((5.55 + i * 0.42, yc - 0.55), 0.30, bh,
                                    color="#d29a52", ec="none"))
    ax.text(7.9, yc + 0.12, "autoscaling", ha="left", family="monospace",
            fontsize=11, color=TITLE_COLOR, weight="bold")
    ax.text(7.9, yc - 0.22, "pay per use", ha="left", family="monospace",
            fontsize=11, color=TITLE_COLOR, weight="bold")
    ax.text(7.0, yc - 0.92, "1 CU → max, following load", ha="center",
            family="monospace", fontsize=9.5, color=SUB_COLOR)
    node(ax, 12.05, yc, 3.2, 1.7, "gray", "scale to zero", "suspends, ~no cost")
    arrow(ax, (3.6, yc), (4.85, yc), "market open", ARROW_GRAY, fs=9)
    arrow(ax, (9.15, yc), (10.45, yc), "idle", ARROW_GRAY, fs=9)
    _save(fig, "labs/01-getting-to-know-lakebase/images/ex1-autoscale.png")


# ── Ex3 — REST vs JDBC ─────────────────────────────────────────────────────
def build_ex3():
    fig, ax = _canvas(1408, 384); H = ax.get_ylim()[1]
    yt, yb = H * 0.70, H * 0.30
    node(ax, 2.1, yt, 3.3, 1.35, "gray", "Serverless / edge app", "any language, no driver")
    node(ax, 6.75, yt, 3.3, 1.35, "teal", "REST data API", "HTTPS · OAuth · JSON")
    node(ax, 2.1, yb, 3.3, 1.35, "gray", "JVM service", "pooled connections")
    node(ax, 6.75, yb, 3.3, 1.35, "teal", "JDBC", "postgresql driver · ssl")
    node(ax, 11.5, H / 2, 3.4, 2.7, "orange", "managed Postgres",
         ["clients · positions", "trades"], caption="◆ YOUR LAKEBASE BRANCH")
    arrow(ax, (3.75, yt), (5.1, yt), "stateless", ARROW_GRAY, fs=9)
    arrow(ax, (3.75, yb), (5.1, yb), "persistent", ARROW_GRAY, fs=9)
    arrow(ax, (8.4, yt), (9.8, H / 2 + 0.5), color=ARROW_GRAY)
    arrow(ax, (8.4, yb), (9.8, H / 2 - 0.5), color=ARROW_GRAY)
    _save(fig, "labs/03-data-api/images/ex3-rest-vs-jdbc.png")


# ── Ex4 — online feature store + assistant ─────────────────────────────────
def build_ex4():
    fig, ax = _canvas(1440, 640); H = ax.get_ylim()[1]
    yt, yb = H * 0.72, H * 0.24
    grouplabel(ax, 0.3, H - 0.35, "BUILD ▸ offline → Lakebase")
    node(ax, 1.6, yt, 2.7, 1.4, "gray", "Capital-markets data", "Spark DataFrame", ts=12)
    node(ax, 5.2, yt, 2.7, 1.4, "teal", "client_risk_features", "Delta · UC · PK + CDF", ts=12)
    node(ax, 8.8, yt, 2.7, 1.4, "gray", "Sync pipeline", "serverless Lakeflow", ts=12)
    node(ax, 12.4, yt, 2.7, 1.85, "orange", "Online store",
         ["client_risk_features_online", "managed Postgres"], caption="◆ LAKEBASE", ts=13, ss=9)
    arrow(ax, (2.95, yt), (3.85, yt), "aggregate", ARROW_GRAY, fs=8, ldy=0.5)
    arrow(ax, (6.55, yt), (7.45, yt), "publish", ARROW_GRAY, fs=8, ldy=0.5)
    arrow(ax, (10.15, yt), (11.05, yt), "CDF sync", ARROW_ORANGE, fs=8, ldy=0.5)

    grouplabel(ax, 0.3, H * 0.46, "SERVE ▸ Lakebase → agent")
    node(ax, 2.7, yb, 3.1, 1.4, "teal", "Feature Serving", "endpoint (Model Serving)", ts=12)
    node(ax, 6.9, yb, 3.3, 1.4, "gray", "lookup_client_risk", "tool · MLflow deploy", ts=12)
    node(ax, 11.4, yb, 3.6, 1.4, "gray", "Coverage Desk Assistant", "LangChain + ChatDatabricks", ts=12)
    arrow(ax, (4.25, yb), (5.25, yb), "predict()", ARROW_GRAY, fs=8.5)
    arrow(ax, (9.6, yb), (8.55, yb), "calls", ARROW_GRAY, fs=8.5)   # assistant -> tool
    # orange connector: online store -> feature serving, an elbow through the mid band
    midy = H * 0.50
    ax.plot([12.4, 12.4], [yt - 0.95, midy], color=ARROW_ORANGE, lw=2.2, solid_capstyle="round")
    ax.plot([12.4, 2.7], [midy, midy], color=ARROW_ORANGE, lw=2.2, solid_capstyle="round")
    arrow(ax, (2.7, midy), (2.7, yb + 0.72), color=ARROW_ORANGE)
    ax.text(7.4, midy + 0.18, "FeatureSpec → serves online features", ha="center",
            va="bottom", family="monospace", fontsize=9, color=ARROW_ORANGE)
    _save(fig, "labs/04-online-feature-store/images/ex4-feature-store.png")


# ── Ex5 — agentic memory ───────────────────────────────────────────────────
def build_ex5():
    fig, ax = _canvas(1440, 600); H = ax.get_ylim()[1]
    yt = H * 0.76
    node(ax, 1.9, yt, 2.8, 1.35, "gray", "Coverage officer", "chat turn")
    node(ax, 5.6, yt, 3.3, 1.55, "teal", "LangGraph agent",
         ["StateGraph · 1 node", "compiled w/ checkpointer"], ts=13)
    node(ax, 9.75, yt, 3.1, 1.35, "gray", "ChatDatabricks", "pay-per-token LLM")
    arrow(ax, (3.3, yt), (3.95, yt), "message", ARROW_GRAY, fs=9, ldy=0.5)
    arrow(ax, (7.25, yt + 0.22), (8.2, yt + 0.22), "invoke", ARROW_GRAY, fs=9)
    arrow(ax, (8.2, yt - 0.28), (7.25, yt - 0.28), "reply", ARROW_GRAY, fs=9, ldy=-0.34, va="top")

    ym = H * 0.34
    node(ax, 4.4, ym, 3.3, 1.4, "teal", "PostgresSaver", "LangGraph checkpointer", ts=13)
    arrow(ax, (5.6, yt - 0.78), (4.4, ym + 0.72), "checkpoint each turn",
          ARROW_ORANGE, fs=8.5, ldx=1.3, ldy=0.05)
    # Lakebase box with 4 nested checkpoint tables
    bx, by, bw, bh = 10.4, ym - 0.05, 6.2, 3.1
    _rrect(ax, bx, by, bw, bh, "orange")
    ax.text(bx, by + bh / 2 - 0.32, "◆ LAKEBASE · managed Postgres", ha="center",
            family="monospace", fontsize=9.5, color=ORANGE_EDGE, weight="bold")
    tables = ["checkpoints", "checkpoint_writes", "checkpoint_blobs", "checkpoint_migrations"]
    for i, t in enumerate(tables):
        col, row = i % 2, i // 2
        tx = bx - 1.55 + col * 3.1
        ty = by + 0.62 - row * 0.78
        _rrect(ax, tx, ty, 2.7, 0.6, "gray")
        ax.text(tx, ty, t, ha="center", va="center", family="monospace",
                fontsize=9.5, color=TITLE_COLOR)
    ax.text(bx, by - bh / 2 + 0.55, "messages live in checkpoint_blobs", ha="center",
            family="monospace", fontsize=8.8, color=SUB_COLOR)
    ax.text(bx, by - bh / 2 + 0.28, "keyed by thread_id — survives turns & sessions",
            ha="center", family="monospace", fontsize=8.8, color=SUB_COLOR)
    arrow(ax, (6.05, ym), (bx - bw / 2, ym), "writes", ARROW_ORANGE, fs=9)
    _save(fig, "labs/05-agentic-memory/images/ex5-agent-memory.png")


# ── Ex6 — Delta -> Lakebase sync ───────────────────────────────────────────
def build_ex6():
    fig, ax = _canvas(1408, 320); yc = ax.get_ylim()[1] / 2
    node(ax, 2.1, yc, 3.5, 1.7, "teal", "src_client_reference", "Delta · UC · PK + CDF")
    node(ax, 7.0, yc, 3.5, 1.7, "gray", "synced-table pipeline", "serverless · SNAPSHOT / TRIGGERED")
    node(ax, 11.9, yc, 3.5, 1.7, "orange", "client_reference", "operational Postgres",
         caption="◆ YOUR LAKEBASE BRANCH")
    arrow(ax, (3.95, yc), (5.15, yc), "sync", ARROW_GRAY)
    arrow(ax, (8.85, yc), (10.05, yc), "CDF", ARROW_ORANGE)
    _save(fig, "labs/06-delta-to-lakebase-sync/images/ex6-delta-sync.png")


# ── Ex7 — Lakebase CDF -> SCD1 ─────────────────────────────────────────────
def build_ex7():
    fig, ax = _canvas(1408, 336); yc = ax.get_ylim()[1] / 2
    bw, bh = 3.35, 1.95
    node(ax, 1.9, yc, bw, bh, "orange", "positions · limits", "REPLICA IDENTITY FULL",
         caption="◆ YOUR LAKEBASE BRANCH")
    node(ax, 7.0, yc, bw, bh, "teal", "lb_positions_history", "lb_limits_history · Delta")
    node(ax, 12.1, yc, bw, bh, "teal", "positions_current", "limits_current · SCD Type 1")
    arrow(ax, (3.7, yc), (5.2, yc), "CDF", ARROW_ORANGE)
    arrow(ax, (8.8, yc), (10.3, yc), "AUTO CDC", ARROW_GRAY)
    ax.text(7.0, yc - bh / 2 - 0.35, "Unity Catalog — auto-created by CDF",
            ha="center", family="monospace", fontsize=9, color=SUB_COLOR)
    ax.text(12.1, yc - bh / 2 - 0.35, "Lakeflow (DLT) AUTO CDC output",
            ha="center", family="monospace", fontsize=9, color=SUB_COLOR)
    _save(fig, "labs/07-lakebase-cdf-to-scd1/images/ex7-cdf-scd1.png")


BUILDERS = {"ex1": build_ex1, "ex3": build_ex3, "ex4": build_ex4,
            "ex5": build_ex5, "ex6": build_ex6, "ex7": build_ex7}

if __name__ == "__main__":
    which = [a.lower() for a in sys.argv[1:]] or list(BUILDERS)
    for k in which:
        BUILDERS[k]()
