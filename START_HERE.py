# Databricks notebook source
# MAGIC %md
# MAGIC # The Trading Desk on Lakebase — CIBC Capital Markets Workshop
# MAGIC
# MAGIC Build a CIBC Capital Markets coverage desk on **Lakebase** (managed Postgres on Databricks):
# MAGIC connect to it, generate a desk's worth of data, reach it over **REST and JDBC**, serve live
# MAGIC client-risk features to a **chatbot**, give that chatbot a **memory**, sync data both ways with
# MAGIC the lakehouse, and finish by shipping your own dashboard **app**. One managed Postgres, the whole desk.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **~5h** · nine exercises | **Serverless** · no cluster |
# MAGIC | Hero client · **Air Canada (CL-AC)** | You work on **your own branch** of a shared project |
# MAGIC
# MAGIC **Before you start (once):** open <a href="$./labs/_setup">labs/_setup</a> and set the two
# MAGIC constants near the top — `SHARED_PROJECT_ID` (your facilitator's Lakebase project id, e.g.
# MAGIC `"cibc-cm-workshop"`) and `SHARED_CATALOG` (the workshop's Unity Catalog catalog, e.g. `"main"`).
# MAGIC Set them once, no commit needed, and every exercise inherits them. Enter the base project id even
# MAGIC if the room was split across projects; you're auto-routed to the project that holds your branch.
# MAGIC Your `cm_<user>` schema is created for you.
# MAGIC
# MAGIC **How to use this guide:** ▶ **Run all** — the next cell prints clickable links to every exercise
# MAGIC notebook *in your clone* (resolved at runtime by object id, so they work no matter where you cloned
# MAGIC the repo). Then work the exercises **in order 1 → 9** and **Run all** in each; every exercise folder
# MAGIC has its own `README.md` with the walkthrough and an architecture diagram.

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Where does THIS clone live? This notebook sits at the repo root, so its folder IS the repo root.
REPO_ROOT = os.path.dirname(ctx.notebookPath().get())
try:
    _host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    _host = "https://" + (ctx.browserHostName().get() or "")
try:
    _org = ctx.workspaceId().get()
except Exception:
    _org = ""


def _api_path(p):
    return p if p.startswith("/Workspace") else "/Workspace" + p


def nb_link(rel_path):
    """Clickable workspace URL for a repo-relative object, resolved by its LIVE id in this clone.

    Resolves the object with the Workspace API (so it works regardless of where the repo was cloned)
    and links straight to it by id — more reliable than a $-relative markdown link."""
    try:
        st = w.workspace.get_status(_api_path(f"{REPO_ROOT}/{rel_path}"))
        kind = "notebooks" if str(st.object_type).endswith("NOTEBOOK") else "files"
        return f"{_host}/editor/{kind}/{st.object_id}" + (f"?o={_org}" if _org else "")
    except Exception:
        return None


# (label, repo-relative path). Notebooks have NO extension; plain files (Ex9's README) keep theirs.
EXERCISES = [
    ("1 · Getting to know Lakebase (autoscale/load)", "labs/01-getting-to-know-lakebase/Autoscale_Load"),
    ("2 · Authentication + generate the desk's data", "labs/02-authentication/Connect_And_Generate_Data"),
    ("3 · Data APIs — REST vs JDBC",                  "labs/03-data-api/Data_API"),
    ("4 · Online Feature Store",                      "labs/04-online-feature-store/Feature_Store"),
    ("4 · Coverage Desk Chatbot",                     "labs/04-online-feature-store/Coverage_Desk_Chatbot"),
    ("5 · Agentic Memory",                            "labs/05-agentic-memory/Agent_Memory"),
    ("6 · Delta → Lakebase sync",                     "labs/06-delta-to-lakebase-sync/Delta_To_Lakebase"),
    ("7 · Lakebase → Delta, SCD1 (driver)",           "labs/07-lakebase-cdf-to-scd1/Lakebase_CDF_To_SCD1"),
    ("7 · SCD1 pipeline (DLT source)",                "labs/07-lakebase-cdf-to-scd1/scd1_pipeline"),
    ("8 · Lakebase Search (pgvector + agent)",        "labs/08-lakebase-search/Lakebase_Search"),
    ("9 · Build your own — Dashboard App (README)",   "labs/09-trading-dashboard/README.md"),
]

print(f"Repo detected at: {REPO_ROOT}   (host={_host}, org={_org})")
_items = [(label, nb_link(path)) for label, path in EXERCISES]
for label, u in _items:
    print(f"  {label:52}  {u or '(not found)'}")

_rows = "".join(
    f'<li style="margin:7px 0">{label} — '
    + (f'<a href="{u}" target="_blank" rel="noopener">open →</a>' if u
       else '<span style="color:#C0392B">not found — is START_HERE at the repo root?</span>')
    + "</li>"
    for label, u in _items)
displayHTML(f"""
<div style="font-family:system-ui,-apple-system,sans-serif;max-width:840px">
  <h3 style="margin:0 0 6px">Your exercise notebooks</h3>
  <p style="color:#586673;margin:0 0 10px">Repo detected at <code>{REPO_ROOT}</code> — these links open
     the notebooks in <b>this</b> clone. Work them in order.</p>
  <ol style="line-height:1.6">{_rows}</ol>
</div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **The hero thread:** Air Canada (`CL-AC`) runs through every exercise — its energy book is the OLTP
# MAGIC data (Ex2), the rows read over REST/JDBC (Ex3), the HIGH-risk features the assistant looks up (Ex4)
# MAGIC and remembers (Ex5), the limit raised in the sync (Ex6), the position that changes in SCD1 (Ex7),
# MAGIC and the live numbers on your dashboard (Ex9). One managed Postgres for OLTP data, feature serving,
# MAGIC and agent memory.
# MAGIC
# MAGIC *Visual guide: `docs/attendee.html` · Facilitator setup: `docs/facilitator.html` · Data model:
# MAGIC `DATA_MODEL.md`.*
