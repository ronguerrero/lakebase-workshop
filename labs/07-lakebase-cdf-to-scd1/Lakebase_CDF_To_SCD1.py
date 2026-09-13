# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase → Delta (CDC) with an SCD Type 1 pipeline
# MAGIC
# MAGIC **Path:** Lakehouse sync (outbound) &nbsp;|&nbsp; **Prerequisite:** your Lakebase branch (Ex1–2)
# MAGIC
# MAGIC The trading app mutates `lb_*` tables in Lakebase all day (positions get re-marked, limits get
# MAGIC re-set). Risk analytics needs the **current picture in the lakehouse**. This exercise captures
# MAGIC changes out of Lakebase into **bronze Delta**, then a **Lakeflow (DLT) pipeline applies SCD
# MAGIC Type 1** — one row per key, latest wins — into current-state Delta tables analysts can query.
# MAGIC
# MAGIC ```
# MAGIC  Lakebase lb_positions / lb_limits  ──(watermark extract)──▶  bronze Delta (append-only changes)
# MAGIC                                                                      │  Lakeflow AUTO CDC, SCD1
# MAGIC                                                                      ▼
# MAGIC                                              cm_scd_<you>.positions_current / limits_current
# MAGIC ```
# MAGIC
# MAGIC > **How changes leave Lakebase — read this.** Lakebase is managed **Postgres**; it does *not*
# MAGIC > expose a Delta-style Change Data Feed. To get operational changes into the lakehouse you
# MAGIC > either (a) use a managed CDC / logical-replication feed (evolving / preview — verify what's
# MAGIC > available in your workspace), or (b) do an **incremental watermark extract** — read rows whose
# MAGIC > `updated_at` advanced, append them to a bronze Delta table. This lab uses **(b)**, the robust,
# MAGIC > GA-today pattern, and treats each extract as the change source for AUTO CDC. Swap in a native
# MAGIC > CDC feed here if your workspace has one.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

import os
from pyspark.sql import functions as F

dbutils.widgets.text("uc_catalog", "main", "Unity Catalog catalog")
UC_CATALOG = dbutils.widgets.get("uc_catalog") or "main"
u = _sanitize(user_email).replace("-", "_")
BRONZE_SCHEMA = f"cm_bronze_{u}"      # append-only change extracts land here
SCD_SCHEMA = f"cm_scd_{u}"            # SCD1 current-state targets (pipeline writes here)

# Where scd1_pipeline.py lives in the workspace (sibling of this notebook). Override the widget if
# your import path differs.
try:
    _here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _default_pipe = f"/Workspace{os.path.dirname(_here)}/scd1_pipeline"
except Exception:
    _default_pipe = ""
dbutils.widgets.text("pipeline_notebook_path", _default_pipe, "scd1_pipeline notebook path")
PIPELINE_PATH = dbutils.widgets.get("pipeline_notebook_path")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{SCD_SCHEMA}")
print(f"Catalog:        {UC_CATALOG}")
print(f"Bronze schema:  {BRONZE_SCHEMA}   (change extracts)")
print(f"SCD1 schema:    {SCD_SCHEMA}       (pipeline targets)")
print(f"Pipeline src:   {PIPELINE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The operational `lb_*` tables in Lakebase
# MAGIC
# MAGIC These are what the trading app writes to. (If you ran the Delta→Lakebase sync exercise they
# MAGIC already exist; we create them idempotently so this lab is self-contained.) Both carry an
# MAGIC `updated_at` column — that's our change marker.

# COMMAND ----------

conn = get_connection()   # your branch
with conn.cursor() as cur:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.lb_positions (
            position_id   INTEGER PRIMARY KEY,
            client_id     TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
            net_qty       NUMERIC NOT NULL,
            book          TEXT,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.lb_limits (
            limit_id      INTEGER PRIMARY KEY,
            client_id     TEXT NOT NULL,
            limit_type    TEXT NOT NULL,
            limit_cad     NUMERIC NOT NULL,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    # seed an initial book (idempotent: only if empty)
    cur.execute(f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.lb_positions")
    if cur.fetchone()["n"] == 0:
        cur.execute(f"""
            INSERT INTO {PG_SCHEMA}.lb_positions (position_id, client_id, instrument_id, net_qty, book) VALUES
              (1,'CL-AC','WTI',250,'ENERGY'),
              (2,'CL-AC','HO',1500,'ENERGY'),
              (3,'CL-SU','WTI',400,'ENERGY'),
              (4,'CL-BCE','BCE.TO',30000,'EQUITY');
            INSERT INTO {PG_SCHEMA}.lb_limits (limit_id, client_id, limit_type, limit_cad) VALUES
              (1,'CL-AC','GROSS_NOTIONAL',50000000),
              (2,'CL-SU','GROSS_NOTIONAL',30000000);
        """)
conn.commit()
with conn.cursor() as cur:
    cur.execute(f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.lb_positions"); npos = cur.fetchone()["n"]
    cur.execute(f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.lb_limits"); nlim = cur.fetchone()["n"]
print(f"✓ lb_positions rows: {npos}  |  lb_limits rows: {nlim}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Incremental extract → bronze Delta (the change source)
# MAGIC
# MAGIC `extract_changes` reads the current `lb_*` snapshot from Lakebase over `psycopg` and **appends**
# MAGIC it to an append-only bronze Delta table, tagging each row with `_op` and a `_batch_ts`. Because
# MAGIC AUTO CDC de-duplicates by key + `updated_at`, re-appending snapshots is safe — a re-marked
# MAGIC position simply arrives with a newer `updated_at` and wins.
# MAGIC
# MAGIC > A production feed would ship only the *changed* rows (watermark on `updated_at`, or a real CDC
# MAGIC > stream). Snapshot-append keeps the lab simple while still exercising SCD1 correctly. Deletes
# MAGIC > need an explicit `_op='DELETE'` row (shown in the optional cell below).

# COMMAND ----------

def extract_changes(entity: str):
    """Read lb_<entity> from Lakebase and append it (as UPSERTs) to bronze Delta."""
    c = get_connection()
    try:
        with c.cursor() as cur:
            cur.execute(f"SELECT * FROM {PG_SCHEMA}.lb_{entity}")
            rows = cur.fetchall()          # list[dict] (dict_row)
    finally:
        c.close()
    if not rows:
        print(f"  (no rows in lb_{entity})"); return 0
    df = (spark.createDataFrame(rows)
          .withColumn("_op", F.lit("UPSERT"))
          .withColumn("_batch_ts", F.current_timestamp()))
    (df.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{UC_CATALOG}.{BRONZE_SCHEMA}.lb_{entity}_changes"))
    return df.count()

for e in ("positions", "limits"):
    n = extract_changes(e)
    print(f"✓ appended {n} {e} change rows to bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mutate Lakebase, then re-extract — this is the change we'll watch collapse
# MAGIC
# MAGIC Re-mark Air Canada's WTI position and tighten a limit (both bump `updated_at`), add a new
# MAGIC position, then extract again. Bronze now holds *two* versions of the changed keys.

# COMMAND ----------

conn = get_connection()
with conn.cursor() as cur:
    cur.execute(f"UPDATE {PG_SCHEMA}.lb_positions SET net_qty = 900, updated_at = now() WHERE position_id = 1")
    cur.execute(f"UPDATE {PG_SCHEMA}.lb_limits    SET limit_cad = 40000000, updated_at = now() WHERE limit_id = 1")
    cur.execute(f"""INSERT INTO {PG_SCHEMA}.lb_positions (position_id, client_id, instrument_id, net_qty, book)
                    VALUES (5,'CL-CVE','NG',800,'ENERGY')
                    ON CONFLICT (position_id) DO UPDATE SET net_qty = EXCLUDED.net_qty, updated_at = now()""")
conn.commit()
conn.close()
print("✓ mutated lb_positions (pos 1 → 900, added pos 5) and lb_limits (limit 1 → 40M)")

for e in ("positions", "limits"):
    extract_changes(e)
print("✓ re-extracted — bronze now has multiple versions of the changed keys")

display(spark.sql(f"""
  SELECT position_id, net_qty, updated_at, _batch_ts
  FROM {UC_CATALOG}.{BRONZE_SCHEMA}.lb_positions_changes
  WHERE position_id = 1 ORDER BY updated_at
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create & run the Lakeflow (DLT) SCD1 pipeline
# MAGIC
# MAGIC The pipeline source is `scd1_pipeline.py`. It reads the bronze change tables and applies **AUTO
# MAGIC CDC with `stored_as_scd_type=1`** into `positions_current` / `limits_current`. We create it via
# MAGIC the SDK and trigger one update. (UI steps are in the README if you'd rather click.)

# COMMAND ----------

# ⚠ VALIDATE the pipeline create shape for your SDK version. Field names that vary:
#   • UC target: newer SDKs take `catalog=` + `schema=`; older ones take `target=<schema>` (+ `catalog`).
#   • `serverless=True` for serverless DLT; drop it / supply `clusters=` if serverless isn't enabled.
#   • library: PipelineLibrary(notebook=NotebookLibrary(path=...)).
# If create() rejects an arg, read the error and adjust — or build the pipeline in the Lakeflow UI
# (README §UI) pointing at scd1_pipeline.py with the same two configuration keys.
from databricks.sdk.service.pipelines import (
    PipelineLibrary, NotebookLibrary,
)

PIPELINE_NAME = f"cm-scd1-{_sanitize(user_email)}"

existing = next((p for p in w.pipelines.list_pipelines() if p.name == PIPELINE_NAME), None)
if existing:
    pipeline_id = existing.pipeline_id
    print(f"✓ pipeline exists: {PIPELINE_NAME} ({pipeline_id})")
else:
    created = w.pipelines.create(
        name=PIPELINE_NAME,
        serverless=True,
        catalog=UC_CATALOG,          # UC target catalog
        schema=SCD_SCHEMA,           # UC target schema (positions_current / limits_current land here)
        photon=True,
        continuous=False,
        development=True,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=PIPELINE_PATH))],
        configuration={"cm.catalog": UC_CATALOG, "cm.bronze_schema": BRONZE_SCHEMA},
    )
    pipeline_id = created.pipeline_id
    print(f"✓ pipeline created: {PIPELINE_NAME} ({pipeline_id})")

# Trigger one update and wait for it to finish.
upd = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
print(f"Started update {getattr(upd, 'update_id', '')} — waiting ...")
# ⚠ VALIDATE: helper name for waiting varies; poll get_update state if wait_get_pipeline_* differs.
import time
for _ in range(60):
    ev = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=upd.update_id)
    state = str(getattr(getattr(ev, "update", None), "state", "") or getattr(ev, "state", "")).upper()
    print(f"  update state: {state}")
    if any(s in state for s in ("COMPLETED", "FAILED", "CANCELED")):
        break
    time.sleep(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify SCD Type 1 — latest only, no history
# MAGIC
# MAGIC After the pipeline runs, `positions_current` holds **one row per `position_id`**, reflecting the
# MAGIC latest `updated_at`. Position 1 should show `net_qty = 900` (the update), not 250, and there is
# MAGIC no second row for it.

# COMMAND ----------

cur_tbl = f"{UC_CATALOG}.{SCD_SCHEMA}.positions_current"
display(spark.sql(f"SELECT position_id, client_id, instrument_id, net_qty, updated_at FROM {cur_tbl} ORDER BY position_id"))

total = spark.sql(f"SELECT COUNT(*) n FROM {cur_tbl}").first()["n"]
distinct = spark.sql(f"SELECT COUNT(DISTINCT position_id) n FROM {cur_tbl}").first()["n"]
pos1 = spark.sql(f"SELECT net_qty FROM {cur_tbl} WHERE position_id = 1").collect()
print(f"rows={total}  distinct keys={distinct}  → {'✓ SCD1 (one row per key)' if total==distinct else '✗ duplicates!'}")
if pos1:
    print(f"position 1 net_qty = {pos1[0]['net_qty']}  → {'✓ latest value (900)' if float(pos1[0]['net_qty'])==900 else '✗ stale'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✓ Checks
# MAGIC - Bronze `lb_positions_changes` holds **multiple versions** of position 1 (250 then 900).
# MAGIC - `positions_current` holds **one row per `position_id`** (rows == distinct keys).
# MAGIC - Position 1 shows **net_qty = 900** — the latest value won (SCD Type 1).
# MAGIC
# MAGIC ## SCD Type 1 vs Type 2
# MAGIC - **SCD1 (this lab):** overwrite — keep only the current value per key. Perfect for *current-state*
# MAGIC   risk analytics ("what is the book right now?"). Smaller, simpler, no history.
# MAGIC - **SCD2:** keep history — each change is a new versioned row with validity windows. Use it for
# MAGIC   "what did the book look like at 3pm?" / audit. AUTO CDC does SCD2 with `stored_as_scd_type=2`.
# MAGIC
# MAGIC ## Optional — capturing deletes
# MAGIC A snapshot extract can't see a deleted row. To propagate deletes, emit a change row with
# MAGIC `_op = 'DELETE'` for the removed key (the pipeline's `apply_as_deletes=F.expr("_op='DELETE'")`
# MAGIC removes it from the current table). A watermark/real-CDC feed carries deletes natively.
