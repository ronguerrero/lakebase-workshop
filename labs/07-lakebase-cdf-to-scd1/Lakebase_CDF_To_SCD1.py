# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase → Delta with Change Data Feed (CDF) + an SCD Type 1 pipeline
# MAGIC
# MAGIC **Path:** Lakehouse sync (outbound) &nbsp;|&nbsp; **Prerequisite:** your Lakebase branch (Ex1–2)
# MAGIC
# MAGIC The trading app mutates operational tables in Lakebase all day (positions get re-marked, limits
# MAGIC get re-set). Risk analytics needs the **current picture in the lakehouse**. Rather than hand-roll
# MAGIC an extract, we turn on **Lakebase Change Data Feed (CDF)**: Databricks tails the Postgres WAL and
# MAGIC **automatically materializes every insert / update / delete into Unity Catalog Delta tables named
# MAGIC `lb_<table>_history`** in your schema. A **Lakeflow (DLT) pipeline** then applies **AUTO CDC, SCD
# MAGIC Type 1** — one row per key, latest wins — into current-state tables analysts query.
# MAGIC
# MAGIC ```
# MAGIC  Lakebase positions / limits ──CDF (auto, WAL)──▶ cm_<you>.lb_positions_history / lb_limits_history
# MAGIC                                                            │  Lakeflow AUTO CDC, SCD1
# MAGIC                                                            ▼
# MAGIC                                     cm_<you>.positions_current / limits_current
# MAGIC ```
# MAGIC
# MAGIC > **Lakebase CDF is in Public Preview.** Requirements this lab depends on: `REPLICA IDENTITY FULL`
# MAGIC > on each source table (we set it), a destination **catalog that does NOT use default storage**,
# MAGIC > and **CAN MANAGE on the Lakebase project** to create the feed. If the create step is denied or
# MAGIC > the history tables never appear, see the troubleshooting note in `README.md` / `FACILITATOR.md`.
# MAGIC >
# MAGIC > This is the **reference solution** — what the Genie Code prompt in `README.md` should produce.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

import os
import time

from pyspark.sql import functions as F

# CDF is **schema-scoped** in Postgres — it materializes EVERY table in one Postgres schema. So we put
# the operational trading tables in a small dedicated schema on your branch (`cm_<user>_ops`) to keep the
# feed to just these two tables (instead of everything in cm_<user>). The Delta history tables CDF
# creates, and the SCD1 targets, land in your Unity Catalog schema (UC_CATALOG.UC_SCHEMA from _setup).
PG_OPS_SCHEMA = f"{PG_SCHEMA}_ops"           # dedicated Postgres source schema (this branch)
HISTORY_SCHEMA = UC_SCHEMA                   # where CDF writes lb_*_history (your UC schema)
SCD_SCHEMA = UC_SCHEMA                        # SCD1 current-state targets (your UC schema)
CDF_CONFIG_ID = f"cdf_{_sanitize(user_email).replace('-', '_')}_ops"[:63]  # [a-z][a-z0-9_]*

# scd1_pipeline.py lives next to this notebook. Override the widget if your import path differs.
try:
    _here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _default_pipe = f"/Workspace{os.path.dirname(_here)}/scd1_pipeline"
except Exception:
    _default_pipe = ""
dbutils.widgets.text("pipeline_notebook_path", _default_pipe, "scd1_pipeline notebook path")
PIPELINE_PATH = dbutils.widgets.get("pipeline_notebook_path")

# Your facilitator pre-creates cm_<user> and makes you its owner (harmless no-op if it exists).
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
except Exception as _e:
    print(f"(using pre-provisioned schema {UC_CATALOG}.{UC_SCHEMA}: {str(_e)[:80]})")

POS_HIST = f"{UC_CATALOG}.{HISTORY_SCHEMA}.lb_positions_history"
LIM_HIST = f"{UC_CATALOG}.{HISTORY_SCHEMA}.lb_limits_history"

print(f"Postgres source:   {PG_OPS_SCHEMA}   (positions, limits)")
print(f"UC catalog/schema: {UC_CATALOG}.{UC_SCHEMA}   (lb_*_history + *_current land here)")
print(f"CDF config id:     {CDF_CONFIG_ID}")
print(f"Pipeline src:      {PIPELINE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The operational tables in Lakebase (what the trading app writes)
# MAGIC
# MAGIC `positions` and `limits` are native Postgres tables the app mutates. Two CDF prerequisites:
# MAGIC each needs a **primary key** and **`REPLICA IDENTITY FULL`** (so update/delete WAL records carry
# MAGIC the full old row), and each must have **at least one row** to be picked up.

# COMMAND ----------

conn = get_connection()   # your branch
with conn.cursor() as cur:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_OPS_SCHEMA}"')
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PG_OPS_SCHEMA}.positions (
            position_id   INTEGER PRIMARY KEY,
            client_id     TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
            net_qty       NUMERIC NOT NULL,
            book          TEXT,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS {PG_OPS_SCHEMA}.limits (
            limit_id      INTEGER PRIMARY KEY,
            client_id     TEXT NOT NULL,
            limit_type    TEXT NOT NULL,
            limit_cad     NUMERIC NOT NULL,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    # CDF needs the full old row in the WAL for updates/deletes:
    cur.execute(f"ALTER TABLE {PG_OPS_SCHEMA}.positions REPLICA IDENTITY FULL")
    cur.execute(f"ALTER TABLE {PG_OPS_SCHEMA}.limits    REPLICA IDENTITY FULL")
    # seed an initial book (idempotent: only if empty)
    cur.execute(f"SELECT COUNT(*) AS n FROM {PG_OPS_SCHEMA}.positions")
    if cur.fetchone()["n"] == 0:
        cur.execute(f"""
            INSERT INTO {PG_OPS_SCHEMA}.positions (position_id, client_id, instrument_id, net_qty, book) VALUES
              (1,'CL-AC','WTI',250,'ENERGY'),
              (2,'CL-AC','HO',1500,'ENERGY'),
              (3,'CL-SU','WTI',400,'ENERGY'),
              (4,'CL-BCE','BCE.TO',30000,'EQUITY');
            INSERT INTO {PG_OPS_SCHEMA}.limits (limit_id, client_id, limit_type, limit_cad) VALUES
              (1,'CL-AC','GROSS_NOTIONAL',50000000),
              (2,'CL-SU','GROSS_NOTIONAL',30000000);
        """)
conn.commit()
conn.close()
print(f"✓ {PG_OPS_SCHEMA}.positions + .limits ready (PK + REPLICA IDENTITY FULL, seeded)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Turn on Lakebase CDF → Delta history tables appear automatically
# MAGIC
# MAGIC One SDK call maps the `cm_<user>_ops` Postgres schema to your Unity Catalog schema. Databricks
# MAGIC then tails the WAL and creates + continuously appends `lb_positions_history` / `lb_limits_history`
# MAGIC Delta tables (no extract code of ours). Each change row carries CDF bookkeeping columns —
# MAGIC `_pg_change_type` (`insert` / `update_preimage` / `update_postimage` / `delete`), `_pg_lsn`,
# MAGIC `_pg_xid`, `_timestamp`, and `_sort_by` (a monotonic ordering key we sequence SCD1 by).

# COMMAND ----------

from databricks.sdk.service.postgres import CdfConfig

parent = f"projects/{PROJECT_ID}/branches/{USER_BRANCH}/databases/{PG_DATABASE}"
cfg = CdfConfig(catalog=UC_CATALOG, schema=HISTORY_SCHEMA, postgres_schema=PG_OPS_SCHEMA)
try:
    op = w.postgres.create_cdf_config(parent=parent, cdf_config=cfg, cdf_config_id=CDF_CONFIG_ID)
    try:
        op.result()          # wait if this returns a long-running-op waiter; harmless otherwise
    except Exception:
        pass
    print(f"✓ CDF feed requested: {PG_OPS_SCHEMA} → {UC_CATALOG}.{HISTORY_SCHEMA}  (id={CDF_CONFIG_ID})")
except Exception as e:
    msg = str(e)
    if "already exists" in msg.lower() or "ALREADY_EXISTS" in msg:
        print(f"✓ CDF feed already exists (id={CDF_CONFIG_ID})")
    elif "permission" in msg.lower() or "PERMISSION_DENIED" in msg:
        raise RuntimeError(
            "Creating a Lakebase CDF feed needs CAN MANAGE on the Lakebase project (you have CAN USE). "
            "Ask your facilitator to grant CAN MANAGE on the project, or to create the CDF feed for your "
            f"'{PG_OPS_SCHEMA}' schema → {UC_CATALOG}.{HISTORY_SCHEMA}. See README / FACILITATOR.md."
        ) from e
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the initial snapshot, then look at the change feed
# MAGIC CDF creates the tables and back-fills the current rows (an initial snapshot), then keeps appending.
# MAGIC First materialization can take a couple of minutes.

# COMMAND ----------

def _rows(fqn):
    try:
        return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").first()["n"]
    except Exception:
        return None   # table not created yet

print("Waiting for CDF to materialize the history tables (initial snapshot; ~1-3 min) ...")
for _ in range(40):
    p, l = _rows(POS_HIST), _rows(LIM_HIST)
    print(f"  lb_positions_history={p}  lb_limits_history={l}")
    if p and l:
        break
    time.sleep(15)
else:
    raise TimeoutError(
        "CDF history tables were not populated. Check the branch's Lakebase CDF tab, and that "
        f"'{UC_CATALOG}' does NOT use default storage (a CDF requirement). See README troubleshooting.")

print("✓ CDF is materializing changes into Delta")
display(spark.sql(f"SELECT position_id, net_qty, _pg_change_type, _sort_by, _timestamp "
                  f"FROM {POS_HIST} ORDER BY _sort_by"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mutate Lakebase — watch CDF capture it
# MAGIC
# MAGIC Re-mark Air Canada's WTI position, tighten a limit (both bump `updated_at`), and add a new
# MAGIC position. CDF captures each as `update_*` / `insert` rows in the history tables within ~15s — no
# MAGIC action from us. (A delete would arrive as a `delete` row, which the SCD1 pipeline applies too.)

# COMMAND ----------

conn = get_connection()
with conn.cursor() as cur:
    cur.execute(f"UPDATE {PG_OPS_SCHEMA}.positions SET net_qty = 900, updated_at = now() WHERE position_id = 1")
    cur.execute(f"UPDATE {PG_OPS_SCHEMA}.limits    SET limit_cad = 40000000, updated_at = now() WHERE limit_id = 1")
    cur.execute(f"""INSERT INTO {PG_OPS_SCHEMA}.positions (position_id, client_id, instrument_id, net_qty, book)
                    VALUES (5,'CL-CVE','NG',800,'ENERGY')
                    ON CONFLICT (position_id) DO UPDATE SET net_qty = EXCLUDED.net_qty, updated_at = now()""")
conn.commit()
conn.close()
print("✓ mutated positions (pos 1 → 900, added pos 5) and limits (limit 1 → 40M)")

print("Waiting for CDF to capture the update to position 1 (~15s flush) ...")
for _ in range(20):
    n = spark.sql(f"SELECT COUNT(*) n FROM {POS_HIST} "
                  f"WHERE position_id = 1 AND _pg_change_type LIKE 'update%'").first()["n"]
    if n:
        break
    time.sleep(10)
display(spark.sql(f"""
  SELECT position_id, net_qty, _pg_change_type, _sort_by, _timestamp
  FROM {POS_HIST} WHERE position_id = 1 ORDER BY _sort_by
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create & run the Lakeflow (DLT) SCD1 pipeline
# MAGIC
# MAGIC The pipeline source is `scd1_pipeline.py`. It streams the `lb_*_history` change tables and applies
# MAGIC **AUTO CDC with `stored_as_scd_type=1`** into `positions_current` / `limits_current`, sequencing by
# MAGIC `_sort_by` and treating `_pg_change_type = 'delete'` as a delete. We create it via the SDK and
# MAGIC trigger one update. (UI steps are in the README if you'd rather click.)

# COMMAND ----------

# ⚠ VALIDATE the pipeline create shape for your SDK version. Field names that vary:
#   • UC target: newer SDKs take `catalog=` + `schema=`; older ones take `target=<schema>` (+ `catalog`).
#   • `serverless=True` for serverless DLT; drop it / supply `clusters=` if serverless isn't enabled.
#   • library: PipelineLibrary(notebook=NotebookLibrary(path=...)).
# If create() rejects an arg, read the error and adjust — or build the pipeline in the Lakeflow UI
# (README §UI) pointing at scd1_pipeline.py with the same two configuration keys.
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

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
        # tell the pipeline where the CDF history tables live
        configuration={"cm.catalog": UC_CATALOG, "cm.history_schema": HISTORY_SCHEMA},
    )
    pipeline_id = created.pipeline_id
    print(f"✓ pipeline created: {PIPELINE_NAME} ({pipeline_id})")

# Trigger one update and wait for it to finish.
upd = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
print(f"Started update {getattr(upd, 'update_id', '')} — waiting ...")
# ⚠ VALIDATE: helper name for waiting varies; poll get_update state if wait_get_pipeline_* differs.
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
# MAGIC latest change. Position 1 should show `net_qty = 900` (the update), not 250, and there's no second
# MAGIC row for it.

# COMMAND ----------

cur_tbl = f"{UC_CATALOG}.{SCD_SCHEMA}.positions_current"
display(spark.sql(f"SELECT position_id, client_id, instrument_id, net_qty, updated_at "
                  f"FROM {cur_tbl} ORDER BY position_id"))

total = spark.sql(f"SELECT COUNT(*) n FROM {cur_tbl}").first()["n"]
distinct = spark.sql(f"SELECT COUNT(DISTINCT position_id) n FROM {cur_tbl}").first()["n"]
pos1 = spark.sql(f"SELECT net_qty FROM {cur_tbl} WHERE position_id = 1").collect()
print(f"rows={total}  distinct keys={distinct}  → {'✓ SCD1 (one row per key)' if total==distinct else '✗ duplicates!'}")
if pos1:
    print(f"position 1 net_qty = {pos1[0]['net_qty']}  → {'✓ latest value (900)' if float(pos1[0]['net_qty'])==900 else '✗ stale'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✓ Checks
# MAGIC - `lb_positions_history` holds **multiple change rows** for position 1 (insert 250, then an
# MAGIC   `update_postimage` 900) — created by **CDF**, not by us.
# MAGIC - `positions_current` holds **one row per `position_id`** (rows == distinct keys).
# MAGIC - Position 1 shows **net_qty = 900** — the latest value won (SCD Type 1).
# MAGIC
# MAGIC ## SCD Type 1 vs Type 2
# MAGIC - **SCD1 (this lab):** overwrite — keep only the current value per key. Perfect for *current-state*
# MAGIC   risk analytics ("what is the book right now?"). Smaller, simpler, no history.
# MAGIC - **SCD2:** keep history — each change is a new versioned row with validity windows. Use it for
# MAGIC   "what did the book look like at 3pm?" / audit. AUTO CDC does SCD2 with `stored_as_scd_type=2`
# MAGIC   over the same CDF feed.
# MAGIC
# MAGIC ## Deletes come for free
# MAGIC Unlike a snapshot extract, CDF captures **deletes** natively: a `DELETE` in Postgres arrives as a
# MAGIC `_pg_change_type = 'delete'` row (carrying the key, thanks to `REPLICA IDENTITY FULL`), and the
# MAGIC pipeline's `apply_as_deletes=F.expr("_pg_change_type = 'delete'")` removes it from the current table.
# MAGIC
# MAGIC ## Cleanup
# MAGIC After the workshop: delete the DLT pipeline; delete the CDF feed
# MAGIC (`w.postgres.delete_cdf_config`, which can drop or preserve the `lb_*_history` tables); the
# MAGIC `*_current` tables and history tables live in your `cm_<user>` schema (dropping the branch clears
# MAGIC the Postgres side).
