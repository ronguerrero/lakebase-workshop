# Exercise 7 — Lakebase → Delta (CDC) with an SCD Type 1 pipeline

**Track:** Lakehouse sync (outbound / round-trip) · **Prerequisite:** your Lakebase branch (Ex1–2) ·
**Notebook-first** (Genie prompt below)

The trading app writes to Lakebase all day — positions get re-marked, limits get re-set. Risk
analytics needs the **current picture in the lakehouse**. This exercise captures changes *out* of the
Lakebase `lb_*` tables into **bronze Delta**, then runs a **Lakeflow (DLT) pipeline that applies SCD
Type 1** to produce current-state Delta tables (`positions_current`, `limits_current`) — one row per
key, latest wins.

This is the **outbound** direction. The Delta→Lakebase *sync* exercise pushes reference data the other
way; together they're the round trip.

## What you'll learn
- How operational changes leave **Lakebase (Postgres)** and land in the **lakehouse as Delta**.
- **Lakeflow / DLT AUTO CDC** (`create_auto_cdc_flow`, formerly `apply_changes`).
- **SCD Type 1** — upsert-latest, no history — and when to use it vs SCD Type 2.

## How changes leave Lakebase (read this — it's the honest part)
Lakebase is managed **Postgres**; it does **not** expose a Delta-style Change Data Feed. Two ways to
get its changes into the lakehouse:
1. **A managed CDC / logical-replication feed** into Delta — this surface is **evolving / preview**;
   confirm what your workspace offers before relying on it.
2. **An incremental watermark extract** — read rows whose `updated_at` advanced and append them to a
   bronze Delta table. This is the **robust, GA-today** pattern, and it's what this lab uses (as a
   snapshot-append for simplicity). The bronze change table is then the source for AUTO CDC.

Swap in a native CDC feed at the bronze step if you have one — the SCD1 pipeline downstream is
unchanged.

## Run it (notebook-first)
1. Open `Lakebase_CDF_To_SCD1.py` (the driver) on serverless and run it top to bottom. It:
   creates/seeds the `lb_*` tables on your branch → extracts them to bronze Delta → **mutates** a
   position and a limit → re-extracts → **creates the Lakeflow pipeline** from `scd1_pipeline.py` and
   triggers one update → verifies the result.
2. `scd1_pipeline.py` is the **pipeline source** — it is deployed as a Lakeflow (DLT) pipeline, not run
   inline. The driver creates the pipeline pointing at it and passes `cm.catalog` / `cm.bronze_schema`
   as configuration; the UC target is the pipeline's `catalog` + `schema` (`cm_scd_<you>`).

### Create the pipeline in the UI instead (optional)
If you'd rather click: **Workflows → Delta Live Tables / Lakeflow → Create pipeline** → Serverless →
add `scd1_pipeline.py` as a notebook library → set the target catalog and schema `cm_scd_<you>` → add
configuration `cm.catalog=<your catalog>` and `cm.bronze_schema=cm_bronze_<you>` → Start.

## The prompt (Genie Code)
Paste into a fresh Genie Code chat on a serverless notebook (the reviewed reference is the two `.py`
files here):

```
Build two Databricks notebooks that move changes from my Lakebase lb_* tables into the lakehouse as
current-state Delta using a Lakeflow DLT pipeline with SCD Type 1.

DRIVER notebook:
- %pip install "psycopg[binary]>=3.1.0", restart, then %run ../_setup.
- Ensure lb_positions(position_id PK, client_id, instrument_id, net_qty, book, updated_at timestamptz)
  and lb_limits(limit_id PK, client_id, limit_type, limit_cad, updated_at) exist on my branch
  (get_connection()); seed a small book if empty.
- Incremental extract: read each lb_* table over psycopg and APPEND it to an append-only bronze Delta
  table main.cm_bronze_<me>.lb_<entity>_changes, tagging _op='UPSERT' and _batch_ts.
- Then mutate Lakebase (UPDATE a position's net_qty and a limit, bump updated_at, INSERT a new
  position) and re-extract, so bronze has two versions of the changed keys.
- Create a serverless Lakeflow (DLT) pipeline from scd1_pipeline.py with UC target catalog + schema
  cm_scd_<me> and configuration cm.catalog / cm.bronze_schema; start one update and wait.
- Verify positions_current has one row per position_id and position 1 shows the updated net_qty.

PIPELINE notebook (scd1_pipeline.py): use dlt. Read each bronze change table as a streaming table,
create_streaming_table the target, and dlt.create_auto_cdc_flow (fallback dlt.apply_changes) with
keys=[the PK], sequence_by=updated_at, apply_as_deletes on _op='DELETE', except_column_list _op/_batch_ts,
stored_as_scd_type=1, for positions (position_id) and limits (limit_id).
```

## ✓ Validation
- Bronze `lb_positions_changes` holds **multiple versions** of position 1 (250 → 900).
- `positions_current` has **one row per `position_id`** (rows == distinct keys) — SCD Type 1.
- Position 1 reads **net_qty = 900** (latest wins), and there's no second row for it.

## SCD Type 1 vs Type 2
- **SCD1 (here):** overwrite to current value per key — best for *current-state* risk analytics
  ("what's the book right now?"). No history, smaller, simpler.
- **SCD2:** version every change with validity windows — for point-in-time/audit ("the book at 3pm").
  Same AUTO CDC call with `stored_as_scd_type=2`.

## Docs
- Lakeflow AUTO CDC / `apply_changes` (SCD 1 & 2): https://docs.databricks.com/aws/en/dlt/cdc
- Lakeflow Declarative Pipelines (DLT): https://docs.databricks.com/aws/en/dlt/
