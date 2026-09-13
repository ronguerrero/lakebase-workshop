# Exercise 7 — Lakebase → Delta with Change Data Feed (CDF) + an SCD Type 1 pipeline

**Track:** Lakehouse sync (outbound / round-trip) · **Prerequisite:** your Lakebase branch (Ex1–2) ·
**Notebook-first** (Genie prompt below)

The trading app writes to Lakebase all day — positions get re-marked, limits get re-set. Risk
analytics needs the **current picture in the lakehouse**. This exercise turns on **Lakebase Change
Data Feed (CDF)**, which automatically materializes every Postgres insert/update/delete into Unity
Catalog Delta tables (`lb_<table>_history`), then runs a **Lakeflow (DLT) pipeline that applies SCD
Type 1** to produce current-state tables (`positions_current`, `limits_current`) — one row per key,
latest wins.

This is the **outbound** direction. The Delta→Lakebase *sync* exercise (Ex6) pushes reference data the
other way; together they're the round trip.

## What you'll learn
- **Lakebase Change Data Feed** — how operational Postgres changes are captured, with no extract code,
  into `lb_<table>_history` Delta tables in Unity Catalog.
- **Lakeflow / DLT AUTO CDC** (`create_auto_cdc_flow`, formerly `apply_changes`).
- **SCD Type 1** — upsert-latest, no history — and when to use it vs SCD Type 2.

## How CDF works here (read this)
Lakebase **Change Data Feed** (Public Preview) tails the Postgres WAL and continuously writes each
change into a Unity Catalog–managed Delta table named **`lb_<table>_history`** in a schema you choose.
Every change row carries:

| Column | Meaning |
|---|---|
| `_pg_change_type` | `insert` · `update_preimage` · `update_postimage` · `delete` |
| `_pg_lsn` | Postgres log sequence number |
| `_pg_xid` | Postgres transaction id |
| `_timestamp` | when the change was processed |
| `_sort_by` | monotonic key used to order all changes (we `sequence_by` this) |

CDF is **schema-scoped**, so the lab puts the operational tables in a small dedicated Postgres schema
`cm_<you>_ops` (just `positions` + `limits`) and points the feed there — its history tables land in
your Unity Catalog schema `cm_<you>`, next to the SCD1 outputs.

### Requirements & gotchas (Public Preview)
- **`REPLICA IDENTITY FULL`** on each source table (the lab sets it) so update/delete WAL records carry
  the full old row; each table also needs a primary key and **at least one row**.
- The **destination catalog must not use default storage** — if the history tables never appear, this
  is the most likely cause (see your facilitator / `FACILITATOR.md`).
- Creating a feed needs **CAN MANAGE on the Lakebase project**. Attendees get `CAN USE` by default, so
  the notebook fails with a clear message if it can't create the feed — your facilitator either grants
  CAN MANAGE or creates the feed for your `cm_<you>_ops` schema.

## Run it (notebook-first)
1. Open `Lakebase_CDF_To_SCD1.py` (the driver) on serverless and run it top to bottom. It:
   creates/seeds `positions` + `limits` on your branch (with `REPLICA IDENTITY FULL`) → **enables CDF**
   (`w.postgres.create_cdf_config`) → waits for the `lb_*_history` tables to materialize → **mutates** a
   position and a limit → shows CDF capturing them → **creates the Lakeflow pipeline** from
   `scd1_pipeline.py` and triggers one update → verifies the result.
2. `scd1_pipeline.py` is the **pipeline source** — deployed as a Lakeflow (DLT) pipeline, not run
   inline. The driver creates the pipeline pointing at it and passes `cm.catalog` / `cm.history_schema`
   as configuration; the UC target is the pipeline's `catalog` + `schema` (`cm_<you>`).

### Create the pipeline in the UI instead (optional)
**Workflows → Delta Live Tables / Lakeflow → Create pipeline** → Serverless → add `scd1_pipeline.py` as
a notebook library → set target catalog + schema `cm_<you>` → add configuration
`cm.catalog=<your catalog>` and `cm.history_schema=cm_<you>` → Start.

## The prompt (Genie Code)
Paste into a fresh Genie Code chat on a serverless notebook (the reviewed reference is the two `.py`
files here):

```
Build two Databricks notebooks that move changes from Lakebase into the lakehouse as current-state
Delta using Lakebase Change Data Feed (CDF) + a Lakeflow DLT pipeline with SCD Type 1.

DRIVER notebook:
- %pip install "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0", restart, then %run ../_setup.
- In a dedicated Postgres schema cm_<me>_ops on my branch (get_connection()), create
  positions(position_id PK, client_id, instrument_id, net_qty, book, updated_at timestamptz) and
  limits(limit_id PK, client_id, limit_type, limit_cad, updated_at); set REPLICA IDENTITY FULL on each;
  seed a small book if empty.
- Enable Lakebase CDF with w.postgres.create_cdf_config(parent="projects/<project>/branches/<branch>/
  databases/databricks_postgres", cdf_config=CdfConfig(catalog=<my UC catalog>, schema=cm_<me>,
  postgres_schema=cm_<me>_ops)). Poll until the Delta tables cm_<me>.lb_positions_history and
  lb_limits_history exist and have rows.
- Mutate Lakebase (UPDATE a position's net_qty and a limit, bump updated_at, INSERT a new position),
  wait for CDF to capture it, and show the history rows for position 1.
- Create a serverless Lakeflow (DLT) pipeline from scd1_pipeline.py with UC target catalog + schema
  cm_<me> and configuration cm.catalog / cm.history_schema; start one update and wait.
- Verify positions_current has one row per position_id and position 1 shows the updated net_qty.

PIPELINE notebook (scd1_pipeline.py): use dlt. For positions (key position_id) and limits (key
limit_id): read cm_<me>.lb_<entity>_history as a streaming table, filtering out
_pg_change_type='update_preimage'; create_streaming_table the target <entity>_current; and
dlt.create_auto_cdc_flow (fallback dlt.apply_changes) with keys=[the PK], sequence_by=_sort_by,
apply_as_deletes on _pg_change_type='delete', except_column_list of the _pg_* / _timestamp / _sort_by
columns, stored_as_scd_type=1.
```

## ✓ Validation
- `lb_positions_history` (created by **CDF**) holds **multiple change rows** for position 1 — an
  `insert` (250) and an `update_postimage` (900).
- `positions_current` has **one row per `position_id`** (rows == distinct keys) — SCD Type 1.
- Position 1 reads **net_qty = 900** (latest wins), and there's no second row for it.

## SCD Type 1 vs Type 2
- **SCD1 (here):** overwrite to current value per key — best for *current-state* risk analytics
  ("what's the book right now?"). No history, smaller, simpler.
- **SCD2:** version every change with validity windows — for point-in-time/audit ("the book at 3pm").
  Same AUTO CDC call with `stored_as_scd_type=2` over the same CDF feed.

## Deletes come for free
Unlike a snapshot extract, CDF captures **deletes** natively — a `DELETE` arrives as a
`_pg_change_type='delete'` row (carrying the key via `REPLICA IDENTITY FULL`), and the pipeline's
`apply_as_deletes` removes it from the current table.

## Docs
- Lakebase Change Data Feed: https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
- Lakeflow AUTO CDC / `apply_changes` (SCD 1 & 2): https://docs.databricks.com/aws/en/dlt/cdc
- Lakeflow Declarative Pipelines (DLT): https://docs.databricks.com/aws/en/dlt/
