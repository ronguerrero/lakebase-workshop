# Exercise 6 — Delta → Lakebase Sync (reverse ETL)

**Build surface:** notebook-first (Genie Code optional) · **Prerequisite:** your Lakebase branch (Ex1–2)

Your lakehouse curates a **client reference / risk-limits** table every night in Delta. The trading
app, though, needs it **live over Postgres**. A Lakebase **synced table** bridges the two: it
continuously replicates a Unity Catalog Delta table into your Lakebase branch as an operational
`lb_*` table — lakehouse-curated data, served at OLTP latency.

## What you'll learn
- **Reverse ETL with a Lakebase synced table** — Delta (analytical) → Lakebase (operational), managed
  by a serverless Lakeflow sync pipeline (the same machinery online feature stores use).
- Why the source needs a **primary key + Change Data Feed** (the sync reads CDF to replicate).
- **Sync modes** — `SNAPSHOT` (one full load), `TRIGGERED` (incremental on demand), `CONTINUOUS`
  (streaming, always-on) — and when to pick each.
- That the synced table lands on **your branch** and is a real Postgres table an app can read.

## Run the notebook
Open `Delta_To_Lakebase.py` on serverless and **Run all**. It:
1. Builds a curated Delta source `client_reference` (9 clients, `risk_limit_cad`) with a PK + CDF.
2. Creates a synced table `lb_client_reference` into **your Lakebase branch** via
   `w.postgres.create_synced_table` (SNAPSHOT first load), and waits for it to come online.
3. Reads it back over Postgres (`get_connection()`) and checks the row count matches the source.
4. Raises Air Canada's risk limit in Delta, re-syncs (TRIGGERED incremental), and shows the new value
   land in Lakebase.

> ⏳ **Provisioning wait (~a few minutes, expected):** creating the sync pipeline + first snapshot is
> fixed overhead — it's the pipeline lifecycle, not the 9 rows. Not a hang.

### Optional — generate it with Genie
Fresh Genie Code chat on a serverless notebook, paste:

> Create a Databricks notebook that syncs a Unity Catalog Delta table into my Lakebase branch as an
> operational `lb_` table (reverse ETL). `%run ../_setup` for helpers (`w`, `PROJECT_ID`,
> `USER_BRANCH`, `PG_DATABASE`, `get_connection`). Steps: (1) build a Delta table
> `main.cm_features_<me>.client_reference` — 9 CIBC capital-markets clients with `client_id` PK,
> `legal_name`, `sector`, `credit_rating`, `risk_limit_cad`, `coverage_officer`, `updated_at` — with a
> PRIMARY KEY and Change Data Feed enabled. (2) Create a Lakebase synced table `lb_client_reference`
> from it with `w.postgres.create_synced_table` (`SyncedTable` + spec: `source_table_full_name`,
> `primary_key_columns=["client_id"]`, `branch=USER_BRANCH`, `postgres_database="databricks_postgres"`,
> `scheduling_policy=SNAPSHOT`, `create_database_objects_if_missing=True`, a `new_pipeline_spec`), and
> wait for it to come online. (3) Verify by reading `lb_client_reference` over psycopg and comparing
> the count to the Delta source. (4) Update a row in Delta, re-sync (TRIGGERED), and show it propagate.

## ✓ Validation
- Delta source `client_reference` has 9 rows, a primary key, and CDF enabled.
- `lb_client_reference` exists on **your branch** and its row count matches the source.
- After raising Air Canada's limit and re-syncing, Lakebase shows **CAD 300,000,000**.

## Notes & docs
- The synced table is **project/branch-scoped** and materializes as a Postgres table — an app reads it
  with the same OAuth connection pattern as every other lab.
- Synced tables / reverse ETL into Lakebase is an actively evolving surface — check the Databricks
  docs for the current API and any **Preview** status:
  [Sync data to Lakebase / synced tables](https://docs.databricks.com/aws/en/oltp/) ·
  [Online tables & sync](https://docs.databricks.com/aws/en/machine-learning/feature-store/online-feature-store).

## What's next
**Exercise 7 — Lakebase → Delta (SCD Type 1)** is the reverse direction: it captures writes to your
`lb_*` operational tables back into the lakehouse and transforms them into **SCD1** analytical tables.
