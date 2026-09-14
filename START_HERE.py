# Databricks notebook source
# MAGIC %md
# MAGIC # The Trading Desk on Lakebase — CIBC Capital Markets Workshop
# MAGIC
# MAGIC Build a CIBC Capital Markets coverage desk on **Lakebase** (managed Postgres on Databricks):
# MAGIC connect to it, generate a desk's worth of data, reach it over **REST and JDBC**, serve live
# MAGIC client-risk features to a **chatbot**, give that chatbot a **memory**, and sync data both ways
# MAGIC with the lakehouse. One managed Postgres, the whole desk.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **~5h** · eight exercises | **Serverless** · no cluster |
# MAGIC | Hero client · **Air Canada (CL-AC)** | You work on **your own branch** of a shared project |
# MAGIC
# MAGIC **Before you start (once):** open <a href="$./labs/_setup">labs/_setup</a> and set the two
# MAGIC constants near the top — `SHARED_PROJECT_ID` (your facilitator's Lakebase project id, e.g.
# MAGIC `"cibc-cm-workshop"`) and `SHARED_CATALOG` (the workshop's Unity Catalog catalog, e.g. `"main"`).
# MAGIC Set them once, no commit needed, and every exercise inherits them — nothing to configure per
# MAGIC notebook. Enter the base project id even if the room was split across projects; you're
# MAGIC auto-routed to the project that holds your branch. Your `cm_<user>` schema is created for you.
# MAGIC
# MAGIC **How to use this guide:** the links below open each exercise notebook in your clone. Work the
# MAGIC exercises **in order 1 → 8** — open each notebook and **Run all**. Each exercise folder has its
# MAGIC own `README.md` with the full walkthrough and an **architecture diagram**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your exercise notebooks
# MAGIC
# MAGIC Open each in order. These are **workspace-relative** links (Databricks `$`-notebook links), so
# MAGIC they resolve to the notebooks in *this* clone wherever you cloned the repo — no setup, nothing to run.
# MAGIC Each links to its folder's `README.md` for the walkthrough, prompts, and diagram.
# MAGIC
# MAGIC 1. <a href="$./labs/01-getting-to-know-lakebase/Autoscale_Load">Getting to know Lakebase</a> — UI walkthrough + watch your branch autoscale under load.
# MAGIC 2. <a href="$./labs/02-authentication/Connect_And_Generate_Data">Authentication &amp; generate the desk's data</a> — OAuth from Python; create the CM dataset.
# MAGIC 3. <a href="$./labs/03-data-api/Data_API">Data APIs — REST vs JDBC</a> — reach your branch over HTTP and over a pooled JDBC connection.
# MAGIC 4. <a href="$./labs/04-online-feature-store/Feature_Store">Online Feature Store</a> &nbsp;&middot;&nbsp; <a href="$./labs/04-online-feature-store/Coverage_Desk_Chatbot">Coverage Desk Chatbot</a> — serve client-risk features to a LangChain assistant.
# MAGIC 5. <a href="$./labs/05-agentic-memory/Agent_Memory">Agentic Memory</a> — give the assistant durable memory in Lakebase (LangGraph + PostgresSaver).
# MAGIC 6. <a href="$./labs/06-delta-to-lakebase-sync/Delta_To_Lakebase">Delta → Lakebase sync</a> — reverse-ETL a curated Delta table into your branch.
# MAGIC 7. <a href="$./labs/07-lakebase-cdf-to-scd1/Lakebase_CDF_To_SCD1">Lakebase → Delta, SCD1 (driver)</a> &nbsp;&middot;&nbsp; <a href="$./labs/07-lakebase-cdf-to-scd1/scd1_pipeline">scd1_pipeline</a> — Lakebase CDF → `lb_*_history` → an SCD1 pipeline.
# MAGIC 8. <a href="$./labs/08-lakebase-search/Lakebase_Search">Lakebase Search</a> — earnings-call PDFs → pgvector → an agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **The hero thread:** Air Canada (`CL-AC`) runs through every exercise — its energy book is the OLTP
# MAGIC data (Ex2), the rows read over REST/JDBC (Ex3), the HIGH-risk features the assistant looks up (Ex4)
# MAGIC and remembers (Ex5), the limit raised in the sync (Ex6), and the position that changes in SCD1
# MAGIC (Ex7). One managed Postgres for OLTP data, feature serving, and agent memory.
# MAGIC
# MAGIC *Visual guide: `docs/attendee.html` · Facilitator setup: `docs/facilitator.html` · Data model:
# MAGIC `DATA_MODEL.md`.*
