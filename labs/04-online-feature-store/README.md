# Exercise 4 — Online Feature Store + the Coverage Desk Assistant

**Genie Code driven.** You will paste the prompts below into **Genie Code (Agent Mode)**.
The prompts are fully-qualified so everyone in the room generates near-identical code — we
review it together. The reference solutions (`Feature_Store.py`, `Coverage_Desk_Chatbot.py`)
are what Genie Code should produce; use them to compare or as a fallback.

## What you'll learn
- Build a **capital-markets feature store**: per-client risk features (exposure, PnL,
  concentration, volatility, risk tier) as an offline Delta table.
- **Publish** those features to your **Lakebase** project as an online table for
  low-latency serving.
- Access features through **MLflow**: a `FeatureSpec` → a **Feature Serving endpoint** →
  the MLflow deploy client.
- Build the **Coverage Desk Assistant** — a LangChain chatbot whose feature-lookup **tool**
  calls that endpoint, so the bot answers with a client's *live* risk profile.

## Why Feature Serving + the MLflow deploy client (not a raw Postgres read)
The Databricks docs make the Feature Serving endpoint the recommended way for an
application/agent to fetch online features: it's governed by Unity Catalog, versioned and
monitored through MLflow, and built for low-latency, high-concurrency reads. For an agent,
the docs' RAG guidance is explicit — *wrap the endpoint in a LangChain tool and let the
agent call it*. So "is it a tool or an MLflow API?" is a false choice: **the tool is an
MLflow deploy-client call.**
- [Feature & function serving](https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-function-serving)
- [Feature serving tutorial](https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-serving-tutorial)
- [RAG / agent integration](https://docs.databricks.com/aws/en/machine-learning/feature-store/rag)

A **direct Postgres read** of the online table (we do this once in §4 to verify the
publish) is fine for a quick check or a batch job, but it bypasses the governance,
versioning, and serving layer — so it's not how the chatbot should look features up.

---

## Part A — Build & publish the feature store

**Be on a serverless notebook.** Start a fresh Genie Code chat and paste:

> Create a Databricks notebook that builds a **capital-markets online feature store** on my
> **Lakebase** project. Steps:
> 1. `%pip install "databricks-feature-engineering>=0.13.0" "psycopg[binary]>=3.1.0" "protobuf>=5.29.5,<6" "mlflow"`, then `dbutils.library.restartPython()`.
> 2. In Unity Catalog, use catalog `main` and schema `cm_<my_username>` (sanitize my
>    email local-part, `-`/`.`→`_`). Build a Spark DataFrame of **per-client risk features**,
>    one row per `client_id`, for 9 Canadian capital-markets clients (Air Canada `CL-AC`,
>    Suncor `CL-SU`, Cenovus `CL-CVE`, BCE `CL-BCE`, Manulife `CL-MFC`, Barrick `CL-ABX`, CN
>    Rail `CL-CNR`, Magna `CL-MG`, Brookfield `CL-BAM`). Compute the features from an
>    in-notebook positions + trades + instruments DataFrame (spot, volatility, contract size,
>    FX-to-CAD). Seed **Air Canada with a large WTI/heating-oil energy book so its risk_tier
>    is HIGH**. Features: `gross_notional_cad`, `net_exposure_cad`, `num_open_positions`,
>    `num_trades_30d`, `avg_trade_notional_cad`, `largest_position_pct`,
>    `realized_pnl_30d_cad`, `portfolio_volatility`, `days_since_last_trade`,
>    `credit_rating_score`, `risk_tier` (LOW/MEDIUM/HIGH). Use a fixed random seed.
> 3. Create the offline feature table `main.cm_<my_username>.client_risk_features`
>    with `FeatureEngineeringClient.create_table(primary_keys=["client_id"], df=...)`, then
>    enable Change Data Feed on it.
> 4. Publish it to my Lakebase project as the online store: `fe.get_online_store(name=<my
>    Lakebase project id>)` then `fe.publish_table(...)` into `client_risk_features_online`.
>    Include a retry that tolerates "destination already exists".
> 5. Verify the online table exists by connecting to Lakebase over psycopg (SDK OAuth
>    credential, keyword args, `sslmode=require`) and listing `pg_tables` matching
>    `%client_risk%`.
> 6. Create a `FeatureSpec` `main.cm_<my_username>.client_risk_spec` with a
>    `FeatureLookup(table_name=<feature table>, lookup_key="client_id")`, then create a
>    **Feature Serving endpoint** `cm-client-risk-<my_username>` with `scale_to_zero_enabled`,
>    and query it with `mlflow.deployments.get_deploy_client("databricks").predict(...)` for
>    `client_id="CL-AC"`.

> ⏳ **Two steps here run long (and that's normal, not a hang):** `publish_table` stands up a
> serverless sync pipeline into Lakebase (**~15–20 min the first time**), and the Feature Serving
> endpoint takes **~10–15 min** to provision. Both are fixed overhead regardless of data size. Kick
> them off and your facilitator will use the time to walk through how the online store and feature
> serving work. (If your facilitator pre-warmed the publish, your re-publish will be much faster.)

**✓ Validation:** 9 clients; Air Canada is `risk_tier=HIGH`; the online table appears in
Lakebase; the serving endpoint returns Air Canada's features.

---

## Part B — The Coverage Desk Assistant chatbot

Run Part A first (it creates the endpoint). Fresh Genie Code chat, paste:

> Create a Databricks notebook with a LangChain chatbot called the **Coverage Desk
> Assistant** for a CIBC Capital Markets coverage officer.
> 1. `%pip install "langchain>=0.3,<0.4" "langchain-databricks" "mlflow" "databricks-sdk>=0.118.0"`, then restart Python. (Pin langchain <0.4 — 1.x removes `create_tool_calling_agent` and breaks `langchain-databricks` 0.1.2.)
> 2. LLM: `ChatDatabricks` on a pay-per-token endpoint (`databricks-claude-sonnet-4` — swap
>    if not served). **Subclass ChatDatabricks and override `_prepare_inputs` to pop
>    "temperature"** (some Claude endpoints reject it).
> 3. Define a LangChain `@tool` `lookup_client_risk(client)` that resolves a client
>    name/ticker/id to a `client_id` and calls my Feature Serving endpoint
>    `cm-client-risk-<my_username>` with the **MLflow deploy client**
>    (`get_deploy_client("databricks").predict(endpoint=..., inputs={"dataframe_records":[{"client_id": ...}]})`),
>    returning the feature dict as JSON.
> 4. Build a tool-calling agent (`create_tool_calling_agent` + `AgentExecutor`, verbose) with
>    a system prompt that tells it to ALWAYS call the tool before answering and interpret the
>    features for a trader (gross notional, concentration, volatility, risk_tier, energy
>    exposure).
> 5. Demo: ask *"What's Air Canada's risk profile right now, and should I be concerned about
>    their oil exposure?"* and print the trace + answer.

**✓ Validation:** the verbose trace shows `lookup_client_risk` firing (tool → Feature
Serving → Lakebase); Air Canada's answer reflects HIGH risk / large energy exposure.

---

## Cost & cleanup
- The Feature Serving endpoint uses `scale_to_zero_enabled=True`, but **delete it after the
  workshop** (`w.serving_endpoints.delete(name=...)`). Online stores/tables incur cost while
  they hold data — remove the online table with `delete_online_table` (never `DROP TABLE`,
  which orphans data in the instance).
- Do **not** delete the Lakebase project — later labs share it.

## What's next
**Exercise 5 — Agentic Memory:** the assistant currently forgets each turn. Back it with a
LangGraph `PostgresSaver` on Lakebase so the conversation persists.
