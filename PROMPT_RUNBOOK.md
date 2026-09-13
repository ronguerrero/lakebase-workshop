# PROMPT_RUNBOOK — CIBC Capital Markets Lakebase Workshop

Every module carries two paths — pick per participant:
- **A (Genie Code):** paste the module's prompt into **Genie Code** (Databricks Assistant, Agent
  Mode). Zero install. The prompts are fully-qualified so the whole room generates near-identical
  code — review it together.
- **B (Reference notebook):** the checked-in `.py` notebook is the validated version that prompt
  should produce. Run it directly, or diff the generated code against it.

The **verbatim prompts** live in each exercise's `README.md` (the single source of truth). This
runbook is the run-of-show: what each module does, which path to use, and the **✓ validation** to
confirm before moving on.

## Driving Genie Code (Path A)
- **One fresh Genie Code chat per module.** The prompts are self-contained, so a new chat keeps
  context small.
- **Don't tell it to "confirm" or "approve."** It plans and executes end-to-end — paste, glance at
  the plan, let it run, then run the generated cells top-to-bottom.
- **Be on the right surface:** a **serverless notebook** in the same folder as `labs/` so
  `%run ../_setup` resolves. The notebook-based exercises start with `%pip install` +
  `dbutils.library.restartPython()` — let the restart happen before the later cells run.
- Everything runs on **serverless** — no cluster needed.

> **Prerequisites:** see `FACILITATOR.md → Prerequisites` — Unity Catalog + a catalog, serverless
> notebooks/jobs (**no SQL warehouse required**), Lakebase (project created by the facilitator via the
> `scripts/facilitator_setup_notebook` notebook), a served pay-per-token Claude endpoint, and Model
> Serving + serverless DLT enabled (attendees create the endpoint/pipeline with their workspace access).

---

## Phase 0 — Facilitator setup (once, before the room)
Create the **shared** Lakebase project and a **branch per participant**, and grant them — **entirely
in Databricks**. Import the repo, open the `scripts/facilitator_setup_notebook` notebook, and set the
widgets (idempotent):
```
grant_users              = a@cibc.com, b@cibc.com          # attendee emails
project_id               = cibc-cm-workshop
uc_catalog               = main                            # or your catalog (blank = skip UC)
branch_max_cu            = 4
dry_run                  = true                            # preview first, then set false and re-run
# large room past the per-project branch limit:
max_branches_per_project = 18   # splits attendees across cibc-cm-workshop-1, -2, …; each sets SHARED_PROJECT_ID
# warehouse_id           = <id>  # OPTIONAL — the workshop needs no warehouse; set only to use one
```
The notebook sets the per-user grants (project + UC). It then reminds you to confirm the workspace
**toggles** — serverless notebooks/jobs, Model Serving, and serverless DLT enabled. The Claude
endpoint is a **system foundation model** (workspace users query it with no grant), and attendees
create the Feature Serving endpoint (Ex4) and DLT pipeline (Ex7) with their workspace access (no
entitlement to grant).

✓ Project AVAILABLE; each participant has their own branch `br · <username>` and can connect and
create their `cm_<username>` schema on it.

---

## Exercise 1 — Getting to Know Lakebase  *(no Genie code)*
Guided UI walkthrough: what Lakebase is, the project/branch/endpoint model, scale-to-zero, and the
in-UI **SQL Editor**. Then a hands-on **autoscale/load** step: run `Autoscale_Load.py` to drive a
concurrent query load at your branch and watch the compute **CU curve** rise in the Monitoring UI and
settle afterward. Full steps: `labs/01-getting-to-know-lakebase/README.md`.

✓ Participant opens their project, runs `SELECT version();` / `information_schema` queries in the SQL
Editor, creates their `cm_<username>` schema, and sees the branch scale under load (CU rises above
`min` during the run, settles after). *(Branch endpoints need `max_cu > min_cu` for the curve to
show — the facilitator sets this with the `branch_max_cu` widget.)*

---

## Exercise 2 — Authentication + data generation
**A:** `labs/02-authentication/README.md` → *The prompt (paste verbatim)*. Generates the connection
(`%run ../_setup`, `get_connection()`) + loads the 5 capital-markets tables.
**B:** `labs/02-authentication/Connect_And_Generate_Data.py`.
**External tool:** `labs/02-authentication/VSCODE_CONNECT.md` (native password role, read the data).

✓ 9 clients, 18 instruments, ~540 market prices, ~95 positions, 600 trades. Clients group under the 3
coverage officers. **Air Canada's gross notional is dominated by WTI + heating oil** (the hero). VS
Code connects over SSL and reads `cm_<username>.clients`.

---

## Exercise 3 — Data APIs: REST vs JDBC
**A:** `labs/03-data-api/README.md` → *Generate with Genie* prompt. **B:** `labs/03-data-api/Data_API.py`.
Reads a few `clients` rows from your branch two ways — Part A over the **REST data API** (OAuth bearer
token, JSON back), Part B over **JDBC** (Spark's JDBC reader, same driver/URL a JVM app uses).

✓ REST returned JSON rows (or you saw the body and adjusted `REST_BASE` to your workspace); the JDBC
URL printed and the Spark JDBC read returned your `clients` rows; you can say which path a browser
dashboard vs a JVM service should use, and why.

> **REST is preview/evolving** — the notebook parameterizes `REST_BASE` and degrades gracefully; JDBC
> is the always-works path. Both hit the **same branch** and obey the **same grants** (your Databricks
> identity is the Postgres role).

---

## Exercise 4 — Online Feature Store + Coverage Desk Assistant
**A (Part A — feature store):** `labs/04-online-feature-store/README.md` → *Part A* prompt. Offline
Delta features (PK + CDF) → publish to Lakebase online store → `FeatureSpec` → **Feature Serving
endpoint** → query via the **MLflow deploy client**.
**A (Part B — chatbot):** same README → *Part B* prompt. LangChain agent whose `lookup_client_risk`
tool calls the serving endpoint.
**B:** `Feature_Store.py` + `Coverage_Desk_Chatbot.py`.

✓ 9 clients; **Air Canada = `risk_tier=HIGH`**; the online table appears in Lakebase; the serving
endpoint returns AC's features; the chatbot's verbose trace shows `lookup_client_risk` firing and its
answer reflects AC's HIGH risk / large energy exposure.

> **Why Feature Serving (not a raw Postgres read):** the documented agent pattern is a Feature
> Serving endpoint wrapped as a LangChain tool and queried with the MLflow deploy client — governed,
> versioned, monitored. So "tool vs MLflow API" is a false choice: the tool *is* the MLflow call.

---

## Exercise 5 — Agentic Memory on Lakebase
**A:** `labs/05-agentic-memory/README.md` → *The prompt*. LangGraph `PostgresSaver` on Lakebase for
durable conversation memory; inspect + de-serialize the `checkpoint%` tables.
**B:** `labs/05-agentic-memory/Agent_Memory.py`.

✓ Turn 2 recalls what Turn 1 said on the same `thread_id` (Sofia covers Air Canada + Suncor); a fresh
agent object still recalls it; the four `checkpoint%` tables exist; de-serializing `checkpoint_blobs`
with `JsonPlusSerializer` finds the row holding "Air Canada".

> **Two gotchas (baked into the reference notebook):** (1) subclass `ChatDatabricks` to pop
> `temperature` (Claude endpoints 400 on it); (2) connect with psycopg **keyword args, not a URL**
> (the OAuth token breaks URL parsing). Database name is `databricks_postgres` (underscores).

---

## Exercise 6 — Delta → Lakebase sync (reverse ETL)
**A:** `labs/06-delta-to-lakebase-sync/README.md` → *generate it with Genie* prompt.
**B:** `labs/06-delta-to-lakebase-sync/Delta_To_Lakebase.py`. Builds a curated Delta `client_reference`
(PK + CDF) → creates a Lakebase **synced table** `lb_client_reference` on your branch via
`w.postgres.create_synced_table` (SNAPSHOT) → reads it back over Postgres → raises Air Canada's limit
and re-syncs (TRIGGERED) to show the update land.

✓ Delta source has 9 rows + PK + CDF; `lb_client_reference` exists on **your branch** with a matching
row count; after raising AC's limit and re-syncing, Lakebase shows **CAD 300,000,000**.

> **Short provisioning wait (expected):** the sync pipeline + first snapshot is fixed overhead, not
> the 9 rows. Synced tables are an evolving/preview surface — confirm the API in your workspace docs.

---

## Exercise 7 — Lakebase → Delta (SCD Type 1) via Change Data Feed
**A:** `labs/07-lakebase-cdf-to-scd1/README.md` → *The prompt*. **B:** `Lakebase_CDF_To_SCD1.py` (driver)
+ `scd1_pipeline.py` (the DLT source). The driver seeds `positions` + `limits` in a dedicated Postgres
schema `cm_<me>_ops` (with `REPLICA IDENTITY FULL`), turns on **Lakebase Change Data Feed**
(`w.postgres.create_cdf_config`) — which auto-creates `cm_<me>.lb_positions_history` /
`lb_limits_history` Delta tables — mutates a position + a limit, then creates a **Lakeflow (DLT)
pipeline** that applies **AUTO CDC SCD Type 1** (`create_auto_cdc_flow`, fallback `apply_changes`,
sequencing by `_sort_by`) into `positions_current` / `limits_current`.

✓ `lb_positions_history` holds **multiple change rows** for position 1 (insert 250, update 900);
`positions_current` has **one row per `position_id`** (SCD1) and position 1 reads **net_qty = 900**
(latest wins), with no second row.

> **Preview caveats (facilitator):** Lakebase CDF is Public Preview — the destination catalog must not
> use default storage, and creating a feed needs CAN MANAGE on the project (attendees have CAN_USE), so
> either bump the grant or pre-create the feed. Deletes come for free (`_pg_change_type='delete'`).
> Pipeline creation runs a few minutes.

---

## Exercise 8 — Lakebase Search (pgvector + agent)
**A:** `labs/08-lakebase-search/README.md` → *The prompt*. **B:** `Lakebase_Search.py`. Generates
synthetic earnings-call PDFs (Air Canada, Suncor, BCE) into a UC Volume, parses them (pypdf), chunks +
embeds via a Databricks FM embeddings endpoint (`databricks-gte-large-en`), stores the vectors in
**Lakebase `pgvector`** (`cm_<me>.earnings_chunks`, HNSW cosine index), and puts a **LangChain agent**
in front whose one tool runs the `<=>` similarity search. The point: **Lakebase is your vector store**
— retrieval where the data already lives.

✓ `earnings_chunks` exists in Lakebase with a `vector` column; a search for "Air Canada fuel exposure"
returns Air Canada passages; the agent answers by calling the tool and citing the call.

> **Endpoints:** embeddings endpoint name varies by region (`w.serving_endpoints.list()`); the LLM is
> the `llm_endpoint` widget. `pgvector` is enabled server-side (`CREATE EXTENSION vector`).

---

## The hero thread (keep it consistent)
**Air Canada (`CL-AC`)** carries a large WTI/heating-oil energy book → HIGH-risk features in the
feature-store exercise, the client the coverage officer tells the assistant about in the memory
exercise, the limit raised in the Delta→Lakebase sync, the position that changes in SCD1, and the name
whose earnings-call fuel exposure the search agent surfaces (Ex8). Same client, threaded through every
Lakebase use: OLTP data (Ex2), REST/JDBC reads (Ex3), feature serving (Ex4), agent memory (Ex5),
reverse-ETL reference (Ex6), change capture (Ex7), vector search (Ex8).
