# CIBC Capital Markets — Lakebase Workshop

A hands-on **Lakebase** (managed Postgres on Databricks) workshop tailored to **CIBC Capital
Markets**. Participants get to know Lakebase, connect to it, generate a realistic capital-markets
dataset, build an **online feature store** and a feature-aware **Coverage Desk Assistant** chatbot,
and give that assistant **persistent memory** — all on their own Lakebase project.

The teaching approach is deliberate: most exercises are driven by **Genie Code** (Databricks
Assistant, Agent Mode). Each exercise ships an **exact prompt** you paste into Genie Code, written
precisely enough that every participant generates near-identical code — so the room can review the
result together. Each exercise folder also contains the **reference solution** that prompt should
produce (already validated), so a facilitator can compare and unblock anyone.

---

## The seven exercises

| # | Exercise | Folder | What you build |
|---|----------|--------|----------------|
| 1 | **Getting to Know Lakebase** (+ autoscaling/load) | `labs/01-getting-to-know-lakebase/` | Orientation + the in-UI SQL Editor; drive load and watch your branch autoscale (`Autoscale_Load.py`) |
| 2 | **Authentication** | `labs/02-authentication/` | Connect from Python (OAuth) + generate the CM dataset; connect VS Code as an external tool |
| 3 | **Data APIs — REST vs JDBC** | `labs/03-data-api/` | Reach your branch over a REST data API and over JDBC; when to use each |
| 4 | **Online Feature Store + Chatbot** | `labs/04-online-feature-store/` | Client-risk features published to Lakebase, served via a Feature Serving endpoint; a LangChain "Coverage Desk Assistant" that looks them up via the MLflow deploy client |
| 5 | **Agentic Memory** | `labs/05-agentic-memory/` | A LangGraph agent with persistent conversation memory in Lakebase Postgres |
| 6 | **Delta → Lakebase sync** | `labs/06-delta-to-lakebase-sync/` | Reverse-ETL a curated Delta table into your branch as an `lb_*` synced table |
| 7 | **Lakebase → Delta (SCD Type 1)** | `labs/07-lakebase-cdf-to-scd1/` | Capture `lb_*` changes into bronze Delta, then a Lakeflow AUTO CDC pipeline applies SCD1 |

Exercise numbers match the folder names (`labs/0N-…`) and the recommended flow in `docs/attendee.html`
/ `PROMPT_RUNBOOK.md`.

Every attendee shares **one Lakebase project** but works on their **own branch** (isolated, own
endpoint) — the exercises connect to it automatically. The **Coverage Desk Assistant** persona ties
exercises 4 and 5 together: it *looks up live client risk features*, then *remembers the conversation*.

See **`DATA_MODEL.md`** for the shared capital-markets data model, and **`FACILITATOR.md`** for
prerequisites, the permissions to grant participants, the agenda, and honest talking points.

---

## For facilitators — set up before the room

1. **Import this repo into the workspace** (Repos → Add Repo / Git folder). Everything runs
   inside Databricks — no CLI, no laptop setup.
2. **Create the shared project + a branch per participant** by opening the
   **`scripts/facilitator_setup_notebook`** notebook and running it. It runs with your notebook
   identity (no CLI/profile) and is idempotent. Everyone shares **one Lakebase project**; each
   attendee gets their **own branch** (isolated, own endpoint) that the exercises connect to
   automatically. *Run all* to render the widgets, then fill them in — at minimum:
   - **`grant_users`** — attendee emails (comma/space separated)
   - **`project_id`** — `cibc-cm-workshop` (default)
   - **`uc_catalog`** — the catalog to use (e.g. `main`, or blank to skip UC)

   Keep **`dry_run = true`** for a no-op preview first, then set it `false` and re-run to
   provision. Too many attendees for one project's branch limit? Set
   **`max_branches_per_project`** and the notebook splits them across `cibc-cm-workshop-1`,
   `-2`, …. **`branch_max_cu`** (default 4) gives each branch an autoscaling envelope
   (min 1 → max 4 CU) so the Exercise 1 load demo shows scaling.
3. **Confirm the workspace toggles** the notebook reminds you about — the per-user grants
   (project + UC) are set for you. What's left is workspace-level: serverless notebooks/jobs,
   Model Serving, and serverless DLT enabled. There's **no SQL warehouse requirement**, the Claude
   endpoint is a **system foundation model** (no `CAN QUERY` grant needed), and there's no
   "create endpoint/pipeline" entitlement to grant — attendees create those with their workspace
   access. See `FACILITATOR.md → Permissions`.
4. **Tell participants the project id.** Everyone sets `SHARED_PROJECT_ID` in `labs/_setup.py`
   (or exports `LAKEBASE_SHARED_PROJECT_ID`) to the project you created (e.g. `cibc-cm-workshop`).
   If you split a large room across `cibc-cm-workshop-1`, `-2`, …, they can still set the **base**
   id — `_setup` auto-routes each attendee to the split project that holds *their* branch. If the
   project id is unset or wrong, the exercises **fail fast with a clear message** (no long hang).
   The notebook prints the attendee → project map at the end.

See **`docs/facilitator.html`** for the visual setup walkthrough (topology diagram + step-by-step).

## For participants — get started

1. Clone this repo into your Databricks workspace (Repos → Add Repo / Git folder).
2. Open **`START_HERE`** and **Run all** — it's the guided walkthrough, and its first cell prints
   working links to every exercise notebook *in your clone* (resolved at runtime, so they work no
   matter where you cloned it). Prefer a browser? Open `docs/attendee.html` for the visual version.
3. Work the exercises in order 1 → 7. Each has a reference notebook to run **and** a Genie Code
   prompt you can paste into the Databricks Assistant instead.

---

## Prerequisites (short version)

Workspace needs: **Unity Catalog** + a writable catalog; **serverless notebooks/jobs** (the exercises
run on serverless notebook compute — **no SQL warehouse required**); **Lakebase** (managed Postgres,
autoscaling); **Foundation Model APIs** with a served pay-per-token Claude endpoint; **Model Serving**
and **serverless DLT** enabled (attendees create the Feature Serving endpoint and SCD1 pipeline with
their workspace access — no special entitlement). Full checklist and versions in `FACILITATOR.md`.

## Layout

```
START_HERE.py                    run-first notebook guide: prints live links to every exercise + walkthrough
scripts/facilitator_setup_notebook.py   create shared Lakebase project + a branch per participant (Databricks notebook, widgets, no CLI)
labs/_setup.py                   shared connection helper (%run by each exercise)
labs/01-getting-to-know-lakebase/   README + Autoscale_Load.py (drive load, watch the branch scale)
labs/02-authentication/             Connect_And_Generate_Data.py + VSCODE_CONNECT.md
labs/03-data-api/                   Data_API.py — REST vs JDBC access to your branch
labs/04-online-feature-store/       Feature_Store.py + Coverage_Desk_Chatbot.py
labs/05-agentic-memory/             Agent_Memory.py
labs/06-delta-to-lakebase-sync/     Delta_To_Lakebase.py — reverse-ETL Delta → lb_* synced table
labs/07-lakebase-cdf-to-scd1/       Lakebase_CDF_To_SCD1.py + scd1_pipeline.py — lb_* → bronze Delta → SCD1
docs/attendee.html · docs/facilitator.html   self-contained lab guides (open in a browser)
DATA_MODEL.md · FACILITATOR.md · PROMPT_RUNBOOK.md
```

Everything is synthetic and generated from scratch — no customer data, no external files.

---

*Modeled on the [databricks-solutions/lakebase-workshop](https://github.com/databricks-solutions/lakebase-workshop)
structure and the CM Data Products "Trading Floor Navigator" workshop style.*
