# Facilitator guide — CIBC Capital Markets Lakebase Workshop

## Shape
One capital-markets desk, seven exercises. All participants share **one Lakebase project**, and each
works on their **own branch** of it — a Git-like, isolated, copy-on-write clone with its own compute
endpoint (the exercises connect to the attendee's own branch automatically). They get to know the
platform (and watch a branch **autoscale** under load), connect to it, generate a realistic CM
dataset, reach it over **REST vs JDBC**, **sync a Delta table into Lakebase** and **capture Lakebase
changes back out into SCD1 Delta**, then build an **online feature store** + a feature-aware
**Coverage Desk Assistant** chatbot and give it **persistent memory**.
See **`docs/facilitator.html`** for the visual setup walkthrough (model diagram + step-by-step). Most exercises are driven by **Genie Code**
(Databricks Assistant, Agent Mode) using the exact prompts in each exercise README and in
`PROMPT_RUNBOOK.md` — so every participant's generated code is near-identical and reviewable as a
group. The reference solution notebook in each folder is the validated version to fall back on.

---

## Prerequisites — services & features (facilitator, before the room)

| Capability | Used for | Status / how to enable |
|---|---|---|
| **Unity Catalog** + a writable catalog | feature-store offline table (`<catalog>.cm_features_<user>`) + the sync/SCD1 exercises' Delta tables | Required. Placeholder `main`. |
| **Serverless SQL warehouse** + serverless notebooks/jobs | all exercises run on serverless | Required — no cluster needed. Note the warehouse id. |
| **Lakebase** (managed Postgres, autoscaling projects) | the whole workshop | Required. The facilitator creates one shared project + **a branch per attendee** via `scripts/facilitator_setup.py`. **Mind the per-project branch limit** — past it, the script splits attendees across extra projects (`--max-branches-per-project`). |
| **`databricks_auth` Postgres extension** | OAuth login roles for participants | Installed by the setup script (`CREATE EXTENSION databricks_auth`). |
| **Foundation Model APIs** — a served pay-per-token Claude endpoint (e.g. `databricks-claude-sonnet-4-5`) | feature-store chatbot + memory agent LLM | Must be **served in your region** — check `system.ai` / serving endpoints directly; docs lag. Swap the endpoint name in the notebooks if yours differs. |
| **Feature Engineering / Online Feature Store** | Ex4 (feature store) publish to Lakebase + Feature Serving | GA. `databricks-feature-engineering>=0.13.0`. |
| **Model Serving — create endpoints** | Ex4 Feature Serving endpoint | Participants need permission to create serving endpoints. |
| **Change Data Feed** | Ex6 (Delta→Lakebase sync) + Ex4 offline feature table (publish prerequisite) | GA (the notebooks set it). |
| **Lakebase synced tables** (reverse ETL) | Ex6 Delta → `lb_*` on the branch (`w.postgres.create_synced_table`) | Evolving/preview surface — confirm the API + any Preview status in the workspace. Uses the same serverless sync machinery as online tables. |
| **Lakeflow / DLT (serverless)** + a second UC schema `cm_scd_<user>` | Ex7 SCD Type 1 pipeline (`create_auto_cdc_flow`, fallback `apply_changes`) | Participants need permission to create serverless DLT pipelines. |
| **Lakebase REST data API** | Ex3 REST-vs-JDBC access | Preview/evolving — confirm the base path + query convention in App Connect / Data API. The notebook parameterizes `REST_BASE` and degrades gracefully. |

### Versions / packages (pinned in the notebooks)
- `psycopg[binary]>=3.1.0` (most notebooks); `psycopg[binary,pool]>=3.1.0` (agentic memory)
- `databricks-sdk>=0.118.0` in every SDK-using notebook (the feature-store notebook verifies the
  resolved version at runtime rather than co-pinning, to avoid a pip backtracking loop with
  `databricks-feature-engineering`)
- `databricks-feature-engineering>=0.13.0`, `protobuf>=5.29.5,<6` (feature store)
- `langgraph`, `langgraph-checkpoint-postgres`, `langchain-databricks`, `langchain>=0.3,<0.4`, `mlflow` (feature-store chatbot / memory agent)

---

## Permissions to grant participants (facilitator, before the room)

Most of this is done for you by **`scripts/facilitator_setup.py`**; the rest it prints for you to
apply. Simplest: put the attendees in one workspace group and grant that group.

**Set by the setup script:**
- **Lakebase project** — `CAN_USE` (control plane) for each attendee, plus an OAuth login role
  (data plane) on production.
- **A branch per attendee** — forked off production (`br · <username>`, `no_expiry`) with its own
  primary READ/WRITE endpoint. The exercises connect to the attendee's own branch automatically.
- (with `--app-name`) the Lab app's service principal → project `CAN_MANAGE` + a Postgres role.
- (with `--uc-catalog`) emits the UC `USE CATALOG` + `CREATE SCHEMA` grants for the feature-store /
  sync / SCD1 exercises.

**You still apply manually (the script prints these):**
- **Compute:** `CAN USE` on the serverless SQL warehouse; serverless notebooks/jobs enabled.
- **Unity Catalog:** `USE CATALOG` + `CREATE SCHEMA` on the workshop catalog (feature-store Delta
  table + the sync/SCD1 exercises) — or pass `--uc-catalog` and let the script emit them.
- **Model Serving / Foundation Models:** `CAN QUERY` on the pay-per-token FM endpoint (feature-store
  chatbot + memory agent), and the entitlement to **create serving endpoints** (feature-store Feature
  Serving endpoint).
- **Lakeflow / DLT:** the entitlement to **create serverless DLT pipelines** (the SCD1 exercise).

> **No account admin needed** — a workspace admin can run the whole workshop. Projects are created
> and granted per-workspace; the CM data is synthetic and self-generated.

### Project / branch layout
- **One shared project, a branch per attendee (default).** Fewest projects; each attendee still gets a
  fully-isolated branch (own endpoint, own data) they can freely load and break. Run:
  ```
  python scripts/facilitator_setup.py -p <profile> --project-id cibc-cm-workshop \
      --grant-user a@cibc.com --grant-user b@cibc.com ... --uc-catalog main
  ```
- **Too many attendees for one project's branch limit?** Add `--max-branches-per-project N` — the
  script creates `cibc-cm-workshop-1`, `-2`, … and splits attendees across them. Attendees can set
  `LAKEBASE_SHARED_PROJECT_ID` (or `SHARED_PROJECT_ID`) to the **base** id — `labs/_setup.py`
  auto-routes each one to the split project that holds *their* branch (setting the exact project from
  the printed map also works). Their branch is unchanged. Isolation is by branch, but Lakebase ACL is
  project-level, so it's isolation by convention (fine for a workshop). Full walkthrough:
  `docs/facilitator.html`.

---

## Suggested agenda (full day, ~6–6.5h — or split across two half-days at the break)

The recommended flow is 1 → 7 (as ordered in `docs/attendee.html`). Folder names are numbered
`labs/01…` – `labs/07…` to match the flow; the numbers below are the exercise/folder numbers.

1. **Kickoff + Exercise 1 — Getting to know Lakebase (45m)** — what Lakebase is; tour the UI; run
   queries in the SQL Editor; then kick off `Autoscale_Load.py` and watch the branch **autoscale**
   in the Monitoring graph.
2. **Exercise 2 — Authentication + data (45m)** — connect from Python via Genie Code, generate the
   CM dataset; connect VS Code as an external tool and read data.
3. **Exercise 3 — Data APIs: REST vs JDBC (30m)** — reach the branch over an HTTP data API and over
   JDBC; when to use each.
4. **Break (10m)**
5. **Exercise 4 — Online feature store + chatbot (75m)** — build the client-risk feature store,
   publish to Lakebase, stand up the Feature Serving endpoint, build the Coverage Desk Assistant and
   watch it look up features. This is the money shot. **Two steps here run long and are meant to run
   in the background while you talk — see "Filling the provisioning waits" below.**
6. **Exercise 5 — Agentic memory (45m)** — give the assistant persistent memory with LangGraph +
   Lakebase; inspect the checkpoint tables to see exactly where memory lives.
7. **Lunch / long break (30–45m)**
8. **Exercise 6 — Delta → Lakebase sync (30m)** — reverse-ETL a curated Delta table into the branch
   as an `lb_*` synced table; watch an update propagate. **Short provisioning wait — teach through it.**
9. **Exercise 7 — Lakebase → Delta, SCD Type 1 (40m)** — capture `lb_*` changes into bronze Delta,
   then a Lakeflow AUTO CDC pipeline applies SCD1. **Pipeline creation runs a few minutes.**
10. **Wrap-up (15m)** — where Lakebase fits in a CM stack; cleanup.

> **Tight on time?** The core arc is 1 → 2 → 4 → 5 (get-to-know → connect → feature store → memory).
> Exercises 3, 6 and 7 (data APIs, Delta↔Lakebase sync) are self-contained and can be dropped or run
> as a shorter integration-focused half-day.

---

## Filling the provisioning waits (feature-store exercise talk track)

Two steps in the **feature-store exercise** (`labs/04-online-feature-store/`, flow #4) provision
cloud infrastructure and block for a while. **This is expected, not a hang** — kick each one off,
tell the room what's happening, and use the time to teach. Both waits are **fixed overhead,
independent of data size** (the feature table is 9 rows). *(The Delta→Lakebase sync and the SCD1
pipeline earlier in the flow also provision — same "expected, not a hang" story, just shorter.)*

**Wait 1 — `publish_table` to the Lakebase online store (~15–20 min the first time).**
It isn't a row copy: it stands up a **serverless Lakeflow sync pipeline** that replicates the offline
Delta feature table into Lakebase Postgres (using the table's Change Data Feed), creates the
destination table, and runs an initial snapshot; the call blocks until that settles. While it runs:
- **What's actually happening** — Delta (analytical) → synced table → Postgres (operational). Why an
  *online* store exists at all: sub-ms point lookups by key, which a lakehouse scan can't serve.
- **Lakebase in a CM context** — one managed Postgres serving OLTP *and* features; autoscaling +
  scale-to-zero; where it fits vs a warehouse. Real CIBC scenarios: pre-trade limit checks, live
  exposure/risk lookups, a coverage dashboard reading current book state.
- **The CDF requirement** — why the offline table needs a primary key + Change Data Feed (the sync is
  incremental), and TRIGGERED vs CONTINUOUS publish modes.
- Recap the **two-layer auth** model from Ex2 while you're at it.

**Wait 2 — the Feature Serving endpoint provisioning to READY (~10–15 min).**
Creating a fresh serving endpoint provisions serving infrastructure and blocks until ready. While it
runs:
- **Why a Feature Serving endpoint, not a direct Postgres read** — governance (UC), versioning, and
  monitoring through MLflow; the documented pattern for an app/agent to fetch online features.
- **The agent pattern coming up next** — the chatbot's feature-lookup *tool* is an MLflow deploy-client
  call; the LLM decides *when* to fetch. Tee up the Coverage Desk Assistant demo.
- MLOps angle: how a trained model would resolve the same features at inference from the online store.

**Facilitator pre-warm option (removes Wait 1 for participants):** run the feature-store publish once
yourself before the room. Participants then hit an **incremental re-publish** (seconds–minutes) against the
existing pipeline instead of the full cold provision. (Note: serverless is *not* the lever — the sync
is already serverless; the cost is pipeline orchestration + initial snapshot, which pre-warming skips.)

## The hero thread (keep it consistent across exercises)
**Air Canada (`CL-AC`)** is seeded with a large **WTI / heating-oil energy exposure**, so:
- In the **feature-store exercise**, AC's risk features come out **HIGH tier** (elevated gross
  notional, concentration, volatility) — the Coverage Desk Assistant flags the oil exposure when asked.
- In the **agentic-memory exercise**, the coverage officer tells the assistant which clients they
  cover (incl. AC) and the assistant recalls it in a later turn / thread.
- It's also the row whose risk limit is raised in the **Delta→Lakebase sync** exercise and the
  position that changes in the **SCD1** exercise — the same client threads through the whole day.

---

## Honest talking points (say these plainly)
- **Feature Serving vs direct reads.** The chatbot looks features up through a **Feature Serving
  endpoint** queried with the **MLflow deploy client** — the documented agent pattern (governed,
  versioned, monitored). You *can* read the published online table directly over Postgres (and the
  feature-store notebook shows that too, for verification), but the endpoint is the production path.
- **Cost.** The online store and the Feature Serving endpoint incur cost while up. The Feature
  Serving endpoint is created with `scale_to_zero_enabled=True`; the Lakebase project scales down but
  **does not fully auto-stop** — stop/delete it after the workshop if cost matters.
- **OAuth tokens rotate (~hourly).** Notebooks/apps mint a fresh database credential per connection
  (`generate_database_credential`) — never cache one for a process. External tools (VS Code) use a
  **native Postgres password role** from the Lakebase App Connect dialog instead.
- **FM endpoint names drift.** If `databricks-claude-*` isn't served in your region, swap the
  endpoint name in the feature-store + agentic-memory notebooks. Claude endpoints reject the
  `temperature` param, so both notebooks subclass `ChatDatabricks` to drop it.
- **The database name is `databricks_postgres`** (underscores). The SDK resource path sometimes uses
  hyphens; the actual Postgres db is underscores — a classic first-connection gotcha.

---

## Reset / cleanup
- **Attendee branches:** delete each `br · <username>` after the workshop (Lakebase UI → project →
  Branches, or `w.postgres.delete_branch(...)`) — this frees the branch count and its endpoint.
  Created with `no_expiry`, so they persist until you remove them. Dropping a branch also clears
  everything on it: the `cm_<user>` tables, the `lb_*` synced tables, and the `checkpoint%` tables.
- **Delta → Lakebase sync exercise:** delete the synced table (removes its sync pipeline); the `lb_*`
  table lives on the branch, so dropping the branch also clears it.
- **SCD1 exercise:** delete the Lakeflow/DLT pipeline; optionally drop the `cm_bronze_<user>` and
  `cm_scd_<user>` UC schemas.
- **Feature-store exercise:** delete the online table with the SDK (`delete_online_table`) — not
  `DROP TABLE`, which orphans data; delete the Feature Serving endpoint; optionally drop the Delta
  feature schema.
- **Agentic-memory exercise:** the `checkpoint%` tables live on the attendee's branch — dropping the
  branch clears them.
- **Lakebase:** stop/delete the shared project(s) after the workshop (Compute → Database Instances /
  Lakebase). One project normally; more if you split with `--max-branches-per-project`.
- **Reset one attendee mid-workshop:** delete their branch and re-run the setup script — it recreates
  just the missing branch.
- Everything is idempotent — re-running a notebook rebuilds its artifacts.

## Get the workshop into the customer's workspace
Import this repo folder: `databricks workspace import-dir . "/Workspace/Users/<them>/cibc-cm-lakebase-workshop"`
(or Repos → Add Repo from Git). The `.py` files import as runnable notebooks; nothing installs
locally. Then run `scripts/facilitator_setup.py` from a CLI authenticated to their workspace.
