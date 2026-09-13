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

1. **Authenticate** the Databricks CLI to the workshop workspace:
   ```bash
   databricks auth login <workspace-host> --profile <profile>
   ```
2. **Create the shared project + a branch per participant** with the setup script. Everyone
   shares **one Lakebase project**; each attendee gets their **own branch** (isolated, own
   endpoint) that the exercises connect to automatically:
   ```bash
   python scripts/facilitator_setup.py -p <profile> \
       --project-id cibc-cm-workshop \
       --grant-user sofia@cibc.com --grant-user david@cibc.com \
       --uc-catalog main
   ```
   Too many attendees for one project's branch limit? Add `--max-branches-per-project N` and the
   script splits them across `cibc-cm-workshop-1`, `-2`, …. Add `--branch-max-cu 4` to give each
   branch an autoscaling envelope (min 1 → max 4 CU) so the Exercise 1 load demo shows scaling.
   Add `--dry-run` first to see exactly what it will do. The script is idempotent.
3. **Finish the workspace/UC grants** the script prints (serverless warehouse `CAN USE`,
   Foundation Model endpoint `CAN QUERY`, ability to create serving endpoints) — see
   `FACILITATOR.md → Permissions`.
4. **Only if you split across multiple projects:** tell each participant to set
   `LAKEBASE_SHARED_PROJECT_ID=<their-project>` (from the printed attendee → project/branch map),
   or the `SHARED_PROJECT_ID` constant in `labs/_setup.py`. With a single project this is unset.

See **`docs/facilitator.html`** for the visual setup walkthrough (topology diagram + step-by-step).

## For participants — get started

1. Clone/import this repo into your Databricks workspace (Repos → Add Repo, or
   `databricks workspace import-dir`).
2. Open `labs/01-getting-to-know-lakebase/README.md` and work through the exercises in order.
3. Each Genie-driven exercise tells you exactly what to paste into **Genie Code**; the reference
   notebook in the same folder is there if you get stuck.

---

## Prerequisites (short version)

Workspace needs: **Unity Catalog** + a writable catalog; a **serverless SQL warehouse** +
serverless notebooks; **Lakebase** (managed Postgres, autoscaling); **Foundation Model APIs** with a
served pay-per-token Claude endpoint; the ability to **create Model Serving endpoints** (feature-store
Feature Serving) and **serverless DLT pipelines** (SCD1 exercise). Full checklist and versions in
`FACILITATOR.md`.

## Layout

```
scripts/facilitator_setup.py     create shared Lakebase project + a branch per participant (facilitator)
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
