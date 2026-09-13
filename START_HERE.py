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
# MAGIC | **~5h** · seven exercises | **Serverless** · no cluster |
# MAGIC | Hero client · **Air Canada (CL-AC)** | You work on **your own branch** of a shared project |
# MAGIC
# MAGIC **How to use this guide:** ▶ **Run all** — the next cell prints clickable links to every exercise
# MAGIC notebook *in this clone* (they resolve wherever you cloned the repo). Then work the exercises in
# MAGIC order 1 → 7. Each has two build paths: **run the reference notebook**, or **paste the Genie Code
# MAGIC prompt** (Databricks Assistant, Agent Mode) and let it write the same code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ▶ Your exercise notebooks (run this cell for working links)

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Where does THIS clone live? (this notebook sits at the repo root)
_my_path = ctx.notebookPath().get()
REPO_ROOT = os.path.dirname(_my_path)
try:
    _host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    _host = "https://" + (ctx.browserHostName().get() or "")
try:
    _org = ctx.workspaceId().get()
except Exception:
    _org = ""

def _api_path(p):
    return p if p.startswith("/Workspace") else "/Workspace" + p

def nb_link(rel_path):
    """Clickable workspace URL for a repo-relative object, resolved by its live id in THIS clone."""
    try:
        st = w.workspace.get_status(_api_path(f"{REPO_ROOT}/{rel_path}"))
        kind = "notebooks" if str(st.object_type).endswith("NOTEBOOK") else "files"
        return f"{_host}/editor/{kind}/{st.object_id}" + (f"?o={_org}" if _org else "")
    except Exception:
        return None

def fig(svg, caption):
    """Render an inline-SVG architecture diagram (self-contained styles)."""
    for k, v in {"var(--lakebase)": "#B96C13", "var(--muted)": "#586673",
                 "var(--accent)": "#0F6E78", "var(--ink)": "#16212B"}.items():
        svg = svg.replace(k, v)
    style = ("<style>.dfig{background:#fff;border:1px solid #DEE5E9;border-radius:12px;"
             "padding:16px;max-width:900px;font-family:system-ui,sans-serif}"
             ".dfig svg{width:100%;height:auto}"
             ".d-box{fill:#fff;stroke:#C7D1D8;stroke-width:1.4}"
             ".d-lake{fill:#FBEEDD;stroke:#B96C13;stroke-width:2}"
             ".d-accent{fill:#E4F0F1;stroke:#0F6E78;stroke-width:1.4}"
             ".d-tb{fill:#16212B;font:600 13px system-ui,sans-serif}"
             ".d-t{fill:#16212B;font:12px system-ui,sans-serif}"
             ".d-ts{fill:#586673;font:10.5px ui-monospace,monospace}"
             ".d-lbl{fill:#586673;font:11px ui-monospace,monospace}"
             ".d-line{stroke:#586673;stroke-width:1.5;fill:none}"
             ".d-line-lake{stroke:#B96C13;stroke-width:1.8;fill:none}"
             ".dcap{color:#586673;font-size:13px;margin-top:10px}</style>")
    displayHTML(f'<div class="dfig">{style}{svg}<div class="dcap">{caption}</div></div>')

EXERCISES = [
    ("1 · Getting to know Lakebase (autoscale/load)", "labs/01-getting-to-know-lakebase/Autoscale_Load"),
    ("2 · Authentication + generate the desk's data", "labs/02-authentication/Connect_And_Generate_Data"),
    ("3 · Data APIs — REST vs JDBC",                  "labs/03-data-api/Data_API"),
    ("4 · Online Feature Store",                      "labs/04-online-feature-store/Feature_Store"),
    ("4 · Coverage Desk Chatbot",                     "labs/04-online-feature-store/Coverage_Desk_Chatbot"),
    ("5 · Agentic Memory",                            "labs/05-agentic-memory/Agent_Memory"),
    ("6 · Delta → Lakebase sync",                     "labs/06-delta-to-lakebase-sync/Delta_To_Lakebase"),
    ("7 · Lakebase → Delta, SCD1 (driver)",           "labs/07-lakebase-cdf-to-scd1/Lakebase_CDF_To_SCD1"),
    ("7 · SCD1 pipeline (DLT source)",                "labs/07-lakebase-cdf-to-scd1/scd1_pipeline"),
]
print(f"Repo detected at: {REPO_ROOT}   (host={_host}, org={_org})")
_links = {label: nb_link(path) for label, path in EXERCISES}
for label, u in _links.items():
    print(f"  {label:48}  {u or '(not found)'}")

_rows = "".join(
    f'<li style="margin:6px 0">{label} — '
    + (f'<a href="{u}" target="_blank" rel="noopener">open →</a>' if u
       else '<span style="color:#C0392B">not found</span>')
    + "</li>"
    for label, u in _links.items())
displayHTML(f"""
<div style="font-family:system-ui,-apple-system,sans-serif;max-width:760px">
  <p style="color:#586673;margin:0 0 8px">Repo detected at <code>{REPO_ROOT}</code> — links open the
     notebooks in <b>this</b> clone.</p>
  <ol style="line-height:1.55">{_rows}</ol>
</div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Get to know Lakebase &nbsp; <sub>UI walkthrough · no code</sub>
# MAGIC
# MAGIC Lakebase is **fully-managed PostgreSQL (16/17)** built into the lakehouse — the OLTP store behind
# MAGIC your desk's apps and agents, next to the Delta tables you already query. It autoscales, scales to
# MAGIC zero when idle, branches like Git, and syncs with Unity Catalog.
# MAGIC
# MAGIC 1. Open **Compute → Database Instances** (a.k.a. **Lakebase**) and open the shared workshop
# MAGIC    project. You share it with the room but work on **your own branch** — `br · <your-username>` —
# MAGIC    an isolated copy the facilitator created for you. Every exercise connects to it automatically.
# MAGIC 2. Open your branch's compute endpoint. `IDLE` is normal — it scaled to zero and wakes on the next
# MAGIC    connection.
# MAGIC 3. Open the built-in **SQL Editor** against **your branch** and run (works on an empty branch —
# MAGIC    Exercise 2 fills it):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT version();
# MAGIC SELECT current_user, current_database();
# MAGIC SHOW search_path;
# MAGIC CREATE SCHEMA IF NOT EXISTS cm_<you>;   -- underscores, not hyphens
# MAGIC ```
# MAGIC
# MAGIC **Then — watch your branch autoscale under load.** Run `Autoscale_Load.py` (link above): it fires a
# MAGIC concurrent workload at your branch; compute climbs from the minimum toward its max, then settles
# MAGIC back to zero when the load stops. Open the Lakebase **Monitoring** graph alongside it to see the CU
# MAGIC curve (the live CU number isn't in the API, so the notebook polls endpoint *state*).
# MAGIC
# MAGIC > **✓ Check** — `version()` returns a PostgreSQL 16/17 banner, `current_user` is your email, your
# MAGIC > `cm_<you>` schema exists, and compute rises above the minimum under load then settles.

# COMMAND ----------

fig(r'''<svg viewBox="0 0 860 190" role="img" aria-label="A concurrent query load hits the branch endpoint; compute autoscales from 1 CU toward max under load, then back to zero when idle.">
<defs><marker id="au" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker></defs>
<rect class="d-box" x="16" y="66" width="176" height="60" rx="9"/>
<text class="d-tb" x="104" y="92" text-anchor="middle">Load (N workers)</text>
<text class="d-ts" x="104" y="109" text-anchor="middle">concurrent queries</text>
<line class="d-line" x1="192" y1="96" x2="250" y2="96" marker-end="url(#au)"/>
<text class="d-lbl" x="221" y="88" text-anchor="middle">market open</text>
<rect class="d-lake" x="252" y="40" width="330" height="112" rx="10"/>
<text class="d-lbl" x="272" y="62" style="fill:var(--lakebase)">&#9670; YOUR BRANCH &#183; compute endpoint</text>
<rect x="272" y="96" width="26" height="34" rx="3" fill="var(--lakebase)" opacity=".9"/>
<rect x="304" y="80" width="26" height="50" rx="3" fill="var(--lakebase)" opacity=".75"/>
<rect x="336" y="60" width="26" height="70" rx="3" fill="var(--lakebase)" opacity=".6"/>
<rect x="368" y="46" width="26" height="84" rx="3" fill="var(--lakebase)" opacity=".45"/>
<text class="d-ts" x="330" y="146" text-anchor="middle">1 CU &#8594; max, following load</text>
<text class="d-ts" x="500" y="92" text-anchor="middle">autoscaling</text>
<text class="d-ts" x="500" y="108" text-anchor="middle">pay per use</text>
<line class="d-line" x1="582" y1="96" x2="640" y2="96" marker-end="url(#au)"/>
<text class="d-lbl" x="611" y="88" text-anchor="middle">idle</text>
<rect class="d-box" x="642" y="66" width="200" height="60" rx="9"/>
<text class="d-tb" x="742" y="92" text-anchor="middle">scale to zero</text>
<text class="d-ts" x="742" y="109" text-anchor="middle">suspends, ~no cost</text>
</svg>''', "Under load your branch's compute climbs from 1 CU toward its max; when the load stops it scales back to zero. Watch the curve on the Lakebase Monitoring graph.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Authenticate &amp; generate the desk's data &nbsp; <sub>OAuth · psycopg · synthetic CM data</sub>
# MAGIC
# MAGIC In a notebook your **Databricks identity is your Postgres role**: mint a short-lived OAuth database
# MAGIC credential and connect — no stored password. Then load five capital-markets tables into your
# MAGIC schema (`clients, instruments, market_prices, positions, trades`), with Air Canada seeded as a
# MAGIC large WTI + heating-oil (energy) book.
# MAGIC
# MAGIC **▶ Run the notebook** — `labs/02-authentication/Connect_And_Generate_Data.py` (Run all). Connect
# MAGIC with **keyword args, not a URL** (OAuth tokens break URL parsing):
# MAGIC
# MAGIC ```python
# MAGIC %pip install "psycopg[binary]>=3.1.0" --quiet     # then dbutils.library.restartPython()
# MAGIC %run ../_setup                 # gives get_connection(), PG_SCHEMA, user_email, w
# MAGIC conn = get_connection()        # OAuth cred · sslmode=require · search_path = your schema
# MAGIC # … create the 5 tables and INSERT deterministic rows (seed 1867) …
# MAGIC ```
# MAGIC
# MAGIC **✨ Generate with Genie** (full prompt in `labs/02-authentication/README.md`):
# MAGIC
# MAGIC > Generate a Databricks notebook that `%run ../_setup`, opens `get_connection()`, and loads a
# MAGIC > synthetic CIBC Capital Markets dataset into my Postgres schema: five tables (`clients`,
# MAGIC > `instruments`, `market_prices`, `positions`, `trades`) with the workshop data-model columns,
# MAGIC > deterministic with `random.Random(1867)`, 9 Canadian issuers with coverage officers, and Air
# MAGIC > Canada (`CL-AC`) seeded with a large WTI + heating-oil long so its energy exposure dominates.
# MAGIC
# MAGIC Part B (optional): connect an external tool — see `labs/02-authentication/VSCODE_CONNECT.md`.
# MAGIC
# MAGIC > **✓ Check** — 9 clients, 18 instruments, 540 prices, ~76 positions, 600 trades; Air Canada's
# MAGIC > gross notional is dominated by WTI + heating oil (~$32M of ~$33M).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Data APIs — how apps reach Lakebase &nbsp; <sub>REST vs JDBC</sub>
# MAGIC
# MAGIC Applications reach your branch two ways: a stateless **REST** data API (serverless/edge front ends,
# MAGIC any language, per-request auth) or a pooled **JDBC** connection (lowest latency for chatty JVM
# MAGIC services). Same data, two access patterns — and **two different tokens** (see below).

# COMMAND ----------

fig(r'''<svg viewBox="0 0 880 240" role="img" aria-label="A serverless app reaches the branch over an HTTPS REST data API with a bearer token; a JVM service reaches the same branch over pooled JDBC.">
<defs><marker id="da" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker></defs>
<rect class="d-box" x="16" y="34" width="168" height="58" rx="9"/>
<text class="d-tb" x="100" y="58" text-anchor="middle">Serverless / edge app</text>
<text class="d-ts" x="100" y="76" text-anchor="middle">any language, no driver</text>
<rect class="d-box" x="16" y="150" width="168" height="58" rx="9"/>
<text class="d-tb" x="100" y="174" text-anchor="middle">JVM service</text>
<text class="d-ts" x="100" y="192" text-anchor="middle">pooled connections</text>
<rect class="d-accent" x="300" y="34" width="196" height="58" rx="9"/>
<text class="d-tb" x="398" y="58" text-anchor="middle">REST data API</text>
<text class="d-ts" x="398" y="76" text-anchor="middle">HTTPS &#183; OAuth bearer &#183; JSON</text>
<rect class="d-accent" x="300" y="150" width="196" height="58" rx="9"/>
<text class="d-tb" x="398" y="174" text-anchor="middle">JDBC</text>
<text class="d-ts" x="398" y="192" text-anchor="middle">postgresql driver &#183; sslmode</text>
<line class="d-line" x1="184" y1="63" x2="298" y2="63" marker-end="url(#da)"/>
<text class="d-lbl" x="241" y="55" text-anchor="middle">stateless req</text>
<line class="d-line" x1="184" y1="179" x2="298" y2="179" marker-end="url(#da)"/>
<text class="d-lbl" x="241" y="171" text-anchor="middle">persistent</text>
<rect class="d-lake" x="612" y="70" width="252" height="100" rx="10"/>
<text class="d-lbl" x="738" y="96" text-anchor="middle" style="fill:var(--lakebase)">&#9670; YOUR LAKEBASE BRANCH</text>
<text class="d-tb" x="738" y="126" text-anchor="middle">managed Postgres</text>
<text class="d-ts" x="738" y="145" text-anchor="middle">clients &#183; positions &#183; trades</text>
<line class="d-line" x1="496" y1="63" x2="640" y2="108" marker-end="url(#da)"/>
<line class="d-line" x1="496" y1="179" x2="640" y2="134" marker-end="url(#da)"/>
</svg>''', "Both paths read the same branch. REST for stateless/edge and quick integrations; JDBC for pooled, low-latency JVM services.")

# COMMAND ----------

# MAGIC %md
# MAGIC **▶ Run the notebook** — `labs/03-data-api/Data_API.py`: it reads your branch over the REST Data API
# MAGIC and over JDBC (Spark JDBC read), then compares. The REST base URL is auto-discovered from
# MAGIC `get_data_api().status.url`; if the Data API isn't enabled the notebook skips REST gracefully and
# MAGIC JDBC still runs.
# MAGIC
# MAGIC **Two different tokens (a real gotcha):** the REST Data API bearer is your **workspace OAuth token**
# MAGIC (`w.config.authenticate()`); JDBC uses the **DB credential** from `generate_database_credential` as
# MAGIC the connection password. The REST path is `{url}/{schema}/{table}?select=…` — **schema in the path**.
# MAGIC
# MAGIC > **Facilitator, once per project:** enable the Data API on the project's **Data API** page and add
# MAGIC > each attendee's schema to its `db_schemas`, or Part A returns 400/404 (JDBC is unaffected).
# MAGIC >
# MAGIC > **✓ Check** — rows back over the Data API (JSON) and over JDBC; you can say which path a browser
# MAGIC > dashboard vs a JVM service should use, and which token each uses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Online Feature Store &amp; the Coverage Desk Assistant &nbsp; <sub>feature serving · MLflow · LangChain</sub>
# MAGIC
# MAGIC Turn the desk's data into **per-client risk features**, publish them to Lakebase for low-latency
# MAGIC serving, expose them through a **Feature Serving endpoint**, and build a chatbot whose
# MAGIC feature-lookup **tool** calls that endpoint. The diagram shows what gets spun up and how each piece
# MAGIC relates to Lakebase.

# COMMAND ----------

fig(r'''<svg viewBox="0 0 900 400" role="img" aria-label="A Spark-computed offline Delta feature table is published through a serverless sync pipeline into the Lakebase online store; a FeatureSpec serves it through a Feature Serving endpoint the chatbot's lookup tool calls via the MLflow deploy client.">
<defs>
<marker id="a3" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker>
<marker id="a3l" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--lakebase)"/></marker>
</defs>
<text class="d-lbl" x="16" y="24">BUILD  &#9656;  offline &#8594; Lakebase</text>
<rect class="d-box" x="16" y="40" width="176" height="66" rx="9"/>
<text class="d-tb" x="104" y="68" text-anchor="middle">Capital-markets data</text>
<text class="d-ts" x="104" y="86" text-anchor="middle">Spark DataFrame</text>
<rect class="d-accent" x="248" y="40" width="176" height="66" rx="9"/>
<text class="d-tb" x="336" y="64" text-anchor="middle">client_risk_features</text>
<text class="d-ts" x="336" y="81" text-anchor="middle">Delta &#183; UC &#183; PK + CDF</text>
<rect class="d-box" x="480" y="40" width="176" height="66" rx="9"/>
<text class="d-tb" x="568" y="64" text-anchor="middle">Sync pipeline</text>
<text class="d-ts" x="568" y="81" text-anchor="middle">serverless Lakeflow</text>
<rect class="d-lake" x="712" y="34" width="172" height="150" rx="10"/>
<text class="d-lbl" x="798" y="54" text-anchor="middle" style="fill:var(--lakebase)">&#9670; LAKEBASE</text>
<text class="d-tb" x="798" y="98" text-anchor="middle">Online store</text>
<text class="d-ts" x="798" y="116" text-anchor="middle">client_risk_</text>
<text class="d-ts" x="798" y="131" text-anchor="middle">features_online</text>
<text class="d-ts" x="798" y="158" text-anchor="middle">managed Postgres</text>
<line class="d-line" x1="192" y1="73" x2="246" y2="73" marker-end="url(#a3)"/>
<text class="d-lbl" x="219" y="66" text-anchor="middle">aggregate</text>
<line class="d-line" x1="424" y1="73" x2="478" y2="73" marker-end="url(#a3)"/>
<text class="d-lbl" x="451" y="66" text-anchor="middle">publish_table</text>
<line class="d-line-lake" x1="656" y1="73" x2="710" y2="73" marker-end="url(#a3l)"/>
<text class="d-lbl" x="683" y="66" text-anchor="middle" style="fill:var(--lakebase)">CDF sync</text>
<text class="d-lbl" x="16" y="248">SERVE  &#9656;  Lakebase &#8594; agent</text>
<path class="d-line-lake" d="M798,184 L798,214 L336,214 L336,262" marker-end="url(#a3l)"/>
<text class="d-lbl" x="560" y="208" text-anchor="middle" style="fill:var(--lakebase)">FeatureSpec &#8594; serves online features</text>
<rect class="d-accent" x="248" y="264" width="176" height="66" rx="9"/>
<text class="d-tb" x="336" y="288" text-anchor="middle">Feature Serving</text>
<text class="d-ts" x="336" y="305" text-anchor="middle">endpoint (Model Serving)</text>
<rect class="d-box" x="480" y="264" width="176" height="66" rx="9"/>
<text class="d-tb" x="568" y="288" text-anchor="middle">lookup_client_risk</text>
<text class="d-ts" x="568" y="305" text-anchor="middle">tool &#183; MLflow deploy client</text>
<rect class="d-box" x="700" y="264" width="184" height="66" rx="9"/>
<text class="d-tb" x="792" y="288" text-anchor="middle">Coverage Desk Assistant</text>
<text class="d-ts" x="792" y="305" text-anchor="middle">LangChain + ChatDatabricks</text>
<line class="d-line" x1="424" y1="297" x2="478" y2="297" marker-end="url(#a3)"/>
<text class="d-lbl" x="451" y="290" text-anchor="middle">predict()</text>
<line class="d-line" x1="700" y1="297" x2="658" y2="297" marker-end="url(#a3)"/>
<text class="d-lbl" x="679" y="290" text-anchor="middle">calls</text>
</svg>''', "The offline Delta table is published into Lakebase (amber) via a serverless sync pipeline; a FeatureSpec serves those online features through an endpoint the chatbot's tool calls with the MLflow deploy client.")

# COMMAND ----------

# MAGIC %md
# MAGIC > **⏳ Two long steps (expected, not a hang):** `publish_table` stands up the sync pipeline (~15–20
# MAGIC > min the first time) and the Feature Serving endpoint takes ~10–15 min to provision. Both are fixed
# MAGIC > overhead regardless of data size — kick them off and talk through the architecture above.
# MAGIC
# MAGIC **▶ Run the notebook** — `Feature_Store.py` first (creates the endpoint), then
# MAGIC `Coverage_Desk_Chatbot.py`. The heart of it:
# MAGIC
# MAGIC ```python
# MAGIC fe.create_table(name=FEATURE_TABLE, primary_keys=["client_id"], df=features)
# MAGIC fe.publish_table(online_store=fe.get_online_store(name=PROJECT_ID),
# MAGIC                  source_table_name=FEATURE_TABLE, online_table_name=ONLINE_TABLE)
# MAGIC fe.create_feature_spec(name=FEATURE_SPEC,
# MAGIC      features=[FeatureLookup(table_name=FEATURE_TABLE, lookup_key="client_id")])
# MAGIC w.serving_endpoints.create_and_wait(name=EP, config=EndpointCoreConfigInput(
# MAGIC      name=EP,   # required by the SDK — omitting it errors
# MAGIC      served_entities=[ServedEntityInput(entity_name=FEATURE_SPEC,
# MAGIC          workload_size="Small", scale_to_zero_enabled=True)]))
# MAGIC ```
# MAGIC
# MAGIC The chatbot's tool is the exact lookup — an MLflow call the LLM decides when to make:
# MAGIC
# MAGIC ```python
# MAGIC @tool
# MAGIC def lookup_client_risk(client: str) -> str:
# MAGIC     """Live risk features (exposure, PnL, volatility, risk tier)."""
# MAGIC     return get_deploy_client("databricks").predict(endpoint=EP,
# MAGIC         inputs={"dataframe_records":[{"client_id": resolve(client)}]})
# MAGIC ```
# MAGIC
# MAGIC **✨ Generate with Genie** — two prompts (full versions in `labs/04-online-feature-store/README.md`):
# MAGIC build the feature store + publish to Lakebase + FeatureSpec + serving endpoint; then a LangChain
# MAGIC assistant whose `@tool` calls that endpoint. Demo: *"What's Air Canada's risk profile and should I
# MAGIC worry about their oil exposure?"*
# MAGIC
# MAGIC > **Why Feature Serving (not a raw Postgres read):** the documented agent pattern is a Feature
# MAGIC > Serving endpoint queried with the MLflow deploy client — governed, versioned, monitored. So "tool
# MAGIC > vs MLflow API" is a false choice: the tool *is* the MLflow call.
# MAGIC >
# MAGIC > **✓ Check** — Air Canada is `risk_tier=HIGH`; the online table appears in Lakebase; the agent's
# MAGIC > trace shows `lookup_client_risk` firing and the answer reflects the large energy exposure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Give the assistant a memory &nbsp; <sub>LangGraph · PostgresSaver · Lakebase</sub>
# MAGIC
# MAGIC The assistant forgets each turn. Back it with a LangGraph **PostgresSaver** checkpointer pointed at
# MAGIC Lakebase and its conversation state becomes durable — surviving turns, new processes, and sessions —
# MAGIC because the memory lives in Postgres, not the process.

# COMMAND ----------

fig(r'''<svg viewBox="0 0 900 380" role="img" aria-label="A coverage officer messages a one-node LangGraph agent that invokes ChatDatabricks; the agent checkpoints its state each turn through a PostgresSaver into Lakebase, which holds the four checkpoint tables keyed by thread_id.">
<defs>
<marker id="a4" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker>
<marker id="a4l" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--lakebase)"/></marker>
</defs>
<rect class="d-box" x="20" y="40" width="150" height="60" rx="9"/>
<text class="d-tb" x="95" y="66" text-anchor="middle">Coverage officer</text>
<text class="d-ts" x="95" y="83" text-anchor="middle">chat turn</text>
<rect class="d-accent" x="240" y="34" width="180" height="72" rx="9"/>
<text class="d-tb" x="330" y="62" text-anchor="middle">LangGraph agent</text>
<text class="d-ts" x="330" y="80" text-anchor="middle">StateGraph &#183; 1 node</text>
<text class="d-ts" x="330" y="95" text-anchor="middle">compiled w/ checkpointer</text>
<rect class="d-box" x="490" y="40" width="170" height="60" rx="9"/>
<text class="d-tb" x="575" y="66" text-anchor="middle">ChatDatabricks</text>
<text class="d-ts" x="575" y="83" text-anchor="middle">pay-per-token LLM</text>
<line class="d-line" x1="170" y1="70" x2="238" y2="70" marker-end="url(#a4)"/>
<text class="d-lbl" x="204" y="63" text-anchor="middle">message</text>
<line class="d-line" x1="420" y1="70" x2="488" y2="70" marker-end="url(#a4)"/>
<text class="d-lbl" x="454" y="63" text-anchor="middle">invoke</text>
<line class="d-line" x1="488" y1="86" x2="422" y2="86" marker-end="url(#a4)"/>
<text class="d-lbl" x="454" y="99" text-anchor="middle">reply</text>
<line class="d-line-lake" x1="330" y1="106" x2="330" y2="150" marker-end="url(#a4l)"/>
<text class="d-lbl" x="342" y="132" style="fill:var(--lakebase)">checkpoint state each turn</text>
<rect class="d-accent" x="240" y="152" width="180" height="60" rx="9"/>
<text class="d-tb" x="330" y="178" text-anchor="middle">PostgresSaver</text>
<text class="d-ts" x="330" y="195" text-anchor="middle">LangGraph checkpointer</text>
<line class="d-line-lake" x1="420" y1="182" x2="486" y2="182" marker-end="url(#a4l)"/>
<text class="d-lbl" x="453" y="175" text-anchor="middle" style="fill:var(--lakebase)">writes</text>
<rect class="d-lake" x="488" y="140" width="392" height="200" rx="10"/>
<text class="d-lbl" x="508" y="162" style="fill:var(--lakebase)">&#9670; LAKEBASE &#183; managed Postgres</text>
<rect class="d-box" x="508" y="176" width="168" height="42" rx="7"/>
<text class="d-t" x="592" y="202" text-anchor="middle">checkpoints</text>
<rect class="d-box" x="692" y="176" width="168" height="42" rx="7"/>
<text class="d-t" x="776" y="202" text-anchor="middle">checkpoint_writes</text>
<rect class="d-box" x="508" y="228" width="168" height="42" rx="7"/>
<text class="d-t" x="592" y="254" text-anchor="middle">checkpoint_blobs</text>
<rect class="d-box" x="692" y="228" width="168" height="42" rx="7"/>
<text class="d-t" x="776" y="254" text-anchor="middle">checkpoint_migrations</text>
<text class="d-ts" x="684" y="298" text-anchor="middle">the messages live in checkpoint_blobs</text>
<text class="d-ts" x="684" y="320" text-anchor="middle" style="fill:var(--lakebase)">memory keyed by thread_id — survives turns &amp; sessions</text>
</svg>''', "Each turn the agent writes its state through PostgresSaver into Lakebase (amber). Reuse the thread_id — even from a new process tomorrow — and the assistant picks up where it left off.")

# COMMAND ----------

# MAGIC %md
# MAGIC **▶ Run the notebook** — `labs/05-agentic-memory/Agent_Memory.py`. Two Lakebase gotchas are baked in
# MAGIC — connect with **keyword args (not a URL)**, and subclass `ChatDatabricks` to drop `temperature`
# MAGIC (Claude endpoints reject it):
# MAGIC
# MAGIC ```python
# MAGIC conn = psycopg.connect(host=host, dbname="databricks_postgres",
# MAGIC     user=user_email, password=cred.token, sslmode="require",
# MAGIC     autocommit=True, options=f"-c search_path={PG_SCHEMA},public")
# MAGIC checkpointer = PostgresSaver(conn); checkpointer.setup()   # makes the 4 tables
# MAGIC agent = StateGraph(MessagesState)...compile(checkpointer=checkpointer)
# MAGIC ```
# MAGIC
# MAGIC **✨ Generate with Genie** (full prompt in the README): a LangGraph agent with persistent memory in
# MAGIC Lakebase — psycopg keyword args, `ChatDatabricks` subclassed to pop `temperature`, `PostgresSaver` +
# MAGIC `.setup()`, a one-node `StateGraph(MessagesState)`, proving memory across two turns on one
# MAGIC `thread_id` and inspecting the `checkpoint%` tables.
# MAGIC
# MAGIC > **✓ Check** — Turn 2 recalls what Turn 1 said on the same `thread_id`; the four `checkpoint%`
# MAGIC > tables exist; deserializing a blob finds the remembered client.
# MAGIC
# MAGIC *This completes the Coverage Desk Assistant arc. The last two exercises round-trip data between
# MAGIC Lakebase and the lakehouse.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Delta → Lakebase sync (reverse ETL) &nbsp; <sub>synced tables</sub>
# MAGIC
# MAGIC Take a lakehouse-curated **Delta** table (a nightly client reference / risk-limits table) and
# MAGIC continuously sync it into your branch as an operational `lb_*` table, so the trading app reads
# MAGIC fresh, governed data over Postgres.

# COMMAND ----------

fig(r'''<svg viewBox="0 0 880 200" role="img" aria-label="A Delta table in UC with a PK and change data feed is synced by a serverless pipeline into an lb-prefixed table on the branch.">
<defs><marker id="dl" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker>
<marker id="dll" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--lakebase)"/></marker></defs>
<rect class="d-accent" x="16" y="66" width="220" height="66" rx="9"/>
<text class="d-tb" x="126" y="90" text-anchor="middle">client_reference</text>
<text class="d-ts" x="126" y="108" text-anchor="middle">Delta &#183; UC &#183; PK + CDF</text>
<rect class="d-box" x="332" y="66" width="200" height="66" rx="9"/>
<text class="d-tb" x="432" y="90" text-anchor="middle">synced-table pipeline</text>
<text class="d-ts" x="432" y="108" text-anchor="middle">serverless &#183; SNAPSHOT/TRIGGERED/CONT.</text>
<rect class="d-lake" x="628" y="60" width="236" height="80" rx="10"/>
<text class="d-lbl" x="746" y="84" text-anchor="middle" style="fill:var(--lakebase)">&#9670; YOUR LAKEBASE BRANCH</text>
<text class="d-tb" x="746" y="112" text-anchor="middle">lb_client_reference</text>
<line class="d-line" x1="236" y1="99" x2="330" y2="99" marker-end="url(#dl)"/>
<text class="d-lbl" x="283" y="91" text-anchor="middle">sync</text>
<line class="d-line-lake" x1="532" y1="99" x2="626" y2="99" marker-end="url(#dll)"/>
<text class="d-lbl" x="579" y="91" text-anchor="middle" style="fill:var(--lakebase)">CDF</text>
</svg>''', "A synced table replicates the Delta source into your branch as lb_* via a serverless pipeline (CDF-driven, incremental). Update the Delta row → re-sync → it lands in Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC **▶ Run the notebook** — `labs/06-delta-to-lakebase-sync/Delta_To_Lakebase.py`: builds the Delta
# MAGIC source (PK + CDF), creates a Lakebase **synced table** into your branch, verifies the row count,
# MAGIC then updates a row and re-syncs. (~minutes to provision the sync pipeline the first time.)
# MAGIC
# MAGIC **✨ Generate with Genie** (full prompt in the README): create a Delta table with PK + CDF, then a
# MAGIC Lakebase synced table (`lb_client_reference`) into your branch; verify over psycopg, update a row
# MAGIC and re-sync. Explain SNAPSHOT / TRIGGERED / CONTINUOUS.
# MAGIC
# MAGIC > **✓ Check** — `lb_client_reference` exists on your branch with the same rows as the Delta source;
# MAGIC > a Delta update propagates on re-sync (Air Canada's limit → CAD 300,000,000).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lakebase → Delta (SCD Type 1 pipeline) &nbsp; <sub>CDC · Lakeflow · SCD1</sub>
# MAGIC
# MAGIC The round trip: the trading app mutates `lb_*` tables all day; the lakehouse needs the **current**
# MAGIC picture for risk analytics. Capture the changes and land them in Delta with a **Lakeflow (DLT) AUTO
# MAGIC CDC** flow applying **SCD Type 1** (latest value wins, no history).

# COMMAND ----------

fig(r'''<svg viewBox="0 0 880 210" role="img" aria-label="Changes to the lb-prefixed tables are extracted by watermark into an append-only bronze Delta table, then a Lakeflow DLT AUTO CDC flow applies SCD type 1 to produce current-state Delta tables.">
<defs><marker id="sc" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--muted)"/></marker>
<marker id="scl" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--lakebase)"/></marker></defs>
<rect class="d-lake" x="16" y="66" width="200" height="72" rx="10"/>
<text class="d-lbl" x="116" y="90" text-anchor="middle" style="fill:var(--lakebase)">&#9670; LAKEBASE BRANCH</text>
<text class="d-tb" x="116" y="112" text-anchor="middle">lb_positions &#183; lb_limits</text>
<text class="d-ts" x="116" y="128" text-anchor="middle">app mutations (updated_at)</text>
<rect class="d-box" x="304" y="66" width="184" height="72" rx="9"/>
<text class="d-tb" x="396" y="94" text-anchor="middle">bronze Delta</text>
<text class="d-ts" x="396" y="112" text-anchor="middle">append-only changes</text>
<text class="d-ts" x="396" y="127" text-anchor="middle">watermark extract</text>
<rect class="d-accent" x="576" y="60" width="288" height="84" rx="9"/>
<text class="d-tb" x="720" y="88" text-anchor="middle">Lakeflow (DLT) AUTO CDC</text>
<text class="d-ts" x="720" y="107" text-anchor="middle">stored_as_scd_type = 1</text>
<text class="d-ts" x="720" y="124" text-anchor="middle">&#8594; positions_current &#183; limits_current</text>
<line class="d-line-lake" x1="216" y1="102" x2="302" y2="102" marker-end="url(#scl)"/>
<text class="d-lbl" x="259" y="94" text-anchor="middle" style="fill:var(--lakebase)">extract</text>
<line class="d-line" x1="488" y1="102" x2="574" y2="102" marker-end="url(#sc)"/>
<text class="d-lbl" x="531" y="94" text-anchor="middle">AUTO CDC</text>
</svg>''', "Changed lb_* rows are extracted (by watermark) into append-only bronze Delta, then a DLT AUTO CDC flow applies SCD Type 1 — one current row per key — for analytics.")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Honest note:** Lakebase has no Delta-style change feed today, so this uses the GA pattern — a
# MAGIC > watermark/incremental extract of the `lb_*` tables into bronze, then DLT AUTO CDC for SCD1. (If
# MAGIC > your workspace gets native Lakebase→Delta CDC, swap the extract step.)
# MAGIC
# MAGIC **▶ Run the notebook + pipeline** — `labs/07-lakebase-cdf-to-scd1/Lakebase_CDF_To_SCD1.py` (seeds +
# MAGIC mutates `lb_*`, extracts to bronze, creates the Lakeflow pipeline from `scd1_pipeline.py`) and
# MAGIC confirm each key shows only its latest value.
# MAGIC
# MAGIC **✨ Generate with Genie** (full prompt in the README): extract changed `lb_*` rows by `updated_at`
# MAGIC watermark into append-only bronze Delta, then a Lakeflow DLT AUTO CDC flow with
# MAGIC `stored_as_scd_type=1` keyed by PK → current-state Delta; prove one latest row per key.
# MAGIC
# MAGIC > **✓ Check** — after mutating a position/limit, the SCD1 target has exactly one row per key showing
# MAGIC > the latest value (position 1 → net_qty 900).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **The hero thread:** Air Canada (`CL-AC`) runs through every exercise — its energy book is the OLTP
# MAGIC data (Ex2), the rows read over REST/JDBC (Ex3), the HIGH-risk features the assistant looks up (Ex4)
# MAGIC and remembers (Ex5), the limit raised in the sync (Ex6), and the position that changes in SCD1
# MAGIC (Ex7). One managed Postgres for OLTP data, feature serving, and agent memory.
# MAGIC
# MAGIC *Visual guide: `docs/attendee.html` · Facilitator setup: `docs/facilitator.html` · Data model:
# MAGIC `DATA_MODEL.md`.*
