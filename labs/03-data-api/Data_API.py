# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Data APIs — how applications reach Lakebase (REST vs JDBC)
# MAGIC
# MAGIC **Path:** Data APIs &nbsp;|&nbsp; **Prerequisite:** your Lakebase branch (Ex1–2). Runs standalone.
# MAGIC
# MAGIC A trading app or dashboard needs the desk's data. Two ways to get it out of your
# MAGIC Lakebase branch:
# MAGIC 1. **REST** — HTTP + a bearer token, JSON rows back. Great for serverless / edge
# MAGIC    front ends and any language, no driver to ship.
# MAGIC 2. **JDBC** — a pooled, persistent Postgres connection. Lowest latency for chatty
# MAGIC    JVM services, full SQL + driver features.
# MAGIC
# MAGIC This notebook is the **reference solution** — what the Genie Code prompt in
# MAGIC `README.md` should produce. Both paths hit **your own branch** (resolved by `_setup`).

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1.0" "requests>=2.31" "databricks-sdk>=0.118.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Make sure there's something to read
# MAGIC
# MAGIC If you did Exercise 2, your branch already has `clients`. If not, we create a tiny
# MAGIC capital-markets demo table so this lab runs standalone. Either way it lives in your
# MAGIC schema on **your branch**.

# COMMAND ----------

conn = get_connection()   # defaults to YOUR branch, search_path = your schema
with conn.cursor() as cur:
    cur.execute("""
        SELECT to_regclass(%s) IS NOT NULL AS has_clients
    """, (f"{PG_SCHEMA}.clients",))
    has_clients = cur.fetchone()["has_clients"]

    if not has_clients:
        print("No clients table found — creating a small demo table.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id        TEXT PRIMARY KEY,
                ticker           TEXT NOT NULL,
                legal_name       TEXT NOT NULL,
                sector           TEXT NOT NULL,
                coverage_officer TEXT NOT NULL
            );
            TRUNCATE clients;
            INSERT INTO clients VALUES
              ('CL-AC','AC.TO','Air Canada','Airlines','Sofia Martins'),
              ('CL-SU','SU.TO','Suncor Energy','Energy','Priya Nair'),
              ('CL-BCE','BCE.TO','BCE Inc.','Telecom','David Chen'),
              ('CL-CNR','CNR.TO','Canadian National Railway','Industrials','Priya Nair'),
              ('CL-MFC','MFC.TO','Manulife Financial','Financials','David Chen');
        """)
        conn.commit()
    cur.execute("SELECT COUNT(*) AS n FROM clients")
    print(f"✓ clients rows available on your branch: {cur.fetchone()['n']}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — REST: reach Lakebase over the Data API
# MAGIC
# MAGIC Lakebase has a built-in **Data API** — a **PostgREST-compatible** REST surface — so an app can
# MAGIC read/write rows over HTTP with a **bearer token** and get **JSON** back: no Postgres driver,
# MAGIC works from any language, ideal for serverless/edge front ends and browsers.
# MAGIC
# MAGIC Three things must line up for a REST read to succeed (all learned the hard way — the
# MAGIC **caveats cell** right after this one gives the exact fixes):
# MAGIC 1. **Base URL** — copy the **API URL** from the project's **Data API** page. The request path is
# MAGIC    `{url}/{schema}/{table}?select=…` — **the schema goes in the path** (PostgREST convention), no
# MAGIC    `Accept-Profile` header. Auto-discovery via `get_data_api()` works on some workspaces but
# MAGIC    **404s on others**, so pasting the URL into the `rest_base` widget is the reliable path.
# MAGIC 2. **Bearer token** — must be the **Lakebase-minted** OAuth token from
# MAGIC    `generate_database_credential` (the same short-lived DB credential psycopg/JDBC use), **not**
# MAGIC    `w.config.authenticate()`. A workspace/account token is rejected with
# MAGIC    `PGRST301 invalid token permissions`.
# MAGIC 3. **Your schema is exposed and your role is granted** — the facilitator adds `cm_<you>` to the
# MAGIC    Data API's exposed schemas **and** grants your Postgres role to the internal `authenticator`
# MAGIC    role. Miss either and you get `PGRST106` (schema not exposed) or `42501 permission denied to
# MAGIC    set role`.
# MAGIC
# MAGIC > If the Data API isn't fully set up yet, this cell **degrades gracefully** and prints the precise
# MAGIC > fix, and the **JDBC path (Part B) always works** regardless — it authenticates directly as your
# MAGIC > role and never touches `authenticator`. Docs: https://docs.databricks.com/aws/en/oltp/projects/data-api

# COMMAND ----------

import json
import time
import requests

# ►► Paste the "API URL" from your project's Data API page into this widget. Auto-discovery via
# get_data_api() runs only when it's blank — and that management endpoint 404s on some workspaces —
# so the widget is the reliable path. The URL looks like:
#   https://<endpoint-host>/api/2.0/workspace/<workspace_id>/rest/databricks_postgres
dbutils.widgets.text("rest_base", "", "Data API base URL (paste from the Data API page)")
REST_BASE = dbutils.widgets.get("rest_base").strip().rstrip("/")

# GATE 2: the Data API bearer must be the LAKEBASE-MINTED OAuth token (generate_database_credential),
# NOT w.config.authenticate(). PostgREST validates this token and maps it to your Postgres role; a
# workspace/account token is rejected with PGRST301 "invalid token permissions". Mint fresh per call
# (it rotates ~hourly).
ep = get_endpoint()                                    # your branch's primary endpoint
def _bearer_token():
    return w.postgres.generate_database_credential(endpoint=ep.name).token

exposed_schemas = None
if not REST_BASE:
    try:
        da = w.postgres.get_data_api(name=f"projects/{PROJECT_ID}/dataApi")
        REST_BASE = (getattr(da.status, "url", "") or "").rstrip("/")
        exposed_schemas = getattr(da.status, "available_schemas", None) or getattr(da.status, "db_schemas", None)
        print(f"✓ Data API auto-discovered: {REST_BASE}")
        print(f"  exposed schemas: {exposed_schemas}")
    except Exception as e:
        print(f"Auto-discovery unavailable on this workspace ({str(e)[:100]}).")
        print("→ Open the project's Data API page, copy its API URL, paste it into the 'rest_base'")
        print("  widget above, and re-run. (The JDBC path in Part B works regardless.)")


def _diagnose(resp):
    """Map the common Data API failures to the exact fix — see the caveats cell below."""
    body = resp.text[:500]
    if '"PGRST106"' in body:
        print(f"→ GATE 3a: schema '{PG_SCHEMA}' is NOT exposed. Facilitator: add it to the Data API's")
        print("  exposed schemas (db_schemas) on the project's Data API page.")
    elif '"PGRST301"' in body:
        print("→ GATE 2: wrong bearer token — must be generate_database_credential (Lakebase-minted),")
        print("  not w.config.authenticate(). This notebook already uses the right one.")
    elif '"42501"' in body or "set role" in body:
        print("→ GATE 3b: your Postgres role isn't granted to 'authenticator', so PostgREST can't")
        print(f'  SET ROLE to you. Facilitator (as the role\'s creator): GRANT "{user_email}" TO')
        print("  authenticator;  — see the caveats cell for why a plain GRANT can fail.")
    elif '"PGRST205"' in body:
        print("→ Schema-cache lag: the table was created recently; PostgREST re-introspects every")
        print("  ~15-20s. Re-run in a moment.")


REST_TABLE = "clients"
if REST_BASE:
    if exposed_schemas is not None and PG_SCHEMA not in (exposed_schemas or []):
        print(f"⚠ Your schema '{PG_SCHEMA}' isn't in the exposed schemas {exposed_schemas} — see caveats.")
    url = f"{REST_BASE}/{PG_SCHEMA}/{REST_TABLE}"        # schema is IN THE PATH (PostgREST convention)
    params = {"select": "client_id,legal_name,sector,coverage_officer", "limit": "5"}
    print("GET", url, params)
    for attempt in range(4):                             # PGRST205 (schema cache) can lag ~15-20s
        try:
            resp = requests.get(url, timeout=20, params=params,
                                headers={"Authorization": f"Bearer {_bearer_token()}",
                                         "Accept": "application/json"})
        except Exception as e:
            print(f"REST call did not complete: {e}")
            print("The JDBC path below does not depend on the Data API and works regardless.")
            break
        print("HTTP", resp.status_code)
        if resp.ok:
            rows = resp.json()
            print(f"✓ REST returned {len(rows)} JSON rows:")
            print(json.dumps(rows, indent=2)[:1200])
            break
        if '"PGRST205"' in resp.text and attempt < 3:
            print("  schema cache warming up — retrying in 8s ...")
            time.sleep(8)
            continue
        print("Response body:", resp.text[:600])
        _diagnose(resp)
        break

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened:** the app authenticated with a short-lived **Lakebase-minted** OAuth bearer
# MAGIC token (no driver, no connection to manage) and got rows back as JSON. A React dashboard, a
# MAGIC Lambda/edge function, or a Python microservice would call it exactly the same way. Auth is
# MAGIC **per-request** — mint a fresh token when it rotates (~hourly).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Caveats — how to actually make the REST Data API work
# MAGIC
# MAGIC The REST path needs **three gates** to line up. **JDBC (Part B) needs none of them** — it
# MAGIC connects directly as your role — so if REST is blocked, the workshop still proceeds on JDBC.
# MAGIC
# MAGIC | Gate | Symptom if missing | Who fixes it |
# MAGIC |---|---|---|
# MAGIC | **1. Base URL** | auto-discovery 404s / no REST call is made | You — paste the API URL from the Data API page into the `rest_base` widget |
# MAGIC | **2. Lakebase-minted token** | `PGRST301 invalid token permissions` | Already handled — this notebook uses `generate_database_credential`, not `w.config.authenticate()` |
# MAGIC | **3a. Schema exposed** | `PGRST106 … Invalid schema` | Facilitator — add `cm_<you>` to the Data API's exposed schemas (`db_schemas`) |
# MAGIC | **3b. Role granted to `authenticator`** | `42501 permission denied to set role` | Facilitator — grant your role to `authenticator` (recipe below) |
# MAGIC
# MAGIC **Why gate 3b is subtle.** PostgREST connects as an `authenticator` role and runs
# MAGIC `SET ROLE "<you>"` on **every** request, so `authenticator` must be a *member* of your role. A
# MAGIC plain `GRANT "<you>" TO authenticator` only works when the grantor **created your role themselves**
# MAGIC via `databricks_create_role` (the creator automatically gets `ADMIN OPTION` on it). A role
# MAGIC auto-provisioned by the control plane has **no ADMIN holder**, so the grant is refused for everyone
# MAGIC except a Databricks platform superuser.
# MAGIC
# MAGIC **Facilitator recipe** — run in the SQL Editor on the branch that serves the Data API, *after*
# MAGIC the Data API is enabled (which creates `authenticator`):
# MAGIC ```sql
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC SELECT databricks_create_role('attendee@corp.com', 'USER');  -- you become ADMIN of this role
# MAGIC GRANT "attendee@corp.com" TO authenticator;                  -- now succeeds
# MAGIC GRANT USAGE ON SCHEMA cm_attendee TO "attendee@corp.com";    -- (attendee already owns their schema)
# MAGIC ```
# MAGIC If the login role **already exists** (control-plane pre-provisioned), `databricks_create_role`
# MAGIC no-ops and you won't hold `ADMIN` — that attendee then needs a Databricks superuser, or the Data
# MAGIC API page's principal-access control, to issue the `authenticator` grant. Full detail in
# MAGIC `FACILITATOR.md`.
# MAGIC
# MAGIC > **Bottom line for the room:** REST is a great "any-language, no-driver" story to *show*, but its
# MAGIC > per-user provisioning is fiddly. If it isn't wired up, demo REST from the facilitator's set-up
# MAGIC > account and have everyone do the hands-on part over **JDBC**, which just works.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — JDBC: a pooled Postgres connection
# MAGIC
# MAGIC A JVM service (Spring Boot, a Kafka consumer, a Spark job) connects over **JDBC** with
# MAGIC the standard **PostgreSQL driver** (`org.postgresql.Driver`) and a connection **pool**
# MAGIC (HikariCP). Persistent pooled connections give the lowest per-query latency for
# MAGIC chatty services and full SQL/driver features.
# MAGIC
# MAGIC The JDBC URL points at **your branch's** endpoint host; the password is the same
# MAGIC OAuth database credential.

# COMMAND ----------

# Unlike the Data API, JDBC connects over the Postgres wire protocol, so its password is the
# short-lived OAuth DB credential from generate_database_credential (rotate ~hourly).
ep = get_endpoint()                                   # your branch's primary endpoint
host = ep.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=ep.name)

# The JDBC URL a JVM app would use (password = the OAuth DB credential, rotate on refresh):
jdbc_url = f"jdbc:postgresql://{host}:5432/{PG_DATABASE}?sslmode=require"
print("JDBC URL:", jdbc_url)
print("user    :", user_email)
print("driver  : org.postgresql.Driver   (pool with HikariCP in a JVM service)")

# A notebook is Python, so we demonstrate the JDBC path via Spark's JDBC reader — it uses
# the very same driver + URL under the hood.
try:
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", f"{PG_SCHEMA}.clients")
          .option("user", user_email)
          .option("password", cred.token)
          .option("driver", "org.postgresql.Driver")
          .load())
    print(f"✓ JDBC read returned {df.count()} rows")
    display(df)
except Exception as e:
    print(f"JDBC read did not complete: {e}")
    print("If the PostgreSQL JDBC driver isn't on the cluster, install the Maven coordinate")
    print("  org.postgresql:postgresql:42.7.4  (Compute → Libraries), or run on DBR that bundles it.")
    print("The URL/credentials above are still exactly what a JVM/JDBC client would use.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use which
# MAGIC
# MAGIC | | **REST data API** | **JDBC** |
# MAGIC |---|---|---|
# MAGIC | Transport | HTTP, JSON | Postgres wire protocol |
# MAGIC | Client needs | just an HTTP client | Postgres JDBC driver + pool |
# MAGIC | Connection | stateless, per-request | persistent, pooled |
# MAGIC | Auth | bearer token per request | token as connection password |
# MAGIC | Best for | serverless/edge front ends, browsers, any language, quick reads/writes | chatty JVM services, lowest latency, full SQL, transactions/prepared statements |
# MAGIC | Latency | HTTP overhead per call | lowest once the pool is warm |
# MAGIC
# MAGIC **Rule of thumb:** a front end or a serverless function → **REST**; a long-running
# MAGIC JVM/service doing many queries → **JDBC** with a pool. Both hit the same branch and
# MAGIC honor the same Unity Catalog / Postgres grants.
# MAGIC
# MAGIC ### ✓ Checks
# MAGIC - REST returned JSON rows (or you saw the response body to adjust `REST_BASE`).
# MAGIC - The JDBC URL for your branch is printed, and the Spark JDBC read returned your rows.
# MAGIC - You can say which path a browser dashboard vs a Spring Boot service should use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next
# MAGIC - **Lakebase Sync** — keep a Delta table mirrored into Lakebase for low-latency serving.
# MAGIC - **CDF → Delta** — stream Lakebase changes back out and shape them into SCD1 tables.