# Databricks notebook source
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

# MAGIC %pip install "psycopg[binary]>=3.1.0" "requests>=2.31" --quiet

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
# MAGIC Two facts that make this "just work" (both learned the hard way — see the notes):
# MAGIC 1. **The base URL comes from the project's Data API config** (`get_data_api().status.url`), *not*
# MAGIC    hand-built from the Postgres host. The request path is `{url}/{schema}/{table}?select=…` —
# MAGIC    **the schema is in the path** (PostgREST convention), no `Accept-Profile` header.
# MAGIC 2. **The bearer is a Databricks workspace OAuth token** (your notebook session identity), **not**
# MAGIC    the Postgres DB credential you use for psycopg/JDBC. The Data API maps your Databricks
# MAGIC    identity to your Postgres role via an internal `authenticator` role.
# MAGIC
# MAGIC > **Prerequisites (facilitator, once per project):** on the project's **Data API** page click
# MAGIC > **Enable Data API** (creates the `authenticator` role), and **expose your schema** by adding it
# MAGIC > to the Data API's `db_schemas`. The Data API is GA in current Lakebase but the management
# MAGIC > endpoints may not be present on older workspaces — this cell **degrades gracefully** and the
# MAGIC > JDBC path below always works. Docs: https://docs.databricks.com/aws/en/oltp/projects/data-api

# COMMAND ----------

import json
import requests

# Optional override: paste the "API URL" from the project's Data API page if get_data_api()
# isn't available on your workspace. Leave blank to auto-discover it from the Data API config.
dbutils.widgets.text("rest_base", "", "Data API base URL (blank = auto-discover)")
REST_BASE = dbutils.widgets.get("rest_base").strip().rstrip("/")

# The Data API bearer is the WORKSPACE OAuth token (session identity) — NOT the DB credential.
# w.config.authenticate() returns the right Authorization header for however this notebook is authed.
def _bearer_token():
    hdr = w.config.authenticate() or {}
    return hdr.get("Authorization", "").split(" ", 1)[-1]

exposed_schemas = None
if not REST_BASE:
    try:
        da = w.postgres.get_data_api(name=f"projects/{PROJECT_ID}/dataApi")
        REST_BASE = (getattr(da.status, "url", "") or "").rstrip("/")
        exposed_schemas = getattr(da.status, "available_schemas", None) or getattr(da.status, "db_schemas", None)
        print(f"✓ Data API discovered: {REST_BASE}")
        print(f"  exposed schemas: {exposed_schemas}")
    except Exception as e:
        print(f"Data API config not available on this workspace ({str(e)[:120]}).")
        print("→ Enable it on the project's Data API page and expose your schema, or paste its URL")
        print("  into the 'rest_base' widget. Skipping the live REST call; JDBC below still runs.")

REST_TABLE = "clients"
if REST_BASE:
    if exposed_schemas is not None and PG_SCHEMA not in (exposed_schemas or []):
        print(f"⚠ Your schema '{PG_SCHEMA}' isn't in the Data API's exposed schemas {exposed_schemas}.")
        print(f"  Add it to the Data API 'db_schemas' (Data API page) or the call will 404/400.")
    url = f"{REST_BASE}/{PG_SCHEMA}/{REST_TABLE}"        # schema is IN THE PATH
    headers = {"Authorization": f"Bearer {_bearer_token()}", "Accept": "application/json"}
    params = {"select": "client_id,legal_name,sector,coverage_officer", "limit": "5"}
    print("GET", url, params)
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        print("HTTP", resp.status_code)
        if resp.ok:
            rows = resp.json()
            print(f"✓ REST returned {len(rows)} JSON rows:")
            print(json.dumps(rows, indent=2)[:1200])
        else:
            print("Response body (check the Data API is enabled + your schema is exposed):")
            print(resp.text[:800])
    except Exception as e:
        print(f"REST call did not complete: {e}")
        print("The JDBC path below does not depend on the Data API and works regardless.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened:** the app authenticated with a workspace OAuth bearer token (no driver,
# MAGIC no connection to manage) and got rows as JSON. A React dashboard, a Lambda/edge function, or a
# MAGIC Python microservice would call it exactly the same way. Auth is **per-request** — refresh the
# MAGIC OAuth token when it rotates (~hourly).

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
