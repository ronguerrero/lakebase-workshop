# Exercise — Data APIs: how applications reach Lakebase (REST vs JDBC)

**Persona:** a CIBC Capital Markets trading app / dashboard needs the desk's data out of your
Lakebase branch — over **HTTP** from a serverless front end, or over a pooled **JDBC** connection
from a JVM service. This exercise shows both, and when to use each.

**Prerequisite:** your Lakebase branch (Ex1–2). Runs standalone — if your branch has no `clients`
table, the notebook creates a small demo one. Both paths hit **your own branch** (resolved by
`_setup`).

## What you'll learn
- **Data API (REST)** — reach Lakebase over HTTP with a **bearer token**, JSON rows back. No Postgres
  driver, works from any language, ideal for serverless/edge front ends and browsers. It's
  **PostgREST-compatible**: `{API_URL}/{schema}/{table}?select=…&limit=…` — the **schema is in the
  path**, no `Accept-Profile` header.
- **JDBC** — a persistent, **pooled** Postgres connection with the standard PostgreSQL driver.
  Lowest per-query latency for chatty JVM services; full SQL, transactions, prepared statements.
- The **two paths use different tokens** (a real gotcha): the Data API bearer is your **Databricks
  workspace OAuth token** (session identity), while JDBC (Postgres wire protocol) uses the short-lived
  **OAuth database credential** from `generate_database_credential`. Either way your Databricks
  identity maps to your Postgres role, so grants are honored on both paths.

> **Facilitator, once per project:** the Data API must be **enabled** on the project's **Data API**
> page (**Enable Data API** — creates the `authenticator` role), and each attendee's schema must be
> **exposed** by adding it to the Data API's `db_schemas`. The base URL then comes from
> `w.postgres.get_data_api(...).status.url` (the notebook auto-discovers it). The Data API is GA in
> current Lakebase; on older workspaces the management endpoints may be absent — the notebook degrades
> gracefully and the JDBC path always works.

---

## Run it

### Notebook (primary)
Open `Data_API.py` on **serverless** and **Run all**. It reads a few `clients` rows from your branch
two ways: Part A over REST (JSON), Part B over JDBC (via Spark's JDBC reader, which uses the same
driver + URL a JVM app would). The reference notebook is the version we review together.

### Generate with Genie (optional)
Fresh Genie Code chat on a serverless notebook, paste verbatim:

```
Create a Databricks notebook (Python, serverless) that shows two ways an application reads my
Lakebase branch — a REST data API and JDBC — and compares them. Requirements:
- First cell: %pip install "psycopg[binary]>=3.1.0" "requests>=2.31"; then restartPython; then
  %run ../_setup (it provides w, user_email, PROJECT_ID, USER_BRANCH, PG_SCHEMA, PG_DATABASE,
  get_connection(), get_endpoint()). Everything targets MY branch (the _setup defaults).
- Ensure data exists: with get_connection(), if PG_SCHEMA.clients is missing, create a small demo
  clients table (client_id, ticker, legal_name, sector, coverage_officer) with ~5 CIBC issuers.
- Part A (REST): use the Lakebase Data API (PostgREST-compatible). Discover its base URL from
  w.postgres.get_data_api(name=f"projects/{PROJECT_ID}/dataApi").status.url (fall back to a rest_base
  widget if that management call isn't available). Use my WORKSPACE OAuth token as the Bearer
  (w.config.authenticate()["Authorization"]) — NOT the database credential. GET
  {API_URL}/{my schema}/clients?select=client_id,legal_name&limit=5 (schema is IN THE PATH, no
  Accept-Profile header). Wrap the call so the notebook continues if the Data API isn't enabled/exposed,
  and parse and print the JSON.
- Part B (JDBC): build the JDBC URL jdbc:postgresql://<branch host>:5432/databricks_postgres?
  sslmode=require, print user + driver org.postgresql.Driver, and demonstrate it via
  spark.read.format("jdbc") reading PG_SCHEMA.clients with the OAuth token as the password. Note
  HikariCP pooling for JVM services and installing the org.postgresql:postgresql driver if absent.
- End with a markdown table comparing REST vs JDBC and a rule of thumb for when to use each.
```

---

## REST vs JDBC — when to use which

| | **REST data API** | **JDBC** |
|---|---|---|
| Transport | HTTP, JSON | Postgres wire protocol |
| Client needs | any HTTP client | Postgres JDBC driver + pool |
| Connection | stateless, per request | persistent, pooled (HikariCP) |
| Auth | workspace OAuth token as Bearer, per request | DB credential (`generate_database_credential`) as the connection password |
| Best for | serverless/edge front ends, browsers, any language, quick reads/writes | chatty JVM services, lowest latency, full SQL, transactions |
| Latency | HTTP overhead per call | lowest once the pool is warm |

**Rule of thumb:** a browser dashboard or serverless function → **REST**; a long-running Spring Boot /
Kafka / Spark service doing many queries → **JDBC** with a connection pool. Both hit the same branch
and obey the same grants.

## ✓ Validation
- REST returned JSON rows (or you saw the response body and adjusted `REST_BASE` to your workspace).
- The JDBC URL for your branch printed, and the Spark JDBC read returned your `clients` rows.
- You can say which path a browser dashboard vs a JVM service should use, and why.

## Docs
- Lakebase **Data API** (PostgREST-compatible), enablement, schema exposure, tokens:
  <https://docs.databricks.com/aws/en/oltp/projects/data-api>
- Lakebase (managed Postgres) — connecting apps: <https://docs.databricks.com/aws/en/oltp/>
- PostgreSQL JDBC driver: `org.postgresql:postgresql` (Maven).

## What's next
- **Lakebase Sync** — mirror a Delta table into Lakebase for low-latency serving.
- **CDF → Delta** — stream Lakebase changes out and shape them into SCD1 tables.
