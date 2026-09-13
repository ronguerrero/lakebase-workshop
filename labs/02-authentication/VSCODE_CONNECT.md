# Part B — Connect VS Code to your Lakebase project

Goal: connect a normal desktop SQL tool (**VS Code**) to the same Lakebase project you loaded in
Part A, and read the coverage desk's data. This is the workflow a developer or analyst uses day to
day — no notebook required.

> **Why this is different from the notebook.** In the notebook you minted a rotating **OAuth
> database credential**. An external tool can't refresh that ~1-hour JWT on its own, so for VS Code
> we use a **native Postgres password role** created from the Lakebase **Connect** dialog. It behaves
> like any Postgres login (host / port / db / user / password + SSL).

---

## 1. Install a PostgreSQL client for VS Code

Pick one:

- **SQLTools + SQLTools PostgreSQL/Redshift Driver** (recommended — richest UI). Install both
  extensions from the VS Code Marketplace:
  - `mtxr.sqltools`
  - `mtxr.sqltools-driver-pg`
- **or** the **PostgreSQL** extension by Chris Kolkman (`ckolkman.vscode-postgres`).

If you'll also connect from **Python** in VS Code, install the driver there too:

```bash
pip install "psycopg[binary]>=3.1.0"
```

---

## 2. Get your connection details

You need five things. Get them from the **Lakebase UI**:

1. In the Databricks workspace, go to **Compute → Database Instances** (Lakebase) and open the
   shared workshop project.
2. Open the **Connect** (a.k.a. **App Connect** / **Connection details**) dialog on **your own
   branch's** endpoint (`br · <username>`) — the same branch every exercise uses.

| Field | Value |
|---|---|
| **Host** | the endpoint host from the Connect dialog, e.g. `instance-xxxx.database.cloud.databricks.com` |
| **Port** | `5432` |
| **Database** | `databricks_postgres` |
| **User** | see step 3 below |
| **Password** | see step 3 below |
| **SSL mode** | `require` (Lakebase requires TLS) |

> Prefer the CLI? You can read the host from the SDK:
> ```python
> from databricks.sdk import WorkspaceClient
> w = WorkspaceClient(profile="lakebase")
> # use YOUR branch (your username, e.g. sofia-martins) — not production:
> ep = w.postgres.get_endpoint(name="projects/<project-id>/branches/<your-username>/endpoints/primary")
> print(ep.status.hosts.host)
> ```

---

## 3. Create a native password role (the User + Password)

In the **Connect** dialog, choose **Create / manage a Postgres role with a password** (native
password credential). Lakebase creates a role and shows you a **username** and a **one-time
password** — copy both now. Use those as the **User** and **Password** in VS Code.

- This role is a normal Postgres login, so VS Code can reconnect without minting tokens.
- Grant it access to your schema if needed. Connected as yourself in the notebook (or psql), run:
  ```sql
  GRANT USAGE ON SCHEMA cm_<username> TO "<the_native_role>";
  GRANT SELECT ON ALL TABLES IN SCHEMA cm_<username> TO "<the_native_role>";
  ```
  (Replace `cm_<username>` with your schema — it's printed by `_setup` as `PG_SCHEMA`.)

> **Alternative (advanced):** you *can* use your email as the user and a freshly-minted OAuth token
> as the password (`databricks ... generate-database-credential`), but it expires in ~1 hour, so
> you'd re-paste it every session. The native password role is the practical choice for VS Code.

---

## 4. Create the connection in VS Code

**SQLTools:** open the SQLTools sidebar → **Add New Connection** → **PostgreSQL**, then fill in:

- Connection name: `CIBC CM Lakebase`
- Server/Host: *(your host)*
- Port: `5432`
- Database: `databricks_postgres`
- Username: *(native role)*
- Password: *(native password)* — choose "Save as plaintext in settings" or use the secure prompt
- **SSL:** enabled → set `sslmode` = `require` (in SQLTools, set **PG SSL** to `Enabled`, or add
  `"pgOptions": { "ssl": true }` in the connection's advanced settings)

Click **Test Connection**, then **Save**.

**PostgreSQL (Kolkman) extension:** run **PostgreSQL: Add Connection** from the Command Palette and
answer the same host/port/db/user/password prompts; select **secure (SSL)** when asked.

---

## 5. Read the data (hands-on)

Open a new SQL file in VS Code, connect it to `CIBC CM Lakebase`, and run:

```sql
-- your 9 issuers
SELECT * FROM cm_<username>.clients ORDER BY client_id LIMIT 10;

-- positions joined to the client master, with the covering officer
SELECT c.legal_name, c.coverage_officer, p.instrument_id, p.book, p.net_qty
FROM cm_<username>.positions p
JOIN cm_<username>.clients c ON c.client_id = p.client_id
ORDER BY c.legal_name, p.instrument_id
LIMIT 25;

-- Air Canada's book (the hero exposure)
SELECT p.instrument_id, i.description, i.asset_class, p.net_qty
FROM cm_<username>.positions p
JOIN cm_<username>.instruments i ON i.instrument_id = p.instrument_id
WHERE p.client_id = 'CL-AC'
ORDER BY p.instrument_id;
```

*(Replace `cm_<username>` with your schema — `_setup` prints it as `PG_SCHEMA`. If you set
`search_path` on the connection you can drop the schema prefix.)*

## ✓ Validation (Part B)
- VS Code shows a successful, SSL-secured connection and lists your `cm_<username>` schema and its
  five tables.
- The `clients` query returns your 9 Canadian issuers.
- The `positions ⋈ clients` join runs; Air Canada's book shows the large WTI + heating-oil longs.

---

**Back to:** [Exercise 2 README](./README.md) &nbsp;·&nbsp; **Next:** Exercise 3 — Data APIs (REST vs JDBC)
