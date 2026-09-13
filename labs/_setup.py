# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup (shared)
# MAGIC This notebook is `%run` by each exercise notebook to provide the common
# MAGIC Lakebase connection helpers. **Do not run it directly.**
# MAGIC
# MAGIC It resolves the participant's Lakebase **project** and **schema**, and gives every
# MAGIC lab one tested way to connect (`get_connection()`), so the Genie-generated code in
# MAGIC each exercise stays identical across the room.

# COMMAND ----------

import os
import re
import time
import uuid

# NOTE: psycopg is imported lazily inside get_connection() — not every exercise that
# %runs this helper connects to Postgres (e.g. the Ex3 chatbot only calls a serving
# endpoint), so we don't force psycopg to be installed just to import the helper.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
user_email = w.current_user.me().user_name


def _sanitize(email):
    """username portion of an email → a safe identifier fragment (a-z0-9-)."""
    name = email.split("@")[0]
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", name.lower())).strip("-")


# ── Shared-project vs per-user mode ─────────────────────────────────────────
# The facilitator decides how projects are laid out (see scripts/facilitator_setup.py
# and FACILITATOR.md):
#   • SHARED  — everyone connects to ONE facilitator-created project, each in their
#     own per-user schema.  Set SHARED_PROJECT_ID (env var wins, else the constant).
#   • PER-USER — each participant gets their own project  cm-lab-<username>.
# The SCHEMA is always per-user (cm_<username>), so both modes stay isolated.
SHARED_PROJECT_ID = ""   # e.g. "cibc-cm-workshop-shared"  (facilitator sets this)

_shared_id = (os.environ.get("LAKEBASE_SHARED_PROJECT_ID", "").strip()
              or SHARED_PROJECT_ID).strip()
SHARED_MODE = bool(_shared_id)

PROJECT_ID = _shared_id if SHARED_MODE else f"cm-lab-{_sanitize(user_email)}"

# Each participant works on their OWN branch of the shared project — a Git-like,
# copy-on-write, fully-isolated clone with its own compute endpoint. The branch id is
# derived from your username; your facilitator created it (see the facilitator guide).
# Every helper below defaults to this branch, so the exercises run against YOUR branch.
USER_BRANCH = _sanitize(user_email)

PG_SCHEMA = f"cm_{_sanitize(user_email).replace('-', '_')}"

# The default Lakebase database is databricks_postgres (underscores). Note the SDK
# resource path uses hyphens for some names but the actual Postgres db is underscores.
PG_DATABASE = "databricks_postgres"

# An endpoint is usable once it leaves INIT. IDLE means it scaled to zero — the normal
# resting state; it wakes on the next connection, so IDLE is "ready", not "wait".
_READY_ENDPOINT_STATES = ("ACTIVE", "IDLE", "DEGRADED")


def get_endpoint(branch=None):
    """Return the SDK endpoint object for a branch (waits past provisioning).

    Defaults to your own branch (USER_BRANCH). Pass branch="production" to reach the
    shared main branch explicitly."""
    branch = branch or USER_BRANCH
    # A freshly-created branch endpoint (or one waking from scale-to-zero) can take a few
    # minutes to first become reachable; wait generously (~10 min) so the first exercise of
    # the day doesn't trip on a cold branch. Facilitators can pre-warm branches to avoid it.
    for attempt in range(120):
        try:
            eps = list(w.postgres.list_endpoints(
                parent=f"projects/{PROJECT_ID}/branches/{branch}"))
            if eps:
                ep = w.postgres.get_endpoint(name=eps[0].name)
                state = str(getattr(ep.status, "current_state", "")).upper()
                if any(r in state for r in _READY_ENDPOINT_STATES):
                    return ep
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(
        f"Endpoint for '{PROJECT_ID}/{branch}' is still provisioning after 10 minutes. "
        "Check the Lakebase UI (Compute → Database Instances / Lakebase)."
    )


def get_endpoint_name(branch=None):
    """The full endpoint resource name for a branch's primary endpoint (your branch by default)."""
    return get_endpoint(branch or USER_BRANCH).name


def get_connection(branch=None, set_search_path=True, connect_retries=3):
    """Connect to a Lakebase branch. Returns a psycopg connection with dict_row.

    Uses Databricks SDK OAuth (`generate_database_credential`) + sslmode=require — the
    recommended path for notebooks/apps that can mint short-lived tokens. The token is a
    JWT that rotates ~hourly, so mint one per connection (never cache it for a process).

    NOTE (a gotcha the labs rely on): connect with keyword arguments, NOT a URL/DSN
    string — OAuth tokens contain characters (`.`/`-`) that break URL parsing.
    """
    import psycopg
    from psycopg.rows import dict_row

    ep = get_endpoint(branch)
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    params = {
        "host": host,
        "dbname": PG_DATABASE,
        "user": user_email,           # your identity IS the Postgres role
        "password": cred.token,       # short-lived OAuth JWT (rotate per connection)
        "sslmode": "require",
        "connect_timeout": 15,
    }
    if set_search_path:
        params["options"] = f"-c search_path={PG_SCHEMA},public"
    last_err = None
    for _ in range(max(1, connect_retries)):
        try:
            conn = psycopg.connect(**params, row_factory=dict_row)
            with conn.cursor() as cur:                       # ensure the schema exists
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"')
            conn.commit()
            return conn
        except psycopg.OperationalError as e:                # scale-to-zero wake / net blip
            last_err = e
            time.sleep(3)
    raise last_err


WORKSPACE_HOST = w.config.host.rstrip("/") if w.config.host else ""


def _workspace_org_id():
    try:
        wid = w.get_workspace_id()
        if wid:
            return str(wid)
    except Exception:
        pass
    if WORKSPACE_HOST:
        m = re.search(r"adb-(\d+)\.", WORKSPACE_HOST)
        if m:
            return m.group(1)
    return None


_ORG_ID = _workspace_org_id()


def _with_org(url):
    if _ORG_ID:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}o={_ORG_ID}"
    return url


def uc_table_url(catalog, schema, table):
    """Catalog Explorer deep link for a Unity Catalog table."""
    if not WORKSPACE_HOST:
        return ""
    return _with_org(f"{WORKSPACE_HOST}/explore/data/{catalog}/{schema}/{table}")


def show_view_link(label, url):
    """Render a clickable 'View → ' banner; falls back to print outside a notebook."""
    if not url:
        return
    try:
        displayHTML(f"""
    <div style="padding:10px 16px;margin:8px 0;border-radius:8px;background:#e6f4ea;border:1px solid #a8dab5;display:flex;align-items:center;gap:12px;font-family:Inter,sans-serif">
      <div style="flex:1;color:#137333;font-weight:600">{label}</div>
      <a href="{url}" target="_blank" style="background:#137333;color:#fff;padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none">View →</a>
    </div>""")
    except Exception:
        print(f"{label}: {url}")


print(f"Mode:     {'SHARED project' if SHARED_MODE else 'per-user project'}")
print(f"Project:  {PROJECT_ID}")
print(f"Branch:   {USER_BRANCH}   (your isolated branch — all exercises use it)")
print(f"Schema:   {PG_SCHEMA}")
print(f"Database: {PG_DATABASE}")
print(f"User:     {user_email}")
