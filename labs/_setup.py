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


# ── The shared Lakebase project ─────────────────────────────────────────────
# All participants connect to ONE facilitator-created Lakebase project and each works
# on their OWN branch of it (see scripts/facilitator_setup.py and FACILITATOR.md).
#
# ►► ATTENDEE: SET THIS ONE LINE ◄◄
# Set SHARED_PROJECT_ID to your facilitator's project id (e.g. "cibc-cm-workshop"). Edit it
# once in your OWN clone — no commit/push and no repo access needed — and EVERY exercise
# inherits it, because they all `%run ../_setup`. Enter the BASE id even if the room was
# split across projects; you're auto-routed to the project that actually holds your branch.
# (Alternatively export LAKEBASE_SHARED_PROJECT_ID, which wins over the constant.)
# If it's unset or wrong, the helpers fail fast with a clear message — no 10-minute hang.
SHARED_PROJECT_ID = ""   # ◄◄ set me, e.g. "cibc-cm-workshop"

PROJECT_ID = (os.environ.get("LAKEBASE_SHARED_PROJECT_ID", "").strip()
              or SHARED_PROJECT_ID).strip()

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


_SETUP_HINT = (
    "Set SHARED_PROJECT_ID in labs/_setup (the '◄◄ set me' line) to your facilitator's Lakebase "
    "project id — one line, once, in your clone — or export LAKEBASE_SHARED_PROJECT_ID. Enter the "
    "base id even if the room was split; you'll be routed to the project that holds your branch. "
    "See docs/facilitator.html / FACILITATOR.md."
)


def _validate_target(branch):
    """Fail FAST (one API call each) if the project or branch can't be resolved, instead of
    looping for 10 minutes on a project/branch that doesn't exist (the classic 'empty
    SHARED_PROJECT_ID' hang)."""
    if not PROJECT_ID:
        raise RuntimeError("No Lakebase project is configured. " + _SETUP_HINT)
    try:
        w.postgres.get_project(name=f"projects/{PROJECT_ID}")
    except Exception as e:
        raise RuntimeError(
            f"Lakebase project '{PROJECT_ID}' was not found ({str(e)[:150]}). " + _SETUP_HINT
        ) from e
    try:
        w.postgres.get_branch(name=f"projects/{PROJECT_ID}/branches/{branch}")
    except Exception as e:
        raise RuntimeError(
            f"Branch '{branch}' does not exist in project '{PROJECT_ID}' or its split siblings "
            f"({str(e)[:120]}). If the facilitator split attendees across multiple projects "
            "(--max-branches-per-project), set LAKEBASE_SHARED_PROJECT_ID to YOUR project from "
            "their attendee→project map. Otherwise ask them to create your branch: "
            f"python scripts/facilitator_setup.py --project-id {PROJECT_ID} --grant-user {user_email}"
        ) from e


def get_endpoint(branch=None):
    """Return the SDK endpoint object for a branch (waits past provisioning).

    Defaults to your own branch (USER_BRANCH). Pass branch="production" to reach the
    shared main branch explicitly. Fails fast with a clear message if the project/branch
    can't be resolved (rather than hanging on a nonexistent project)."""
    branch = branch or USER_BRANCH
    _validate_target(branch)   # fast, clear error on misconfig — no 10-minute hang
    # The project + branch exist; the endpoint may still be provisioning (a freshly-created
    # branch, or one waking from scale-to-zero) — wait generously (~10 min) for that only.
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
        f"Branch '{PROJECT_ID}/{branch}' exists but its endpoint is still provisioning after "
        "10 minutes. Check the Lakebase UI (Compute → Database Instances / Lakebase)."
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


# ── Multi-project routing ───────────────────────────────────────────────────
# If the room is large, the facilitator splits attendees across several projects
# (`<base>-1`, `<base>-2`, … via --max-branches-per-project). Your branch lives in exactly
# one of them. So an attendee can point SHARED_PROJECT_ID / LAKEBASE_SHARED_PROJECT_ID at
# EITHER their specific project OR just the base id, and we route to the project that
# actually holds YOUR branch.
def _project_family(pid):
    root = re.sub(r"-\d+$", "", pid)                       # strip a trailing -N
    fam = [pid, root] + [f"{root}-{i}" for i in range(1, 21)]
    seen, out = set(), []
    for p in fam:
        if p and p not in seen:
            seen.add(p); out.append(p)
    return out


def _resolve_effective_project():
    """Point PROJECT_ID at the split project that actually holds USER_BRANCH (if the
    configured id doesn't). Best-effort — never breaks `%run ../_setup`."""
    global PROJECT_ID
    if not PROJECT_ID:
        return
    for cand in _project_family(PROJECT_ID):
        try:
            w.postgres.get_branch(name=f"projects/{cand}/branches/{USER_BRANCH}")
        except Exception:
            continue
        if cand != PROJECT_ID:
            print(f"↪ Your branch '{USER_BRANCH}' is on project '{cand}' (attendees were split "
                  f"across projects) — using '{cand}' instead of '{PROJECT_ID}'.")
        PROJECT_ID = cand
        return
    # branch not found in the family — leave PROJECT_ID as configured; get_endpoint() explains.

try:
    _resolve_effective_project()
except Exception:
    pass


if PROJECT_ID:
    print(f"Project:  {PROJECT_ID}")
else:
    print("Project:  ⚠ NOT SET — " + _SETUP_HINT)
print(f"Branch:   {USER_BRANCH}   (your isolated branch — all exercises use it)")
print(f"Schema:   {PG_SCHEMA}")
print(f"Database: {PG_DATABASE}")
print(f"User:     {user_email}")
