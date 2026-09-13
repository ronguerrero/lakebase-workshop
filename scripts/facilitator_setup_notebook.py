# Databricks notebook source
# MAGIC %md
# MAGIC # Facilitator Setup — run the workshop provisioning from a notebook
# MAGIC
# MAGIC Same logic as the `scripts/facilitator_setup.py` CLI, but runnable **in Databricks** with
# MAGIC widgets and your notebook identity (no CLI profile needed). It:
# MAGIC 1. Creates the shared **Lakebase project** and waits for its production endpoint.
# MAGIC 2. Grants each attendee `CAN_USE` + a Postgres OAuth login role (installs `databricks_auth`).
# MAGIC 3. Forks a **branch per attendee** (`br · <username>`, own endpoint, 1 → `branch_max_cu` CU).
# MAGIC 4. (with a `uc_catalog`) creates the shared **Unity Catalog** catalog + a `cm_<user>` schema per
# MAGIC    attendee, **owned by that attendee**, and grants everyone `USE CATALOG`.
# MAGIC
# MAGIC **Run it:** *Run all* once to render the widgets, fill them in (at least **grant_users**), then
# MAGIC re-run the last cell (or *Run all* again). Leave `dry_run = true` for a no-op preview first.
# MAGIC
# MAGIC **Prereqs:** you're a **workspace admin**; creating the UC catalog needs `CREATE CATALOG` /
# MAGIC metastore admin (reported, not fatal, if missing).

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.118.0" "psycopg[binary]>=3.1.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("project_id", "cibc-cm-workshop", "1 · Lakebase project id (base)")
dbutils.widgets.text("grant_users", "", "2 · Attendee emails (comma/space separated)")
dbutils.widgets.text("uc_catalog", "main", "3 · Unity Catalog catalog (blank = skip UC)")
dbutils.widgets.text("branch_max_cu", "4", "4 · Max CU per attendee branch")
dbutils.widgets.text("max_branches_per_project", "18", "5 · Max branches per project (split past this)")
dbutils.widgets.text("grant_groups", "", "6 · Groups to grant (optional)")
dbutils.widgets.text("pg_version", "17", "7 · PostgreSQL version")
dbutils.widgets.text("app_name", "", "8 · Lab app name (optional)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "9 · Dry run (preview only)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fill the widgets above** (at least `grant_users`), then run the cell below. Keep
# MAGIC `dry_run = true` to preview what it would do; set it to `false` to actually provision.

# COMMAND ----------

import base64
import os
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

# Load the shared logic from the sibling scripts/facilitator_setup by exporting its source and
# exec-ing it — this works whether the repo was cloned as a Git folder (a .py file) or imported
# with `workspace import-dir` (a notebook object), and needs no sys.path juggling.
_w = WorkspaceClient()
_base = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
_bases = [_base] + ([f"/Workspace{_base}"] if not _base.startswith("/Workspace") else [])
_cands = [f"{b}/facilitator_setup{ext}" for b in _bases for ext in ("", ".py")]
_src = None
for _cand in _cands:
    try:
        _src = base64.b64decode(_w.workspace.export(_cand, format=ExportFormat.SOURCE).content).decode()
        break
    except Exception:
        continue
if _src is None:
    raise RuntimeError("Could not find scripts/facilitator_setup next to this notebook — keep the "
                       "two files together in the same folder.")
_fs = {"__name__": "facilitator_setup_module"}      # not '__main__', so its main() guard won't fire
exec(compile(_src, "facilitator_setup.py", "exec"), _fs)


def _split(s):
    return [x.strip() for x in re.split(r"[,\s]+", s or "") if x.strip()]


g = dbutils.widgets.get
users = _split(g("grant_users"))
if not users:
    raise ValueError("Enter at least one attendee email in the 'grant_users' widget, then re-run.")

rc = _fs["run"](
    profile=None,                                   # ambient notebook auth (your identity)
    project_id=g("project_id").strip() or "cibc-cm-workshop",
    pg_version=g("pg_version").strip() or "17",
    grant_users=users,
    grant_groups=_split(g("grant_groups")),
    max_branches_per_project=int(g("max_branches_per_project") or "18"),
    branch_max_cu=int(g("branch_max_cu") or "4"),
    app_name=(g("app_name").strip() or None),
    uc_catalog=(g("uc_catalog").strip() or None),
    dry_run=(g("dry_run") == "true"),
)
print("\n(return code:", rc, "— 0 = success)")
