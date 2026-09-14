# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Facilitator Setup — provision the workshop from a notebook
# MAGIC
# MAGIC Run the whole workshop provisioning **in Databricks**, with widgets and your notebook
# MAGIC identity (no CLI, no profile, nothing installed on a laptop). It is idempotent — safe to
# MAGIC re-run. It:
# MAGIC 1. Creates the shared **Lakebase project(s)** and waits for the production endpoint.
# MAGIC 2. Grants each attendee `CAN_USE` (control plane) + a Postgres OAuth login role (data
# MAGIC    plane — installs the `databricks_auth` extension).
# MAGIC 3. Forks a **branch per attendee** (`br · <username>`, own endpoint, 1 → `branch_max_cu` CU).
# MAGIC 4. (with a `uc_catalog`) creates the shared **Unity Catalog** catalog + a `cm_<user>` schema
# MAGIC    per attendee, **owned by that attendee**, and grants everyone `USE CATALOG`.
# MAGIC 5. (optionally, with a `warehouse_id`) grants `CAN USE` on a SQL warehouse — the workshop
# MAGIC    doesn't need one; the exercises run on serverless notebook compute + Lakebase's built-in
# MAGIC    editor.
# MAGIC
# MAGIC The chatbot + memory agent call a **system foundation-model endpoint** (pay-per-token Claude),
# MAGIC which workspace users can already query — **no per-user grant needed**, so this notebook sets
# MAGIC nothing for it.
# MAGIC
# MAGIC **Model:** all attendees share ONE Lakebase project and each gets their own **branch** (a
# MAGIC Git-like, copy-on-write, fully-isolated clone with its own compute endpoint). More attendees
# MAGIC than a project's branch limit? It **splits them across extra projects** (`<project>-2`, `-3`, …)
# MAGIC — see the `max_branches_per_project` widget.
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
dbutils.widgets.text("warehouse_id", "", "4 · SQL warehouse id (optional — not required)")
dbutils.widgets.text("branch_max_cu", "4", "5 · Max CU per attendee branch")
dbutils.widgets.text("max_branches_per_project", "18", "6 · Max branches per project (split past this)")
dbutils.widgets.text("grant_groups", "", "7 · Groups to grant (optional)")
dbutils.widgets.text("pg_version", "17", "8 · PostgreSQL version")
dbutils.widgets.text("app_name", "", "9 · Lab app name (optional)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "10 · Dry run (preview only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provisioning logic
# MAGIC Everything below runs with **your notebook identity** (ambient auth). You don't normally edit
# MAGIC this cell — fill the widgets above and run the final cell.

# COMMAND ----------

import math
import re
import time

READY = ("ACTIVE", "IDLE", "DEGRADED")
PG_DATABASE = "databricks_postgres"


def log(msg=""):
    print(msg, flush=True)


def _sanitize(email):
    name = email.split("@")[0]
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", name.lower())).strip("-")


# ── Project ───────────────────────────────────────────────────────────────────
def ensure_project(w, pid, pg_version, display_name, dry_run):
    from databricks.sdk.service.postgres import Project, ProjectSpec
    try:
        w.postgres.get_project(name=f"projects/{pid}")
        log(f"✓ Project exists: {pid}")
        return
    except Exception:
        pass
    if dry_run:
        log(f"[dry-run] would create project '{pid}' (PostgreSQL {pg_version})")
        return
    log(f"Creating project '{pid}' (PostgreSQL {pg_version}) ...")
    w.postgres.create_project(
        project=Project(spec=ProjectSpec(display_name=display_name, pg_version=pg_version)),
        project_id=pid,
    ).wait()
    log(f"✓ Project created: {pid}")


def wait_for_endpoint(w, pid, branch="production", max_attempts=90, delay=5):
    for _ in range(max_attempts):
        try:
            eps = list(w.postgres.list_endpoints(parent=f"projects/{pid}/branches/{branch}"))
            if eps:
                ep = w.postgres.get_endpoint(name=eps[0].name)
                if any(r in str(getattr(ep.status, "current_state", "")).upper() for r in READY):
                    return ep
        except Exception as e:
            log(f"  waiting for {pid}/{branch} endpoint ... ({e})")
        time.sleep(delay)
    raise TimeoutError(f"Endpoint for {pid}/{branch} still provisioning.")


def pg_connect(w, ep, me, retries=4):
    import psycopg
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    params = {"host": ep.status.hosts.host, "dbname": PG_DATABASE, "user": me,
              "password": cred.token, "sslmode": "require", "connect_timeout": 30}
    last = None
    for _ in range(max(1, retries)):
        try:
            return psycopg.connect(**params)
        except psycopg.OperationalError as e:
            last = e
            time.sleep(4)
    raise last


# ── Branch per attendee ───────────────────────────────────────────────────────
def ensure_branch(w, pid, branch_id, dry_run, max_cu=4):
    """Create branch_id off production (+ a primary READ_WRITE endpoint). Idempotent.

    The endpoint is provisioned with an autoscaling range (min 1 → max `max_cu`) so the
    Ex1 load/autoscale demo can actually show compute scaling. NOTE: the SDK does not read
    back the autoscaling limits (they surface as None), so confirm the range in the Lakebase
    UI (branch → endpoint → compute) if you need to verify it."""
    from databricks.sdk.service.postgres import (
        Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType,
    )
    full = f"projects/{pid}/branches/{branch_id}"
    try:
        w.postgres.get_branch(name=full)
        log(f"  ✓ branch exists: {branch_id}")
    except Exception:
        if dry_run:
            log(f"  [dry-run] would create branch '{branch_id}' off production")
        else:
            try:
                w.postgres.create_branch(
                    parent=f"projects/{pid}",
                    branch=Branch(spec=BranchSpec(
                        source_branch=f"projects/{pid}/branches/production",
                        no_expiry=True)),  # persist through the workshop; delete after (see guide)
                    branch_id=branch_id,
                ).wait()
                log(f"  ✓ branch created: {branch_id}")
            except Exception as e:
                log(f"  ⚠ could not create branch {branch_id}: {e}")
                return
    # primary endpoint on the branch
    if dry_run:
        log(f"  [dry-run] would create primary READ_WRITE endpoint on {branch_id}")
        return
    try:
        # replace_existing so we override the auto-created 1/1 endpoint with a 1→max_cu
        # range (branches otherwise inherit the project default of 1 CU → no scaling to show).
        w.postgres.create_endpoint(
            parent=full,
            endpoint=Endpoint(spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=1, autoscaling_limit_max_cu=max_cu)),
            endpoint_id="primary", replace_existing=True,
        ).wait()
        log(f"    ✓ endpoint on {branch_id} set to 1→{max_cu} CU (autoscaling)")
    except Exception as e:
        log(f"    ⚠ could not set endpoint on {branch_id} (raise max CU in the Lakebase UI): {e}")


# ── Grants ────────────────────────────────────────────────────────────────────
def grant_control_plane(w, pid, groups, users, app_sp, dry_run):
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    entries = []
    for g in groups:
        entries.append((g, "group", AccessControlRequest(
            group_name=g, permission_level=PermissionLevel.CAN_USE)))
    for u in users:
        entries.append((u, "user", AccessControlRequest(
            user_name=u, permission_level=PermissionLevel.CAN_USE)))
    if app_sp:
        entries.append((app_sp, "app SP", AccessControlRequest(
            service_principal_name=app_sp, permission_level=PermissionLevel.CAN_MANAGE)))
    if not entries:
        return
    if dry_run:
        for who, kind, _ in entries:
            log(f"  [dry-run] project ACL: {who} ({kind})")
        return
    try:
        w.permissions.update(request_object_type="database-projects",
                             request_object_id=pid, access_control_list=[e[2] for e in entries])
        for who, kind, _ in entries:
            log(f"  ✓ project ACL: {who} ({kind})")
    except Exception as e:
        log(f"  ⚠ batch project-ACL failed ({e}); one at a time ...")
        for who, kind, req in entries:
            try:
                w.permissions.update(request_object_type="database-projects",
                                     request_object_id=pid, access_control_list=[req])
                log(f"  ✓ project ACL: {who} ({kind})")
            except Exception as e2:
                log(f"  ⚠ {who} ({kind}) — set in Lakebase UI: {e2}")


def enable_data_plane(conn, groups, users, app_sp):
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
            conn.commit()
            log("  ✓ databricks_auth extension ready")
        except Exception as e:
            conn.rollback()
            log(f"  ⚠ CREATE EXTENSION databricks_auth failed: {e}")

        def make_role(identity, kind):
            try:
                cur.execute("SELECT databricks_create_role(%s, %s)", (identity, kind))
                conn.commit()
                log(f"  ✓ OAuth role: {identity} ({kind})")
            except Exception as e:
                conn.rollback()
                log(f"  ✓ role exists: {identity}" if "already exists" in str(e)
                    else f"  ⚠ role {identity}: {e}")

        for g in groups:
            make_role(g, "group")
        for u in users:
            make_role(u, "user")
        if app_sp:
            make_role(app_sp, "service_principal")


def ensure_uc(w, catalog, groups, users, dry_run):
    """Create the shared UC catalog + one schema per attendee (cm_<user>), and make each
    attendee the OWNER of their schema so they can create tables/objects in it. Grants
    USE CATALOG to every attendee (+ groups). All via the UC SDK — no SQL warehouse needed.

    Creating the catalog needs CREATE CATALOG on the metastore (metastore admin); if the
    facilitator lacks it, that's reported (not fatal) so someone can create the catalog once.
    """
    if not catalog:
        return
    from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType

    def grant(sec, full, principal, privs):
        if dry_run:
            log(f"  [dry-run] grant {[p.value for p in privs]} on {full} → {principal}")
            return
        try:
            w.grants.update(securable_type=sec.value, full_name=full,
                            changes=[PermissionsChange(principal=principal, add=list(privs))])
            log(f"  ✓ grant {[p.value for p in privs]} on {full} → {principal}")
        except Exception as e:
            log(f"  ⚠ grant on {full} → {principal}: {str(e)[:110]}")

    log("")
    log(f"### Unity Catalog: {catalog}")
    # 1) the shared catalog
    try:
        w.catalogs.get(catalog)
        log(f"✓ catalog exists: {catalog}")
    except Exception:
        if dry_run:
            log(f"[dry-run] would create catalog '{catalog}'")
        else:
            try:
                w.catalogs.create(name=catalog, comment="CIBC CM Lakebase workshop")
                log(f"✓ catalog created: {catalog}")
                log("  ⚠ NOTE: this catalog uses the metastore's DEFAULT managed storage, which Ex7")
                log("    Lakebase CDF REJECTS. For the full 8-exercise workshop, pre-create the catalog")
                log("    with an EXPLICIT external location instead and pass it as uc_catalog. See FACILITATOR.md.")
            except Exception as e:
                log(f"⚠ could not create catalog '{catalog}' — needs CREATE CATALOG / metastore admin: {str(e)[:150]}")
                log("  Have a metastore admin create it once, then re-run (schemas/grants will proceed).")
                return

    # 2) USE CATALOG for everyone (attendees + groups)
    for g in groups:
        grant(SecurableType.CATALOG, catalog, g, [Privilege.USE_CATALOG])
    for u in users:
        grant(SecurableType.CATALOG, catalog, u, [Privilege.USE_CATALOG])

    # 3) one schema per attendee, owned by them (owner can create any object in it)
    for u in users:
        sch = f"cm_{_sanitize(u).replace('-', '_')}"
        full = f"{catalog}.{sch}"
        try:
            w.schemas.get(full)
            log(f"  ✓ schema exists: {full}")
        except Exception:
            if dry_run:
                log(f"  [dry-run] would create schema {full} (owner {u})")
                continue
            try:
                w.schemas.create(name=sch, catalog_name=catalog)
                log(f"  ✓ schema created: {full}")
            except Exception as e:
                log(f"  ⚠ create schema {full}: {str(e)[:120]}")
                continue
        if dry_run:
            continue
        try:
            w.schemas.update(full_name=full, owner=u)
            log(f"    ✓ owner → {u}  (can create tables/objects in {full})")
        except Exception as e:
            # fallback if ownership transfer isn't allowed: grant explicit create privileges
            log(f"    ⚠ set owner failed ({str(e)[:70]}); granting create privileges instead")
            grant(SecurableType.SCHEMA, full, u,
                  [Privilege.USE_SCHEMA, Privilege.CREATE_TABLE, Privilege.CREATE_MATERIALIZED_VIEW,
                   Privilege.CREATE_VOLUME, Privilege.CREATE_FUNCTION, Privilege.MODIFY, Privilege.SELECT])


def _grant_object(w, obj_type, obj_id, level, groups, users, dry_run, label):
    """Additively grant `level` (a PermissionLevel) on a workspace object — a serving endpoint,
    a SQL warehouse, … — to attendees + groups via the permissions API. Idempotent: update
    MERGES the given ACLs onto whatever's already there (it does not replace)."""
    from databricks.sdk.service.iam import AccessControlRequest
    entries = [(g, "group", AccessControlRequest(group_name=g, permission_level=level)) for g in groups]
    entries += [(u, "user", AccessControlRequest(user_name=u, permission_level=level)) for u in users]
    if not entries:
        return
    if dry_run:
        for who, kind, _ in entries:
            log(f"  [dry-run] {label}: {level.value} → {who} ({kind})")
        return
    try:
        w.permissions.update(request_object_type=obj_type, request_object_id=str(obj_id),
                             access_control_list=[e[2] for e in entries])
        for who, kind, _ in entries:
            log(f"  ✓ {label}: {who} ({kind})")
    except Exception as e:
        log(f"  ⚠ batch {label} failed ({str(e)[:90]}); one at a time ...")
        for who, kind, req in entries:
            try:
                w.permissions.update(request_object_type=obj_type, request_object_id=str(obj_id),
                                     access_control_list=[req])
                log(f"  ✓ {label}: {who} ({kind})")
            except Exception as e2:
                log(f"  ⚠ {label} {who} — set in the UI: {str(e2)[:80]}")


def ensure_workspace_grants(w, warehouse_id, groups, users, dry_run):
    """Automate the workspace-object grants the exercises need — in practice there are almost none.
    The chatbot + memory agent call a **system foundation-model endpoint** (pay-per-token Claude),
    which workspace users can already query, so no per-user grant is set for it. A SQL warehouse is
    optional (the workshop uses serverless notebook compute + Lakebase's built-in editor), so it's
    granted only if you supply a warehouse_id. Everything else that gates the workshop is a
    workspace-level admin toggle or plain workspace access — not a per-user grant (see below)."""
    from databricks.sdk.service.iam import PermissionLevel

    log("")
    log("### Workspace object grants")
    # Optional SQL warehouse — CAN USE (NOT required by the workshop; only if you choose to use one)
    if warehouse_id:
        _grant_object(w, "sql/warehouses", warehouse_id, PermissionLevel.CAN_USE,
                      groups, users, dry_run, f"warehouse CAN_USE ({warehouse_id})")
    else:
        log("  • No per-user object grants needed. (Set warehouse_id only if you choose to use a")
        log("    SQL warehouse — the workshop doesn't require one.)")

    log("")
    log("Workspace settings to confirm (admin toggles / plain access — NOT per-user grants)")
    log("-" * 68)
    log("  • Serverless notebooks/jobs enabled — the exercises run on serverless notebook compute.")
    log("  • Foundation Model APIs + Model Serving enabled in this workspace/region. The Claude")
    log("    endpoint is a SYSTEM foundation model — workspace users can query it with no grant.")
    log("  • Attendees are workspace users (workspace-access). That access — plus the toggles above —")
    log("    is what lets them create the serverless DLT pipeline (Ex7) and Feature Serving endpoint")
    log("    (Ex4); there is NO separate 'create pipeline'/'create endpoint' entitlement to grant.")


def chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)] or [[]]


# ── orchestration ─────────────────────────────────────────────────────────────
def run(*, project_id="cibc-cm-workshop", pg_version="17", grant_users=None,
        grant_groups=None, max_branches_per_project=18, branch_max_cu=4, app_name=None,
        uc_catalog=None, warehouse_id=None, dry_run=False):
    """Run the full facilitator setup with the notebook's ambient identity (WorkspaceClient())."""
    from databricks.sdk import WorkspaceClient
    grant_users = list(grant_users or [])
    grant_groups = list(grant_groups or [])

    w = WorkspaceClient()
    try:
        me = w.current_user.me().user_name
    except Exception as e:
        log(f"✗ Could not authenticate (ambient notebook auth): {e}")
        return 1

    app_sp = None
    if app_name:
        try:
            app = w.apps.get(name=app_name)
            app_sp = (getattr(app, "effective_service_principal_client_id", None)
                      or app.service_principal_client_id)
        except Exception as e:
            log(f"⚠ app '{app_name}' lookup failed: {e}")

    attendees = grant_users
    maxb = max(1, max_branches_per_project)
    groups_chunks = chunk(attendees, maxb)
    n_projects = len(groups_chunks) if attendees else 1
    projects = ([(project_id, attendees)] if n_projects == 1
                else [(f"{project_id}-{i + 1}", c) for i, c in enumerate(groups_chunks)])

    log("=" * 68)
    log("  CIBC CM — LAKEBASE WORKSHOP FACILITATOR SETUP  (shared project · branch per attendee)")
    log("=" * 68)
    log(f"  Auth:        ambient (notebook/runtime)   Workspace: {(w.config.host or '').rstrip('/')}")
    log(f"  Run as:      {me}")
    log(f"  Attendees:   {len(attendees)}   Max branches/project: {maxb}   Projects: {n_projects}")
    log(f"  Dry run:     {dry_run}")
    log("=" * 68)
    if n_projects > 1:
        log(f"⚠ {len(attendees)} attendees exceeds {maxb}/project — splitting across "
            f"{n_projects} projects (each attendee sets SHARED_PROJECT_ID to THEIR project).")

    mapping = []  # (attendee, project, branch)
    for pid, chunk_users in projects:
        log("")
        log(f"### Project: {pid}   ({len(chunk_users)} attendees)")
        ensure_project(w, pid, pg_version, f"CIBC CM Workshop ({pid})", dry_run)
        grant_control_plane(w, pid, grant_groups, chunk_users, app_sp, dry_run)

        if not dry_run:
            ep = wait_for_endpoint(w, pid)
            log(f"  ✓ production endpoint ready ({ep.status.hosts.host})")
            conn = pg_connect(w, ep, me)
            try:
                enable_data_plane(conn, grant_groups, chunk_users, app_sp)
            finally:
                conn.close()

        log(f"  Creating {len(chunk_users)} attendee branch(es) ...")
        for u in chunk_users:
            b = _sanitize(u)
            ensure_branch(w, pid, b, dry_run, max_cu=branch_max_cu)
            mapping.append((u, pid, b))

    ensure_uc(w, uc_catalog, grant_groups, attendees, dry_run)
    ensure_workspace_grants(w, warehouse_id, grant_groups, attendees, dry_run)

    if mapping:
        log("")
        log("Attendee → project / branch  (share this with attendees)")
        log("-" * 68)
        for u, pid, b in mapping:
            log(f"  {u:<32} project={pid:<22} branch={b}")
        log("")
        log("Each attendee sets ONE value — SHARED_PROJECT_ID in labs/_setup.py (or export")
        log("LAKEBASE_SHARED_PROJECT_ID) — to THEIR project below:")
        for pid in sorted({m[1] for m in mapping}):
            who = ", ".join(m[0] for m in mapping if m[1] == pid)
            log(f"  SHARED_PROJECT_ID = \"{pid}\"")
            log(f"      → {who}")
        log("")
        log("labs/_setup.py derives each attendee's branch from their username automatically,")
        log("so the exercises connect to the attendee's OWN branch with no extra config.")
    log("")
    log("Full run-of-show + the optional extra-project step: docs/facilitator.html / FACILITATOR.md")
    return 0

# COMMAND ----------

# MAGIC %md
# MAGIC **Fill the widgets above** (at least `grant_users`), then run the cell below. Keep
# MAGIC `dry_run = true` to preview what it would do; set it to `false` to actually provision.

# COMMAND ----------

def _split(s):
    return [x.strip() for x in re.split(r"[,\s]+", s or "") if x.strip()]


g = dbutils.widgets.get
_users = _split(g("grant_users"))
if not _users:
    raise ValueError("Enter at least one attendee email in the 'grant_users' widget, then re-run.")

_rc = run(
    project_id=g("project_id").strip() or "cibc-cm-workshop",
    pg_version=g("pg_version").strip() or "17",
    grant_users=_users,
    grant_groups=_split(g("grant_groups")),
    max_branches_per_project=int(g("max_branches_per_project") or "18"),
    branch_max_cu=int(g("branch_max_cu") or "4"),
    app_name=(g("app_name").strip() or None),
    uc_catalog=(g("uc_catalog").strip() or None),
    warehouse_id=(g("warehouse_id").strip() or None),
    dry_run=(g("dry_run") == "true"),
)
print("\n(return code:", _rc, "— 0 = success)")