#!/usr/bin/env python3
"""Facilitator setup for the CIBC Capital Markets Lakebase Workshop — repeatable.

Model: **all attendees share ONE Lakebase project, and each gets their own BRANCH**
(a Git-like, copy-on-write, fully-isolated clone with its own compute endpoint). The
exercises connect to the attendee's own branch (labs/_setup.py resolves it from their
username). If you have more attendees than a single project's branch limit allows, this
script automatically **splits them across additional projects** (`<project>-2`, `-3`, …)
— see --max-branches-per-project.

What it does (idempotent — safe to re-run):
  1. Create the shared project(s) if missing; wait for the production endpoint.
  2. (control plane) Grant attendees/groups CAN_USE on each project (+ the Lab app SP
     CAN_MANAGE if --app-name given).
  3. (data plane, on production) CREATE EXTENSION databricks_auth; create an OAuth login
     role for each attendee/group (+ the app SP).
  4. Create a **branch per attendee** off production, each with a primary READ_WRITE
     endpoint. Prints the attendee → project/branch map + the env var each attendee sets.

Pass --uc-catalog to also create the shared **Unity Catalog** catalog + one schema per
attendee (cm_<user>), owned by each attendee so they can create tables/objects in it, and
grant everyone USE CATALOG (all via the UC SDK — no SQL warehouse). Model-serving grants
still can't be set here and are printed for you to apply.

Usage:
  python scripts/facilitator_setup.py -p lakebase \
      --project-id cibc-cm-workshop \
      --grant-user sofia@cibc.com --grant-user david@cibc.com --uc-catalog main
  # large room — split across projects at 18 branches each:
  python scripts/facilitator_setup.py -p lakebase --project-id cibc-cm-workshop \
      --grant-user a@x --grant-user b@x ... --max-branches-per-project 18
  python scripts/facilitator_setup.py -p lakebase --dry-run

Requires: databricks-sdk>=0.81.0, psycopg[binary]>=3.0, and a Databricks CLI profile.
"""
from __future__ import annotations

import argparse
import math
import re
import sys
import time

READY = ("ACTIVE", "IDLE", "DEGRADED")
PG_DATABASE = "databricks_postgres"


def log(msg: str = "") -> None:
    print(msg, flush=True)


def _sanitize(email: str) -> str:
    name = email.split("@")[0]
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", name.lower())).strip("-")


# ── Project ─────────────────────────────────────────────────────────────────
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


# ── Branch per attendee ─────────────────────────────────────────────────────
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


# ── Grants ──────────────────────────────────────────────────────────────────
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


def print_workspace_grants(uc_catalog):
    log("")
    log("Manual grants to finish (workspace/UC — not settable from this script)")
    log("-" * 68)
    log("  • CAN USE on a serverless SQL warehouse; serverless notebooks/jobs enabled.")
    if uc_catalog:
        log(f"  • UC: catalog `{uc_catalog}` + a per-attendee schema `cm_<user>` (owned by each attendee)")
        log("       were created above — nothing to apply by hand.")
    else:
        log("  • UC: pass --uc-catalog <name> to auto-create the shared catalog + a per-attendee schema.")
    log("  • Model Serving: CAN QUERY on a Claude FM endpoint + create-serving-endpoint (feature store / memory).")


def chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)] or [[]]


# ── orchestration (callable from the CLI or a Databricks notebook) ──────────
def run(*, profile=None, project_id="cibc-cm-workshop", pg_version="17", grant_users=None,
        grant_groups=None, max_branches_per_project=18, branch_max_cu=4, app_name=None,
        uc_catalog=None, dry_run=False):
    """Run the full facilitator setup. Called by the CLI (main) and by the notebook
    scripts/facilitator_setup_notebook. profile=None → ambient auth (WorkspaceClient()), which is
    what a Databricks notebook / runtime uses; pass a CLI profile name to read ~/.databrickscfg."""
    import types
    args = types.SimpleNamespace(
        profile=profile, project_id=project_id, pg_version=pg_version,
        grant_user=list(grant_users or []), grant_group=list(grant_groups or []),
        max_branches_per_project=max_branches_per_project, branch_max_cu=branch_max_cu,
        app_name=app_name, uc_catalog=uc_catalog, dry_run=bool(dry_run))

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    w = (WorkspaceClient(config=Config(profile=args.profile, http_timeout_seconds=120))
         if args.profile else WorkspaceClient())
    try:
        me = w.current_user.me().user_name
    except Exception as e:
        log(f"✗ Could not authenticate ({'profile ' + args.profile if args.profile else 'ambient auth'}): {e}")
        return 1

    app_sp = None
    if args.app_name:
        try:
            app = w.apps.get(name=args.app_name)
            app_sp = (getattr(app, "effective_service_principal_client_id", None)
                      or app.service_principal_client_id)
        except Exception as e:
            log(f"⚠ app '{args.app_name}' lookup failed: {e}")

    attendees = args.grant_user
    maxb = max(1, args.max_branches_per_project)
    groups_chunks = chunk(attendees, maxb)
    n_projects = len(groups_chunks) if attendees else 1
    projects = ([(args.project_id, attendees)] if n_projects == 1
                else [(f"{args.project_id}-{i + 1}", c) for i, c in enumerate(groups_chunks)])

    log("=" * 68)
    log("  CIBC CM — LAKEBASE WORKSHOP FACILITATOR SETUP  (shared project · branch per attendee)")
    log("=" * 68)
    log(f"  Auth:        {'profile ' + args.profile if args.profile else 'ambient (notebook/runtime)'}   Workspace: {(w.config.host or '').rstrip('/')}")
    log(f"  Run as:      {me}")
    log(f"  Attendees:   {len(attendees)}   Max branches/project: {maxb}   Projects: {n_projects}")
    log(f"  Dry run:     {args.dry_run}")
    log("=" * 68)
    if n_projects > 1:
        log(f"⚠ {len(attendees)} attendees exceeds {maxb}/project — splitting across "
            f"{n_projects} projects (each attendee sets LAKEBASE_SHARED_PROJECT_ID to THEIR project).")

    mapping = []  # (attendee, project, branch)
    for pid, chunk_users in projects:
        log("")
        log(f"### Project: {pid}   ({len(chunk_users)} attendees)")
        ensure_project(w, pid, args.pg_version, f"CIBC CM Workshop ({pid})", args.dry_run)
        grant_control_plane(w, pid, args.grant_group, chunk_users, app_sp, args.dry_run)

        if not args.dry_run:
            ep = wait_for_endpoint(w, pid)
            log(f"  ✓ production endpoint ready ({ep.status.hosts.host})")
            conn = pg_connect(w, ep, me)
            try:
                enable_data_plane(conn, args.grant_group, chunk_users, app_sp)
            finally:
                conn.close()

        log(f"  Creating {len(chunk_users)} attendee branch(es) ...")
        for u in chunk_users:
            b = _sanitize(u)
            ensure_branch(w, pid, b, args.dry_run, max_cu=args.branch_max_cu)
            mapping.append((u, pid, b))

    ensure_uc(w, args.uc_catalog, args.grant_group, attendees, args.dry_run)
    print_workspace_grants(args.uc_catalog)

    if mapping:
        log("")
        log("Attendee → project / branch  (share this with attendees)")
        log("-" * 68)
        for u, pid, b in mapping:
            log(f"  {u:<32} project={pid:<22} branch={b}")
        log("")
        log("Each attendee sets ONE env var (or the SHARED_PROJECT_ID constant in labs/_setup.py):")
        for pid in sorted({m[1] for m in mapping}):
            who = ", ".join(m[0] for m in mapping if m[1] == pid)
            log(f"  LAKEBASE_SHARED_PROJECT_ID={pid}")
            log(f"      → {who}")
        log("")
        log("labs/_setup.py derives each attendee's branch from their username automatically,")
        log("so the exercises connect to the attendee's OWN branch with no extra config.")
    log("")
    log("Full run-of-show + the optional extra-project step: docs/facilitator.html / FACILITATOR.md")
    return 0


# ── CLI ─────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="Facilitator setup — shared project, branch per attendee.")
    ap.add_argument("-p", "--profile", default=None, help="CLI profile; omit to use ambient auth")
    ap.add_argument("--project-id", default="cibc-cm-workshop", help="Base shared-project id")
    ap.add_argument("--pg-version", default="17")
    ap.add_argument("--grant-user", action="append", default=[], metavar="EMAIL",
                    help="Attendee (repeatable) — one branch is created per attendee")
    ap.add_argument("--grant-group", action="append", default=[], metavar="GROUP",
                    help="Group granted project access (branches still need --grant-user)")
    ap.add_argument("--max-branches-per-project", type=int, default=18,
                    help="Split attendees across extra projects past this many branches/project")
    ap.add_argument("--branch-max-cu", type=int, default=4,
                    help="Max autoscaling CU per attendee branch endpoint (min is 1) — lets the Ex1 load demo show scaling")
    ap.add_argument("--app-name", default=None)
    ap.add_argument("--uc-catalog", default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    return run(profile=a.profile, project_id=a.project_id, pg_version=a.pg_version,
               grant_users=a.grant_user, grant_groups=a.grant_group,
               max_branches_per_project=a.max_branches_per_project, branch_max_cu=a.branch_max_cu,
               app_name=a.app_name, uc_catalog=a.uc_catalog, dry_run=a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
