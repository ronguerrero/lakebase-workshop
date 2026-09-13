# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1 · Step 6 — Watch your branch autoscale under load
# MAGIC
# MAGIC **Scenario:** *market open.* A burst of order and pricing queries hits your book. Lakebase
# MAGIC autoscaling should grow your branch's compute from its **minimum** toward its **maximum** to
# MAGIC absorb the load — then scale back down (and eventually **to zero**) once things go quiet, so you
# MAGIC **pay only for what you use**.
# MAGIC
# MAGIC In this step you'll:
# MAGIC 1. Read your branch endpoint's autoscaling envelope (min / max CU).
# MAGIC 2. Fire a concurrent query load at it for a few minutes.
# MAGIC 3. Watch the compute scale up (in the **Lakebase Monitoring** graph), then settle.
# MAGIC
# MAGIC > This runs against **your own branch** — the isolated one the facilitator created for you.
# MAGIC > It's self-contained (seeds its own table), so it works even before Exercise 2.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase autoscaling, in one minute
# MAGIC
# MAGIC A branch's compute endpoint has an **autoscaling envelope** measured in **CU** (compute units):
# MAGIC an `autoscaling_limit_min_cu` and an `autoscaling_limit_max_cu`. Under load the endpoint scales
# MAGIC up toward `max`; when idle it scales down toward `min`, and a non-production branch **suspends to
# MAGIC zero** after its idle timeout, waking automatically on the next connection.
# MAGIC
# MAGIC For a capital-markets desk that means: your book's operational store rides out the 9:30 open
# MAGIC without you pre-provisioning peak capacity, and costs almost nothing overnight.
# MAGIC
# MAGIC > **You need headroom to *see* scaling.** If your branch endpoint has `min == max` (e.g. 1/1),
# MAGIC > there's nothing to scale into. The facilitator provisions attendee branches with
# MAGIC > `max_cu > min_cu` (e.g. min 1, max 4) so this demo is visible.

# COMMAND ----------

dbutils.widgets.text("workers", "16", "Concurrent load workers")
dbutils.widgets.text("duration_sec", "240", "Load duration (seconds)")
WORKERS = int(dbutils.widgets.get("workers") or "16")
DURATION = int(dbutils.widgets.get("duration_sec") or "240")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect your branch endpoint's scaling envelope

# COMMAND ----------

ep = get_endpoint()                       # defaults to YOUR branch (USER_BRANCH)
st = ep.status
MIN_CU = getattr(st, "autoscaling_limit_min_cu", None)
MAX_CU = getattr(st, "autoscaling_limit_max_cu", None)
# The read/write POOLED host is the right target for many concurrent connections.
hosts = st.hosts
POOLED_HOST = getattr(hosts, "read_write_pooled_host", None) or hosts.host
DIRECT_HOST = hosts.host

print(f"Branch:        {USER_BRANCH}")
print(f"Endpoint:      {ep.name.split('/')[-1]}   state={getattr(st,'current_state',None)}")
print(f"Autoscaling:   min={MIN_CU} CU  →  max={MAX_CU} CU")
print(f"Pooled host:   {POOLED_HOST}")
print(f"Last active:   {getattr(st,'last_active_time',None)}")

if MIN_CU is not None and MAX_CU is not None and float(MIN_CU) >= float(MAX_CU):
    print("\n⚠ min_cu == max_cu — there is no room to scale, so this demo won't show a curve.")
    print("  Ask your facilitator to raise this branch endpoint's max_cu (e.g. to 4).")
else:
    print(f"\n✓ Room to scale: {MIN_CU} → {MAX_CU} CU. Load will push it toward {MAX_CU}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Open the Monitoring graph now (this is where you'll SEE the scaling)
# MAGIC The SDK reports the endpoint **state** (ACTIVE / IDLE) and its min/max envelope, but the live
# MAGIC **CU curve** is in the UI. In another tab open **Lakebase → your project → your branch →
# MAGIC Monitoring** and watch **Compute / CU** while the load runs.

# COMMAND ----------

print("Open:  Lakebase  →  project  " + PROJECT_ID +
      "  →  branch  " + USER_BRANCH + "  →  Monitoring, and watch Compute / CU while the load runs.")
# 📸 Optional screenshot: docs/images/ex1-autoscale-monitoring.png — the CU graph rising under load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed a small table on your branch to query against
# MAGIC Self-contained, so this step runs even if you haven't done Exercise 2 yet.

# COMMAND ----------

conn = get_connection()   # your branch
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS load_orders (
            order_id    bigint,
            client_id   text,
            instrument  text,
            qty         numeric,
            px          numeric,
            ts          timestamptz DEFAULT now()
        )
    """)
    cur.execute("SELECT count(*) AS n FROM load_orders")
    if cur.fetchone()["n"] == 0:
        # a few thousand synthetic orders — enough to make joins/sorts do real work
        cur.execute("""
            INSERT INTO load_orders (order_id, client_id, instrument, qty, px)
            SELECT g,
                   'CL-' || (ARRAY['AC','SU','BCE','MFC','ABX'])[1 + (g % 5)],
                   (ARRAY['WTI','HO','NG','GOLD','USDCAD'])[1 + (g % 5)],
                   (random()*500)::int, round((random()*100+20)::numeric, 2)
            FROM generate_series(1, 20000) g
        """)
conn.commit()
conn.close()
print("✓ Seeded load_orders (20k rows) on your branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fire the load
# MAGIC `WORKERS` threads, each on its own connection (via the **pooled** host), run a tight loop of
# MAGIC CPU-heavy SQL — hashing + sorting + aggregating — for `DURATION` seconds. Each worker mints its
# MAGIC own short-lived OAuth credential (tokens rotate ~hourly; per-connection is the documented
# MAGIC pattern). Meanwhile the main thread polls the endpoint state every 15s.

# COMMAND ----------

import time, threading
from concurrent.futures import ThreadPoolExecutor
import psycopg

deadline = time.time() + DURATION
counts = [0] * WORKERS

# A deliberately CPU-ish query: hash + sort + aggregate a big generate_series joined to orders.
LOAD_SQL = """
SELECT count(*) FROM (
    SELECT g.g, md5(g.g::text) h, o.instrument
    FROM generate_series(1, 150000) g
    LEFT JOIN load_orders o ON o.order_id = (g.g % 20000) + 1
    ORDER BY md5(g.g::text)
) t
"""

def worker(i):
    # own connection per worker, minted fresh (do not share connections across threads)
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    c = psycopg.connect(host=POOLED_HOST, dbname=PG_DATABASE, user=user_email,
                        password=cred.token, sslmode="require", connect_timeout=20,
                        options=f"-c search_path={PG_SCHEMA},public")
    try:
        while time.time() < deadline:
            with c.cursor() as cur:
                cur.execute(LOAD_SQL)
                cur.fetchone()
            counts[i] += 1
    finally:
        c.close()

print(f"▶ Firing {WORKERS} workers for {DURATION}s — watch the Monitoring CU graph now.\n")
pool = ThreadPoolExecutor(max_workers=WORKERS)
for i in range(WORKERS):
    pool.submit(worker, i)

# Poll endpoint state on a timeline while the load runs.
start = time.time()
while time.time() < deadline:
    time.sleep(15)
    try:
        s = w.postgres.get_endpoint(name=ep.name).status
        elapsed = int(time.time() - start)
        print(f"  t+{elapsed:>3}s  state={getattr(s,'current_state',None)}"
              f"  pending={getattr(s,'pending_state',None)}"
              f"  envelope={getattr(s,'autoscaling_limit_min_cu',None)}→"
              f"{getattr(s,'autoscaling_limit_max_cu',None)} CU"
              f"  queries_so_far={sum(counts)}")
    except Exception as e:
        print(f"  (poll error: {e})")

pool.shutdown(wait=True)
total = sum(counts)
print(f"\n✓ Load complete: {total} queries across {WORKERS} workers in {DURATION}s "
      f"(~{total/DURATION:.1f} q/s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What you should have seen
# MAGIC
# MAGIC - In the **Monitoring → Compute/CU** graph, compute **climbed from `min` toward `max`** as the
# MAGIC   workers piled on, then **fell back** after the load stopped.
# MAGIC - The SDK `current_state` stayed `ACTIVE` under load; once idle past the suspend timeout the
# MAGIC   branch **scales to zero** (`IDLE`) — you stop paying for compute until the next connection.
# MAGIC - You never pre-provisioned peak capacity: **you paid for the burst, not for the day.**
# MAGIC
# MAGIC > **Note on what the SDK shows:** the endpoint status reports *state* + the *min/max envelope*,
# MAGIC > not a live CU number — the actual CU curve lives in the Monitoring UI, which is why we watched
# MAGIC > it there. (If you saw no movement and your envelope was `1→1`, ask your facilitator to raise
# MAGIC > `max_cu`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✓ Check
# MAGIC - The load ran and reported a query count.
# MAGIC - In Monitoring, CU rose above `min` during the load and settled afterward.
# MAGIC - You can explain min/max CU, scale-to-zero, and pay-per-use for your desk's store.
# MAGIC
# MAGIC **Optional cleanup:** `DROP TABLE load_orders;` on your branch (or leave it — it's tiny).
