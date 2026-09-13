# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3 — Capital Markets Online Feature Store (Lakebase)
# MAGIC
# MAGIC **Persona:** the *Coverage Desk Assistant* needs a client's **live risk profile**.
# MAGIC **Lakebase feature:** real-time feature serving powered by Lakebase.
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Build an **offline** Delta feature table of per-client risk features (computed
# MAGIC    from a capital-markets positions/trades DataFrame).
# MAGIC 2. **Publish** it to your Lakebase project as an online table (low-latency serving).
# MAGIC 3. Verify the online features over a **direct Postgres read**.
# MAGIC 4. Access the features through **MLflow** — a `FeatureSpec` → a **Feature Serving
# MAGIC    endpoint** → the MLflow deploy client. This is the path the chatbot in
# MAGIC    `Coverage_Desk_Chatbot.py` uses to look up features.
# MAGIC
# MAGIC **Requirements:** run on **serverless** or DBR ML; your Lakebase project must exist
# MAGIC (the facilitator created it — see `scripts/facilitator_setup.py`).
# MAGIC
# MAGIC > This is the **reference solution** — what the Genie Code prompt in `README.md`
# MAGIC > should produce. Author your version by pasting that prompt into Genie Code.

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.13.0" "psycopg[binary]>=3.1.0" "protobuf>=5.29.5,<6" "mlflow" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

from importlib.metadata import version

from databricks.feature_engineering import FeatureEngineeringClient

# databricks-sdk arrives as a feature-engineering dependency; co-pinning it in the same
# %pip install can send pip into a backtracking loop, so we verify the resolved version
# here instead. Require >=0.118.0 (consistent with the other SDK-using notebooks).
_sdk_version = version("databricks-sdk")
if tuple(int(p) for p in _sdk_version.split(".")[:2]) < (0, 118):
    raise RuntimeError(
        f"databricks-sdk {_sdk_version} is too old for the Lakebase APIs — need >=0.118.0. "
        "Run: %pip install 'databricks-sdk>=0.118.0' and restartPython.")
print(f"✓ databricks-sdk {_sdk_version}")

fe = FeatureEngineeringClient()
print("✓ Feature Engineering client initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC The offline feature table lives in Unity Catalog; the online store IS your existing
# MAGIC Lakebase project (`PROJECT_ID`), so features serve from the same instance as your
# MAGIC operational data — no separate store to provision.

# COMMAND ----------

# Catalog is a widget so you can point at whatever catalog you can create schemas in
# (many workspaces have `main`; your facilitator may give you a different one).
dbutils.widgets.text("uc_catalog", "main", "Unity Catalog catalog")
UC_CATALOG = dbutils.widgets.get("uc_catalog") or "main"
UC_SCHEMA = f"cm_{_sanitize(user_email).replace('-', '_')}"   # your per-attendee schema (owned by you)

FEATURE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.client_risk_features"
ONLINE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.client_risk_features_online"
FEATURE_SPEC = f"{UC_CATALOG}.{UC_SCHEMA}.client_risk_spec"
ONLINE_STORE_NAME = PROJECT_ID
ENDPOINT_NAME = f"cm-client-risk-{_sanitize(user_email)}"[:63]

print(f"Catalog/Schema:  {UC_CATALOG}.{UC_SCHEMA}")
print(f"Feature table:   {FEATURE_TABLE}")
print(f"Online table:    {ONLINE_TABLE}")
print(f"Feature spec:    {FEATURE_SPEC}")
print(f"Online store:    {ONLINE_STORE_NAME}  (your Lakebase project)")
print(f"Serving endpoint:{ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Compute per-client risk features (offline)
# MAGIC
# MAGIC We build a small **capital-markets world** — instruments (with spot / volatility /
# MAGIC contract size / FX), positions, and trades — then aggregate it into one row of risk
# MAGIC features per client. Self-contained, so this exercise runs even if you skipped
# MAGIC Exercise 2. Air Canada (`CL-AC`) is seeded with a large energy (WTI/heating-oil)
# MAGIC book, so its risk profile comes out **HIGH** — the hero of the chatbot demo.

# COMMAND ----------

# Your facilitator pre-creates this schema and makes you its owner; create it only if you
# have the privilege (harmless no-op when it already exists).
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
except Exception as _e:
    print(f"(using pre-provisioned schema {UC_CATALOG}.{UC_SCHEMA}: {str(_e)[:80]})")

# --- instruments dimension: spot, annualized vol, contract size, FX to CAD ----
instruments = [
    # instrument_id, asset_class, description, currency, contract_size, spot, vol, fx_to_cad
    ("WTI",    "commodity", "WTI Crude Oil",       "USD", 1000.0,   78.50, 0.42, 1.36),
    ("HO",     "commodity", "Heating Oil",         "USD", 42000.0,  2.55,  0.39, 1.36),
    ("NG",     "commodity", "Natural Gas",         "USD", 10000.0,  2.10,  0.55, 1.36),
    ("GOLD",   "commodity", "Gold",                "USD", 100.0,    2380.0,0.16, 1.36),
    ("USDCAD", "fx",        "USD/CAD",             "CAD", 100000.0, 1.36,  0.07, 1.00),
    ("CAD10Y", "rates",     "Canada 10Y Future",   "CAD", 100000.0, 98.20, 0.08, 1.00),
    ("CDX-IG", "credit",    "CDX IG 5Y",           "USD", 100000.0, 0.55,  0.12, 1.36),
    ("AC.TO",  "equity",    "Air Canada Equity",   "CAD", 100.0,    18.40, 0.35, 1.00),
    ("SU.TO",  "equity",    "Suncor Equity",       "CAD", 100.0,    52.10, 0.28, 1.00),
    ("BCE.TO", "equity",    "BCE Equity",          "CAD", 100.0,    44.90, 0.19, 1.00),
]

# --- positions fact: (client_id, instrument_id, book, net_qty, avg_entry_price) ----
positions = [
    # Air Canada — large energy exposure → HIGH risk (hero)
    ("CL-AC",  "WTI",    "ENERGY-HEDGE",  1800,   72.00),
    ("CL-AC",  "HO",     "ENERGY-HEDGE",  -900,    2.40),
    ("CL-AC",  "NG",     "ENERGY-HEDGE",  2500,    2.30),
    ("CL-AC",  "USDCAD", "FX-HEDGE",      140,     1.34),
    ("CL-AC",  "AC.TO",  "EQUITY",        50000,  17.80),
    # Suncor — energy producer, moderate
    ("CL-SU",  "WTI",    "ENERGY",        400,    75.00),
    ("CL-SU",  "SU.TO",  "EQUITY",        20000,  50.00),
    # Cenovus — energy, moderate
    ("CL-CVE", "WTI",    "ENERGY",        300,    76.00),
    ("CL-CVE", "NG",     "ENERGY",        800,     2.20),
    # BCE — telecom, low
    ("CL-BCE", "BCE.TO", "EQUITY",        30000,  46.00),
    ("CL-BCE", "CAD10Y", "RATES",         50,     98.00),
    # Manulife — financials, low-moderate
    ("CL-MFC", "CDX-IG", "CREDIT",        40,      0.52),
    ("CL-MFC", "CAD10Y", "RATES",         80,     98.10),
    # Barrick — gold
    ("CL-ABX", "GOLD",   "COMMODITY",     600,   2200.0),
    # CN Rail — industrials, small
    ("CL-CNR", "USDCAD", "FX-HEDGE",      30,      1.35),
    # Magna — small
    ("CL-MG",  "USDCAD", "FX-HEDGE",      25,      1.35),
    # Brookfield — small rates
    ("CL-BAM", "CAD10Y", "RATES",         40,     98.05),
]

# --- clients dimension: rating (mapped to 1-10) ----
clients = [
    ("CL-AC",  "Air Canada",    "Airlines",     "BB"),
    ("CL-SU",  "Suncor",        "Energy",       "BBB+"),
    ("CL-CVE", "Cenovus",       "Energy",       "BBB"),
    ("CL-BCE", "BCE",           "Telecom",      "A-"),
    ("CL-MFC", "Manulife",      "Financials",   "A"),
    ("CL-ABX", "Barrick Gold",  "Materials",    "BBB+"),
    ("CL-CNR", "CN Rail",       "Industrials",  "A"),
    ("CL-MG",  "Magna",         "Materials",    "A-"),
    ("CL-BAM", "Brookfield",    "Financials",   "A-"),
]

# --- trades fact: (client_id, notional_cad, days_ago, realized_pnl_cad) ----
import random
random.seed(1867)  # deterministic — everyone gets the same numbers
_trades = []
_trade_profile = {  # trades in last 90 days, base notional per client
    "CL-AC": (55, 4_200_000), "CL-SU": (30, 1_800_000), "CL-CVE": (22, 1_200_000),
    "CL-BCE": (14, 900_000),  "CL-MFC": (18, 1_100_000), "CL-ABX": (12, 700_000),
    "CL-CNR": (8, 400_000),   "CL-MG": (6, 300_000),     "CL-BAM": (9, 500_000),
}
for cid, (n, base) in _trade_profile.items():
    for _ in range(n):
        days_ago = random.randint(0, 90)
        notional = round(base * random.uniform(0.4, 1.6), 2)
        pnl = round(notional * random.uniform(-0.03, 0.04), 2)
        _trades.append((cid, notional, days_ago, pnl))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

instr_df = spark.createDataFrame(
    instruments,
    ["instrument_id", "asset_class", "description", "currency",
     "contract_size", "spot_price", "volatility", "fx_to_cad"],
)
pos_df = spark.createDataFrame(
    positions, ["client_id", "instrument_id", "book", "net_qty", "avg_entry_price"]
)
clients_df = spark.createDataFrame(
    clients, ["client_id", "legal_name", "sector", "credit_rating"]
)
trades_df = spark.createDataFrame(
    _trades, ["client_id", "notional_cad", "days_ago", "realized_pnl_cad"]
)

# Position-level economics in CAD
p = (pos_df.join(instr_df, "instrument_id")
     .withColumn("gross_cad", F.abs(F.col("net_qty")) * F.col("spot_price")
                 * F.col("contract_size") * F.col("fx_to_cad"))
     .withColumn("net_cad", F.col("net_qty") * F.col("spot_price")
                 * F.col("contract_size") * F.col("fx_to_cad")))

pos_agg = (p.groupBy("client_id").agg(
    F.sum("gross_cad").alias("gross_notional_cad"),
    F.sum("net_cad").alias("net_exposure_cad"),
    F.count("*").alias("num_open_positions"),
    F.max("gross_cad").alias("_largest_pos_cad"),
    (F.sum(F.col("gross_cad") * F.col("volatility")) / F.sum("gross_cad"))
        .alias("portfolio_volatility"),
))

trade_agg = (trades_df.filter(F.col("days_ago") <= 30).groupBy("client_id").agg(
    F.count("*").alias("num_trades_30d"),
    F.avg("notional_cad").alias("avg_trade_notional_cad"),
    F.sum("realized_pnl_cad").alias("realized_pnl_30d_cad"),
))
recency = trades_df.groupBy("client_id").agg(F.min("days_ago").alias("days_since_last_trade"))

_rating_map = {"AAA": 10, "AA": 9, "A": 8, "A-": 7, "BBB+": 6, "BBB": 5,
               "BBB-": 4, "BB": 3, "B": 2, "CCC": 1}
rating_expr = F.create_map([F.lit(x) for kv in _rating_map.items() for x in kv])

features = (clients_df
    .join(pos_agg, "client_id", "left")
    .join(trade_agg, "client_id", "left")
    .join(recency, "client_id", "left")
    .withColumn("largest_position_pct",
                F.when(F.col("gross_notional_cad") > 0,
                       F.col("_largest_pos_cad") / F.col("gross_notional_cad"))
                 .otherwise(F.lit(0.0)))
    .withColumn("credit_rating_score", rating_expr[F.col("credit_rating")].cast(DoubleType()))
    .na.fill({"gross_notional_cad": 0.0, "net_exposure_cad": 0.0, "num_open_positions": 0,
              "portfolio_volatility": 0.0, "num_trades_30d": 0, "avg_trade_notional_cad": 0.0,
              "realized_pnl_30d_cad": 0.0, "days_since_last_trade": 999, "largest_position_pct": 0.0})
    # risk_tier: elevated by big gross notional, high concentration, or high vol
    .withColumn("risk_tier",
        F.when((F.col("gross_notional_cad") > 300_000_000) |
               (F.col("portfolio_volatility") > 0.34), F.lit("HIGH"))
         .when((F.col("gross_notional_cad") > 80_000_000) |
               (F.col("portfolio_volatility") > 0.22), F.lit("MEDIUM"))
         .otherwise(F.lit("LOW")))
    .select(
        "client_id", "gross_notional_cad", "net_exposure_cad", "num_open_positions",
        "num_trades_30d", "avg_trade_notional_cad", "largest_position_pct",
        "realized_pnl_30d_cad", "portfolio_volatility", "days_since_last_trade",
        "credit_rating_score", "risk_tier"))

display(features.orderBy(F.desc("gross_notional_cad")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the offline feature table (PK + Change Data Feed)
# MAGIC
# MAGIC Publishing to an online store requires a **primary key** and **Change Data Feed**.
# MAGIC The Feature Engineering client creates the table with the PK; we enable CDF next.

# COMMAND ----------

def has_primary_key(table: str) -> bool:
    cat, sch, tbl = table.split(".")
    return spark.sql(f"""
        SELECT 1 FROM {cat}.information_schema.table_constraints
        WHERE table_schema = '{sch}' AND table_name = '{tbl}'
          AND constraint_type = 'PRIMARY KEY'
    """).count() > 0

table_ready = spark.catalog.tableExists(FEATURE_TABLE)
if table_ready and not has_primary_key(FEATURE_TABLE):
    print(f"⚠ {FEATURE_TABLE} has no primary key — rebuilding ...")
    try:
        w.feature_store.delete_online_table(online_table_name=ONLINE_TABLE)
    except Exception:
        pass
    spark.sql(f"DROP TABLE {FEATURE_TABLE}")
    table_ready = False

if table_ready:
    print(f"Feature table already exists: {FEATURE_TABLE} — overwriting rows")
    features.write.format("delta").mode("overwrite").saveAsTable(FEATURE_TABLE)
else:
    fe.create_table(
        name=FEATURE_TABLE,
        primary_keys=["client_id"],
        df=features,
        description="Per-client capital-markets risk features for the Coverage Desk Assistant",
    )
    print(f"✓ Feature table created: {FEATURE_TABLE}")

spark.sql(f"ALTER TABLE {FEATURE_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
print("✓ Change Data Feed enabled")

show_view_link("View the feature table in Unity Catalog",
               uc_table_url(UC_CATALOG, UC_SCHEMA, "client_risk_features"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Publish to the online store (your Lakebase project)

# COMMAND ----------

import time

online_store = fe.get_online_store(name=ONLINE_STORE_NAME)
print(f"✓ Online store ready: {online_store.name} (state={online_store.state})")


def publish_with_retry(attempts=6, wait_s=30):
    """Publish, tolerating a destination that is still being torn down.

    Deleting an online table returns before its Postgres table is gone, so a publish
    right after a delete can be told the destination already exists — retry."""
    for attempt in range(1, attempts + 1):
        try:
            fe.publish_table(online_store=online_store,
                             source_table_name=FEATURE_TABLE,
                             online_table_name=ONLINE_TABLE)
            return
        except Exception as e:
            if "already exists" not in str(e).lower() or attempt == attempts:
                raise
            print(f"  destination still cleaning up — retry {attempt}/{attempts} in {wait_s}s")
            time.sleep(wait_s)


publish_with_retry()
print(f"✓ Published {FEATURE_TABLE} → online table {ONLINE_TABLE} (TRIGGERED mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify online features via a direct Postgres read
# MAGIC
# MAGIC The online store IS your Lakebase project, so the published features land as a
# MAGIC Postgres table you can read with the same `get_connection()` helper from the other
# MAGIC labs. (If the publish pipeline is still running the table may take a minute.)

# COMMAND ----------

try:
    # The online store is project-level (published to production), not your own branch —
    # so verify against the production branch explicitly.
    conn = get_connection(branch="production", set_search_path=False)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema','pg_internal')
              AND tablename LIKE '%client_risk%'
            ORDER BY 1,2
        """)
        for r in cur.fetchall():
            print(f"  {r['schemaname']}.{r['tablename']}  ← online feature table")
    conn.close()
    print("✓ Online feature table present in Lakebase")
except Exception as e:
    print(f"Publish pipeline may still be running — retry shortly. ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Access features via MLflow — FeatureSpec → Feature Serving endpoint
# MAGIC
# MAGIC The **documented** way for an application/agent to fetch online features is a
# MAGIC **Feature Serving endpoint** built from a `FeatureSpec`, queried with the MLflow
# MAGIC deploy client. This is exactly what the chatbot's feature-lookup **tool** calls.
# MAGIC
# MAGIC Docs: [Feature & function serving](https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-function-serving)
# MAGIC · [Serving tutorial](https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-serving-tutorial)

# COMMAND ----------

from databricks.feature_engineering import FeatureLookup

# ⚠ VALIDATE: create_feature_spec signature + whether FeatureLookup should point at the
# OFFLINE feature table (serving resolves the online store automatically). Current docs
# use the offline UC table name here.
try:
    fe.create_feature_spec(
        name=FEATURE_SPEC,
        features=[FeatureLookup(table_name=FEATURE_TABLE, lookup_key="client_id")],
    )
    print(f"✓ FeatureSpec created: {FEATURE_SPEC}")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"✓ FeatureSpec already exists: {FEATURE_SPEC}")
    else:
        raise

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# ServedEntityInput for a FeatureSpec entity: entity_name = the spec's full UC name;
# feature specs are unversioned, so no entity_version. scale_to_zero keeps idle cost down.
# NOTE: EndpointCoreConfigInput requires `name` (the endpoint name) in current SDKs — pass
# it here as well as to create_and_wait, or you get "missing required argument: 'name'".
existing = [e.name for e in w.serving_endpoints.list()]
if ENDPOINT_NAME in existing:
    print(f"✓ Serving endpoint already exists: {ENDPOINT_NAME}")
else:
    print(f"Creating Feature Serving endpoint {ENDPOINT_NAME} (warms ~10-15 min) ...")
    w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            name=ENDPOINT_NAME,
            served_entities=[ServedEntityInput(
                entity_name=FEATURE_SPEC,
                workload_size="Small",
                scale_to_zero_enabled=True,
            )],
        ),
    )
    print(f"✓ Feature Serving endpoint ready: {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the endpoint with the MLflow deploy client
# MAGIC This is the exact call the chatbot's `lookup_client_risk` tool makes.

# COMMAND ----------

import mlflow.deployments

deploy_client = mlflow.deployments.get_deploy_client("databricks")

# ⚠ VALIDATE: feature-serving input schema — dataframe_records with the lookup key.
resp = deploy_client.predict(
    endpoint=ENDPOINT_NAME,
    inputs={"dataframe_records": [{"client_id": "CL-AC"}]},
)
print("Air Canada (CL-AC) live risk features from the online store:")
print(resp)

# COMMAND ----------

# MAGIC %md
# MAGIC ✓ **Checks**
# MAGIC - Feature table has 9 clients, Air Canada (`CL-AC`) is `risk_tier = HIGH`.
# MAGIC - Online table present in Lakebase (§4).
# MAGIC - Serving endpoint returns Air Canada's features via the MLflow deploy client (§5).
# MAGIC
# MAGIC Keep `ENDPOINT_NAME`, `FEATURE_SPEC`, and your `client_id`s handy — the chatbot in
# MAGIC `Coverage_Desk_Chatbot.py` reuses them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC The serving endpoint is `scale_to_zero`, but delete it after the workshop to be
# MAGIC tidy. Use `delete_online_table` (never `DROP TABLE`) for the online table.

# COMMAND ----------

# UNCOMMENT TO CLEAN UP:
# w.serving_endpoints.delete(name=ENDPOINT_NAME)
# w.feature_store.delete_online_table(online_table_name=ONLINE_TABLE)
# spark.sql(f"DROP TABLE IF EXISTS {FEATURE_TABLE}")
# print("✓ Cleaned up feature serving endpoint, online table, feature table")
