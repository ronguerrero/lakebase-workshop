# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2 — Authentication & Capital-Markets Data Generation
# MAGIC
# MAGIC **Path:** Authentication &nbsp;|&nbsp; **Prerequisite:** Facilitator has created your Lakebase project (see `FACILITATOR.md`)
# MAGIC
# MAGIC This notebook is the **reference solution** for Part A — it is what Databricks
# MAGIC Assistant / **Genie Code** should produce from the prompt in this exercise's
# MAGIC `README.md`. Every participant runs the *same* generated code so the room ends up
# MAGIC with an identical capital-markets world and we can review results together.
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Authenticate to your Lakebase project with a short-lived **OAuth database
# MAGIC    credential** (your Databricks identity *is* your Postgres role).
# MAGIC 2. Generate five deterministic **CIBC Capital Markets** tables in your schema:
# MAGIC    `clients`, `instruments`, `market_prices`, `positions`, `trades`.
# MAGIC 3. Verify the load, including Air Canada's hero energy exposure.
# MAGIC
# MAGIC > **Two-layer auth:** in a notebook/app you mint a rotating OAuth JWT
# MAGIC > (`generate_database_credential`) — no password to manage. To connect an
# MAGIC > *external* tool like VS Code you use a native Postgres password role instead —
# MAGIC > see `VSCODE_CONNECT.md` (Part B).

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Authenticate & connect
# MAGIC
# MAGIC `get_connection()` (from `_setup`) uses the Databricks SDK to mint an OAuth
# MAGIC database credential and opens a `psycopg` connection with `sslmode=require`. It
# MAGIC connects with **keyword arguments, not a URL** — OAuth tokens contain characters
# MAGIC that break DSN/URL parsing. Your `search_path` is set to your own schema.

# COMMAND ----------

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("SELECT current_user, current_database(), version()")
    row = cur.fetchone()
print(f"✓ Connected to Lakebase")
print(f"  Role (you):  {row['current_user']}")
print(f"  Database:    {row['current_database']}")
print(f"  Schema:      {PG_SCHEMA}")
print(f"  Server:      {row['version'].split(',')[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the schema (tables)
# MAGIC
# MAGIC Five tables model the coverage desk's world. We drop and recreate them so the
# MAGIC notebook is idempotent. `search_path` already points at your `cm_<username>`
# MAGIC schema, so unqualified names land there.

# COMMAND ----------

DDL = """
DROP TABLE IF EXISTS trades CASCADE;
DROP TABLE IF EXISTS positions CASCADE;
DROP TABLE IF EXISTS market_prices CASCADE;
DROP TABLE IF EXISTS instruments CASCADE;
DROP TABLE IF EXISTS clients CASCADE;

CREATE TABLE clients (
    client_id          TEXT PRIMARY KEY,
    ticker             TEXT NOT NULL,
    legal_name         TEXT NOT NULL,
    sector             TEXT NOT NULL,
    country            TEXT NOT NULL,
    credit_rating      TEXT NOT NULL,
    coverage_officer   TEXT NOT NULL,
    relationship_tier  TEXT NOT NULL,
    onboarded_on       DATE NOT NULL
);

CREATE TABLE instruments (
    instrument_id  TEXT PRIMARY KEY,
    asset_class    TEXT NOT NULL,
    description    TEXT NOT NULL,
    currency       TEXT NOT NULL,
    contract_size  NUMERIC NOT NULL
);

CREATE TABLE market_prices (
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    price_date     DATE NOT NULL,
    spot_price     NUMERIC NOT NULL,
    volatility     NUMERIC NOT NULL,
    PRIMARY KEY (instrument_id, price_date)
);

CREATE TABLE positions (
    position_id      SERIAL PRIMARY KEY,
    client_id        TEXT NOT NULL REFERENCES clients(client_id),
    instrument_id    TEXT NOT NULL REFERENCES instruments(instrument_id),
    book             TEXT NOT NULL,
    net_qty          NUMERIC NOT NULL,
    avg_entry_price  NUMERIC NOT NULL,
    as_of_date       DATE NOT NULL
);

CREATE TABLE trades (
    trade_id       SERIAL PRIMARY KEY,
    client_id      TEXT NOT NULL REFERENCES clients(client_id),
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    side           TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    quantity       NUMERIC NOT NULL,
    price          NUMERIC NOT NULL,
    trade_ts       TIMESTAMPTZ NOT NULL,
    trader         TEXT NOT NULL,
    notional_cad   NUMERIC NOT NULL
);

CREATE INDEX idx_positions_client ON positions(client_id);
CREATE INDEX idx_trades_client ON trades(client_id);
CREATE INDEX idx_trades_ts ON trades(trade_ts);
CREATE INDEX idx_prices_date ON market_prices(price_date);
"""

with conn.cursor() as cur:
    cur.execute(DDL)
conn.commit()
print("✓ Created 5 tables: clients, instruments, market_prices, positions, trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate deterministic synthetic data
# MAGIC
# MAGIC A fixed RNG seed (`1867` — the year of Confederation) makes every participant's
# MAGIC data identical. All names/issuers are **public Canadian large-caps**; everything
# MAGIC else is synthetic. Air Canada (`CL-AC`) is deliberately seeded with a large
# MAGIC **WTI crude + heating-oil** long — the hero exposure the later exercises key off.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta, timezone

rng = random.Random(1867)
TODAY = date(2026, 9, 11)

# FX to CAD (USD priced instruments convert; CAD stays 1.0)
FX_TO_CAD = {"USD": 1.36, "CAD": 1.0, "EUR": 1.47}

# --- clients (9 Canadian large-caps) --------------------------------------
CLIENTS = [
    # client_id, ticker, legal_name, sector, country, rating, officer, tier, onboarded
    ("CL-AC",  "AC.TO",  "Air Canada",                "Airlines",     "CA", "BBB-", "Sofia Martins", "Gold",     date(2019, 3, 15)),
    ("CL-MG",  "MG.TO",  "Magna International",        "Auto",         "CA", "A-",   "Sofia Martins", "Gold",     date(2018, 6, 1)),
    ("CL-BCE", "BCE.TO", "BCE Inc.",                   "Telecom",      "CA", "BBB+", "David Chen",    "Platinum", date(2016, 1, 10)),
    ("CL-MFC", "MFC.TO", "Manulife Financial",         "Financials",   "CA", "A",    "David Chen",    "Platinum", date(2015, 9, 20)),
    ("CL-BAM", "BN.TO",  "Brookfield Corporation",     "Financials",   "CA", "A-",   "David Chen",    "Gold",     date(2017, 11, 5)),
    ("CL-SU",  "SU.TO",  "Suncor Energy",              "Energy",       "CA", "BBB+", "Priya Nair",    "Gold",     date(2018, 2, 14)),
    ("CL-CNR", "CNR.TO", "Canadian National Railway",  "Industrials",  "CA", "A",    "Priya Nair",    "Platinum", date(2014, 5, 30)),
    ("CL-ABX", "ABX.TO", "Barrick Gold",               "Materials",    "CA", "BBB",  "Priya Nair",    "Silver",   date(2019, 8, 22)),
    ("CL-CVE", "CVE.TO", "Cenovus Energy",             "Energy",       "CA", "BBB",  "Priya Nair",    "Silver",   date(2020, 1, 8)),
]

# --- instruments (18 across asset classes) --------------------------------
# instrument_id, asset_class, description, currency, contract_size, base_spot, vol
INSTRUMENTS = [
    ("WTI",      "commodity", "WTI Crude Oil",            "USD", 1000, 78.5,  0.42),
    ("HO",       "commodity", "Heating Oil",              "USD", 1000, 2.45,  0.40),
    ("NG",       "commodity", "Natural Gas",              "USD", 1000, 2.10,  0.55),
    ("GOLD",     "commodity", "Gold Spot",                "USD", 100,  2350.0,0.16),
    ("USDCAD",   "fx",        "USD/CAD",                  "CAD", 1000, 1.36,  0.08),
    ("EURCAD",   "fx",        "EUR/CAD",                  "CAD", 1000, 1.47,  0.09),
    ("CA10Y",    "rates",     "Canada 10Y Bond",          "CAD", 1000, 98.4,  0.06),
    ("US10Y",    "rates",     "US 10Y Treasury",          "USD", 1000, 96.1,  0.07),
    ("CDXIG",    "credit",    "CDX IG Index",             "USD", 1000, 55.0,  0.19),
    ("CADCDS5Y", "credit",    "CAD 5Y CDS Index",         "CAD", 1000, 61.0,  0.21),
    ("AC.TO",    "equity",    "Air Canada Equity",        "CAD", 1,    18.5,  0.38),
    ("MG.TO",    "equity",    "Magna Equity",             "CAD", 1,    62.0,  0.28),
    ("BCE.TO",   "equity",    "BCE Equity",               "CAD", 1,    44.0,  0.22),
    ("MFC.TO",   "equity",    "Manulife Equity",          "CAD", 1,    38.0,  0.25),
    ("SU.TO",    "equity",    "Suncor Equity",            "CAD", 1,    52.0,  0.33),
    ("CNR.TO",   "equity",    "CN Rail Equity",           "CAD", 1,    168.0, 0.20),
    ("ABX.TO",   "equity",    "Barrick Equity",           "CAD", 1,    22.0,  0.31),
    ("CVE.TO",   "equity",    "Cenovus Equity",           "CAD", 1,    26.0,  0.34),
]

TRADERS = ["T. Okafor", "L. Zhang", "M. Dubois", "R. Patel", "K. Nguyen"]
BOOKS = ["ENERGY", "RATES", "CREDIT", "EQUITY", "FX"]

client_rows = [c for c in CLIENTS]
instr_rows = [(i[0], i[1], i[2], i[3], i[4]) for i in INSTRUMENTS]
base = {i[0]: {"spot": i[5], "vol": i[6], "ccy": i[3], "csize": i[4], "ac": i[1]} for i in INSTRUMENTS}

# --- market_prices: 30-day random walk per instrument ---------------------
price_rows = []
latest_spot = {}
for iid, meta in base.items():
    spot = meta["spot"]
    daily_vol = meta["vol"] / (252 ** 0.5)
    for d in range(30, 0, -1):
        pdate = TODAY - timedelta(days=d - 1)
        spot = max(0.01, spot * (1 + rng.gauss(0, daily_vol)))
        price_rows.append((iid, pdate, round(spot, 4), round(meta["vol"], 4)))
    latest_spot[iid] = round(spot, 4)

# --- positions: ~90, AC seeded with a large energy long (hero) ------------
position_rows = []


def add_position(cid, iid, book, net_qty):
    entry = round(base[iid]["spot"] * (1 + rng.uniform(-0.08, 0.08)), 4)
    position_rows.append((cid, iid, book, round(net_qty, 2), entry, TODAY))


# Hero: Air Canada carries a big WTI + heating-oil long (fuel exposure). Sized so its
# energy book clearly dominates AC's gross notional (the story the later exercises tell).
add_position("CL-AC", "WTI", "ENERGY", 250)
add_position("CL-AC", "HO",  "ENERGY", 1500)
add_position("CL-AC", "AC.TO", "EQUITY", 40000)
add_position("CL-AC", "USDCAD", "FX", 120)

equity_of = {c[0]: c[1] for c in CLIENTS}
for cid, ticker, *_ in CLIENTS:
    # AC's book is the hero energy set above — leave it energy-dominated (don't pile on
    # random rates/credit positions that would swamp the WTI/HO story).
    if cid == "CL-AC":
        continue
    n = rng.randint(6, 11)
    pool = [i[0] for i in INSTRUMENTS]
    for _ in range(n):
        iid = rng.choice(pool)
        book = rng.choice(BOOKS)
        qty = rng.choice([-1, 1]) * rng.randint(5, 400)
        add_position(cid, iid, book, qty)
    # every client holds some of its own equity — but only 8 of the 9 issuers have a
    # listed equity instrument (Brookfield/BN.TO trades elsewhere), so guard the lookup.
    eq = equity_of[cid]
    own_qty = rng.randint(2000, 25000)
    if eq in base:
        add_position(cid, eq, "EQUITY", own_qty)

# --- trades: ~600 over the last 90 days -----------------------------------
trade_rows = []
n_trades = 600
for _ in range(n_trades):
    cid = rng.choice([c[0] for c in CLIENTS])
    iid = rng.choice([i[0] for i in INSTRUMENTS])
    side = rng.choice(["BUY", "SELL"])
    qty = rng.randint(1, 500)
    price = round(base[iid]["spot"] * (1 + rng.uniform(-0.12, 0.12)), 4)
    days_ago = rng.randint(0, 89)
    ts = datetime(TODAY.year, TODAY.month, TODAY.day, tzinfo=timezone.utc) \
        - timedelta(days=days_ago, hours=rng.randint(0, 8), minutes=rng.randint(0, 59))
    fx = FX_TO_CAD.get(base[iid]["ccy"], 1.0)
    notional = round(qty * price * float(base[iid]["csize"]) * fx, 2)
    trade_rows.append((cid, iid, side, qty, price, ts, rng.choice(TRADERS), notional))

print(f"Prepared: {len(client_rows)} clients, {len(instr_rows)} instruments, "
      f"{len(price_rows)} prices, {len(position_rows)} positions, {len(trade_rows)} trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load the data
# MAGIC
# MAGIC `executemany` batches the inserts. `SERIAL` columns (`position_id`, `trade_id`)
# MAGIC are omitted so Postgres assigns them.

# COMMAND ----------

with conn.cursor() as cur:
    cur.executemany(
        "INSERT INTO clients (client_id, ticker, legal_name, sector, country, "
        "credit_rating, coverage_officer, relationship_tier, onboarded_on) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", client_rows)
    cur.executemany(
        "INSERT INTO instruments (instrument_id, asset_class, description, currency, "
        "contract_size) VALUES (%s,%s,%s,%s,%s)", instr_rows)
    cur.executemany(
        "INSERT INTO market_prices (instrument_id, price_date, spot_price, volatility) "
        "VALUES (%s,%s,%s,%s)", price_rows)
    cur.executemany(
        "INSERT INTO positions (client_id, instrument_id, book, net_qty, avg_entry_price, "
        "as_of_date) VALUES (%s,%s,%s,%s,%s,%s)", position_rows)
    cur.executemany(
        "INSERT INTO trades (client_id, instrument_id, side, quantity, price, trade_ts, "
        "trader, notional_cad) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", trade_rows)
conn.commit()
print("✓ Loaded all five tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify — row counts, a join, and the hero exposure

# COMMAND ----------

with conn.cursor() as cur:
    print("Row counts")
    for t in ["clients", "instruments", "market_prices", "positions", "trades"]:
        cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
        print(f"  {t:<15} {cur.fetchone()['n']:>6}")

    print("\nCoverage desk — clients by officer:")
    cur.execute("""
        SELECT coverage_officer, COUNT(*) AS clients,
               STRING_AGG(ticker, ', ' ORDER BY ticker) AS book
        FROM clients GROUP BY coverage_officer ORDER BY coverage_officer
    """)
    for r in cur.fetchall():
        print(f"  {r['coverage_officer']:<15} {r['clients']} clients: {r['book']}")

    print("\nHero — Air Canada's gross notional exposure by instrument (CAD):")
    cur.execute("""
        SELECT p.instrument_id, i.description, i.asset_class,
               ROUND(ABS(p.net_qty * mp.spot_price * i.contract_size *
                     CASE i.currency WHEN 'USD' THEN 1.36 WHEN 'EUR' THEN 1.47 ELSE 1.0 END)) AS gross_cad
        FROM positions p
        JOIN instruments i   ON i.instrument_id = p.instrument_id
        JOIN LATERAL (SELECT spot_price FROM market_prices m
                      WHERE m.instrument_id = p.instrument_id
                      ORDER BY price_date DESC LIMIT 1) mp ON true
        WHERE p.client_id = 'CL-AC'
        ORDER BY gross_cad DESC
    """)
    total = 0
    for r in cur.fetchall():
        total += float(r['gross_cad'])
        print(f"  {r['instrument_id']:<8} {r['description']:<22} {r['asset_class']:<10} "
              f"CAD {r['gross_cad']:>15,.0f}")
    print(f"  {'':<8} {'TOTAL GROSS NOTIONAL':<22} {'':<10} CAD {total:>15,.0f}")

conn.close()
print("\n✓ Exercise 2 complete — your capital-markets world is live in Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next
# MAGIC
# MAGIC - **Part B:** connect an **external tool (VS Code)** to this same data — see
# MAGIC   `VSCODE_CONNECT.md`.
# MAGIC - **Exercise 3 — Online Feature Store:** turn these tables into per-client **risk
# MAGIC   features**, publish them to Lakebase for low-latency serving, and build the
# MAGIC   **Coverage Desk Assistant** chatbot that looks them up.
