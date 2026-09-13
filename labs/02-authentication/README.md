# Exercise 2 — Authentication

**Persona:** you're setting up a CIBC Capital Markets coverage desk's operational store on
Lakebase. First you'll connect from a **notebook** and generate the desk's data; then you'll
connect an **external tool (VS Code)** to the same store.

## What you'll learn
- The **two-layer Lakebase auth model**:
  1. **Workspace identity → OAuth database credential.** In a notebook or app, your Databricks
     identity *is* your Postgres role. You mint a short-lived (~1 hour) OAuth JWT with
     `w.postgres.generate_database_credential(...)` and pass it as the connection password. No
     password to store; it rotates automatically.
  2. **Native Postgres password role.** External tools (VS Code, psql, DBeaver, pgAdmin) can't
     easily refresh an OAuth JWT, so for those you create a **native password role** from the
     Lakebase **Connect / App Connect** dialog and use it like any Postgres login.
- How to connect with `psycopg` correctly: **keyword arguments, not a URL** — OAuth tokens contain
  `.` and `-` characters that break DSN/URL parsing — with `sslmode=require`.
- How to generate a realistic, deterministic capital-markets dataset in your own schema.

---

## Part A — Connect from Python & generate the data (Genie Code)

You'll have **Genie Code** (Databricks Assistant, Agent Mode) write the connection + data-generation
code for you. Because the prompt below is fully specified, everyone's generated code comes out
essentially the same — so we can review it together as a group.

### How to run it
1. Open a **new notebook** in your workspace (in the same folder as this workshop's `labs/`, so
   `%run ../_setup` resolves), attach it to **serverless**, and open the **Assistant** panel (Genie
   Code / Agent Mode).
2. **Start a fresh Assistant chat** and paste the prompt below **verbatim**.
3. Glance at the plan, let it run, then run the generated cells top-to-bottom.

> The **reference solution** is checked in as `Connect_And_Generate_Data.py` — compare the
> generated code against it, or just run it directly if you'd rather not drive the Assistant.

### The prompt (paste verbatim)

```
Generate a Databricks notebook (Python, for serverless) that connects to my Lakebase project and
loads a synthetic CIBC Capital Markets dataset into my Postgres schema. Requirements — follow them
exactly so my output matches the rest of the workshop:

CONNECTION
- First cell: %pip install "psycopg[binary]>=3.1.0" then dbutils.library.restartPython().
- Then `%run ../_setup` — reuse its helpers; DO NOT re-implement the connection. It provides
  `get_connection()` (psycopg, OAuth database credential, sslmode=require, search_path set to my
  schema), plus `PG_SCHEMA`, `user_email`, `w`. Open the connection with `conn = get_connection()`.
- Connect using psycopg KEYWORD ARGUMENTS via the helper — never a URL/DSN string (OAuth tokens
  break URL parsing). Print current_user, current_database and the server version to confirm.

SCHEMA — create exactly these five tables in my schema (idempotent: DROP ... CASCADE then CREATE;
create in FK order clients→instruments→market_prices→positions→trades):
- clients(client_id TEXT PK, ticker, legal_name, sector, country, credit_rating, coverage_officer,
  relationship_tier, onboarded_on DATE)
- instruments(instrument_id TEXT PK, asset_class, description, currency, contract_size NUMERIC)
- market_prices(instrument_id FK, price_date DATE, spot_price NUMERIC, volatility NUMERIC,
  PRIMARY KEY(instrument_id, price_date))
- positions(position_id SERIAL PK, client_id FK, instrument_id FK, book, net_qty NUMERIC,
  avg_entry_price NUMERIC, as_of_date DATE)
- trades(trade_id SERIAL PK, client_id FK, instrument_id FK, side CHECK IN ('BUY','SELL'),
  quantity NUMERIC, price NUMERIC, trade_ts TIMESTAMPTZ, trader, notional_cad NUMERIC)
Add indexes on positions(client_id), trades(client_id), trades(trade_ts), market_prices(price_date).

DATA — deterministic (use random.Random(1867)); all issuers are real public Canadian large-caps,
everything else synthetic (no PII/MNPI):
- 9 clients: Air Canada (CL-AC, AC.TO, Airlines, BBB-), Magna (CL-MG, MG.TO, Auto, A-),
  BCE (CL-BCE, BCE.TO, Telecom, BBB+), Manulife (CL-MFC, MFC.TO, Financials, A),
  Brookfield (CL-BAM, BN.TO, Financials, A-), Suncor (CL-SU, SU.TO, Energy, BBB+),
  CN Rail (CL-CNR, CNR.TO, Industrials, A), Barrick (CL-ABX, ABX.TO, Materials, BBB),
  Cenovus (CL-CVE, CVE.TO, Energy, BBB). Coverage officers: Sofia Martins covers AC+Magna;
  David Chen covers BCE+Manulife+Brookfield; Priya Nair covers Suncor+CN+Barrick+Cenovus.
  Relationship tiers Platinum/Gold/Silver.
- 18 instruments across asset classes: commodities WTI, HO (heating oil), NG, GOLD; fx USDCAD,
  EURCAD; rates CA10Y, US10Y; credit CDXIG, CADCDS5Y; plus the 8 issuer equities (AC.TO … CVE.TO).
  Give each a currency (USD/CAD) and a contract_size (equities 1; commodities/fx/rates/credit 1000;
  GOLD 100).
- market_prices: ~30 daily rows per instrument as a small random walk off a base spot, with a
  per-instrument annualized volatility.
- positions: ~75 rows. IMPORTANT hero: give Air Canada (CL-AC) ONLY a hero energy book — a LARGE
  long in WTI (~250 contracts) and heating oil (HO, ~1500 contracts) plus its own equity and a small
  USDCAD hedge — and EXCLUDE CL-AC from the generic random-position loop, so its energy exposure
  clearly dominates its gross notional (makes AC the highest-risk client later). Note only 8 of the 9
  issuers have a listed equity instrument (Brookfield/BN.TO does not), so guard the "own equity"
  position lookup to skip an issuer with no equity instrument.
- trades: ~600 rows across the last 90 days; compute notional_cad = quantity * price *
  contract_size * fx_to_cad (USD→1.36, EUR→1.47, CAD→1.0).

VERIFY — print row counts for all five tables, a group-by of clients per coverage_officer, and
Air Canada's gross notional exposure (CAD) per instrument using the latest market price. Commit and
close the connection. Finish with a ✓ summary line.
```

### ✓ Validation (Part A)
- 9 clients, 18 instruments, 540 market prices, ~76 positions, 600 trades.
- Clients group cleanly under the three coverage officers (Sofia: AC, Magna · David: BCE, Manulife,
  Brookfield · Priya: Suncor, CN, Barrick, Cenovus).
- **Air Canada's gross notional is dominated by WTI + heating oil** (energy — ~$32M of ~$33M) —
  that's the hero.

---

## Part B — Connect an external tool (VS Code)

Now connect **VS Code** to the *same* Lakebase project and read the data you just loaded. Full
step-by-step (drivers, connection fields, the native-password path, and the read exercise) is in:

➡️ **[`VSCODE_CONNECT.md`](./VSCODE_CONNECT.md)**

### ✓ Validation (Part B)
- VS Code connects to your project over SSL and lists your `cm_<username>` schema.
- `SELECT * FROM cm_<username>.clients LIMIT 10;` returns your 9 issuers.
- A `positions ⋈ clients` join runs and shows each position's covering officer.

---

## What's next
**Exercise 3 — Data APIs (REST vs JDBC):** reach this same branch over an HTTP data API and over a
pooled JDBC connection, and learn when an app should use each.
