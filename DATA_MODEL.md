# Data model — CIBC Capital Markets Lakebase Workshop

What the tables are, what they hold, and how they connect across the seven exercises. Read this
before the session so the room understands the shared world every exercise builds on.

Everything is **synthetic and generated from scratch** — no external files, no customer data. The
numbers are deterministic (fixed RNG seed) so every participant produces the same rows and the group
can review results together.

---

## The shape: one Postgres world → features → agents

```
  Lakebase project  (facilitator-created, ONE shared project)
   └── branch  br·<user>            ← each participant's own isolated branch (own endpoint)
        └── schema  cm_<user>        ← their tables live here, on their branch
             ├── clients             9 issuers/counterparties CIBC CM covers
             ├── instruments         18 tradable instruments across asset classes
             ├── market_prices       daily spot + volatility per instrument
             ├── positions           open positions per client/instrument
             ├── trades              executed trades (last 90 days)
             ├── lb_client_reference synced FROM Delta (Ex6 reverse ETL)
             └── lb_positions / lb_limits   operational tables (Ex7 source)
                    │
   Ex2 generates the five core tables directly in Lakebase (Genie Code → psycopg). Ex3 reads them
   two ways (REST vs JDBC). Ex6 syncs Delta → lb_client_reference. Ex7 captures lb_* changes out.
                    │
        ┌───────────┴─────────────────────────────────────────────┐
        ▼ (Ex6: Delta → Lakebase)                                  ▼ (Ex7: Lakebase → Delta)
  Unity Catalog  <catalog>.cm_features_<user>.client_reference    <catalog>.cm_bronze_<user>.lb_*_changes  (append-only)
        (Delta, PK + CDF; synced INTO Lakebase as lb_client_reference)          │  Lakeflow AUTO CDC, SCD Type 1
                                                                                 ▼
                                                              <catalog>.cm_scd_<user>.positions_current / limits_current
                    │
                    ▼  (Ex4: feature store)
  Unity Catalog   <catalog>.cm_features_<user>.client_risk_features   (Delta, offline, PK + CDF)
                    │  Ex4 computes per-client risk features, then PUBLISHES them …
                    ▼
  Lakebase online store (the same project)  client_risk_features_online   (Postgres)
                    │  … served with low latency; the Coverage Desk Assistant looks them up.
                    ▼
  Lakebase (the attendee's branch)   checkpoint%  tables   (LangGraph PostgresSaver)
                       Ex5 gives the assistant persistent conversation memory.
```

The persona that ties the last two exercises together: the **Coverage Desk Assistant** — a chatbot a
CIBC Capital Markets coverage officer / trader talks to about a client's live risk profile. In the
feature-store exercise it *looks up features*; in the agentic-memory exercise it *remembers the
conversation*.

---

## 1. `clients` — 9 rows
The issuer / counterparty master for the coverage desk.

| column | type | notes |
|---|---|---|
| client_id | text | PK, e.g. `CL-AC` |
| ticker | text | `AC.TO` |
| legal_name | text | Air Canada |
| sector | text | Airlines, Energy, Telecom, Financials, Materials, … |
| country | text | `CA` |
| credit_rating | text | `BBB-`, `A-`, … |
| coverage_officer | text | Sofia Martins / David Chen / Priya Nair |
| relationship_tier | text | Platinum / Gold / Silver |
| onboarded_on | date | relationship start |

The 9 clients: **Air Canada, Magna, BCE, Manulife, Brookfield, Suncor, CN Rail, Barrick Gold,
Cenovus** (all Canadian large-caps). Coverage: **Sofia** → AC, Magna · **David** → BCE, Manulife,
Brookfield · **Priya** → Suncor, CN, Barrick, Cenovus.

## 2. `instruments` — 18 rows
Tradable instruments across asset classes.

| column | type | notes |
|---|---|---|
| instrument_id | text | PK, e.g. `WTI` |
| asset_class | text | commodity / fx / rates / credit / equity |
| description | text | "WTI Crude Oil" |
| currency | text | native currency (`USD`, `CAD`, …) |
| contract_size | numeric | multiplier for notional |

## 3. `market_prices` — ~540 rows (18 instruments × ~30 days)
Daily spot price + annualized volatility per instrument.

| column | type | notes |
|---|---|---|
| instrument_id | text | FK → instruments |
| price_date | date | trading day |
| spot_price | numeric | close |
| volatility | numeric | annualized vol (0–1) |

PK: (`instrument_id`, `price_date`).

## 4. `positions` — ~76 rows
Current open positions. Air Canada holds only its hero energy book (large WTI + heating-oil long,
its own equity, a small USDCAD hedge) and is excluded from the random-position generation, so its
energy exposure dominates. Note only 8 of the 9 issuers have a listed equity instrument (Brookfield
trades via BN.TO elsewhere), so the "own equity" position is guarded.

| column | type | notes |
|---|---|---|
| position_id | serial | PK |
| client_id | text | FK → clients |
| instrument_id | text | FK → instruments |
| book | text | trading book |
| net_qty | numeric | signed quantity (long +, short −) |
| avg_entry_price | numeric | in the instrument's currency |
| as_of_date | date | snapshot date |

## 5. `trades` — ~600 rows (last 90 days)
Executed trades.

| column | type | notes |
|---|---|---|
| trade_id | serial | PK |
| client_id | text | FK → clients |
| instrument_id | text | FK → instruments |
| side | text | BUY / SELL |
| quantity | numeric | absolute quantity |
| price | numeric | fill price |
| trade_ts | timestamptz | execution time |
| trader | text | executing trader |
| notional_cad | numeric | CAD notional at fill |

---

## 6. `lb_client_reference` (Lakebase synced table — Ex6, Delta → Lakebase)
Exercise 6's reverse-ETL target. A curated Delta table `<catalog>.cm_features_<user>.client_reference`
(9 clients: `client_id` PK, `legal_name`, `sector`, `credit_rating`, `risk_limit_cad`,
`coverage_officer`, `updated_at`; PK + Change Data Feed) is **synced into the attendee's branch** as a
real Postgres table `lb_client_reference` via `w.postgres.create_synced_table`. Lakehouse-curated
reference/limits data, served at OLTP latency. Air Canada's `risk_limit_cad` is raised to
**300,000,000** and re-synced to demonstrate propagation.

## 7. `lb_positions`, `lb_limits` (Lakebase) → bronze Delta → SCD1 (Ex7, Lakebase → Delta)
Exercise 7's operational source + its lakehouse capture. Two `lb_*` tables the "trading app" writes on
the branch — `lb_positions` (`position_id` PK, `client_id`, `instrument_id`, `net_qty`, `book`,
`updated_at`) and `lb_limits` (`limit_id` PK, `client_id`, `limit_type`, `limit_cad`, `updated_at`) —
are watermark-extracted (Lakebase has **no** Delta-style CDF) into append-only bronze Delta
`<catalog>.cm_bronze_<user>.lb_*_changes`, then a Lakeflow **AUTO CDC SCD Type 1** pipeline produces
current-state tables `<catalog>.cm_scd_<user>.positions_current` / `limits_current` — one row per key,
latest wins. Demo: position 1's `net_qty` goes 250 → 900; bronze holds both versions,
`positions_current` holds only the latest.

---

## 8. `client_risk_features` (Delta, offline) → `client_risk_features_online` (Lakebase)
Exercise 4's feature table — **one row per client**, computed by aggregating positions + trades +
market_prices. Lives in Unity Catalog as a Delta table (primary key + Change Data Feed), then
published to the Lakebase project as an online table for low-latency lookup by the chatbot.

| feature | type | meaning |
|---|---|---|
| client_id | int/text | PK (feature key) |
| gross_notional_cad | double | Σ |net_qty × spot × contract_size × fx| across positions |
| net_exposure_cad | double | signed net exposure |
| num_open_positions | int | open positions |
| num_trades_30d | int | trades in last 30 days |
| avg_trade_notional_cad | double | mean trade notional, 30d |
| largest_position_pct | double | concentration — biggest position / gross |
| realized_pnl_30d_cad | double | realized PnL, last 30 days |
| portfolio_volatility | double | notional-weighted avg instrument vol |
| days_since_last_trade | int | recency |
| credit_rating_score | double | rating mapped to 1–10 |
| risk_tier | string | LOW / MEDIUM / HIGH (derived) |

**Hero client:** Air Canada (`CL-AC`) is seeded with a large WTI/heating-oil energy exposure so its
risk features are visibly elevated (HIGH tier) — the assistant's demo answer keys off this.

---

## 9. `checkpoint%` tables (Lakebase, LangGraph)
Exercise 5's persistent memory. `PostgresSaver.setup()` creates four tables — `checkpoints`,
`checkpoint_writes`, `checkpoint_blobs`, `checkpoint_migrations` — in the participant's schema. The
conversation state (messages) is stored as serialized blobs keyed by `thread_id`; the lab inspects
and de-serializes them to show exactly where the memory lives.

---

## How the exercises use the model

Numbers below are the exercise/folder numbers (`labs/0N-…`), in recommended flow order.

| # | Exercise (folder) | Reads | Writes |
|---|---|---|---|
| 1 | **Getting to know Lakebase** (`01-…`) | (UI walkthrough; light SQL) | `load_orders` (transient, autoscale demo) |
| 2 | **Authentication + data generation** (`02-…`) | — | `clients`, `instruments`, `market_prices`, `positions`, `trades` |
| 3 | **Data APIs — REST vs JDBC** (`03-…`) | `clients` (creates a demo one if absent) | — |
| 4 | **Online feature store + chatbot** (`04-…`) | the five Ex2 tables (or a UC-generated equivalent) | `client_risk_features` (Delta) → `client_risk_features_online` (Lakebase) |
| 5 | **Agentic memory** (`05-…`) | — | `checkpoint%` tables |
| 6 | **Delta → Lakebase sync** (`06-…`) | Delta `client_reference` | `lb_client_reference` (synced onto the branch) |
| 7 | **Lakebase → Delta, SCD1** (`07-…`) | `lb_positions`, `lb_limits` | bronze `lb_*_changes` (Delta) → `positions_current` / `limits_current` (SCD1 Delta) |

> **Note on where the feature-store exercise reads from.** Its offline feature computation runs in a
> Spark/serverless notebook and builds its own Delta source of the same capital-markets shape (so it's
> runnable even if a participant skipped Ex2). The prompt notes how to point it at the Ex2 Lakebase
> tables instead via a federated/JDBC read if you want the two exercises fully joined.
>
> **Standalone by design.** Exercises 3, 6 and 7 each create a small demo table on the branch if the
> expected source isn't present, so any exercise can be run on its own.
