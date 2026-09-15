# Exercise 9 — Build Your Own: a Capital Markets Dashboard App (free-form)

**Track:** Capstone · free-form &nbsp;|&nbsp; **Build surface:** Databricks Apps + Genie Code &nbsp;|&nbsp;
**Prerequisite:** your data from Ex2 (and the `positions` / `limits` you've been mutating in Ex6–7)

Every exercise so far has been guided — same prompt, same reference, review together. **This one is
open-ended.** You ship a real, interactive **Databricks App**: a **Plotly Dash** capital-markets
dashboard that reads *live* from your Lakebase branch. Design it however you like — or, if you'd
rather not do the prompt engineering, paste the **reference prompt** at the bottom into Genie Code
and iterate from there.

There's no single "right" notebook to diff against here. The point is to prove you can take Lakebase
from *"managed Postgres"* to *"the database behind a running application"* — which is the whole reason
it exists.

## What you'll learn
- **Databricks Apps** — serverless, managed web apps that deploy straight from the workspace, and how
  each app runs as its **own service principal**.
- Connecting an app to Lakebase with **psycopg (v3)** and a short-lived **OAuth database credential**
  (`generate_database_credential`) — the app authenticates **directly as its SP's Postgres role**.
- The **service-principal role + `GRANT`** pattern that gives an app read access to your schemas.
- Turning the CM dataset into an interactive, multi-tab **Plotly Dash** dashboard.

## The app, outlined (what a good build looks like)
You don't have to match this exactly — it's the target shape.

- **Data sources** — your two schemas: `cm_<you>` (`clients`, `instruments`, `trades`, `positions`,
  `market_prices`) and `cm_<you>_ops` (`limits`).
- **Connection** — psycopg v3 (not SQLAlchemy); a tiny `_q(conn, sql) -> DataFrame` helper; auth via
  `w.postgres.generate_database_credential(endpoint=…).token` with the user =
  `os.environ["DATABRICKS_CLIENT_ID"]` (the app SP) or your identity when run locally; cache the
  startup load with `@lru_cache`.
- **Enrichment at load** — coerce numeric columns to float; derive `trade_date` / `trade_week` /
  `trade_hour`; join trades↔clients↔instruments; join positions↔clients↔instruments↔latest price↔
  limits; compute `market_value`, `gross_exposure`, and `pnl_cad`.
- **Look** — a dark theme applied through one `_dark()` figure helper (consistent bg / grid / font /
  margins), a `_fmt()` money formatter (B/M/K), and an `_empty_fig()` "No data" state.
- **Layout** — a header, a **global filter bar** (client, sector, asset class, tier, desk/book, date
  range), and **five tabs**:
  | Tab | The story it tells |
  |---|---|
  | **Overview** | KPI cards (notional, trades, active clients, buy/sell flow, gross exposure) + daily flow, mix by asset class, top traders, sector treemap |
  | **Trading** | client × asset-class heatmap, cumulative buy vs sell, quantity-vs-price scatter, trades-by-hour |
  | **Positions** | gross exposure by client, unrealised P&L by desk, exposure sunburst, long vs short |
  | **Market** | price line + implied-vol + returns distribution for one instrument, and a vol comparison |
  | **Clients** | top clients, notional by credit rating, relationship-tier mix, P&L by client |
- **Ship files** — `app.py`, `requirements.txt` (dash / plotly / pandas / numpy / psycopg[binary] /
  databricks-sdk), and `app.yaml` (`command: ["python", "app.py"]`); listen on `DATABRICKS_APP_PORT`.

## Lakebase setup — do this first (grant the app's service principal)
A Databricks App connects to Lakebase **as its own service principal**, and it connects **directly over
the Postgres wire** (psycopg) — exactly like the JDBC path in Exercise 3. So it authenticates *as* the
SP's Postgres role; there is **no `authenticator` / Data API grant** to worry about here (that whole
dance from Ex3 only applies to the REST Data API). You just need two things: a Postgres **role for the
app SP**, and **read grants** on your schemas.

You have `CREATEROLE`, so you can do this yourself — in the **SQL Editor** on **your branch**:

```sql
CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Create a Postgres login role for the app's service principal (use its application/client UUID).
SELECT databricks_create_role('<app-sp-client-id>', 'SERVICE_PRINCIPAL');

-- Let the app read your two schemas.
GRANT USAGE ON SCHEMA cm_<you>       TO "<app-sp-client-id>";
GRANT USAGE ON SCHEMA cm_<you>_ops   TO "<app-sp-client-id>";
GRANT SELECT ON ALL TABLES IN SCHEMA cm_<you>     TO "<app-sp-client-id>";
GRANT SELECT ON ALL TABLES IN SCHEMA cm_<you>_ops TO "<app-sp-client-id>";
```

> **Where does the app SP come from?** Create the app first (`databricks apps create cm-trading-dashboard`
> or the **Apps** UI) — Databricks provisions a service principal for it and shows its **client ID**.
> Use that UUID above. Then add your Lakebase branch as an app **resource** (or pass the project/branch
> as env vars) so the app can reach it.

## Deploy it
1. Put `app.py`, `requirements.txt`, and `app.yaml` in this folder (or let Genie Code scaffold them).
2. `databricks apps deploy cm-trading-dashboard --source-code-path <path-to-this-folder>` (or deploy
   from the **Apps** UI). The app installs `requirements.txt`, runs `python app.py`, and serves on
   `DATABRICKS_APP_PORT`.
3. Open the app URL. It mints a DB credential as the app SP, loads + caches your data, and renders the
   five tabs.

## Free-form challenge — make it yours
Pick your own angle before reaching for the reference prompt:
- A **coverage-officer** view: one client, their positions, limit utilisation, recent trades, and the
  Ex4 risk features side by side.
- A **risk desk** view: limit breaches (`gross_exposure` vs `limit_cad`), largest movers since the last
  mark, concentration by sector.
- A **market** view driven by `market_prices`: price + implied-vol + returns for any instrument.
- Or the full multi-tab dashboard outlined above.

Write your **own** Genie Code prompt for it — being specific about schema, columns, colours, and chart
types is exactly the skill this exercise builds. Only if you'd rather skip that, use the reference
prompt below.

## ✓ Done when
- The app is **deployed and reachable**, and its first screen shows **your** data (not an error or an
  empty shell).
- Its service principal can read `cm_<you>` and `cm_<you>_ops` (the grants above are in place).
- Filtering (client / sector / date / …) updates the charts, and empty filter results show a graceful
  **"No data"** state rather than a stack trace.

---

<details>
<summary><b>Reference prompt</b> — paste into Genie Code if you'd rather not engineer your own</summary>

> **First, substitute your own coordinates:** replace `cibc-cm-workshop-new` with your
> `SHARED_PROJECT_ID`, `ron-guerrero` with **your** branch, and `cm_ron_guerrero` /
> `cm_ron_guerrero_ops` with **your** `cm_<you>` / `cm_<you>_ops` schemas. (The values below are just a
> worked example.)

```text
Create a Databricks App called cm-trading-dashboard that is a Plotly Dash capital markets dashboard
connected to Lakebase. Use the Lakebase project cibc-cm-workshop-new, branch ron-guerrero, endpoint
primary, database databricks_postgres. The data lives in two Postgres schemas: cm_ron_guerrero (tables:
clients, instruments, trades, positions, market_prices) and cm_ron_guerrero_ops (table: limits).

Lakebase setup requirements:
- Create a Lakebase role for the app's service principal (identity type SERVICE_PRINCIPAL) on the
  ron-guerrero branch.
- GRANT USAGE and SELECT on both schemas (cm_ron_guerrero and cm_ron_guerrero_ops) to the app's service
  principal client ID.

Connection pattern: Use psycopg (v3) directly -- not SQLAlchemy. Use a custom _q(conn, sql) helper that
runs cur.execute(sql), reads cur.description for column names, and returns a pd.DataFrame. Authenticate
via w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME).token, with user =
os.environ.get("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name. Cache data at startup with
@lru_cache(maxsize=1).

Data enrichment at load time:
- Coerce all numeric columns (contract_size, quantity, price, notional_cad, net_qty, avg_entry_price,
  spot_price, volatility, limit_cad) to float via pd.to_numeric.
- Derive trade_date, trade_week, trade_hour from trade_ts.
- Join trades with clients and instruments.
- Join positions with clients, instruments, latest market prices (last price per instrument), and limits.
- Compute market_value = net_qty * latest_price * contract_size, gross_exposure = abs(market_value),
  pnl_cad = net_qty * (latest_price - avg_entry_price) * contract_size.

Theme: Dark theme with these exact hex colours: background #020617, card #0f172a, border #1e293b, grid
#1e293b, text #e2e8f0, muted #94a3b8, accent blue #38bdf8, green #34d399, amber #f59e0b, purple #a78bfa,
red #f87171, orange #fb923c. Font family: 'Inter','Segoe UI',Arial,sans-serif. Every figure gets a
_dark() helper that sets paper_bgcolor, plot_bgcolor, font_color, title_font_color, legend_font_color,
margins (l=48, r=16, t=56, b=40), height 370 default, and gridcolor on both axes.

Layout structure:
- Header: title "CIBC CM | Capital Markets Dashboard", subtitle "Live from Lakebase ·
  cibc-cm-workshop-new / ron-guerrero".
- Global filter bar (6-column grid): Client (multi-dropdown), Sector (multi-dropdown), Asset class
  (multi-dropdown), Tier (multi-dropdown), Desk/book (multi-dropdown), Date range picker. Uppercase
  labels, 12px, muted colour.
- 5 tabs: Overview, Trading, Positions, Market, Clients. Tab style: dark background, muted text, no
  border; selected tab has text colour #e2e8f0 and a 2px blue bottom border.

Overview tab (7 filters -> 5 outputs):
- 6 KPI cards in a row: Total notional, Trade count, Active clients, Buy flow, Sell flow, Gross
  exposure. Each card has uppercase 12px muted label, 26px bold coloured value, rounded card with
  shadow. Use a _fmt() helper that formats to B/M/K with $ prefix.
- Daily traded notional: grouped bar by side (BUY=blue, SELL=orange).
- Notional by asset class: donut chart (hole=0.45, Set2 palette).
- Top traders by notional: horizontal bar (purple), text annotation showing trade count inside bars.
- Sector/asset class breakdown: treemap with Blues continuous scale.

Trading tab (7 filters -> 4 outputs):
- Client x asset class heatmap: pivot table of notional, px.imshow with YlOrRd scale.
- Cumulative notional buy vs sell: area chart, sorted by trade_ts, cumsum per side group.
- Trade scatter: quantity vs price, coloured by asset class, sized by notional_cad, hover shows
  instrument/trader/side.
- Trade count by hour (UTC): histogram with 24 bins, green, bargap 0.08.

Positions tab (5 filters, no date -> 4 outputs):
- Gross exposure by client: horizontal bar, top 12, Teal continuous scale.
- Unrealised P&L by desk: vertical bar coloured green (>=0) or red (<0) per bar using go.Bar with
  marker_color list.
- Exposure sunburst: drill-down path [book, legal_name, instrument_id], values=gross_exposure, Pastel
  palette.
- Long vs short by desk: grouped bar, direction determined by np.where(net_qty >= 0, "Long", "Short"),
  Long=green, Short=red.

Market tab (asset class filter + instrument dropdown -> 4 outputs):
- Instrument dropdown cascaded from asset class filter (auto-selects first).
- Price line: single instrument, markers, blue.
- Implied volatility: area chart, amber line with fillcolor="rgba(245,158,11,0.15)".
- Daily returns distribution: pct_change() * 100, histogram 25 bins, purple, no legend.
- Implied vol comparison: all in-scope instruments, horizontal bar, top 15, Inferno continuous scale.

Clients tab (7 filters -> 4 outputs):
- Top clients by notional: horizontal bar, top 15, Viridis continuous scale.
- Notional by credit rating: vertical bar ordered AAA through BB-, Set3 palette, sorted by a custom
  rating_order map.
- Relationship tier donut: hole=0.45, Platinum=blue, Gold=amber, Silver=muted.
- Unrealised P&L by client: horizontal bar, green/red per bar, height 400.

requirements.txt: dash==2.18.2, plotly==5.24.1, pandas==2.2.2, numpy==1.26.4, psycopg[binary]==3.2.3,
databricks-sdk>=0.118.0

app.yaml: command: ["python", "app.py"]

Use suppress_callback_exceptions=True. Every chart shows a grey "No data" annotation via _empty_fig()
when the filtered DataFrame is empty. Listen on DATABRICKS_APP_PORT (default 8000), host 0.0.0.0,
debug=False.
```

</details>

## Docs
- Databricks Apps: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/
- Lakebase from an app (OAuth DB credential): https://learn.microsoft.com/en-us/azure/databricks/oltp/
- Plotly Dash: https://dash.plotly.com/
