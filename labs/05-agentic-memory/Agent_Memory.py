# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Exercise 4 — Agentic Memory on Lakebase
# MAGIC ### The Coverage Desk Assistant that *remembers*
# MAGIC
# MAGIC In Exercise 3 the Coverage Desk Assistant learned to **look up** a client's live risk
# MAGIC features. Here it gains **persistent memory**: a LangGraph agent whose conversation
# MAGIC state is checkpointed into the participant's **Lakebase** Postgres project, so the
# MAGIC assistant remembers what a coverage officer told it — across turns and across sessions.
# MAGIC
# MAGIC **This notebook is Genie-Code generated.** Paste the prompt in `README.md` into Genie
# MAGIC Code (Agent Mode) and it produces essentially this notebook. This file is the reviewed
# MAGIC reference so the whole room can compare against one known-good version.
# MAGIC
# MAGIC You will:
# MAGIC 1. Install LangGraph + the Postgres checkpointer.
# MAGIC 2. Wire up `ChatDatabricks` (with the Claude `temperature` fix).
# MAGIC 3. Point a LangGraph `PostgresSaver` at your Lakebase project.
# MAGIC 4. Build a one-node agent graph and prove memory persists across two turns.
# MAGIC 5. Inspect the four `checkpoint%` tables and de-serialize a blob to *see* the memory.

# COMMAND ----------

# MAGIC %pip install --quiet langgraph langgraph-checkpoint-postgres "psycopg[binary,pool]>=3.1.0" langchain-databricks "databricks-sdk>=0.118.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC The Lakebase target defaults to what `_setup` resolved (your project, `production`
# MAGIC branch, `primary` endpoint). Override the widgets only if your facilitator gave you a
# MAGIC different project/branch. `llm_endpoint` is a **pay-per-token** Foundation Model
# MAGIC serving endpoint — a Claude model is a good choice (it's what makes the `temperature`
# MAGIC fix below matter).

# COMMAND ----------

dbutils.widgets.text("project_id", PROJECT_ID, "Lakebase project id")
dbutils.widgets.text("branch", USER_BRANCH, "Branch (your own branch by default)")
dbutils.widgets.text("endpoint", "primary", "Endpoint")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5", "LLM serving endpoint")

BRANCH = dbutils.widgets.get("branch") or USER_BRANCH
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
print(f"Project:      {dbutils.widgets.get('project_id')}")
print(f"Branch:       {BRANCH}")
print(f"LLM endpoint: {LLM_ENDPOINT}")
print(f"Schema:       {PG_SCHEMA}  (checkpoint tables land here)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LLM — `ChatDatabricks` with the Claude `temperature` fix
# MAGIC
# MAGIC `ChatDatabricks` sends a default `temperature` in its request payload. Some models —
# MAGIC **Claude in particular** — reject `temperature` on the serving endpoint and the call
# MAGIC 400s. We subclass `ChatDatabricks` and override `_prepare_inputs` to **pop
# MAGIC `temperature`** out of the payload before it is sent. This keeps the notebook working
# MAGIC whether the endpoint is a Claude model or a Llama one.

# COMMAND ----------

from langchain_databricks import ChatDatabricks


class ChatDatabricksNoTemp(ChatDatabricks):
    """ChatDatabricks that strips `temperature` from the request payload.

    Claude serving endpoints reject the `temperature` field ChatDatabricks adds by
    default; popping it in `_prepare_inputs` makes the same code work across model
    families. `_prepare_inputs` is an internal hook — if a future langchain-databricks
    renames it, update the method name here (that's the only coupling point)."""

    def _prepare_inputs(self, *args, **kwargs):
        data = super()._prepare_inputs(*args, **kwargs)
        if isinstance(data, dict):
            data.pop("temperature", None)
        return data


llm = ChatDatabricksNoTemp(endpoint=LLM_ENDPOINT)
print("✓ LLM ready:", LLM_ENDPOINT)
print(llm.invoke("In one sentence, what does a capital markets coverage desk do?").content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Connect to Lakebase and create the checkpointer
# MAGIC
# MAGIC LangGraph's `PostgresSaver` needs a raw **psycopg (v3)** connection. We mint a fresh
# MAGIC OAuth database credential (a short-lived JWT) via the Databricks SDK and connect with
# MAGIC **keyword arguments** — never a URL/DSN string, because OAuth tokens contain `.`/`-`
# MAGIC characters that break URL parsing.
# MAGIC
# MAGIC The checkpointer connection is `autocommit=True` (PostgresSaver expects it) and pins
# MAGIC `search_path` to your schema so the four `checkpoint%` tables land in `cm_<user>`.
# MAGIC
# MAGIC > Naming gotcha: the SDK resource path uses **hyphens** in places (`databricks-postgres`),
# MAGIC > but the actual Postgres **database name** uses **underscores** (`databricks_postgres`).

# COMMAND ----------

import psycopg
from psycopg.rows import dict_row

# _setup gives us w, user_email, PG_DATABASE, PG_SCHEMA, and get_endpoint(); reuse them
# so this exercise connects the exact same way as every other lab.
ep = get_endpoint(BRANCH)
host = ep.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=ep.name)  # rotates ~hourly

checkpoint_conn = psycopg.connect(
    host=host,
    dbname=PG_DATABASE,             # "databricks_postgres" — underscores!
    user=user_email,                # your identity is the Postgres role
    password=cred.token,            # short-lived OAuth JWT
    sslmode="require",
    autocommit=True,                # required by PostgresSaver
    prepare_threshold=0,            # recommended for the checkpointer
    row_factory=dict_row,
    options=f"-c search_path={PG_SCHEMA},public",
)
print(f"✓ Connected to Lakebase: {host}")
print(f"  database={PG_DATABASE}  schema={PG_SCHEMA}")

# COMMAND ----------

from langgraph.checkpoint.postgres import PostgresSaver

checkpointer = PostgresSaver(checkpoint_conn)
checkpointer.setup()   # creates checkpoints / checkpoint_writes / checkpoint_blobs / checkpoint_migrations
print("✓ PostgresSaver.setup() complete — checkpoint tables created in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build the agent graph
# MAGIC
# MAGIC A minimal `StateGraph(MessagesState)` with one **agent** node that invokes the LLM on
# MAGIC the running message list. Compiling with the Lakebase-backed checkpointer is what makes
# MAGIC every turn persist: LangGraph reads prior state for the `thread_id`, appends the new
# MAGIC turn, and writes it back to Postgres.

# COMMAND ----------

from langgraph.graph import StateGraph, MessagesState, START, END

SYSTEM_PROMPT = (
    "You are the CIBC Capital Markets Coverage Desk Assistant. You help coverage "
    "officers and traders with their client books. Be concise and remember details "
    "the officer tells you about which clients they cover."
)


def agent_node(state: MessagesState):
    # Prepend a system message so the assistant keeps its persona each turn.
    from langchain_core.messages import SystemMessage
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
    response = llm.invoke(messages)
    return {"messages": [response]}


builder = StateGraph(MessagesState)
builder.add_node("agent", agent_node)
builder.add_edge(START, "agent")
builder.add_edge("agent", END)

agent = builder.compile(checkpointer=checkpointer)
print("✓ Agent compiled with Lakebase-backed memory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prove memory persists across turns
# MAGIC
# MAGIC A `thread_id` is the memory key. Turn 1 tells the assistant which clients this coverage
# MAGIC officer covers; Turn 2 — same `thread_id` — asks it to recall them. Because state is
# MAGIC checkpointed to Lakebase between calls, the second turn sees the first.

# COMMAND ----------

from langchain_core.messages import HumanMessage

THREAD = {"configurable": {"thread_id": "coverage-officer-sofia"}}

# Turn 1 — introduce a capital-markets fact.
turn1 = agent.invoke(
    {"messages": [HumanMessage(content="Hi — I'm Sofia Martins. On the desk I cover Air Canada and Suncor.")]},
    config=THREAD,
)
print("TURN 1 →", turn1["messages"][-1].content)
print("-" * 80)

# COMMAND ----------

# Turn 2 — same thread, no re-stating the fact. The assistant should recall it from Lakebase.
turn2 = agent.invoke(
    {"messages": [HumanMessage(content="Which clients do I cover again, and what sectors are they in?")]},
    config=THREAD,
)
print("TURN 2 →", turn2["messages"][-1].content)
assert "air canada" in turn2["messages"][-1].content.lower(), "Memory did not persist!"
print("\n✓ Memory persisted — the assistant recalled Air Canada / Suncor from Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: prove it survives a brand-new agent object
# MAGIC
# MAGIC Rebuild the agent (fresh Python objects) but point at the **same** checkpointer +
# MAGIC `thread_id`. It still remembers — the memory lives in Lakebase, not in the process.

# COMMAND ----------

agent2 = builder.compile(checkpointer=checkpointer)
recall = agent2.invoke(
    {"messages": [HumanMessage(content="Just my client tickers, comma-separated.")]},
    config=THREAD,
)
print("NEW AGENT OBJECT →", recall["messages"][-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspect the checkpoint tables
# MAGIC
# MAGIC `PostgresSaver.setup()` created four tables in your schema. Let's list them and peek at
# MAGIC one row. Blob columns are `bytea` — we show **byte length**, not raw bytes, so the
# MAGIC output stays readable.

# COMMAND ----------

with checkpoint_conn.cursor() as cur:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name LIKE 'checkpoint%%'
        ORDER BY table_name
        """,
        (PG_SCHEMA,),
    )
    checkpoint_tables = [r["table_name"] for r in cur.fetchall()]

print(f"Checkpoint tables in {PG_SCHEMA}:")
for t in checkpoint_tables:
    with checkpoint_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) AS n FROM "{PG_SCHEMA}"."{t}"')
        n = cur.fetchone()["n"]
    print(f"  • {t:24s} {n} rows")

# COMMAND ----------

# Peek at one checkpoints row, rendering bytea columns as their byte length.
with checkpoint_conn.cursor() as cur:
    cur.execute(f'SELECT * FROM "{PG_SCHEMA}".checkpoints LIMIT 1')
    row = cur.fetchone()

if row:
    print("Sample row from checkpoints:")
    for k, v in row.items():
        if isinstance(v, (bytes, bytearray, memoryview)):
            print(f"  {k:20s} <{len(bytes(v))} bytes>")
        else:
            print(f"  {k:20s} {v}")
else:
    print("No checkpoints yet — run the conversation cells above first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. See the memory — de-serialize a checkpoint blob
# MAGIC
# MAGIC The conversation is stored in `checkpoint_blobs` as serialized bytes tagged with a
# MAGIC type. LangGraph serializes with `JsonPlusSerializer`, so we can round-trip the bytes
# MAGIC back into Python objects with `serde.loads_typed((type, blob))` and find exactly which
# MAGIC row holds "Air Canada".

# COMMAND ----------

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

serde = JsonPlusSerializer()

with checkpoint_conn.cursor() as cur:
    # channel + type + blob are the columns that carry serialized channel values.
    cur.execute(
        f"""
        SELECT channel, type, blob
        FROM "{PG_SCHEMA}".checkpoint_blobs
        WHERE blob IS NOT NULL
        """
    )
    blob_rows = cur.fetchall()

print(f"{len(blob_rows)} blob rows in checkpoint_blobs. Searching for the remembered fact...\n")

NEEDLE = "air canada"
hits = 0
for r in blob_rows:
    blob_type = r["type"]
    raw = bytes(r["blob"])
    try:
        value = serde.loads_typed((blob_type, raw))
    except Exception as e:
        print(f"  channel={r['channel']:14s} type={blob_type:8s} (could not deserialize: {e})")
        continue
    text = str(value)
    if NEEDLE in text.lower():
        hits += 1
        snippet = text if len(text) < 300 else text[:300] + " ..."
        print(f"★ channel={r['channel']!r} type={blob_type!r} — CONTAINS the memory:")
        print(f"    {snippet}\n")
    else:
        print(f"  channel={r['channel']:14s} type={blob_type:8s} len(str)={len(text)}")

print(f"\n✓ Found the remembered fact in {hits} blob row(s). That is your agent's memory, in Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC The checkpoint tables are small and shared with nothing else, so you can leave them.
# MAGIC To reset your memory (drop all four tables), uncomment:

# COMMAND ----------

# for t in ["checkpoint_writes", "checkpoint_blobs", "checkpoints", "checkpoint_migrations"]:
#     with checkpoint_conn.cursor() as cur:
#         cur.execute(f'DROP TABLE IF EXISTS "{PG_SCHEMA}"."{t}" CASCADE')
# print("✓ Dropped checkpoint tables")

checkpoint_conn.close()
print("✓ Connection closed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC - A LangGraph agent whose **entire conversation state lives in Lakebase Postgres**.
# MAGIC - Memory that survives across turns *and* across new agent objects — because it's in
# MAGIC   the database, not the process. Swap the `thread_id` and you get a separate memory;
# MAGIC   reuse it (even from an app, tomorrow) and the assistant picks up where it left off.
# MAGIC - A concrete look at the `checkpoint%` tables and the serialized blobs that hold it.
# MAGIC
# MAGIC **That's a wrap on the workshop.** You've connected to Lakebase (Ex1–2), generated a
# MAGIC capital-markets dataset, served features to a chatbot from a Lakebase online store
# MAGIC (Ex3), and given that chatbot durable memory (Ex4) — all on one managed Postgres.