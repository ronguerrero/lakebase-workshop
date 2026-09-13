# Exercise 4 — Agentic Memory on Lakebase

**Persona:** the CIBC **Coverage Desk Assistant** from Exercise 3 — now with a memory.
**Build surface:** Genie Code (Agent Mode). **Prerequisite:** your Lakebase project (Ex1–2).
**Time:** ~25 min.

A production assistant has to remember what a coverage officer told it — not just within a
chat, but across sessions. LangGraph gives an agent a **checkpointer**; point that
checkpointer at **Lakebase** (managed Postgres) and the agent's memory becomes durable,
governed, and shared across every process that connects. This exercise builds exactly that
and then opens the hood so you can *see* the memory sitting in Postgres.

## What you'll learn
- How a LangGraph `PostgresSaver` turns a Lakebase project into agent memory.
- The four `checkpoint%` tables LangGraph creates, and what each holds.
- Two Lakebase-specific gotchas every agent-on-Databricks build hits (below).
- How to de-serialize a checkpoint blob to prove where the conversation is stored.

## How to run it (Genie Code)
Open a **new Genie Code chat** (Agent Mode), make sure you're on a notebook attached to
**serverless** (or DBR ML), and paste the prompt below **verbatim**. Genie Code will scaffold
the notebook, install the packages, and run the cells. Compare what it produces against the
reviewed reference in [`Agent_Memory.py`](./Agent_Memory.py) — they should match closely.

> One fresh chat per exercise keeps Genie Code's context small. Paste, glance at the plan,
> let it run — don't tell it to "confirm."

### The prompt

```text
Create a Databricks notebook (Python, serverless) that builds a LangGraph agent with
persistent conversation memory backed by my Lakebase Postgres project. Call it the "CIBC
Capital Markets Coverage Desk Assistant." Build it exactly as follows:

1. First cell: %pip install --quiet langgraph langgraph-checkpoint-postgres
   "psycopg[binary,pool]>=3.1.0" langchain-databricks "databricks-sdk>=0.118.0"
   then in the next cell dbutils.library.restartPython().

2. Use the Databricks SDK (WorkspaceClient) to discover my Lakebase connection. Get MY OWN
   branch's 'primary' endpoint (my branch is my username, e.g. sofia-martins — NOT production)
   via w.postgres.list_endpoints / w.postgres.get_endpoint, read the host from
   endpoint.status.hosts.host, and mint an
   OAuth token with w.postgres.generate_database_credential(endpoint=<endpoint name>).
   Connect with psycopg (v3) using KEYWORD ARGUMENTS ONLY — never a URL/DSN string,
   because the OAuth token has characters that break URL parsing. Use dbname
   "databricks_postgres" (underscores — the SDK resource path uses hyphens but the real
   database name uses underscores), user = my workspace username, password = the token,
   sslmode="require", autocommit=True, prepare_threshold=0, row_factory=dict_row, and
   options set so search_path is my per-user schema then public.

3. LLM: use ChatDatabricks from langchain_databricks against a pay-per-token Claude
   endpoint (e.g. "databricks-claude-sonnet-4"). Claude endpoints reject the temperature field that
   ChatDatabricks sends by default, so subclass ChatDatabricks and override
   _prepare_inputs to pop "temperature" from the payload dict before it is sent. Use the
   subclass as the LLM.

4. Create a langgraph.checkpoint.postgres.PostgresSaver from that psycopg connection and
   call checkpointer.setup() to create the checkpoint tables in Lakebase.

5. Build a StateGraph(MessagesState) with a single "agent" node that prepends a coverage-
   desk system prompt and invokes the LLM, then compile it with the checkpointer.

6. Prove memory works: with config {"configurable": {"thread_id": "coverage-officer-sofia"}},
   Turn 1 send "Hi — I'm Sofia Martins. On the desk I cover Air Canada and Suncor.", then
   Turn 2 (same thread_id) send "Which clients do I cover again?" and show it recalls them.

7. Inspect: query information_schema.tables for tables in my schema matching 'checkpoint%'
   and print each with its row count. Show one checkpoints row, printing bytea columns as
   their byte length instead of raw bytes.

8. De-serialize the memory: read checkpoint_blobs, and for each non-null blob use
   JsonPlusSerializer from langgraph.checkpoint.serde.jsonplus via
   serde.loads_typed((type, blob)) to round-trip it back to Python, then find which row
   contains "Air Canada" — that row is the stored conversation memory.
```

## The two gotchas (why the reference notebook looks the way it does)

1. **Claude rejects `temperature`.** `ChatDatabricks` puts a default `temperature` in its
   request payload; Claude serving endpoints 400 on it. The fix is a tiny subclass that pops
   `temperature` in `_prepare_inputs`. (If you point `llm_endpoint` at a Llama model instead,
   the subclass is harmless.)
2. **Connect with keyword args, not a URL.** The Lakebase password is an OAuth JWT full of
   `.` and `-`; passing it inside a DSN/URL string (the `user:password@host` part of a
   `postgresql://` URL) makes psycopg mis-parse it. Always pass `host=`, `dbname=`, `user=`,
   `password=` to `psycopg.connect(...)` as separate keyword arguments.

Plus a naming trap: the SDK resource path uses hyphens in places (`databricks-postgres`) but
the **database name** is `databricks_postgres` (underscores).

## The four `checkpoint%` tables

| Table | Holds |
|---|---|
| `checkpoints` | one row per saved state snapshot for a `thread_id` (the checkpoint metadata) |
| `checkpoint_writes` | the pending writes applied to reach a checkpoint |
| `checkpoint_blobs` | the serialized channel values — **this is where the messages live** |
| `checkpoint_migrations` | the checkpointer's own schema version bookkeeping |

`thread_id` is the memory key: reuse it to continue a conversation (even from a different
process, or an app, tomorrow); change it for a fresh memory.

## What's next
That's the capstone. You've now used one Lakebase project for OLTP data (Ex2), low-latency
feature serving (Ex3), and durable agent memory (Ex4). To take it further: promote this agent
to a **Databricks App** (its service principal gets a Lakebase role + least-privilege grant),
add a **long-term** memory table (a `pgvector` store recalled per user) alongside the
short-term checkpoints, and wire in the Ex3 feature-lookup tool so the assistant both
*remembers* and *looks up* in one conversation.
