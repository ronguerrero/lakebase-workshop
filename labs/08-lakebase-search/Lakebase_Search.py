# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Exercise 8 — Lakebase Search: earnings-call PDFs → pgvector → an agent
# MAGIC
# MAGIC **Persona:** a CIBC Capital Markets analyst covering names they hold or are considering (Air
# MAGIC Canada, Suncor, BCE). They pile up **earnings-call PDFs** and want to *ask questions* across them.
# MAGIC **Lakebase feature:** **`pgvector`** — Lakebase is Postgres, so it doubles as your **vector store**.
# MAGIC No separate vector database: the embeddings live right next to your operational data, governed and
# MAGIC queried with plain SQL (`<=>` cosine distance).
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Generate a few synthetic **earnings-call transcripts** and write them as **PDFs** to a UC Volume.
# MAGIC 2. **Parse** the PDFs (pypdf), **chunk** the text, and **embed** each chunk with a Databricks
# MAGIC    Foundation Model embeddings endpoint.
# MAGIC 3. Store the vectors in **Lakebase** (`pgvector`) on your branch, with an HNSW index.
# MAGIC 4. Do a **similarity search** over Lakebase, then wrap it in a **LangChain agent** whose retrieval
# MAGIC    *tool* queries Lakebase — a Coverage Research Assistant that answers with citations.
# MAGIC
# MAGIC > This is the **reference solution** — what the Genie Code prompt in `README.md` should produce.
# MAGIC > Everything is synthetic and self-contained; no real filings are used.

# COMMAND ----------

# MAGIC %pip install "pypdf>=4.0" "reportlab>=4.0" "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0" "mlflow" "langchain>=0.3,<0.4" "langchain-databricks" --quiet
# MAGIC # langchain pinned <0.4 for the same reason as Ex4 (langchain-databricks requires langchain-core <0.4).

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# Embeddings endpoint: a Databricks pay-per-token FM embeddings model. Name varies by region —
# list what's served with w.serving_endpoints.list(). The dimension is inferred at runtime (below),
# so you don't have to hardcode it. LLM endpoint is a widget (as in Ex4/Ex5).
EMB_ENDPOINT = "databricks-gte-large-en"     # or databricks-bge-large-en; swap to what your region serves
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5", "LLM serving endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")

# A UC Volume (in your own cm_<user> schema) holds the source PDFs — governed file storage, no DBFS.
VOLUME = "earnings_pdfs"
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME}"
CHUNKS_TABLE = f"{PG_SCHEMA}.earnings_chunks"   # the pgvector table lives on YOUR Lakebase branch

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
except Exception as _e:
    print(f"(using pre-provisioned schema {UC_CATALOG}.{UC_SCHEMA}: {str(_e)[:80]})")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{VOLUME}")

print(f"Embeddings endpoint: {EMB_ENDPOINT}")
print(f"LLM endpoint:        {LLM_ENDPOINT}")
print(f"PDF volume:          {VOLUME_PATH}")
print(f"Vector table:        {CHUNKS_TABLE}  (Lakebase / pgvector)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate synthetic earnings-call PDFs
# MAGIC
# MAGIC Three short transcripts for hero names. Air Canada leans on its **fuel / energy exposure** — the
# MAGIC same thread that runs through the feature-store and SCD1 exercises — so the agent has something
# MAGIC concrete (and on-brand) to find.

# COMMAND ----------

import os

TRANSCRIPTS = {
    "Air Canada": """Air Canada (AC) — Q3 Earnings Call Transcript

CEO remarks: Operating revenue rose 9% year over year on strong transatlantic demand and premium-cabin
mix. Load factors held above 85%. We continue to invest in fleet renewal to improve fuel efficiency.

CFO remarks: Adjusted EBITDA margin expanded 180 bps. Our single largest cost sensitivity remains jet
fuel. We hedge a portion of consumption but a sustained rise in WTI crude and heating-oil-linked
distillate prices would pressure unit costs. We estimate a 10% move in jet fuel shifts annual fuel
expense by roughly CAD 300 million.

Q&A: Analyst asked about energy exposure. CFO: our fuel book is our dominant commodity risk; we manage
it with a rolling hedge program and capacity discipline. Analyst asked about capital allocation. CFO:
priority is deleveraging, then fleet, then buybacks.
""",
    "Suncor Energy": """Suncor Energy (SU) — Q3 Earnings Call Transcript

CEO remarks: Upstream production reached record levels this quarter. Oil sands reliability improved and
downstream refining utilization was strong. Higher WTI benchmark prices supported cash flow.

CFO remarks: Funds from operations rose sharply with the move in crude. We returned excess cash to
shareholders via dividends and buybacks. Our breakeven WTI continues to fall on cost discipline.

Q&A: Analyst asked about commodity price sensitivity. CFO: every USD 1 per barrel change in WTI moves
annual funds from operations by several hundred million dollars; we are structurally long crude.
""",
    "BCE Inc.": """BCE Inc. (BCE) — Q3 Earnings Call Transcript

CEO remarks: Wireless net additions were solid and broadband fibre rollout continued on plan. Media
advertising remained soft. We reiterated our full-year guidance.

CFO remarks: Free cash flow supported the dividend. Capital intensity is trending down as the fibre
build matures. Interest expense rose with higher rates, a key sensitivity for a capital-intensive
telecom. We have limited direct commodity exposure; energy is a modest input cost, not a P&L driver.

Q&A: Analyst asked about the dividend. CFO: the dividend remains a priority and is well covered by
free cash flow over our planning horizon.
""",
}


def write_pdf(path, title, body):
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    c = canvas.Canvas(path, pagesize=letter)
    width, height = letter
    y = height - inch
    c.setFont("Helvetica-Bold", 13)
    c.drawString(inch, y, title)
    y -= 0.4 * inch
    c.setFont("Helvetica", 10)
    for line in body.splitlines():
        for wrapped in _wrap(line, 95):
            if y < inch:
                c.showPage(); c.setFont("Helvetica", 10); y = height - inch
            c.drawString(inch, y, wrapped)
            y -= 0.22 * inch
    c.save()


def _wrap(line, n):
    if not line:
        return [""]
    words, out, cur = line.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > n:
            out.append(cur); cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        out.append(cur)
    return out


written = []
for company, text in TRANSCRIPTS.items():
    fname = company.lower().replace(" ", "_").replace(".", "") + "_q3_call.pdf"
    path = f"{VOLUME_PATH}/{fname}"
    write_pdf(path, f"{company} — Q3 Earnings Call", text)
    written.append((company, path))
    print(f"✓ wrote {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse the PDFs and chunk the text
# MAGIC pypdf reads each PDF back to text; we split into overlapping character windows so a passage keeps
# MAGIC enough context to be individually meaningful.

# COMMAND ----------

from pypdf import PdfReader


def pdf_to_text(path):
    reader = PdfReader(path)
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def chunk_text(text, size=700, overlap=120):
    text = " ".join(text.split())          # normalize whitespace
    chunks, start = [], 0
    while start < len(text):
        chunks.append(text[start:start + size])
        start += size - overlap
    return [c for c in chunks if c.strip()]


docs = []   # (company, source, chunk_idx, content)
for company, path in written:
    text = pdf_to_text(path)
    for i, ch in enumerate(chunk_text(text)):
        docs.append((company, os.path.basename(path), i, ch))
print(f"✓ parsed {len(written)} PDFs → {len(docs)} chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Embed the chunks
# MAGIC We call the Databricks FM **embeddings** endpoint via the MLflow deploy client (the same client
# MAGIC Ex4 uses for feature serving). The vector **dimension is read from the first embedding**, so the
# MAGIC Lakebase table matches whatever model your region serves.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

deploy = get_deploy_client("databricks")


def embed(texts):
    """Return a list of embedding vectors for a list of strings."""
    resp = deploy.predict(endpoint=EMB_ENDPOINT, inputs={"input": texts})
    return [d["embedding"] for d in resp["data"]]


# batch the chunk embeddings (keep batches modest for the endpoint)
vectors = []
BATCH = 16
for i in range(0, len(docs), BATCH):
    vectors.extend(embed([d[3] for d in docs[i:i + BATCH]]))
EMB_DIM = len(vectors[0])
print(f"✓ embedded {len(vectors)} chunks — dimension {EMB_DIM} (from {EMB_ENDPOINT})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Store the vectors in Lakebase (`pgvector`)
# MAGIC Enable the `vector` extension, create the table on **your branch**, insert the chunks + embeddings,
# MAGIC and build an **HNSW** cosine index. This is the "Lakebase search" idea: your vector store *is* your
# MAGIC Postgres — one governed instance for operational data **and** retrieval.

# COMMAND ----------


def vec_literal(v):
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


conn = get_connection()   # your branch
with conn.cursor() as cur:
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(
            f"Could not enable pgvector ({str(e)[:120]}). Confirm the `vector` extension is available "
            "in your Lakebase project (it usually is), or ask your facilitator.") from e

    cur.execute(f"DROP TABLE IF EXISTS {CHUNKS_TABLE}")
    cur.execute(f"""
        CREATE TABLE {CHUNKS_TABLE} (
            id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            company    TEXT NOT NULL,
            source     TEXT NOT NULL,
            chunk_idx  INTEGER NOT NULL,
            content    TEXT NOT NULL,
            embedding  vector({EMB_DIM}) NOT NULL
        )
    """)
    for (company, source, idx, content), v in zip(docs, vectors):
        cur.execute(
            f"INSERT INTO {CHUNKS_TABLE} (company, source, chunk_idx, content, embedding) "
            f"VALUES (%s, %s, %s, %s, %s::vector)",
            (company, source, idx, content, vec_literal(v)),
        )
    conn.commit()

    # HNSW cosine index — no training step (unlike ivfflat). Falls back gracefully on older pgvector.
    try:
        cur.execute(f"CREATE INDEX IF NOT EXISTS earnings_chunks_emb_idx "
                    f"ON {CHUNKS_TABLE} USING hnsw (embedding vector_cosine_ops)")
        conn.commit()
        print("✓ HNSW cosine index created")
    except Exception as e:
        conn.rollback()
        print(f"(no HNSW index — {str(e)[:80]}; small table, sequential scan is fine)")

with conn.cursor() as cur:
    cur.execute(f"SELECT COUNT(*) AS n FROM {CHUNKS_TABLE}")
    print(f"✓ stored {cur.fetchone()['n']} chunks in Lakebase ({CHUNKS_TABLE})")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Similarity search over Lakebase
# MAGIC Embed the query, then let Postgres rank by cosine distance (`<=>`). This is the whole retrieval
# MAGIC engine — a single SQL statement against your branch.

# COMMAND ----------


def vector_search(query, k=4):
    qv = vec_literal(embed([query])[0])
    c = get_connection()
    try:
        with c.cursor() as cur:
            cur.execute(
                f"SELECT company, source, chunk_idx, content, "
                f"       1 - (embedding <=> %s::vector) AS score "
                f"FROM {CHUNKS_TABLE} ORDER BY embedding <=> %s::vector LIMIT %s",
                (qv, qv, k),
            )
            return cur.fetchall()
    finally:
        c.close()


for hit in vector_search("What is Air Canada's exposure to fuel and energy prices?"):
    print(f"[{hit['score']:.3f}] {hit['company']} — {hit['content'][:130]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wrap it in an agent — the Coverage Research Assistant
# MAGIC The agent gets one **tool** that searches Lakebase. The LLM decides *when* to call it, reads the
# MAGIC returned passages, and answers **with citations**. (Same `ChatDatabricks` temperature fix as Ex4/Ex5
# MAGIC — Claude endpoints reject the `temperature` field the client adds by default.)

# COMMAND ----------

from langchain_databricks import ChatDatabricks
from langchain.tools import tool
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate


class ChatDatabricksNoTemp(ChatDatabricks):
    """ChatDatabricks that drops the `temperature` kwarg some Claude endpoints reject."""
    def _prepare_inputs(self, *args, **kwargs):
        inputs = super()._prepare_inputs(*args, **kwargs)
        inputs.pop("temperature", None)
        return inputs


@tool
def search_earnings_calls(query: str) -> str:
    """Search the earnings-call transcripts stored in Lakebase for passages relevant to the query.
    Use this whenever the user asks about what a company said on its earnings call. Returns the most
    relevant excerpts, each tagged with its company and source file."""
    hits = vector_search(query, k=4)
    if not hits:
        return "No relevant passages found."
    return "\n\n".join(
        f"[{h['company']} · {h['source']}] {h['content']}" for h in hits
    )


llm = ChatDatabricksNoTemp(endpoint=LLM_ENDPOINT, max_tokens=700)
prompt = ChatPromptTemplate.from_messages([
    ("system",
     "You are a CIBC Capital Markets Coverage Research Assistant. Answer questions about company "
     "earnings calls using ONLY the search_earnings_calls tool. Cite the company (and source file) for "
     "each claim, and if the transcripts don't cover something, say so plainly."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])
agent = create_tool_calling_agent(llm, [search_earnings_calls], prompt)
executor = AgentExecutor(agent=agent, tools=[search_earnings_calls], verbose=True)
print("✓ Coverage Research Assistant ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ask it questions
# MAGIC Watch it call the Lakebase-backed tool, then answer with citations.

# COMMAND ----------

for q in [
    "What did Air Canada say about its fuel and energy exposure?",
    "Which of these companies is structurally long crude oil, and which has little commodity exposure?",
]:
    print("\n" + "=" * 90 + f"\nQ: {q}\n")
    print(executor.invoke({"input": q})["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✓ Checks & takeaways
# MAGIC - The `earnings_chunks` table lives in **Lakebase** (`pgvector`) on your branch — your vector store
# MAGIC   is just Postgres, next to your operational data.
# MAGIC - Retrieval is a **single SQL query** (`<=>` cosine distance), governed and transactional like the
# MAGIC   rest of your data.
# MAGIC - The **agent** decides when to search and answers with citations — the RAG pattern, powered by
# MAGIC   Lakebase search.
# MAGIC
# MAGIC ## Cleanup
# MAGIC The `earnings_chunks` table lives on your branch (dropping the branch clears it). Optionally drop
# MAGIC the UC Volume `{UC_CATALOG}.{UC_SCHEMA}.earnings_pdfs`.