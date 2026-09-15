# Exercise 8 — Lakebase Search: earnings-call PDFs → pgvector → an agent

**Track:** Retrieval / agents · **Prerequisite:** your Lakebase branch (Ex1–2) · **Notebook-first**
(Genie prompt below)

A CIBC Capital Markets analyst covering names they hold or are eyeing (Air Canada, Suncor, BCE) piles
up **earnings-call PDFs** and wants to *ask questions* across them. This exercise builds that: parse
the PDFs, embed the text, store the vectors in **Lakebase** with **`pgvector`**, and put a **LangChain
agent** in front whose retrieval *tool* queries Lakebase — a Coverage Research Assistant that answers
with citations.

The point: **Lakebase is Postgres, so it's also your vector store.** No separate vector database — the
embeddings sit next to your operational data, governed and queried with plain SQL (`<=>` cosine
distance). "Lakebase search" = retrieval where your data already lives.

## What you'll learn
- **`pgvector` in Lakebase** — enabling the extension, a `vector(N)` column, an HNSW cosine index, and
  similarity search with `ORDER BY embedding <=> query`.
- **A PDF RAG pipeline** — parse (pypdf) → chunk → embed (Databricks FM embeddings endpoint) → store.
- **An agent** — a LangChain tool-calling agent whose one tool searches Lakebase; the LLM decides when
  to retrieve and answers with citations.

## Run it (notebook-first)
Open `Lakebase_Search.py` on serverless and run top to bottom. It:
1. Generates synthetic earnings-call transcripts and writes them as **PDFs to a UC Volume**
   (`{catalog}.cm_<you>.earnings_pdfs`).
2. Parses the PDFs, chunks the text, and **embeds** each chunk (dimension inferred from the endpoint).
3. Stores the chunks + vectors in **Lakebase** (`cm_<you>.earnings_chunks`, `pgvector`) with an HNSW
   index.
4. Runs a similarity search, then wraps it in the **agent** and asks a couple of questions.

### Endpoints
- **Embeddings:** `EMB_ENDPOINT` (default `databricks-gte-large-en`) — a pay-per-token FM embeddings
  model. Names vary by region; list what's served with `w.serving_endpoints.list()`.
- **LLM:** the `llm_endpoint` widget (default `databricks-claude-sonnet-4-5`), as in Ex4/Ex5.

## The prompt (Genie Code)
Paste into a fresh Genie Code chat on a serverless notebook (the reviewed reference is
`Lakebase_Search.py`):

```
Build a Databricks notebook that does PDF RAG with Lakebase as the vector store.
- %pip install pypdf reportlab "psycopg[binary]>=3.1.0" "databricks-sdk>=0.118.0" mlflow
  "langchain>=0.3,<0.4" langchain-databricks; restart; %run ../_setup.
- Generate 3 short synthetic earnings-call transcripts (Air Canada, Suncor, BCE; Air Canada leans on
  fuel/energy exposure) and write them as PDFs to a UC Volume in my cm_<me> schema.
- Parse the PDFs with pypdf, chunk into overlapping windows.
- Embed each chunk via the MLflow deploy client against a Databricks FM embeddings endpoint
  (databricks-gte-large-en); infer the vector dimension from the first embedding.
- In Lakebase (get_connection()): CREATE EXTENSION vector; create cm_<me>.earnings_chunks with a
  vector(dim) column; insert chunks + embeddings (cast %s::vector); build an HNSW vector_cosine_ops index.
- Write vector_search(query, k): embed the query and ORDER BY embedding <=> query LIMIT k.
- Build a LangChain tool-calling agent (subclass ChatDatabricks to drop temperature) with one tool
  search_earnings_calls that calls vector_search, and ask it about Air Canada's fuel exposure.
```

## ✓ Validation
- `cm_<you>.earnings_chunks` exists in **Lakebase** with one row per chunk and a `vector` column.
- A similarity search for "Air Canada fuel exposure" returns Air Canada passages at the top.
- The agent answers the fuel-exposure question by calling the tool and **citing** Air Canada's call.

## Notes
- **`pgvector` availability:** the notebook `CREATE EXTENSION IF NOT EXISTS vector` — it's normally
  available in Lakebase; if it errors, confirm with your facilitator.
- **Cost/cleanup:** the embeddings endpoint is pay-per-token. The `earnings_chunks` table lives on your
  branch (dropping the branch clears it); optionally drop the `earnings_pdfs` UC Volume.

## Docs
- pgvector: https://github.com/pgvector/pgvector
- Databricks Foundation Model APIs (embeddings): https://learn.microsoft.com/en-us/azure/databricks/machine-learning/foundation-model-apis/
