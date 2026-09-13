# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3 (part 2) — The Coverage Desk Assistant
# MAGIC
# MAGIC A LangChain chatbot for a CIBC Capital Markets **coverage officer**. When asked
# MAGIC about a client, the assistant calls a **tool** that looks up that client's live risk
# MAGIC features from the **Lakebase online store** — through the Feature Serving endpoint
# MAGIC you built in `Feature_Store.py`, via the MLflow deploy client.
# MAGIC
# MAGIC **The pattern to notice:** the LLM decides *when* to fetch features; the tool does
# MAGIC the fetch. "Tool vs MLflow API" is a false choice — the tool *is* an MLflow deploy-
# MAGIC client call. That is the documented agent pattern
# MAGIC ([RAG with feature serving](https://docs.databricks.com/aws/en/machine-learning/feature-store/rag)).
# MAGIC
# MAGIC **Prerequisite:** run `Feature_Store.py` first (it creates the serving endpoint).

# COMMAND ----------

# MAGIC %pip install "langchain>=0.3,<0.4" "langchain-databricks" "mlflow" "databricks-sdk>=0.118.0" --quiet
# MAGIC # NOTE: langchain is pinned <0.4 on purpose. langchain 1.x moves
# MAGIC # create_tool_calling_agent out of langchain.agents and is incompatible with
# MAGIC # langchain-databricks 0.1.2 (which requires langchain-core <0.4).

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# Must match Feature_Store.py
ENDPOINT_NAME = f"cm-client-risk-{_sanitize(user_email)}"[:63]
# Pay-per-token FM endpoint. Verified served in the build workspace; swap if yours
# differs (e.g. databricks-claude-sonnet-4-5, databricks-claude-haiku-4-5).
LLM_ENDPOINT = "databricks-claude-sonnet-4-5"

print(f"Feature Serving endpoint: {ENDPOINT_NAME}")
print(f"LLM endpoint:             {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The LLM
# MAGIC
# MAGIC Some Claude serving endpoints **reject** the `temperature` parameter that
# MAGIC `ChatDatabricks` sends by default. Subclass it and strip `temperature` from the
# MAGIC payload before it's sent. (Same gotcha handled in Exercise 4.)

# COMMAND ----------

from langchain_databricks import ChatDatabricks


class ChatDatabricksNoTemp(ChatDatabricks):
    """ChatDatabricks that drops the `temperature` kwarg some Claude endpoints reject."""
    def _prepare_inputs(self, *args, **kwargs):
        inputs = super()._prepare_inputs(*args, **kwargs)
        if isinstance(inputs, dict):
            inputs.pop("temperature", None)
        return inputs


llm = ChatDatabricksNoTemp(endpoint=LLM_ENDPOINT, max_tokens=800)
print("✓ LLM ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The feature-lookup tool  ← this is where the chatbot reads Lakebase
# MAGIC
# MAGIC The tool calls the **Feature Serving endpoint** with the MLflow deploy client. The
# MAGIC endpoint resolves the lookup against the online feature table in your Lakebase
# MAGIC project and returns that client's risk features.

# COMMAND ----------

import json
import mlflow.deployments
from langchain_core.tools import tool

_deploy_client = mlflow.deployments.get_deploy_client("databricks")

# CIBC CM coverage universe — maps names/tickers the officer might say to client_ids.
CLIENT_IDS = {
    "air canada": "CL-AC", "ac": "CL-AC", "ac.to": "CL-AC",
    "suncor": "CL-SU", "cenovus": "CL-CVE", "bce": "CL-BCE", "manulife": "CL-MFC",
    "barrick": "CL-ABX", "cn rail": "CL-CNR", "cn": "CL-CNR", "magna": "CL-MG",
    "brookfield": "CL-BAM",
}


def _resolve_client_id(text: str) -> str:
    t = text.strip().lower()
    if t.upper().startswith("CL-"):
        return t.upper()
    return CLIENT_IDS.get(t, text.strip().upper())


def _extract_features(resp) -> dict:
    """Feature Serving returns predictions under 'outputs' (or 'predictions'). Normalize
    to a single feature dict. ⚠ VALIDATE the exact response shape in the test pass."""
    if isinstance(resp, dict):
        for key in ("outputs", "predictions"):
            if key in resp and resp[key]:
                first = resp[key][0]
                return first if isinstance(first, dict) else {"value": first}
        return resp
    return {"raw": str(resp)}


@tool
def lookup_client_risk(client: str) -> str:
    """Look up a CIBC capital-markets client's LIVE risk features (exposure, PnL,
    volatility, concentration, risk tier). Accepts a client name (e.g. 'Air Canada'),
    ticker, or client_id (e.g. 'CL-AC'). Returns the features as JSON."""
    client_id = _resolve_client_id(client)
    resp = _deploy_client.predict(
        endpoint=ENDPOINT_NAME,
        inputs={"dataframe_records": [{"client_id": client_id}]},
    )
    feats = _extract_features(resp)
    return json.dumps({"client_id": client_id, "features": feats}, default=str)


# quick smoke test of the tool alone
print(lookup_client_risk.invoke({"client": "Air Canada"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Wire the tool into a tool-calling agent

# COMMAND ----------

from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

SYSTEM = (
    "You are the CIBC Capital Markets Coverage Desk Assistant. You help coverage "
    "officers understand a client's live risk profile. When a client is mentioned, ALWAYS "
    "call the lookup_client_risk tool to get current features before answering — never "
    "guess numbers. Interpret the features for a trader: call out elevated gross notional, "
    "concentration (largest_position_pct), portfolio volatility, and the risk_tier, and "
    "note oil/energy exposure when relevant. Be concise and specific with the numbers."
)

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM),
    MessagesPlaceholder("chat_history", optional=True),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

tools = [lookup_client_risk]
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
print("✓ Coverage Desk Assistant ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Demo — the hero question
# MAGIC The coverage officer asks about Air Canada. The agent calls the tool, pulls the
# MAGIC **HIGH**-tier features (large energy book) from Lakebase, and answers.

# COMMAND ----------

result = agent_executor.invoke({
    "input": "What's Air Canada's risk profile right now, and should I be concerned "
             "about their oil exposure?"
})
print("\n=== Assistant ===\n")
print(result["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try another client

# COMMAND ----------

result = agent_executor.invoke({
    "input": "Compare Suncor and BCE — which of my clients is carrying more risk?"
})
print("\n=== Assistant ===\n")
print(result["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ✓ **Checks**
# MAGIC - The verbose trace shows `lookup_client_risk` invoked (the tool → Feature Serving
# MAGIC   → Lakebase online store).
# MAGIC - Air Canada's answer reflects **HIGH** risk / large energy exposure.
# MAGIC
# MAGIC **Next:** the assistant has no memory yet — ask a follow-up and it forgets the last
# MAGIC turn. **Exercise 4** gives it persistent memory backed by Lakebase.
