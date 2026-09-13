# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 pipeline — Lakebase CDF history → current-state Delta
# MAGIC
# MAGIC **Deploy this as a Lakeflow (DLT) pipeline — do NOT run it inline in a notebook.**
# MAGIC The driver notebook `Lakebase_CDF_To_SCD1.py` creates the pipeline and points it here.
# MAGIC
# MAGIC It streams the **`lb_<table>_history` change tables** that **Lakebase Change Data Feed** writes
# MAGIC (insert / update / delete rows tailed from the Postgres WAL) and uses **AUTO CDC** to materialize
# MAGIC **current-state (SCD Type 1)** Delta tables — one row per key, latest wins, no history.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# UC target catalog/schema come from the pipeline settings; the CDF history tables live in
# cm.history_schema (the driver passes both as pipeline configuration).
CATALOG = spark.conf.get("cm.catalog")
HISTORY_SCHEMA = spark.conf.get("cm.history_schema")

# Of the CDF bookkeeping columns we keep only `_pg_change_type` (delete detection) and `_sort_by`
# (ordering) into the intermediate view; `except_column_list` then strips those two from the target.
# We DROP `_timestamp` (and `_pg_lsn`/`_pg_xid`) in the source view: `_timestamp` is a TIMESTAMP_NTZ
# column, which would otherwise force the `timestampNtz` Delta table feature onto the streaming table
# and fail the pipeline.
_DROP_COLS = ["_timestamp", "_pg_lsn", "_pg_xid"]


def scd1_from_cdf(entity: str, key: str):
    """lb_<entity>_history (Lakebase CDF) --AUTO CDC--> <entity>_current (SCD1)."""
    src_view = f"{entity}_cdf"

    # Streaming read of the CDF-materialized history table. An update emits BOTH an
    # `update_preimage` (old row) and an `update_postimage` (new row); drop the preimage so only
    # the new value is applied (the postimage has a higher _sort_by and would win anyway).
    @dlt.table(name=src_view,
               comment=f"Streaming read of Lakebase CDF history for {entity}")
    def _src(entity=entity):
        return (spark.readStream.table(f"{CATALOG}.{HISTORY_SCHEMA}.lb_{entity}_history")
                .drop(*_DROP_COLS)
                .filter(F.col("_pg_change_type") != "update_preimage"))

    dlt.create_streaming_table(
        name=f"{entity}_current",
        comment=f"Current-state (SCD Type 1) {entity} from Lakebase CDF — latest row per {key}",
    )

    # AUTO CDC = the modern name for apply_changes.
    # ⚠ VALIDATE: on your runtime this is `dlt.create_auto_cdc_flow`; older runtimes expose the
    #   identical call as `dlt.apply_changes`. Swap the name if create_auto_cdc_flow is not defined.
    cdc = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes
    cdc(
        target=f"{entity}_current",
        source=src_view,
        keys=[key],
        sequence_by=F.col("_sort_by"),                     # CDF monotonic order across all changes
        apply_as_deletes=F.expr("_pg_change_type = 'delete'"),
        except_column_list=["_pg_change_type", "_sort_by"],  # keep only the business columns
        stored_as_scd_type=1,                               # SCD1: latest version per key, no history
    )


# The two operational entities the trading app mutates in Lakebase.
scd1_from_cdf("positions", "position_id")
scd1_from_cdf("limits", "limit_id")
