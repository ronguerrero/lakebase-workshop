# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 pipeline — Lakebase change extracts → current-state Delta
# MAGIC
# MAGIC **Deploy this as a Lakeflow (DLT) pipeline — do NOT run it inline in a notebook.**
# MAGIC The driver notebook `Lakebase_CDF_To_SCD1.py` creates the pipeline and points it here.
# MAGIC
# MAGIC It reads the append-only **bronze change tables** that the driver lands from the Lakebase
# MAGIC `lb_*` tables, and uses **AUTO CDC** to materialize **current-state (SCD Type 1)** Delta
# MAGIC tables — one row per key, latest wins, no history — for risk analytics.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# The pipeline is created with a UC target catalog + schema (that's where positions_current /
# limits_current land). The SOURCE bronze catalog/schema are passed as pipeline configuration.
CATALOG = spark.conf.get("cm.catalog")
BRONZE_SCHEMA = spark.conf.get("cm.bronze_schema")


def scd1_current(entity: str, key: str):
    """Bronze change stream --AUTO CDC--> <entity>_current (SCD1)."""
    src_view = f"{entity}_changes"

    # Streaming read of the append-only bronze change extract the driver writes.
    @dlt.table(name=src_view,
               comment=f"Streaming read of bronze {entity} change extracts from Lakebase")
    def _src(entity=entity):
        return spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.lb_{entity}_changes")

    # Target current-state streaming table.
    dlt.create_streaming_table(
        name=f"{entity}_current",
        comment=f"Current-state (SCD Type 1) {entity} from Lakebase — latest row per {key}",
    )

    # AUTO CDC = the modern name for apply_changes.
    # ⚠ VALIDATE: on your runtime this is `dlt.create_auto_cdc_flow`; older runtimes expose
    #   the identical call as `dlt.apply_changes`. Swap the name if create_auto_cdc_flow is
    #   not defined. Args (target/source/keys/sequence_by/stored_as_scd_type/apply_as_deletes/
    #   except_column_list) are the same across both.
    cdc = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes
    cdc(
        target=f"{entity}_current",
        source=src_view,
        keys=[key],
        sequence_by=F.col("updated_at"),        # newest updated_at wins
        apply_as_deletes=F.expr("_op = 'DELETE'"),
        except_column_list=["_op", "_batch_ts"],  # bronze bookkeeping cols, not in the target
        stored_as_scd_type=1,                    # SCD1: keep only the latest version per key
    )


# The two operational entities the trading app mutates in Lakebase.
scd1_current("positions", "position_id")
scd1_current("limits", "limit_id")
