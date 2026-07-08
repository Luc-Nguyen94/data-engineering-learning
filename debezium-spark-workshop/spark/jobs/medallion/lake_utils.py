"""
lake_utils.py
─────────────
Small shared helpers for the Lakehouse ETL Lab (Tasks 4-8).

Kept intentionally minimal — only plumbing that every task needs:
  - warehouse path constants (bronze / silver / gold)
  - the CDC table registry (topic + flat schema + primary key)
  - parse_cdc(): Kafka value bytes -> flattened Debezium row
  - delta_exists(): does a Delta table already live at this path?

The interesting ETL logic (dedup, MERGE, SCD2, joins, aggregations)
lives in each task file so you actually get to write it.

Both the starter files (jobs/medallion/) and the reference solutions
(jobs/solutions/) import from here.
"""

import os
import sys

# schemas are mounted at /opt/spark-apps/schemas inside the Spark container
sys.path.insert(0, "/opt/spark-apps/schemas")

from pyspark.sql import Window
from pyspark.sql.functions import from_json, col, row_number

from debezium_schemas import (
    order_flat_schema,
    order_item_flat_schema,
    product_flat_schema,
    user_flat_schema,
)

# ── Delta warehouse layout ────────────────────────────────────
#   Persisted on the host through the ../spark:/opt/spark-apps mount,
#   so tables survive between job runs and after a teardown (keep data).
WAREHOUSE = "/opt/spark-apps/warehouse"
BRONZE    = f"{WAREHOUSE}/bronze"
SILVER    = f"{WAREHOUSE}/silver"
GOLD      = f"{WAREHOUSE}/gold"

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"       # internal address (inside Docker)

# ── CDC source registry ───────────────────────────────────────
#   table_name -> (kafka_topic, flat_schema, primary_key)
CDC_TABLES = {
    "orders":      ("dbserver1.ecommerce.orders",      order_flat_schema,      "id"),
    "order_items": ("dbserver1.ecommerce.order_items", order_item_flat_schema, "id"),
    "products":    ("dbserver1.ecommerce.products",    product_flat_schema,    "id"),
    "users":       ("dbserver1.ecommerce.users",       user_flat_schema,       "id"),
}


def parse_cdc(raw_df, schema):
    """Kafka `value` (bytes) -> parsed CDC row.

    The connector already flattened the Debezium envelope, so the parsed
    row is the after-image columns plus the `__`-prefixed metadata.
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("d"))
        .select("d.*")
    )


def delta_exists(path):
    """True if a Delta table already exists at `path`.

    The warehouse lives on the local filesystem inside the container,
    so a plain check for the `_delta_log` directory is enough — no need
    for the DeltaTable Python API (which isn't installed here).
    """
    return os.path.exists(os.path.join(path, "_delta_log"))


def latest_per_key(df, pk="id"):
    """Collapse a CDC changeset to the most recent image per primary key.

    Orders by DB commit time (`__source_ts_ms`), tie-broken by log position
    (`__source_lsn`), and keeps the newest row per key.
    """
    w = Window.partitionBy(pk).orderBy(
        col("__source_ts_ms").desc(), col("__source_lsn").desc()
    )
    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def merge_upsert(path, source_df, pk, business_cols):
    """Idempotent upsert of a CDC changeset into a silver Delta table.

    `source_df` must contain: pk, *business_cols, _event_time,
    __source_ts_ms, __deleted.

    The first call creates the table; later calls apply INSERT / UPDATE /
    DELETE via a Delta `MERGE`. Works both for a batch DataFrame and for a
    per-micro-batch DataFrame inside `foreachBatch` — which is exactly how
    the FACT tables stream from bronze to silver.
    """
    spark = source_df.sparkSession
    target_cols = [pk] + business_cols + ["_event_time", "__source_ts_ms"]

    # First time: no silver table yet -> write everything that isn't a delete.
    if not delta_exists(path):
        (source_df.filter("__deleted <> 'true'")
                  .select(*target_cols)
                  .write.format("delta").mode("overwrite").save(path))
        return

    view = "_merge_src_" + os.path.basename(path)
    source_df.createOrReplaceTempView(view)

    set_clause  = ", ".join(f"t.`{c}` = s.`{c}`" for c in target_cols if c != pk)
    insert_cols = ", ".join(f"`{c}`"   for c in target_cols)
    insert_vals = ", ".join(f"s.`{c}`" for c in target_cols)

    spark.sql(f"""
        MERGE INTO delta.`{path}` AS t
        USING {view} AS s
        ON t.`{pk}` = s.`{pk}`
        WHEN MATCHED AND s.__deleted = 'true' THEN DELETE
        WHEN MATCHED                          THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND s.__deleted <> 'true' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
