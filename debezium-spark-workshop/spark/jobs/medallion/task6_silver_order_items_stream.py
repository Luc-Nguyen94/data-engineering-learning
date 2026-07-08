"""
task6_silver_order_items_stream.py  🥈 SILVER · order_items (FACT) · STREAMING
──────────────────────────────────────────────────────────────────────────────
`order_items` is the other FACT table (order line items). Same streaming
sync pattern as Task 5 — only the key columns, casts and DQ rule change.
This repetition is deliberate: the fact-table streaming sync is a template
you should be able to apply to any new fact table.

  bronze/order_items (Delta stream) -> foreachBatch: dedup + DQ -> MERGE silver/order_items

Run (after Task 4 bronze has landed some data):
  bash scripts/run_job.sh items            # this starter (has TODOs)
  bash scripts/run_job.sh items solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from lake_utils import BRONZE, SILVER, latest_per_key, merge_upsert

PK            = "id"
BUSINESS_COLS = ["order_id", "product_id", "quantity", "unit_price", "created_at"]
CASTS         = {"unit_price": "decimal(15,2)"}
DQ            = "id IS NOT NULL AND quantity > 0"
CHECKPOINT    = "/opt/checkpoints/silver/order_items"

spark = (
    SparkSession.builder
    .appName("Task6-Silver-OrderItems-Stream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def upsert_to_silver(batch_df, batch_id):
    # ── TODO A: latest row per key within the micro-batch ──
    changeset = batch_df   # <-- replace with latest_per_key(batch_df, PK)

    changeset = changeset.withColumn("_event_time", to_timestamp(col("__source_ts_ms") / 1000))

    # ── TODO B: data quality (DQ) + casts (CASTS) ──
    keep = [PK] + BUSINESS_COLS + ["_event_time", "__source_ts_ms", "__deleted"]
    changeset = changeset.select(*keep)

    # ── TODO C: merge_upsert into silver/order_items ──
    print(f"[batch {batch_id}] TODO C not implemented — 0 rows merged")


# ── TODO D: stream bronze/order_items -> foreachBatch upsert (availableNow) ──

print("=" * 60)
print("  Task 6 — Silver order_items (FACT, streaming bronze -> silver)")
print("=" * 60)
print("[HINT] Implement TODO A-D, then re-run. See jobs/solutions/ for the answer.")
