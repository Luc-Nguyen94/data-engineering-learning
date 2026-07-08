"""
task5_silver_orders_stream.py  🥈 SILVER · orders (FACT)  ·  STREAMING
────────────────────────────────────────────────────────────────────────
`orders` is a FACT table — high volume, constantly changing. We sync it
from bronze to silver with **Spark Structured Streaming**, so new CDC data
flows through automatically and incrementally (the checkpoint remembers
what was already processed).

Pattern (stream + foreachBatch + MERGE):
  bronze/orders (Delta stream)
      -> for each micro-batch:
             keep latest row per key  (latest_per_key)
             data quality + casts
             MERGE into silver/orders  (insert / update / delete)

We use trigger `availableNow=True`: it drains everything currently in
bronze in incremental micro-batches, then stops — perfect for the lab and
still fully incremental on the next run.

Run (after Task 4 bronze has landed some data):
  bash scripts/run_job.sh orders            # this starter (has TODOs)
  bash scripts/run_job.sh orders solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from lake_utils import BRONZE, SILVER, latest_per_key, merge_upsert

PK            = "id"
BUSINESS_COLS = ["user_id", "status", "total_amount", "created_at", "updated_at"]
CASTS         = {"total_amount": "decimal(15,2)"}
DQ            = "id IS NOT NULL AND total_amount >= 0"
CHECKPOINT    = "/opt/checkpoints/silver/orders"

spark = (
    SparkSession.builder
    .appName("Task5-Silver-Orders-Stream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def upsert_to_silver(batch_df, batch_id):
    """Runs once per micro-batch of new bronze rows."""
    # ── TODO A: collapse the micro-batch to the latest row per key ──
    #   changeset = latest_per_key(batch_df, PK)
    changeset = batch_df   # <-- replace me

    # reliable event time from the DB commit timestamp
    changeset = changeset.withColumn("_event_time", to_timestamp(col("__source_ts_ms") / 1000))

    # ── TODO B: data quality + casts ────────────────────────────────
    #   changeset = changeset.filter(DQ)
    #   for c, t in CASTS.items(): changeset = changeset.withColumn(c, col(c).cast(t))

    keep = [PK] + BUSINESS_COLS + ["_event_time", "__source_ts_ms", "__deleted"]
    changeset = changeset.select(*keep)

    # ── TODO C: upsert this batch into silver/orders ────────────────
    #   merge_upsert(f"{SILVER}/orders", changeset, PK, BUSINESS_COLS)
    print(f"[batch {batch_id}] TODO C not implemented — 0 rows merged")


# ── TODO D: stream from bronze/orders and drive upsert_to_silver ────
# query = (
#     spark.readStream.format("delta").load(f"{BRONZE}/orders")
#     .writeStream
#     .foreachBatch(upsert_to_silver)
#     .option("checkpointLocation", CHECKPOINT)
#     .trigger(availableNow=True)
#     .start()
# )
# query.awaitTermination()

print("=" * 60)
print("  Task 5 — Silver orders (FACT, streaming bronze -> silver)")
print("=" * 60)
print("[HINT] Implement TODO A-D, then re-run. See jobs/solutions/ for the answer.")
