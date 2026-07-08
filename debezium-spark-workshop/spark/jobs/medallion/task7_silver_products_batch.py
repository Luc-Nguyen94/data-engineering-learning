"""
task7_silver_products_batch.py  🥈 SILVER · products (DIMENSION) · BATCH
─────────────────────────────────────────────────────────────────────────
`products` is a DIMENSION table — small, slowly changing reference data.
It does NOT need streaming: a periodic BATCH job that reads bronze, keeps
the current state, and upserts into silver is simpler and perfectly fine.

  read bronze/products  ->  dedup latest per key  ->  DQ + casts  ->  MERGE silver/products

Note how the transform is identical to the fact tables — the ONLY
difference is batch (`spark.read`) vs streaming (`spark.readStream` +
foreachBatch). Same MERGE, same dedup.

Run (after Task 4 bronze has landed some data):
  bash scripts/run_job.sh products            # this starter (has TODOs)
  bash scripts/run_job.sh products solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from lake_utils import BRONZE, SILVER, latest_per_key, merge_upsert

PK            = "id"
BUSINESS_COLS = ["name", "category", "price", "stock_quantity", "created_at", "updated_at"]
CASTS         = {"price": "decimal(15,2)"}
DQ            = "id IS NOT NULL AND price >= 0"

spark = (
    SparkSession.builder
    .appName("Task7-Silver-Products-Batch")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# batch read — no streaming needed for a dimension
bronze = spark.read.format("delta").load(f"{BRONZE}/products")

# ── TODO A: keep the latest row per key ──
changeset = bronze   # <-- replace with latest_per_key(bronze, PK)

changeset = changeset.withColumn("_event_time", to_timestamp(col("__source_ts_ms") / 1000))

# ── TODO B: data quality (DQ) + casts (CASTS) ──

keep = [PK] + BUSINESS_COLS + ["_event_time", "__source_ts_ms", "__deleted"]
changeset = changeset.select(*keep)

# ── TODO C: merge_upsert into silver/products ──
print("[TODO C] merge_upsert not implemented yet")

print("=" * 60)
print("  Task 7 — Silver products (DIMENSION, batch)")
print("=" * 60)
spark.stop()
