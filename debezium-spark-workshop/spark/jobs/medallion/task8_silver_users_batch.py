"""
task8_silver_users_batch.py  🥈 SILVER · users (DIMENSION) · BATCH
────────────────────────────────────────────────────────────────────
`users` dimension — current state (one row per user). Batch, like
products. Task 9 will additionally build the FULL HISTORY (SCD Type 2)
of this same table.

  read bronze/users  ->  dedup latest per key  ->  DQ  ->  MERGE silver/users

Run (after Task 4 bronze has landed some data):
  bash scripts/run_job.sh users            # this starter (has TODOs)
  bash scripts/run_job.sh users solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from lake_utils import BRONZE, SILVER, latest_per_key, merge_upsert

PK            = "id"
BUSINESS_COLS = ["name", "email", "tier", "created_at", "updated_at"]
CASTS         = {}
DQ            = "id IS NOT NULL"

spark = (
    SparkSession.builder
    .appName("Task8-Silver-Users-Batch")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.format("delta").load(f"{BRONZE}/users")

# ── TODO A: keep the latest row per key ──
changeset = bronze   # <-- replace with latest_per_key(bronze, PK)

changeset = changeset.withColumn("_event_time", to_timestamp(col("__source_ts_ms") / 1000))

# ── TODO B: data quality (DQ). users has no casts. ──

keep = [PK] + BUSINESS_COLS + ["_event_time", "__source_ts_ms", "__deleted"]
changeset = changeset.select(*keep)

# ── TODO C: merge_upsert into silver/users ──
print("[TODO C] merge_upsert not implemented yet")

print("=" * 60)
print("  Task 8 — Silver users (DIMENSION, batch)")
print("=" * 60)
spark.stop()
