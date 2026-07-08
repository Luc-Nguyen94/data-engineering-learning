"""
task9_scd2_users.py  🥈 SILVER · dim_users (DIMENSION) · BATCH · SCD Type 2
────────────────────────────────────────────────────────────────────────────
Task 8 keeps only the CURRENT user; this keeps the FULL HISTORY. Every
version of a user gets its own row with a validity window, so you can ask
"what tier was this customer on 2024-03-01?".

This is a DIMENSION, so batch is fine — no streaming needed.

Output: silver.dim_users_scd2
  surrogate_key  hash id — unique per version
  id             natural key (user id)
  name, email, tier   tracked attributes
  valid_from / valid_to / is_current

CDC makes SCD2 natural: every bronze row is already a dated version. We
just drop rows where nothing tracked changed, then close each version's
window with the start of the next one.

Run:
  bash scripts/run_job.sh scd2            # this starter (has TODOs)
  bash scripts/run_job.sh scd2 solution   # reference solution

Tip: `bash scripts/mysql.sh demo_cdc_changes` upgrades user id=1's tier
     normal -> vip -> premium, giving 3 SCD2 versions to see.
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, row_number, lag, lead,
    concat_ws, coalesce, lit, md5,
)

from lake_utils import BRONZE, SILVER

TRACKED    = ["name", "email", "tier"]
FAR_FUTURE = "9999-12-31 00:00:00"

spark = (
    SparkSession.builder
    .appName("Task9-SCD2-Users")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.format("delta").load(f"{BRONZE}/users")

events = (
    bronze
    .filter("__deleted <> 'true'")
    .withColumn("_event_time", to_timestamp(col("__source_ts_ms") / 1000))
    .select("id", *TRACKED, "_event_time", "__source_ts_ms", "__source_lsn")
)

# ── TODO 1: collapse duplicate events at the same commit time ──
# Window by ("id", "__source_ts_ms") ordered by __source_lsn DESC, keep rn == 1.
dedup = events   # <-- replace me

# ── TODO 2: keep only rows where a TRACKED attribute actually changed ──
#   w = Window.partitionBy("id").orderBy("__source_ts_ms")
#   _hash = md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in TRACKED]))
#   keep rows where lag(_hash) is null OR differs from _hash
changed = dedup   # <-- replace me

# ── TODO 3: build the SCD2 validity windows ──
#   valid_from    = _event_time
#   _next_from    = lead(_event_time) over w
#   is_current    = _next_from IS NULL
#   valid_to      = coalesce(_next_from, timestamp(FAR_FUTURE))
#   surrogate_key = md5(concat_ws("|", id, valid_from))
#   select surrogate_key, id, *TRACKED, valid_from, valid_to, is_current
scd2 = changed   # <-- replace me

(scd2.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_users_scd2"))

print("=" * 60)
print("  Task 9 — SCD Type 2: silver.dim_users_scd2")
print("=" * 60)
spark.read.format("delta").load(f"{SILVER}/dim_users_scd2") \
     .orderBy("id", "valid_from").show(50, truncate=False)
spark.stop()
