"""
task10_gold_revenue_mart.py  🥇 GOLD
──────────────────────────────────────
Gold marts join and aggregate the clean SILVER tables into business-ready
outputs. Gold is cross-entity by nature (it combines facts and dimensions),
so unlike silver it is not split per table.

Two marts:
  1) gold.daily_revenue_by_category   (order_items + orders + products)
  2) gold.customer_rfm                (orders + users)

Batch job. Run after the silver tables exist (Tasks 5, 6, 7, 8):
  bash scripts/run_job.sh gold            # this starter (has TODOs)
  bash scripts/run_job.sh gold solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as _sum, count, countDistinct, max as _max,
    datediff, current_date, when, lit,
)

from lake_utils import SILVER, GOLD

spark = (
    SparkSession.builder
    .appName("Task10-GoldRevenueMart")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders   = spark.read.format("delta").load(f"{SILVER}/orders")
items    = spark.read.format("delta").load(f"{SILVER}/order_items")
products = spark.read.format("delta").load(f"{SILVER}/products")
users    = spark.read.format("delta").load(f"{SILVER}/users")

paid = (
    orders.filter(col("status") == "paid")
    .select(
        col("id").alias("order_id"),
        col("user_id"),
        to_date(col("_event_time")).alias("order_date"),
        col("total_amount"),
    )
)

# ── TODO 1: gold.daily_revenue_by_category ───────────────────────────
# line_revenue = quantity * unit_price (from order_items).
# Join items -> paid (order_id) -> products (product_id for category).
# Group by (order_date, category): revenue = sum(line_revenue),
#   units_sold = sum(quantity), order_count = countDistinct(order_id).
# Write f"{GOLD}/daily_revenue_by_category" (Delta, overwrite).
print("[TODO 1] daily_revenue_by_category not implemented yet")

# ── TODO 2: gold.customer_rfm ────────────────────────────────────────
# Per user_id from `paid`: last_order_date=max(order_date),
#   frequency=count(order_id), monetary=sum(total_amount),
#   recency_days=datediff(current_date(), last_order_date).
# Join users (id -> user_id) for name + tier; add a `segment`.
# Write f"{GOLD}/customer_rfm" (Delta, overwrite).
print("[TODO 2] customer_rfm not implemented yet")

spark.stop()
