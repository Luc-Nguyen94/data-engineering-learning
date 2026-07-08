"""
task11_gold_serving.py  🥇 GOLD (serving)
────────────────────────────────────────────
Add one operational gold table, then SERVE the gold layer with plain
Spark SQL — the way an analyst or BI tool would consume it.

Part A — gold.low_stock_reorder   (silver.products + silver.order_items)
Part B — serving queries over the gold views

Batch job. Run after Task 10 (needs the gold marts):
  bash scripts/run_job.sh serving            # this starter (has TODOs)
  bash scripts/run_job.sh serving solution   # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, coalesce, lit, greatest

from lake_utils import SILVER, GOLD

REORDER_POINT = 15

spark = (
    SparkSession.builder
    .appName("Task11-GoldServing")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

products = spark.read.format("delta").load(f"{SILVER}/products")
items    = spark.read.format("delta").load(f"{SILVER}/order_items")

# ── TODO 1 (Part A): gold.low_stock_reorder ──────────────────────────
# units_sold per product = sum(quantity) from silver.order_items by product_id.
# Join products (left) with units_sold (no sales -> 0), filter
# stock_quantity < REORDER_POINT, and compute
#   suggested_reorder_qty = greatest(REORDER_POINT*2 - stock_quantity, units_sold).
# Select product_id, name, category, stock_quantity, units_sold,
#   reorder_point(=lit(REORDER_POINT)), suggested_reorder_qty.
# Write f"{GOLD}/low_stock_reorder" (Delta, overwrite).
print("[TODO 1] low_stock_reorder not implemented yet")

# ── TODO 2 (Part B): serving queries via Spark SQL ───────────────────
# Register the 3 gold tables as temp views (daily_revenue_by_category,
# customer_rfm, low_stock_reorder), then spark.sql(...).show() for:
#   - Top 5 categories by revenue
#   - Top 10 customers by monetary value
#   - Products to reorder
print("[TODO 2] serving queries not implemented yet")

spark.stop()
