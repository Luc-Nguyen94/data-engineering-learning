"""
task2_revenue_dashboard.py
──────────────────────────
Task 2 (Intermediate): Real-time Revenue Dashboard dùng Tumbling Window.

Mục tiêu:
  - Aggregate doanh thu theo từng phút
  - Dùng withWatermark() để xử lý late data
  - outputMode("append") với window aggregation

Output mẫu:
  +-------------------+-------------+-----------+
  |minute             |total_revenue|order_count|
  +-------------------+-------------+-----------+
  |2024-01-15 10:00:00|   2750000.0 |          3|
  |2024-01-15 10:01:00|   1500000.0 |          2|
  +-------------------+-------------+-----------+

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[*] \
    task2_revenue_dashboard.py
"""

import sys
sys.path.insert(0, "/opt/spark-apps/schemas")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    window, sum as _sum, count, avg
)
from debezium_schemas import order_envelope_schema

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "dbserver1.ecommerce.orders"
CHECKPOINT_DIR  = "/opt/checkpoints/task2"

WINDOW_DURATION  = "20 seconds"
WATERMARK_DELAY  = "10 seconds"
TRIGGER_INTERVAL = "5 seconds"

# ── Spark Session ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Task2-RevenueDashboard")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Step 1: Đọc từ Kafka và parse ────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

orders_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), order_envelope_schema).alias("d"))
    .select("d.*")
    # Chỉ lấy INSERT và UPDATE, bỏ DELETE và SNAPSHOT
    .filter(col("__op").isin("c", "u"))
    # Chỉ tính đơn đã thanh toán
    .filter(col("status") == "paid")
    .select(
        col("id").alias("order_id"),
        col("user_id").alias("user_id"),
        col("total_amount").alias("amount"),
        # event_time: lấy từ __source_ts_ms (milliseconds → seconds)
        to_timestamp(col("__source_ts_ms") / 1000).alias("event_time"),
    )
)

# ── Step 2: Tumbling Window Aggregation ───────────────────────
#
#  Timeline:
#  |──── 10:00 ────|──── 10:01 ────|──── 10:02 ────|
#  |  Window 1     |  Window 2     |  Window 3     |  ← không chồng lấp
#
revenue_df = (
    orders_df
    # QUAN TRỌNG: phải có withWatermark trước khi groupBy window
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION)
    )
    .agg(
        _sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value"),
    )
    .select(
        col("window.start").alias("minute"),
        col("window.end").alias("window_end"),
        col("total_revenue"),
        col("order_count"),
        col("avg_order_value"),
    )
)

# ── Step 3: Output ────────────────────────────────────────────
#
#  outputMode("append"): mỗi window chỉ được emit MỘT LẦN
#  sau khi watermark vượt qua window end
#
query = (
    revenue_df.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .option("numRows", 30)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("=" * 60)
print("  Task 2 — Real-time Revenue Dashboard")
print("=" * 60)
print(f"\n[CONFIG] Window: {WINDOW_DURATION}")
print(f"[CONFIG] Watermark: {WATERMARK_DELAY}")
print(f"[CONFIG] Trigger: every {TRIGGER_INTERVAL}")
print("\n[INFO] Kết quả sẽ xuất hiện sau khi window đóng lại")
print("[INFO] Thử tạo nhiều đơn 'paid' để xem revenue tích lũy:\n")
print("  INSERT INTO orders (user_id, status, total_amount)")
print("  VALUES (1,'paid',500000), (2,'paid',300000), (3,'paid',800000);")

query.awaitTermination()
