"""
task3_fraud_detection.py
────────────────────────
Task 3 (Advanced): Phát hiện gian lận dùng Sliding Window.

Logic:
  - User đặt > 3 đơn trong cửa sổ 5 phút → FRAUD ALERT
  - Sliding window: mỗi 1 phút tính lại cho 5 phút trước
  - Kết quả ghi vào Kafka topic "fraud_alerts"

Output Kafka message:
  {
    "user_id": 6,
    "window_start": "2024-01-15T10:00:00",
    "window_end":   "2024-01-15T10:05:00",
    "order_count":  4,
    "total_amount": 1200000.0,
    "alert_level":  "HIGH"
  }

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[*] \
    task3_fraud_detection.py
"""

import sys
sys.path.insert(0, "/opt/spark-apps/schemas")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    window, count, sum as _sum,
    to_json, struct, lit,
    date_format, when
)
from debezium_schemas import order_envelope_schema

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "kafka:29092"
INPUT_TOPIC       = "dbserver1.ecommerce.orders"
OUTPUT_TOPIC      = "fraud_alerts"
CHECKPOINT_DIR    = "/opt/checkpoints/task3"

WINDOW_DURATION   = "1 minutes"
SLIDE_INTERVAL    = "1 minute"
WATERMARK_DELAY   = "1 minutes"
FRAUD_THRESHOLD   = 3            # số đơn trong window
TRIGGER_INTERVAL  = "30 seconds"

# ── Spark Session ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Task3-FraudDetection")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Step 1: Đọc và parse orders ───────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

orders_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), order_envelope_schema).alias("d"))
    .select("d.*")
    # Chỉ lấy đơn mới tạo hoặc cập nhật
    .filter(col("__op").isin("c", "u"))
    # Chỉ xét đơn không bị cancelled
    .filter(~col("status").isin("cancelled", "refunded"))
    .select(
        col("id").alias("order_id"),
        col("user_id").alias("user_id"),
        col("total_amount").alias("amount"),
        col("status").alias("status"),
        to_timestamp(col("__source_ts_ms") / 1000).alias("event_time"),
    )
)

# ── Step 2: Sliding Window Aggregation ───────────────────────
#
#  Sliding window — các window chồng lấp:
#
#  |────── 10:00–10:05 ──────|
#       |────── 10:01–10:06 ──────|
#            |────── 10:02–10:07 ──────|
#
#  Mỗi event được đếm trong TẤT CẢ các window chứa nó.
#
fraud_df = (
    orders_df
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        col("user_id"),
        window(col("event_time"), WINDOW_DURATION, SLIDE_INTERVAL)
    )
    .agg(
        count("order_id").alias("order_count"),
        _sum("amount").alias("total_amount"),
    )
    # Chỉ giữ lại những user vượt threshold
    .filter(col("order_count") > FRAUD_THRESHOLD)
    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("order_count"),
        col("total_amount"),
        # Phân loại mức độ alert
        when(col("order_count") > FRAUD_THRESHOLD * 2, "CRITICAL")
        .when(col("order_count") > FRAUD_THRESHOLD,    "HIGH")
        .otherwise("MEDIUM")
        .alias("alert_level"),
    )
)

# ── Step 3a: Print alerts ra console (để debug) ───────────────
console_query = (
    fraud_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", CHECKPOINT_DIR + "/console")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# ── Step 3b: Write alerts ra Kafka topic ─────────────────────
#
#  Kafka cần value là bytes, nên dùng to_json(struct(...))
#
kafka_output_df = fraud_df.select(
    col("user_id").cast("string").alias("key"),   # Kafka message key
    to_json(
        struct(
            col("user_id"),
            date_format(col("window_start"), "yyyy-MM-dd'T'HH:mm:ss").alias("window_start"),
            date_format(col("window_end"),   "yyyy-MM-dd'T'HH:mm:ss").alias("window_end"),
            col("order_count"),
            col("total_amount"),
            col("alert_level"),
            lit("fraud_detection_v1").alias("source"),
        )
    ).alias("value")
)

kafka_query = (
    kafka_output_df.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR + "/kafka")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("=" * 60)
print("  Task 3 — Fraud Detection (Sliding Window)")
print("=" * 60)
print(f"\n[CONFIG] Window: {WINDOW_DURATION}, Slide: {SLIDE_INTERVAL}")
print(f"[CONFIG] Fraud threshold: > {FRAUD_THRESHOLD} orders trong window")
print(f"[CONFIG] Output topic: {OUTPUT_TOPIC}")
print("\n[INFO] Để trigger fraud alert, tạo nhiều đơn cho cùng 1 user:")
print("  -- Chạy lặp lại nhiều lần:")
print("  INSERT INTO orders (user_id, status, total_amount)")
print("  VALUES (6, 'pending', 200000);")
print("\n[INFO] Đọc fraud alerts từ Kafka:")
print("  kafka-console-consumer.sh \\")
print("    --bootstrap-server localhost:9092 \\")
print("    --topic fraud_alerts --from-beginning\n")

# Đợi cả hai query
spark.streams.awaitAnyTermination()
