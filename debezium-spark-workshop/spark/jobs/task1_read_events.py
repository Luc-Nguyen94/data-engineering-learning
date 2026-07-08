"""
task1_read_events.py
────────────────────
Task 1 (Beginner): Đọc raw Debezium events từ Kafka và print ra console.

Mục tiêu:
  - Kết nối Spark → Kafka
  - Parse JSON envelope của Debezium
  - Hiểu cấu trúc: before / after / op / ts_ms / source

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[*] \
    task1_read_events.py

Hoặc trong Docker Spark:
  /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-apps/jobs/task1_read_events.py
"""

import sys
sys.path.insert(0, "/opt/spark-apps/schemas")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when
)
from debezium_schemas import order_envelope_schema

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"       # dùng trong Docker
# KAFKA_BOOTSTRAP = "localhost:9092"  # dùng ngoài Docker
KAFKA_TOPIC     = "dbserver1.ecommerce.orders"
CHECKPOINT_DIR  = "/opt/checkpoints/task1"

# ── Spark Session ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Task1-ReadDebeziumEvents")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  Task 1 — Reading Debezium Events from Kafka")
print("=" * 60)

# ── Step 1: Đọc raw bytes từ Kafka ───────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ── Step 2: Parse JSON → structured DataFrame ─────────────────
#   Kafka value là bytes → cast sang STRING trước
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str",
                "timestamp AS kafka_timestamp",
                "partition",
                "offset")
    .select(
        from_json(col("json_str"), order_envelope_schema).alias("data"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset"),
    )
    .select(
        # Kafka metadata
        col("kafka_timestamp"),
        col("partition"),
        col("offset"),
        # Debezium operation (from __op added by ExtractNewRecordState)
        col("data.__op").alias("op"),
        # Human-readable op label
        when(col("data.__op") == "c", "INSERT")
        .when(col("data.__op") == "u", "UPDATE")
        .when(col("data.__op") == "d", "DELETE")
        .when(col("data.__op") == "r", "SNAPSHOT")
        .otherwise("UNKNOWN")
        .alias("op_label"),
        # Timestamp: __source_ts_ms là milliseconds
        to_timestamp(col("data.__source_ts_ms") / 1000).alias("event_time"),
        # Source metadata
        col("data.__source_db").alias("source_db"),
        col("data.__source_table").alias("source_table"),
        col("data.__source_lsn").alias("source_lsn"),
        # Payload — after-state fields (before is not available after unwrap)
        col("data.id").alias("order_id"),
        col("data.user_id").alias("user_id"),
        col("data.status").alias("status"),
        col("data.total_amount").alias("total_amount"),
    )
)

# ── Step 3: Write ra console ──────────────────────────────────
query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="5 seconds")
    .start()
)

print("\n[INFO] Streaming started. Đang chờ events từ Kafka...")
print("[INFO] Thử INSERT / UPDATE trong MySQL để xem events xuất hiện\n")
print("  INSERT INTO orders (user_id, status, total_amount)")
print("  VALUES (1, 'pending', 500000);\n")
print("  UPDATE orders SET status='paid' WHERE id=1;\n")

query.awaitTermination()
