"""
bonus_inventory_alert.py
────────────────────────
Bonus: Inventory Alert dùng Stream-Static Join.

Logic:
  - Đọc stream order_items từ Kafka (Debezium)
  - Join với bảng products (static, đọc từ MySQL)
  - Khi stock_quantity < LOW_STOCK_THRESHOLD → gửi alert

Điểm cần lưu ý:
  - Static DataFrame chỉ được đọc MỘT LẦN lúc job start
  - Nếu products thay đổi → cần restart job
  - Hoặc dùng Stream-Stream join (advanced)

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               mysql:mysql-connector-java:8.0.33 \
    --master local[*] \
    bonus_inventory_alert.py
"""

import sys
sys.path.insert(0, "/opt/spark-apps/schemas")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    to_json, struct, lit, current_timestamp
)
from debezium_schemas import order_item_envelope_schema

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = "kafka:29092"
INPUT_TOPIC          = "dbserver1.ecommerce.order_items"
OUTPUT_TOPIC         = "inventory_alerts"
CHECKPOINT_DIR       = "/opt/checkpoints/bonus_inventory"

MYSQL_URL            = "jdbc:mysql://mysql:3306/ecommerce"
MYSQL_USER           = "debezium"
MYSQL_PASSWORD       = "debezium123"
MYSQL_DRIVER         = "com.mysql.cj.jdbc.Driver"

LOW_STOCK_THRESHOLD  = 10
TRIGGER_INTERVAL     = "15 seconds"

# ── Spark Session ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Bonus-InventoryAlert")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Step 1: Đọc static products table từ MySQL ────────────────
#
#  Static DF: đọc một lần, cache để tránh re-read mỗi batch
#
products_df = (
    spark.read
    .format("jdbc")
    .option("url", MYSQL_URL)
    .option("dbtable", "products")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", MYSQL_DRIVER)
    .load()
    .select("id", "name", "category", "price", "stock_quantity")
    .cache()   # Cache vào memory để tránh hit DB mỗi micro-batch
)

print(f"[INFO] Loaded {products_df.count()} products from MySQL")
print("[INFO] Products với low stock:")
products_df.filter(col("stock_quantity") < LOW_STOCK_THRESHOLD).show()

# ── Step 2: Đọc order_items stream từ Kafka ───────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

order_items_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), order_item_envelope_schema).alias("d"))
    .select("d.*")
    # Chỉ xét order_items mới được tạo
    .filter(col("op") == "c")
    .select(
        col("after.id").alias("item_id"),
        col("after.order_id").alias("order_id"),
        col("after.product_id").alias("product_id"),
        col("after.quantity").alias("quantity"),
        col("after.unit_price").alias("unit_price"),
        to_timestamp(col("ts_ms") / 1000).alias("event_time"),
    )
)

# ── Step 3: Stream-Static Join ────────────────────────────────
#
#  Spark supports:  streaming DF  JOIN  static DF
#  Static DF được broadcast đến tất cả executors
#
alerts_df = (
    order_items_df
    .join(
        products_df,
        order_items_df.product_id == products_df.id,
        how="inner"
    )
    # Chỉ alert khi tồn kho thấp
    .filter(col("stock_quantity") < LOW_STOCK_THRESHOLD)
    .select(
        col("product_id"),
        col("products.name").alias("product_name"),
        col("products.category"),
        col("stock_quantity"),
        col("quantity").alias("ordered_quantity"),
        col("order_id"),
        col("event_time"),
        lit(LOW_STOCK_THRESHOLD).alias("threshold"),
        (col("stock_quantity") - col("quantity")).alias("projected_stock"),
    )
)

# ── Step 4: Output ────────────────────────────────────────────
# Console (debug)
console_query = (
    alerts_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", CHECKPOINT_DIR + "/console")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# Kafka alert
kafka_output = alerts_df.select(
    col("product_id").cast("string").alias("key"),
    to_json(
        struct(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("stock_quantity"),
            col("ordered_quantity"),
            col("projected_stock"),
            col("order_id"),
            col("threshold"),
        )
    ).alias("value")
)

kafka_query = (
    kafka_output.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR + "/kafka")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("=" * 60)
print("  Bonus — Inventory Alert (Stream-Static Join)")
print("=" * 60)
print(f"\n[CONFIG] Low stock threshold: {LOW_STOCK_THRESHOLD}")
print(f"[CONFIG] Output topic: {OUTPUT_TOPIC}")
print("\n[INFO] Để trigger alert, thêm order_item cho sản phẩm stock thấp:")
print("  -- Sản phẩm id=8 (Whey Protein) stock=8, id=10 (Green Tea) stock=5")
print("  INSERT INTO order_items (order_id, product_id, quantity, unit_price)")
print("  VALUES (1, 8, 2, 890000);  -- Whey Protein\n")

spark.streams.awaitAnyTermination()
