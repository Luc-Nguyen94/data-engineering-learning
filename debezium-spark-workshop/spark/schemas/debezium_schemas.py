"""
schemas.py — Debezium Envelope Schemas cho từng bảng
Dùng chung cho tất cả Spark jobs trong workshop
"""

from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, IntegerType, TimestampType
)

# ── Source metadata (chung cho tất cả tables) ─────────────────
source_schema = StructType([
    StructField("db",        StringType(), True),
    StructField("table",     StringType(), True),
    StructField("ts_ms",     LongType(),   True),   # DB commit time (ms)
    StructField("lsn",       LongType(),   True),   # Log Sequence Number
    StructField("server_id", LongType(),   True),
])

# ── orders bảng ──────────────────────────────────────────────
order_record_schema = StructType([
    StructField("id",           LongType(),   True),
    StructField("user_id",      LongType(),   True),
    StructField("status",       StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("created_at",   StringType(), True),  # ISO string từ Debezium
    StructField("updated_at",   StringType(), True),
])

# Flattened by ExtractNewRecordState SMT — after fields promoted to top level,
# before/after/source envelope is removed, metadata added with __ prefix.
order_envelope_schema = StructType([
    # After-state fields (top-level after unwrap)
    StructField("id",             LongType(),   True),
    StructField("user_id",        LongType(),   True),
    StructField("status",         StringType(), True),
    StructField("total_amount",   DoubleType(), True),
    StructField("created_at",     StringType(), True),
    StructField("updated_at",     StringType(), True),
    # Metadata added by ExtractNewRecordState
    StructField("__op",           StringType(), True),
    StructField("__source_ts_ms", LongType(),   True),
    StructField("__source_db",    StringType(), True),
    StructField("__source_table", StringType(), True),
    StructField("__source_lsn",   LongType(),   True),
    StructField("__deleted",      StringType(), True),
])

# ── order_items bảng ─────────────────────────────────────────
order_item_record_schema = StructType([
    StructField("id",         LongType(),   True),
    StructField("order_id",   LongType(),   True),
    StructField("product_id", LongType(),   True),
    StructField("quantity",   IntegerType(),True),
    StructField("unit_price", DoubleType(), True),
    StructField("created_at", StringType(), True),
])

order_item_envelope_schema = StructType([
    StructField("before",         order_item_record_schema, True),
    StructField("after",          order_item_record_schema, True),
    StructField("source",         source_schema,            True),
    StructField("op",             StringType(),             True),
    StructField("ts_ms",          LongType(),               True),
    StructField("__op",           StringType(),             True),
    StructField("__source_ts_ms", LongType(),               True),
    StructField("__source_lsn",   LongType(),               True),
])

# ── products bảng ────────────────────────────────────────────
product_record_schema = StructType([
    StructField("id",             LongType(),    True),
    StructField("name",           StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("price",          DoubleType(),  True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("created_at",     StringType(),  True),
    StructField("updated_at",     StringType(),  True),
])

product_envelope_schema = StructType([
    StructField("before",         product_record_schema, True),
    StructField("after",          product_record_schema, True),
    StructField("source",         source_schema,         True),
    StructField("op",             StringType(),          True),
    StructField("ts_ms",          LongType(),            True),
    StructField("__op",           StringType(),          True),
    StructField("__source_ts_ms", LongType(),            True),
])

# ── users bảng ───────────────────────────────────────────────
user_record_schema = StructType([
    StructField("id",         LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("email",      StringType(), True),
    StructField("tier",       StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

user_envelope_schema = StructType([
    StructField("before",         user_record_schema, True),
    StructField("after",          user_record_schema, True),
    StructField("source",         source_schema,      True),
    StructField("op",             StringType(),       True),
    StructField("ts_ms",          LongType(),         True),
    StructField("__op",           StringType(),       True),
    StructField("__source_ts_ms", LongType(),         True),
])

# =============================================================
#  FLATTENED CDC SCHEMAS  (Lakehouse ETL Lab — Tasks 4-8)
# -------------------------------------------------------------
#  The Debezium connector applies the ExtractNewRecordState SMT
#  (`transforms.unwrap`) to EVERY table, configured with:
#     add.fields           = op, source.ts_ms, source.db, source.table, source.lsn
#     delete.handling.mode = rewrite   -> adds the "__deleted" flag
#
#  So each Kafka message is the flattened after-image plus these
#  metadata columns:
#     __op            c=insert, u=update, d=delete, r=snapshot
#     __source_ts_ms  DB commit time in milliseconds (reliable event time)
#     __source_db / __source_table / __source_lsn
#     __deleted       "true" for delete events, "false" otherwise
#
#  `order_envelope_schema` above already follows this shape, so it
#  doubles as the orders flat schema. The three below mirror it for
#  the remaining tables.
# =============================================================

# Metadata columns appended by the unwrap SMT (shared across tables)
_CDC_META_FIELDS = [
    StructField("__op",           StringType(), True),
    StructField("__source_ts_ms", LongType(),   True),
    StructField("__source_db",    StringType(), True),
    StructField("__source_table", StringType(), True),
    StructField("__source_lsn",   LongType(),   True),
    StructField("__deleted",      StringType(), True),
]

# orders — reuse the existing flat schema under a consistent name
order_flat_schema = order_envelope_schema

# order_items (flattened)
order_item_flat_schema = StructType([
    StructField("id",         LongType(),    True),
    StructField("order_id",   LongType(),    True),
    StructField("product_id", LongType(),    True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
    StructField("created_at", StringType(),  True),
] + _CDC_META_FIELDS)

# products (flattened)
product_flat_schema = StructType([
    StructField("id",             LongType(),    True),
    StructField("name",           StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("price",          DoubleType(),  True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("created_at",     StringType(),  True),
    StructField("updated_at",     StringType(),  True),
] + _CDC_META_FIELDS)

# users (flattened)
user_flat_schema = StructType([
    StructField("id",         LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("email",      StringType(), True),
    StructField("tier",       StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
] + _CDC_META_FIELDS)
