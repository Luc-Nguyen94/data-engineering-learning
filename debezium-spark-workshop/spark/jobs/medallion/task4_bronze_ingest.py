"""
task4_bronze_ingest.py  🥉 BRONZE
─────────────────────────────────
Goal: stream ALL four CDC topics from Kafka and land them, untouched,
into append-only Delta tables — one bronze table per source table.

Bronze rules (memorise these):
  - Faithful copy of the source stream. No filtering, no dedup, no joins.
  - Keep every column, including the `__` CDC metadata AND delete events.
  - Append-only. History is preserved; nothing is ever overwritten.
  - Add ingestion metadata so you can always answer "when did this land?".

This is the ONE streaming job of the lab. Leave it running, generate CDC
changes in another terminal (`bash scripts/mysql.sh demo_cdc_changes`),
watch rows land, then stop it (Ctrl-C) before running the batch jobs.

Run:
  bash scripts/run_job.sh bronze              # this starter (has TODOs)
  bash scripts/run_job.sh bronze solution     # reference solution
"""

import sys
sys.path.insert(0, "/opt/spark-apps/jobs/medallion")

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date

from lake_utils import BRONZE, KAFKA_BOOTSTRAP, CDC_TABLES, parse_cdc

CHECKPOINT_ROOT  = "/opt/checkpoints/bronze"
TRIGGER_INTERVAL = "10 seconds"

spark = (
    SparkSession.builder
    .appName("Task4-BronzeIngest")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def start_bronze_stream(table, topic, schema):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_df = parse_cdc(raw_df, schema)

    # ── TODO 1 ────────────────────────────────────────────────
    # Add ingestion metadata columns to `parsed_df`:
    #   _ingest_timestamp = current_timestamp()
    #   _ingest_date      = to_date(current_timestamp())   # partition column
    bronze_df = parsed_df   # <-- replace me

    # ── TODO 2 ────────────────────────────────────────────────
    # Write `bronze_df` as an APPEND-ONLY Delta stream and RETURN the query:
    #   .writeStream.format("delta").outputMode("append")
    #   .partitionBy("_ingest_date")
    #   .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
    #   .option("path", f"{BRONZE}/{table}")
    #   .trigger(processingTime=TRIGGER_INTERVAL)
    #   .start()
    query = None   # <-- replace me
    return query


queries = []
for table, (topic, schema, _pk) in CDC_TABLES.items():
    print(f"[INFO] Starting bronze ingest: {table:<12} <- {topic}")
    queries.append(start_bronze_stream(table, topic, schema))

print("=" * 60)
print("  Task 4 — Bronze Ingest (streaming CDC -> Delta, append-only)")
print("=" * 60)
print(f"\n[CONFIG] Bronze root : {BRONZE}")
print(f"[CONFIG] Trigger     : every {TRIGGER_INTERVAL}")
print("\n[INFO] Generate changes:  bash scripts/mysql.sh demo_cdc_changes")
print("[INFO] Stop with Ctrl-C, then run:  bash scripts/run_job.sh silver\n")

spark.streams.awaitAnyTermination()
