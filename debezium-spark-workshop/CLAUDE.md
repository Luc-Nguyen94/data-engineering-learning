# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a workshop teaching real-time Change Data Capture (CDC) processing using Debezium + Apache Spark Structured Streaming. MySQL binlog events flow through Debezium → Kafka → Spark for stream processing tasks.

## Environment Management

```bash
# Start full stack (MySQL, Kafka KRaft, Debezium Connect, Spark, Kafka UI)
bash scripts/setup.sh

# Health check all services
bash scripts/check_status.sh

# Stop (keep data) / Stop + delete volumes
bash scripts/teardown.sh
bash scripts/teardown.sh --clean
```

## Running Spark Jobs

```bash
bash scripts/run_job.sh task1    # Read raw Debezium events → console
bash scripts/run_job.sh task2    # Revenue dashboard (Tumbling Window, 1-min)
bash scripts/run_job.sh task3    # Fraud detection (Sliding Window 5-min, output → Kafka)
bash scripts/run_job.sh bonus    # Inventory alert (Stream-Static Join from MySQL)
```

### Part 2 — Lakehouse ETL / Medallion (self-study lab, one task per entity)

```bash
bash scripts/run_job.sh bronze    # Task 4:  streaming CDC → Delta bronze (all entities)
bash scripts/run_job.sh orders    # Task 5:  silver orders      (FACT, streaming bronze→silver)
bash scripts/run_job.sh items     # Task 6:  silver order_items (FACT, streaming bronze→silver)
bash scripts/run_job.sh products  # Task 7:  silver products    (DIM, batch dedup + MERGE)
bash scripts/run_job.sh users     # Task 8:  silver users       (DIM, batch dedup + MERGE)
bash scripts/run_job.sh scd2      # Task 9:  SCD Type 2 dim_users (DIM, batch)
bash scripts/run_job.sh gold      # Task 10: revenue mart + customer RFM
bash scripts/run_job.sh serving   # Task 11: reorder report + Spark SQL serving
```

Fact tables (`orders`, `order_items`) sync bronze→silver via **streaming**
(`readStream` from bronze Delta + `foreachBatch` + `merge_upsert`, trigger
`availableNow`). Dimension tables (`products`, `users`) use **batch**
(`spark.read` + dedup + `merge_upsert`). The dedup and MERGE helpers live in
`spark/jobs/medallion/lake_utils.py` (`latest_per_key`, `merge_upsert`).

Add `solution` as a 2nd arg to run the reference solution instead of the starter:
`bash scripts/run_job.sh orders solution`. Starters (`spark/jobs/medallion/`) contain
`# TODO`s; solutions live in `spark/jobs/solutions/`. Full guide: `docs/LAKEHOUSE_LAB.md`.

These tasks use **Delta Lake** (`io.delta:delta-spark_2.12:3.2.0`); `run_job.sh`
adds the package and the Spark configs (`spark.sql.extensions` +
`spark.sql.catalog.spark_catalog=...DeltaCatalog`) automatically. The warehouse is
written to `spark/warehouse/{bronze,silver,gold}` (persisted via the `../spark` mount
at `/opt/spark-apps/warehouse`). MERGE is done via `spark.sql("MERGE INTO delta.\`path\` ...")`
so the DeltaTable Python API is not required. Reliable event time comes from
`__source_ts_ms` (not `created_at`, which the connector emits as an epoch under this config).

Jobs run inside the `workshop-spark-master` container via `spark-submit --master local[*]`. The `spark/` directory is mounted at `/opt/spark-apps/` and checkpoints at `/opt/checkpoints/`.

To reset checkpoints after schema changes:
```bash
rm -rf spark/checkpoints/task1  # or task2, task3, bonus_inventory
```

## MySQL Helpers

```bash
bash scripts/mysql.sh console         # Open MySQL CLI (root/root123)
bash scripts/mysql.sh show            # View current table state
bash scripts/mysql.sh demo_orders     # Insert paid orders (triggers Task 2)
bash scripts/mysql.sh demo_fraud      # Insert rapid orders for user_id=6 (triggers Task 3)
bash scripts/mysql.sh demo_inventory  # Reduce stock to trigger bonus alert
bash scripts/mysql.sh update_status   # Update pending → paid
bash scripts/mysql.sh reset           # Reset to seed data
```

## Service Endpoints

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Kafka Connect REST | http://localhost:8083 |
| Schema Registry | http://localhost:8081 |
| Spark UI | http://localhost:8888 |
| MySQL | localhost:3306 (root/root123, debezium/debezium123) |

## Architecture

```
MySQL (binlog ROW format)
  → Debezium MySQL Connector (Kafka Connect)
    → Kafka topics: dbserver1.ecommerce.{orders,order_items,products,users}
      → Spark Structured Streaming jobs
```

**Debezium connector** uses `ExtractNewRecordState` SMT (Single Message Transform) with `unwrap` — this flattens the Debezium envelope and adds metadata fields prefixed with `__` (`__op`, `__source_ts_ms`, `__source_lsn`). The `op` field values: `c`=INSERT, `u`=UPDATE, `d`=DELETE, `r`=SNAPSHOT.

**Kafka** runs in KRaft mode (no Zookeeper). Internal broker address is `kafka:29092`; external host address is `localhost:9092`. Spark jobs inside Docker use `kafka:29092`.

**Spark jobs** all import from `spark/schemas/debezium_schemas.py` (added to `sys.path` as `/opt/spark-apps/schemas`). Each schema file defines the Debezium envelope structure with `before`/`after` nested records.

**Key gotchas documented in the codebase:**
- `ts_ms` is milliseconds — always divide by 1000 before `to_timestamp()`
- `before` field is `null` for INSERT events (`op=c`)
- Aggregations require `.withWatermark()` to use `append` output mode
- Task 3 outputs fraud alerts to the `fraud_alerts` Kafka topic

## Kafka Operations

```bash
# List topics
docker exec workshop-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume messages
docker exec workshop-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.ecommerce.orders --from-beginning --max-messages 5

# Read fraud alerts (Task 3 output)
docker exec workshop-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud_alerts --from-beginning

# Check connector status / restart
curl http://localhost:8083/connectors/mysql-ecommerce-connector-v1/status | python3 -m json.tool
curl -X POST http://localhost:8083/connectors/mysql-ecommerce-connector-v1/restart
```
