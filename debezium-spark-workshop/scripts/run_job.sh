#!/usr/bin/env bash
# =============================================================
#  run_job.sh — Run Spark jobs inside Docker
#
#  Streaming CDC tasks (Part 1):
#    bash scripts/run_job.sh task1
#    bash scripts/run_job.sh task2
#    bash scripts/run_job.sh task3
#    bash scripts/run_job.sh bonus
#
#  Lakehouse ETL / Medallion tasks (Part 2, per entity):
#    bash scripts/run_job.sh bronze            # Task 4  — streaming CDC -> Delta bronze (all entities)
#    bash scripts/run_job.sh orders            # Task 5  — silver orders     (FACT, streaming)
#    bash scripts/run_job.sh items             # Task 6  — silver order_items (FACT, streaming)
#    bash scripts/run_job.sh products          # Task 7  — silver products   (DIM, batch)
#    bash scripts/run_job.sh users             # Task 8  — silver users      (DIM, batch)
#    bash scripts/run_job.sh scd2              # Task 9  — SCD Type 2 dim_users (DIM, batch)
#    bash scripts/run_job.sh gold              # Task 10 — revenue mart + customer RFM
#    bash scripts/run_job.sh serving           # Task 11 — reorder report + SQL serving
#
#  Add "solution" as a 2nd arg to run the reference solution instead of
#  the starter (which contains TODOs):
#    bash scripts/run_job.sh silver solution
# =============================================================

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

TASK=${1:-"task1"}
MODE=${2:-"starter"}          # starter | solution  (medallion tasks only)

KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
MYSQL_PACKAGE="mysql:mysql-connector-java:8.0.33"
DELTA_PACKAGE="io.delta:delta-spark_2.12:3.2.0"

USE_DELTA=0                   # 1 -> add Delta jars + SQL extension
JOB_NAME=""                   # filename for medallion tasks (resolved by MODE)
JOB_PATH=""                   # full container path (streaming tasks set it directly)

case "$TASK" in
  # ── Part 1: streaming CDC tasks (jobs/*.py) ────────────────
  task1)
    JOB_PATH="/opt/spark-apps/jobs/task1_read_events.py"
    PACKAGES="$KAFKA_PACKAGE"
    echo -e "${CYAN}Running Task 1: Read Debezium Events${NC}"
    ;;
  task2)
    JOB_PATH="/opt/spark-apps/jobs/task2_revenue_dashboard.py"
    PACKAGES="$KAFKA_PACKAGE"
    echo -e "${CYAN}Running Task 2: Revenue Dashboard (Tumbling Window)${NC}"
    ;;
  task3)
    JOB_PATH="/opt/spark-apps/jobs/task3_fraud_detection.py"
    PACKAGES="$KAFKA_PACKAGE"
    echo -e "${CYAN}Running Task 3: Fraud Detection (Sliding Window)${NC}"
    ;;
  bonus)
    JOB_PATH="/opt/spark-apps/jobs/bonus_inventory_alert.py"
    PACKAGES="$KAFKA_PACKAGE,$MYSQL_PACKAGE"
    echo -e "${CYAN}Running Bonus: Inventory Alert (Stream-Static Join)${NC}"
    ;;

  # ── Part 2: lakehouse ETL / medallion tasks (per entity) ───
  task4|bronze)
    JOB_NAME="task4_bronze_ingest.py"
    PACKAGES="$KAFKA_PACKAGE,$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 4: Bronze Ingest (streaming CDC -> Delta, all entities)${NC}"
    ;;
  task5|orders)
    JOB_NAME="task5_silver_orders_stream.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 5: Silver orders (FACT, streaming bronze -> silver)${NC}"
    ;;
  task6|items)
    JOB_NAME="task6_silver_order_items_stream.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 6: Silver order_items (FACT, streaming bronze -> silver)${NC}"
    ;;
  task7|products)
    JOB_NAME="task7_silver_products_batch.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 7: Silver products (DIMENSION, batch)${NC}"
    ;;
  task8|users)
    JOB_NAME="task8_silver_users_batch.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 8: Silver users (DIMENSION, batch)${NC}"
    ;;
  task9|scd2)
    JOB_NAME="task9_scd2_users.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 9: SCD Type 2 dim_users (DIMENSION, batch)${NC}"
    ;;
  task10|gold)
    JOB_NAME="task10_gold_revenue_mart.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 10: Gold Revenue Mart + Customer RFM${NC}"
    ;;
  task11|serving)
    JOB_NAME="task11_gold_serving.py"
    PACKAGES="$DELTA_PACKAGE"; USE_DELTA=1
    echo -e "${CYAN}Running Task 11: Gold Serving (reorder report + SQL)${NC}"
    ;;

  *)
    echo -e "${RED}Unknown task: $TASK${NC}"
    echo "Usage: $0 [task1|task2|task3|bonus|bronze|orders|items|products|users|scd2|gold|serving] [solution]"
    exit 1
    ;;
esac

# Resolve medallion job path from MODE (starter vs solution)
if [ -n "$JOB_NAME" ]; then
  if [ "$MODE" = "solution" ]; then
    JOB_PATH="/opt/spark-apps/jobs/solutions/$JOB_NAME"
    echo -e "${YELLOW}Mode: SOLUTION${NC}"
  else
    JOB_PATH="/opt/spark-apps/jobs/medallion/$JOB_NAME"
    echo -e "${YELLOW}Mode: starter (contains TODOs)${NC}"
  fi
fi

echo -e "${YELLOW}Job: $JOB_PATH${NC}"
echo ""

# Delta needs the SQL extension + catalog wired into the session
DELTA_CONF=()
if [ "$USE_DELTA" = "1" ]; then
  DELTA_CONF=(
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )
fi

docker exec -i workshop-spark-master \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  --conf "spark.jars.ivy=/tmp/ivy2" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  "${DELTA_CONF[@]}" \
  "$JOB_PATH"
