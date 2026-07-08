#!/usr/bin/env bash
# =============================================================
#  mysql.sh — Tiện ích MySQL cho workshop
#
#  Usage:
#    bash scripts/mysql.sh              — Mở MySQL console
#    bash scripts/mysql.sh demo_orders  — Insert demo orders
#    bash scripts/mysql.sh demo_fraud   — Tạo fraud pattern
#    bash scripts/mysql.sh reset        — Reset về trạng thái ban đầu
# =============================================================

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

CMD=${1:-"console"}

mysql_exec() {
  docker exec -i workshop-mysql mysql -uroot -proot123 ecommerce -e "$1" 2>/dev/null
}

case "$CMD" in

  console)
    echo -e "${CYAN}Opening MySQL console (ecommerce DB)...${NC}"
    echo -e "${YELLOW}Password: root123${NC}"
    docker exec -it workshop-mysql mysql -uroot -proot123 ecommerce
    ;;

  # ── Demo: Tạo các đơn hàng để test Task 2 ────────────────
  demo_orders)
    echo -e "${CYAN}Inserting demo orders for Task 2 (Revenue Dashboard)...${NC}"

    for i in $(seq 1 5); do
      AMOUNT=$(( RANDOM % 900000 + 100000 ))
      USER_ID=$(( RANDOM % 5 + 1 ))
      mysql_exec "INSERT INTO orders (user_id, status, total_amount) VALUES ($USER_ID, 'paid', $AMOUNT);"
      echo -e "  ${GREEN}✓${NC} Order #$i — user_id=$USER_ID, amount=$(printf '%d' $AMOUNT)"
      sleep 1
    done

    echo ""
    echo -e "${GREEN}Done! Check Spark console for revenue aggregation.${NC}"
    ;;

  # ── Demo: Tạo fraud pattern cho Task 3 ───────────────────
  demo_fraud)
    echo -e "${CYAN}Creating fraud pattern for Task 3 (user_id=6 — Bot Account)...${NC}"
    echo -e "${YELLOW}Inserting 5 orders rapidly for user_id=6${NC}"

    for i in $(seq 1 5); do
      AMOUNT=$(( RANDOM % 300000 + 50000 ))
      mysql_exec "INSERT INTO orders (user_id, status, total_amount) VALUES (6, 'pending', $AMOUNT);"
      echo -e "  ${GREEN}✓${NC} Fraud order #$i — amount=$(printf '%d' $AMOUNT)"
      sleep 0.5
    done

    echo ""
    echo -e "${GREEN}Done! Check Spark console and fraud_alerts Kafka topic.${NC}"
    echo -e "${YELLOW}Consumer: kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud_alerts --from-beginning${NC}"
    ;;

  # ── Demo: Tạo inventory alert ─────────────────────────────
  demo_inventory)
    echo -e "${CYAN}Creating inventory alert scenario...${NC}"
    echo -e "${YELLOW}Adding order_item for low-stock product (Whey Protein, stock=8)${NC}"

    mysql_exec "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 8, 2, 890000);"
    echo -e "  ${GREEN}✓${NC} order_item inserted — product_id=8, quantity=2"

    echo ""
    echo -e "${GREEN}Done! Check Spark console and inventory_alerts Kafka topic.${NC}"
    ;;

  # ── Simulate UPDATE ───────────────────────────────────────
  update_status)
    echo -e "${CYAN}Updating order status (pending → paid)...${NC}"
    mysql_exec "UPDATE orders SET status='paid' WHERE status='pending' LIMIT 3;"
    echo -e "${GREEN}Done! Check Spark for UPDATE events (op='u').${NC}"
    ;;

  # ── Show current state ────────────────────────────────────
  show)
    echo -e "${CYAN}Current DB state:${NC}"
    echo ""
    echo -e "${YELLOW}--- orders ---${NC}"
    mysql_exec "SELECT id, user_id, status, total_amount, created_at FROM orders ORDER BY id DESC LIMIT 10;"
    echo ""
    echo -e "${YELLOW}--- products (low stock) ---${NC}"
    mysql_exec "SELECT id, name, stock_quantity FROM products WHERE stock_quantity < 15 ORDER BY stock_quantity;"
    ;;

  # ── Reset dữ liệu ─────────────────────────────────────────
  reset)
    echo -e "${YELLOW}Resetting orders and order_items to seed state...${NC}"
    # Seed data ends at orders id=36 and order_items id=54 (see data/init/01_init.sql).
    # This removes only rows added at runtime by the demos.
    mysql_exec "DELETE FROM order_items WHERE id > 54;"
    mysql_exec "DELETE FROM orders WHERE id > 36;"
    mysql_exec "UPDATE orders SET status='pending' WHERE id IN (4);"
    mysql_exec "UPDATE products SET stock_quantity=8  WHERE id=8;"
    mysql_exec "UPDATE products SET stock_quantity=5  WHERE id=10;"
    echo -e "${GREEN}Done! DB reset to initial seed data.${NC}"
    ;;

  # ── Demo: mixed CDC changes for the Lakehouse ETL lab (Part 2) ──
  #   Produces inserts, updates, a delete, and SCD2 tier changes so the
  #   bronze -> silver -> gold pipeline has interesting data to process.
  demo_cdc_changes)
    echo -e "${CYAN}Generating mixed CDC changes for the Medallion lab...${NC}"

    echo -e "${YELLOW}1) New paid orders (INSERT)${NC}"
    for i in $(seq 1 4); do
      AMOUNT=$(( RANDOM % 900000 + 100000 ))
      USER_ID=$(( RANDOM % 5 + 1 ))
      mysql_exec "INSERT INTO orders (user_id, status, total_amount) VALUES ($USER_ID, 'paid', $AMOUNT);"
      echo -e "  ${GREEN}✓${NC} order user_id=$USER_ID amount=$AMOUNT"
    done

    echo -e "${YELLOW}2) Status updates pending -> paid (UPDATE -> silver keeps latest)${NC}"
    mysql_exec "UPDATE orders SET status='paid' WHERE status='pending' LIMIT 2;"

    echo -e "${YELLOW}3) Delete a cancelled order (DELETE -> silver MERGE removes it)${NC}"
    mysql_exec "DELETE FROM orders WHERE id=6;"
    echo -e "  ${GREEN}✓${NC} deleted order id=6"

    echo -e "${YELLOW}4) SCD2: upgrade user id=1 tier normal -> vip -> premium${NC}"
    mysql_exec "UPDATE users SET tier='vip'     WHERE id=1;"
    sleep 1
    mysql_exec "UPDATE users SET tier='premium' WHERE id=1;"
    echo -e "  ${GREEN}✓${NC} user id=1 now has 3 tier versions in history"

    echo -e "${YELLOW}5) New order_items for gold revenue joins${NC}"
    mysql_exec "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 29990000), (2, 7, 3, 290000), (3, 9, 5, 45000);"

    echo ""
    echo -e "${GREEN}Done! If bronze (Task 4) is running, changes are landing now.${NC}"
    echo -e "${YELLOW}Then run:  silver -> scd2 -> gold -> serving${NC}"
    ;;

  *)
    echo "Usage: $0 [console|demo_orders|demo_fraud|demo_inventory|demo_cdc_changes|update_status|show|reset]"
    ;;
esac
