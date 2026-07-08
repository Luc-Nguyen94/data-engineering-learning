#!/usr/bin/env bash
# =============================================================
#  setup.sh — Khởi động và cấu hình toàn bộ workshop environment
#  Usage: bash scripts/setup.sh
# =============================================================
set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $1"; }
ok()   { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$ROOT_DIR/docker"

echo ""
echo "  ╔═══════════════════════════════════════════╗"
echo "  ║   Debezium + Spark Workshop Setup         ║"
echo "  ╚═══════════════════════════════════════════╝"
echo ""

# ── Check dependencies ─────────────────────────────────────
log "Checking dependencies..."
command -v docker  >/dev/null || err "Docker not found. Install Docker first."
command -v docker  >/dev/null && docker compose version >/dev/null 2>&1 || \
  command -v docker-compose >/dev/null || err "docker compose plugin not found."
command -v curl    >/dev/null || err "curl not found."
ok "All dependencies found"

# ── Create checkpoint directory ────────────────────────────
log "Creating checkpoint directories..."
mkdir -p "$ROOT_DIR/spark/checkpoints"
mkdir -p "$ROOT_DIR/spark/checkpoints/task1"
mkdir -p "$ROOT_DIR/spark/checkpoints/task2"
mkdir -p "$ROOT_DIR/spark/checkpoints/task3"
mkdir -p "$ROOT_DIR/spark/checkpoints/bonus_inventory"
# Bronze streaming checkpoints (Part 2 — one per CDC table)
mkdir -p "$ROOT_DIR/spark/checkpoints/bronze/orders"
mkdir -p "$ROOT_DIR/spark/checkpoints/bronze/order_items"
mkdir -p "$ROOT_DIR/spark/checkpoints/bronze/products"
mkdir -p "$ROOT_DIR/spark/checkpoints/bronze/users"
# Silver streaming checkpoints (Part 2 — FACT tables only)
mkdir -p "$ROOT_DIR/spark/checkpoints/silver/orders"
mkdir -p "$ROOT_DIR/spark/checkpoints/silver/order_items"
ok "Checkpoint dirs created"

# ── Create Delta lakehouse warehouse (Part 2) ──────────────
log "Creating Delta warehouse directory..."
mkdir -p "$ROOT_DIR/spark/warehouse"
# Spark runs as a non-root user inside the container; make the mounted
# warehouse + checkpoints writable to avoid permission errors on Delta writes.
chmod -R 777 "$ROOT_DIR/spark/warehouse" "$ROOT_DIR/spark/checkpoints" 2>/dev/null || true
ok "Delta warehouse ready ($ROOT_DIR/spark/warehouse)"

# ── Start Docker services ──────────────────────────────────
log "Starting Docker services..."
cd "$DOCKER_DIR"
docker compose up -d

# ── Wait for MySQL ─────────────────────────────────────────
log "Waiting for MySQL to be healthy..."
for i in $(seq 1 30); do
  if docker exec workshop-mysql mysqladmin ping -h localhost -uroot -proot123 --silent 2>/dev/null; then
    ok "MySQL is ready"
    break
  fi
  if [ $i -eq 30 ]; then err "MySQL failed to start after 60s"; fi
  echo -n "."
  sleep 2
done

# ── Wait for Kafka Connect ─────────────────────────────────
log "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 40); do
  if curl -s http://localhost:8083/connectors >/dev/null 2>&1; then
    ok "Kafka Connect is ready"
    break
  fi
  if [ $i -eq 40 ]; then err "Kafka Connect failed to start after 80s"; fi
  echo -n "."
  sleep 2
done

echo ""

# ── Register Debezium connector ───────────────────────────
log "Registering Debezium MySQL connector..."

# Check if connector already exists
EXISTING=$(curl -s http://localhost:8083/connectors/mysql-ecommerce-connector-v1 | grep -c '"name"' || true)

if [ "$EXISTING" -gt "0" ]; then
  warn "Connector already exists. Deleting and re-creating..."
  curl -s -X DELETE http://localhost:8083/connectors/mysql-ecommerce-connector-v1 >/dev/null
  sleep 2
fi

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
  http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$ROOT_DIR/config/connector.json")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  ok "Connector registered successfully"
else
  err "Failed to register connector. HTTP $HTTP_CODE\n$BODY"
fi

# ── Wait for connector to be RUNNING ──────────────────────
log "Waiting for connector to start streaming..."
sleep 5
for i in $(seq 1 15); do
  STATE=$(curl -s http://localhost:8083/connectors/mysql-ecommerce-connector-v1/status \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
  
  if [ "$STATE" = "RUNNING" ]; then
    ok "Connector is RUNNING"
    break
  fi
  
  if [ $i -eq 15 ]; then
    warn "Connector state: $STATE (may still be snapshotting, check logs)"
  fi
  echo -n "."
  sleep 3
done

echo ""
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✓  Workshop environment is ready!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""
echo "  Service URLs:"
echo -e "  ${CYAN}Kafka UI${NC}           http://localhost:8080"
echo -e "  ${CYAN}Kafka Connect REST${NC} http://localhost:8083"
echo -e "  ${CYAN}Schema Registry${NC}    http://localhost:8081"
echo -e "  ${CYAN}Spark UI${NC}           http://localhost:8888"
echo -e "  ${CYAN}MySQL${NC}              localhost:3306  (root/root123)"
echo ""
echo "  Kafka Topics (sau khi snapshot xong):"
echo "    dbserver1.ecommerce.orders"
echo "    dbserver1.ecommerce.order_items"
echo "    dbserver1.ecommerce.products"
echo "    dbserver1.ecommerce.users"
echo ""
echo "  Bước tiếp theo:"
echo -e "  ${YELLOW}bash scripts/run_job.sh task1${NC}  — Chạy Task 1"
echo -e "  ${YELLOW}bash scripts/mysql.sh${NC}          — Mở MySQL console"
echo -e "  ${YELLOW}bash scripts/check_status.sh${NC}   — Kiểm tra health"
echo ""
