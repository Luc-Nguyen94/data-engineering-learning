#!/usr/bin/env bash
# =============================================================
#  check_status.sh — Kiểm tra health của tất cả services
# =============================================================

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

check() {
  local NAME=$1
  local CMD=$2
  if eval "$CMD" >/dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC}  $NAME"
  else
    echo -e "  ${RED}✗${NC}  $NAME  ${RED}(not responding)${NC}"
  fi
}

echo ""
echo -e "${CYAN}═══ Service Health Check ═══${NC}"
echo ""

check "MySQL"           "docker exec workshop-mysql mysqladmin ping -h localhost -uroot -proot123 --silent"
check "Kafka (KRaft)"  "docker exec workshop-kafka kafka-broker-api-versions --bootstrap-server localhost:9092"
check "Kafka Connect"  "curl -sf http://localhost:8083/connectors"
check "Schema Registry""curl -sf http://localhost:8081/subjects"
check "Kafka UI"       "curl -sf http://localhost:8080"
check "Spark Master"   "curl -sf http://localhost:8888"

echo ""
echo -e "${CYAN}═══ Kafka Topics ═══${NC}"
echo ""
docker exec workshop-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list 2>/dev/null | grep -v "^_" | sort | while read topic; do
    echo -e "  ${GREEN}•${NC}  $topic"
  done

echo ""
echo -e "${CYAN}═══ Debezium Connector Status ═══${NC}"
echo ""
CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/mysql-ecommerce-connector-v1/status 2>/dev/null)
if [ -n "$CONNECTOR_STATUS" ]; then
  echo "$CONNECTOR_STATUS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d['connector']['state']
color = '\033[0;32m' if state == 'RUNNING' else '\033[0;31m'
print(f'  Connector: {color}{state}\033[0m')
for t in d.get('tasks', []):
    tstate = t['state']
    tcolor = '\033[0;32m' if tstate == 'RUNNING' else '\033[0;31m'
    print(f'  Task {t[\"id\"]}: {tcolor}{tstate}\033[0m')
" 2>/dev/null || echo "  (Could not parse status)"
else
  echo -e "  ${RED}Connector not found${NC}. Run: bash scripts/setup.sh"
fi

echo ""
echo -e "${CYAN}═══ Consumer Lag (orders topic) ═══${NC}"
echo ""
docker exec workshop-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups 2>/dev/null | grep "orders" | head -5 || echo "  (No active consumer groups yet)"

echo ""
