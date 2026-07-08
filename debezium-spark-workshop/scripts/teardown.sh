#!/usr/bin/env bash
# =============================================================
#  teardown.sh — Dừng và dọn dẹp toàn bộ environment
#
#  Usage:
#    bash scripts/teardown.sh          — Dừng, giữ data volumes
#    bash scripts/teardown.sh --clean  — Dừng + xóa hết volumes
# =============================================================

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$ROOT_DIR/docker"

CLEAN_MODE=false
if [ "${1}" = "--clean" ]; then
  CLEAN_MODE=true
fi

echo ""
echo -e "${CYAN}Stopping workshop environment...${NC}"
echo ""

cd "$DOCKER_DIR"

if $CLEAN_MODE; then
  echo -e "${YELLOW}[--clean mode] Removing all volumes, checkpoints and Delta warehouse${NC}"
  docker compose down -v --remove-orphans
  rm -rf "$ROOT_DIR/spark/checkpoints"
  rm -rf "$ROOT_DIR/spark/warehouse"
  echo -e "${GREEN}Done. All data removed.${NC}"
else
  docker compose down --remove-orphans
  echo -e "${GREEN}Done. Data volumes preserved.${NC}"
  echo -e "${YELLOW}To also remove volumes: bash scripts/teardown.sh --clean${NC}"
fi

echo ""
