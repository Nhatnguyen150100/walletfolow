#!/usr/bin/env bash
# =====================================================================
# Dọn sạch stack E2E: container + volume của project compose `wf-e2e` — CHỈ project đó, stack dev
# (project mặc định) không bị đụng.
#
#   ./scripts/e2e/down.sh
#
# docker-compose.yml bắt buộc JWT_PRIVATE_KEY/JWT_PUBLIC_KEY/BANK_PROVIDER (`:?`) ngay cả khi chỉ để
# `down`; khoá tạm của up.sh đã mất theo tiến trình, nên đặt giá trị giả — `down` không khởi động gì cả.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."

JWT_PRIVATE_KEY=unused JWT_PUBLIC_KEY=unused BANK_PROVIDER=unused \
  docker compose -p wf-e2e -f docker-compose.yml -f docker-compose.e2e.yml down -v --remove-orphans
