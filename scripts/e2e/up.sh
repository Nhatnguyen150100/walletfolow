#!/usr/bin/env bash
# =====================================================================
# Dựng TOÀN BỘ stack cho E2E (roadmap 1.5a) rồi chờ tới khi gateway định tuyến được.
#
#   ./scripts/e2e/up.sh && ./scripts/e2e/phase1-dod.sh
#
# - Project compose riêng `wf-e2e` => volume Postgres riêng, không đụng dữ liệu của stack dev.
#   Tên container vẫn là wf-* (cố định trong docker-compose.yml): tắt stack dev trước khi chạy.
# - Chưa có JWT_PRIVATE_KEY trong môi trường thì sinh một cặp khoá TẠM cho lần chạy này, chỉ sống
#   trong biến môi trường của tiến trình — không ghi vào .env, không có khoá nào nằm trong repo.
# - Dọn sạch: ./scripts/e2e/down.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

ensure_jwt_keys
ensure_bank_provider

log "Build + khởi động stack (lần đầu mất vài phút: mỗi image build Maven từ source)"
# Chỉ những service E2E cần — bỏ prometheus/grafana/jaeger cho nhẹ runner CI (8 JVM + Kafka + PG).
# Trace gửi tới jaeger không có thì exporter chỉ log cảnh báo, không ảnh hưởng nghiệp vụ.
# bank-adapter chưa nằm trên luồng nào của DoD Phase 1, nhưng vẫn bật và chờ healthy: một service chỉ có
# IT thì không ai biết nó có khởi động được trong compose hay không (bài học gateway ở 1.5a).
docker compose -p wf-e2e -f docker-compose.yml -f docker-compose.e2e.yml up -d --build \
  postgres redis kafka discovery-server config-server \
  auth-service ledger-service wallet-service transaction-service bank-adapter api-gateway

for c in wf-ledger wf-wallet wf-transaction wf-bank-adapter; do
  wait_healthy "$c" 420
done

gateway_up() {
  TOKEN='' status_is 200 GET /actuator/health
}
# JWKS đi qua route của gateway tới auth-service => auth đã đăng ký Eureka và gateway thấy nó
jwks_routed() {
  TOKEN='' status_is 200 GET /.well-known/jwks.json
}
wait_until "gateway trả lời" 300 gateway_up
wait_until "gateway định tuyến được tới auth-service" 300 jwks_routed
log "Stack đã sẵn sàng tại $GATEWAY"
