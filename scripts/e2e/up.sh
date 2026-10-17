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

if [[ -z "${JWT_PRIVATE_KEY:-}" || -z "${JWT_PUBLIC_KEY:-}" ]]; then
  log "Sinh cặp khoá JWT tạm thời cho lần chạy này"
  # PKCS#8 — định dạng JwtKeyProvider đọc được (giống scripts/gen-keys.sh). Đi hoàn toàn qua pipe:
  # private key không chạm đĩa lần nào.
  PRIVATE_PEM=$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null) \
    || die "openssl genpkey thất bại"
  JWT_PRIVATE_KEY=$(grep -v -- '-----' <<<"$PRIVATE_PEM" | tr -d '\r\n')
  JWT_PUBLIC_KEY=$(openssl rsa -pubout 2>/dev/null <<<"$PRIVATE_PEM" | grep -v -- '-----' | tr -d '\r\n')
  unset PRIVATE_PEM
  [[ -n "$JWT_PRIVATE_KEY" && -n "$JWT_PUBLIC_KEY" ]] || die "Không sinh được khoá JWT (cần openssl)"
  # Khoá dùng một lần, nhưng vẫn không để nó lọt vào log CI nếu Spring in giá trị property khi bind lỗi
  [[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::add-mask::$JWT_PRIVATE_KEY"
  export JWT_PRIVATE_KEY JWT_PUBLIC_KEY
fi

log "Build + khởi động stack (lần đầu mất vài phút: mỗi image build Maven từ source)"
# Chỉ những service E2E cần — bỏ prometheus/grafana/jaeger cho nhẹ runner CI (7 JVM + Kafka + PG).
# Trace gửi tới jaeger không có thì exporter chỉ log cảnh báo, không ảnh hưởng nghiệp vụ.
docker compose -p wf-e2e -f docker-compose.yml -f docker-compose.e2e.yml up -d --build \
  postgres redis kafka discovery-server config-server \
  auth-service ledger-service wallet-service transaction-service api-gateway

for c in wf-ledger wf-wallet wf-transaction; do
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
