#!/usr/bin/env bash
# =====================================================================
# Dựng stack cho LOAD TEST (roadmap 1.5b): cấu hình PRODUCTION + Prometheus.
#
#   ./scripts/load/up.sh && ./scripts/load/seed.sh && ./scripts/load/run.sh cold
#
# Khác scripts/e2e/up.sh ở hai điểm, đều để số đo phản ánh hệ thống thật:
# - KHÔNG áp docker-compose.e2e.yml: deadline saga 10s của E2E làm job quét gửi lại lệnh hàng loạt
#   khi tải cao (lệnh chưa kịp ghi sổ đã bị coi là treo) — đo cái đó là đo một cấu hình không ai chạy.
# - Bật prometheus: outbox lag, consumer lag, Hikari chỉ đọc được qua nó (service không mở cổng).
#
# Dùng chung project `wf-e2e` để tái dùng image đã build (build Maven 7 image mất hàng chục phút) và
# volume. Image chưa có thì compose tự build. Muốn build lại từ code hiện tại: LOAD_BUILD=1.
# Dọn: ./scripts/e2e/down.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

ensure_jwt_keys

BUILD_FLAG=()
if [[ "${LOAD_BUILD:-0}" == 1 ]]; then BUILD_FLAG=(--build); fi

log "Khởi động stack cấu hình production + prometheus (project wf-e2e)"
docker compose -p wf-e2e -f docker-compose.yml up -d "${BUILD_FLAG[@]}" \
  postgres redis kafka discovery-server config-server \
  auth-service ledger-service wallet-service transaction-service api-gateway prometheus

for c in wf-ledger wf-wallet wf-transaction; do
  wait_healthy "$c" 420
done
gateway_up() { TOKEN='' status_is 200 GET /actuator/health; }
jwks_routed() { TOKEN='' status_is 200 GET /.well-known/jwks.json; }
wait_until "gateway trả lời" 300 gateway_up
wait_until "gateway định tuyến được tới auth-service" 300 jwks_routed

# Gateway khởi động TRƯỚC auth thì lần nạp JWKS đầu hỏng và nó giữ 0 khoá; token hợp lệ đầu tiên bị
# 401 rồi mới kích hoạt nạp lại (JwksKeyLocator.find). Chờ tới khi gateway verify được token thật —
# không thì những request đầu của seed/k6 bị tính là lỗi của hệ thống dưới tải.
out=$(register_user load-probe); read -r _ _ _ PROBE_TOKEN <<<"$out"
TOKEN=$PROBE_TOKEN wait_until "gateway verify được JWT (đã nạp JWKS)" 120 status_is 200 GET /api/v1/wallets/me

# Xác nhận đúng cấu hình production: override E2E còn sót trên container là số đo sai mà không ai biết
if docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' wf-transaction \
     | grep -q '^SPRING_APPLICATION_JSON='; then
  die "wf-transaction còn mang override E2E (SPRING_APPLICATION_JSON) — số đo sẽ không phải cấu hình production"
fi
prometheus_up() { curl -sf http://localhost:9090/-/ready >/dev/null; }
wait_until "prometheus sẵn sàng" 120 prometheus_up
log "Stack load test sẵn sàng: gateway $GATEWAY, prometheus http://localhost:9090"
