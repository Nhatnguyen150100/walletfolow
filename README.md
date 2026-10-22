# WalletFlow — Ví điện tử Microservices

Hệ thống ví điện tử phân tán xây bằng **Java 21 · Spring Boot 3.5 · Spring Cloud 2025**, thiết kế
để giải các bài toán khó & phổ biến của hệ phân tán trong lĩnh vực fintech: **sổ cái double-entry**,
**idempotency (chống double-charge)**, **Saga orchestration**, **Transactional Outbox**,
**Kafka at-least-once + dedup**, **reconciliation**.

> 📚 [Kế hoạch kỹ thuật](docs/walletflow-master-plan.md) ·
> [Lộ trình lên production](docs/walletflow-production-roadmap.md) ·
> [Hướng dẫn chạy local](docs/local-setup-plan.md) ·
> [ADR](docs/adr/) · [Runbook](docs/runbooks/)

---

## 🧩 Thành phần

| Thành phần | Cổng | Vai trò | Trạng thái |
|---|---|---|---|
| discovery-server (Eureka) | 8761 | Danh bạ service | ✅ |
| config-server | 8888 | Cấu hình tập trung | ✅ |
| api-gateway | 8080 | Routing, verify JWT qua JWKS, rate limit | ✅ |
| auth-service | 8081 | Đăng ký/đăng nhập, ký JWT RS256, refresh token | ✅ |
| common-lib | — | Money, outbox, dedup, xử lý lỗi Kafka + DLT | ✅ |
| ledger-service | 8083 | Sổ cái double-entry (nguồn sự thật) | ✅ |
| wallet-service | 8082 | Read model số dư (CQRS), dựng lại được từ sổ cái | ✅ |
| transaction-service | 8084 | Nhận lệnh chuyển idempotent + Saga orchestration | ✅ |
| bank-adapter | 8085 | Cổng thanh toán: thu tiền nạp ví idempotent, Resilience4j, VNPay sandbox — [ADR-016](docs/adr/ADR-016-bank-adapter-charge-model.md), [ADR-017](docs/adr/ADR-017-vnpay-session-expiry-log-masking.md) | 🟡 Phase 2 (2.2a mock ✅; 2.2b VNPay + hạn phiên + masking log) |
| notification-service | 8086 | Thông báo kết quả | ✂️ gộp vào webhook khách hàng (Phase 5.1) — [ADR-015](docs/adr/ADR-015-product-positioning.md) |

**Hạ tầng:** PostgreSQL (DB-per-service), Redis, Apache Kafka, Jaeger, Prometheus + Grafana.

---

## 🚀 Chạy

### Lần đầu — bắt buộc

```bash
cp .env.example .env
./scripts/gen-keys.sh        # sinh cặp khoá RSA cho JWT
```

Không có bước này thì service **không khởi động** — cố ý như vậy: một khoá mặc định "chỉ dùng cho
dev" là khoá sẽ theo lên production (xem [ADR-009](docs/adr/ADR-009-asymmetric-jwt-jwks.md)).

### Chế độ Lite — dùng hằng ngày (~1,3 GB RAM)

Hạ tầng trong Docker, service chạy bằng JVM trên host. Vòng lặp sửa–chạy tính bằng giây.

```bash
docker compose -f docker-compose.infra.yml up -d
set -a && source .env && set +a

./mvnw -o -q install -DskipTests          # một lần, để common-lib vào local repo
./mvnw -pl services/ledger-service spring-boot:run -Dspring-boot.run.profiles=local
```

> ⚠️ **Không thêm `-am` vào `spring-boot:run`.** `-am` kéo cả parent pom vào reactor, và
> `spring-boot:run` sẽ chạy trên parent trước → `Unable to find a suitable main class`.
> `-am` chỉ dùng cho `package`/`verify`.

### Chế độ Full — toàn bộ stack (~7 GB RAM)

```bash
set -a && source .env && set +a
docker compose up -d --build
```

| Thành phần | URL |
|---|---|
| API Gateway | http://localhost:8080 |
| Eureka | http://localhost:8761 |
| Jaeger | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana (admin/admin) | http://localhost:3000 → dashboard **WalletFlow — Tổng quan** |

**Thử nhanh auth:**

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"a@b.com","password":"mat-khau-du-dai-12","fullName":"Nguyen Van A"}'

# -> accessToken (15 phút) + refreshToken (30 ngày, xoay vòng mỗi lần dùng)
curl -X POST http://localhost:8080/api/auth/refresh \
  -H 'Content-Type: application/json' -d '{"refreshToken":"<token>"}'

# Public key để tự verify token
curl http://localhost:8080/.well-known/jwks.json
```

---

## 🧪 Test

```bash
./mvnw verify          # toàn bộ: unit + integration (cần Docker cho Testcontainers)
```

**153 test** (CI, cuối Phase 1); integration test chạy trên Postgres + Kafka thật. Mỗi tính chất về
tiền có một test chứng minh:

| Tính chất | Test |
|---|---|
| Bảo toàn tiền: `SUM(mọi posting) = 0` | `LedgerServiceIT` |
| Bút toán lệch / tràn số / thiếu vế đều bị từ chối trọn vẹn | `LedgerServiceIT` |
| Idempotent: lệnh trùng **trả về** bút toán cũ, không nhân đôi tiền | `LedgerServiceIT` |
| 100 luồng rút cùng một ví: đúng 10 lần thành công, không bao giờ âm | `LedgerServiceIT` |
| Sổ cái append-only: DB từ chối UPDATE/DELETE kể cả bằng SQL trực tiếp | `LedgerServiceIT` |
| Outbox thật sự publish được lên Kafka, quét lại không gửi lặp | `OutboxRelayIT` |
| Cả chuỗi: lệnh vào Kafka → ghi sổ → event đi ra; dedup message trùng | `LedgerCommandFlowIT` |
| **Message hỏng vào DLT nguyên văn, không chặn message hợp lệ phía sau** | `KafkaDltIT` |
| **Dọn outbox không bao giờ đụng hàng chưa gửi** | `OutboxRetentionIT` |
| **Gauge tuổi outbox phản ánh đúng tồn đọng** (phát hiện relay chết) | `OutboxRetentionIT` |
| **Token ký RS256, verify được bằng khoá từ JWKS; JWKS không lộ private key** | `AuthFlowIT` |
| **Refresh xoay vòng; dùng lại token đã thu hồi giết cả chuỗi** | `AuthFlowIT` |
| **Bộ đếm đăng nhập sai sống sót qua rollback** | `AuthFlowIT` |
| Tenant A không đọc/ghi được dữ liệu tenant B, kể cả khi truyền thẳng ID; `SUM = 0` theo từng tenant | `TenantIsolationIT` |
| Ví `FROZEN` không chi được nhưng vẫn nhận; `REVERSAL` chỉ hoàn tác đúng và một lần | `AccountLifecycleIT`, `LedgerServiceIT` |
| Read model giao hoán: event lặp/lộn thứ tự/song song → số dư khớp chính xác | `WalletProjectionIT` |
| Rebuild ví khớp sổ cái, không làm ví khác mất tiền dù chung một event | `WalletRebuildIT` |
| Gửi lại cùng `Idempotency-Key` → câu trả lời khớp từng byte; gửi song song → đúng một lệnh | `IdempotencyIT` (+ E2E) |
| Câu trả lời của ledger bị mất → saga vẫn kết thúc; saga treo → `MANUAL_REVIEW` + metric cho alert | `SagaFlowIT`, `StuckSagaIT` |
| Record và fixture của mọi topic khớp JSON Schema; message thật trên Kafka khớp schema | `EventContractTest`, `SagaFlowIT` |

Coverage gate trên integration test trong CI: ledger/wallet/transaction 85%, auth 70%.

### E2E — cả stack thật, qua gateway (Definition of Done Phase 1)

```bash
./scripts/e2e/up.sh          # dựng stack (project compose riêng wf-e2e, khoá JWT tạm trong bộ nhớ)
./scripts/e2e/phase1-dod.sh  # chuyển tiền, gửi lại cùng key, giết service giữa saga, rebuild, tenant
./scripts/e2e/down.sh        # dọn sạch (chỉ project wf-e2e)
```

Tắt stack dev trước khi chạy (tên container `wf-*` trùng nhau). Không có `jq` thì script tự chạy
`jq` qua Docker — chậm hơn; cài `jq` cho nhanh. Công cụ dev đi kèm: `scripts/e2e/seed-topup.sh`
(nạp tiền qua lệnh Kafka — Phase 1 chưa có API top-up), `scripts/e2e/make-admin.sh`.

### Load test — số liệu cơ sở ([docs/benchmarks/phase1.md](docs/benchmarks/phase1.md))

```bash
./scripts/load/up.sh         # stack cấu hình PRODUCTION + prometheus (dùng lại image của wf-e2e)
./scripts/load/seed.sh       # 40 người gửi + 1 ví merchant, nạp tiền (k6/.data, gitignore)
./scripts/load/run.sh cold   # k6 qua gateway, 5→80 TPS; in độ trễ, throughput, lag + kiểm bất biến tiền
./scripts/load/run.sh hot    # như trên, mọi lệnh chuyển vào MỘT ví (tranh khoá tài khoản nhận)
```

---

## 🛡️ Những gì Phase 0 đã siết

| Trước | Sau |
|---|---|
| DLT chỉ tồn tại trong javadoc; message lỗi bị **log rồi bỏ qua** sau 10 lần thử | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` thật, poison message đi thẳng DLT không chặn partition |
| Secret JWT HS256 **commit trong repo**, gateway cũng ký được token | RS256 + JWKS, gateway chỉ verify; thiếu khoá thì service chết lúc khởi động |
| Access token 24 giờ, không thu hồi được | 15 phút + refresh token xoay vòng, phát hiện tái sử dụng |
| `ddl-auto: update` ở auth-service | Flyway, `ddl-auto: validate` |
| Header `X-User-*` của client đi thẳng xuống service trên path công khai | Strip vô điều kiện trên **mọi** path |
| Mọi service trên Eureka tự có đường vào qua gateway | Discovery locator tắt, chỉ route khai tường minh |
| Auto-create topic: gõ sai tên topic là lỗi câm | Khai báo topic tường minh, auto-create tắt |
| Outbox phình vô hạn, không metric | Job dọn + partial index + gauge/counter + alert |
| Không có CI | `mvn verify` với Testcontainers + quét secret + guardrail + coverage gate |
| 1/8 ADR, không dashboard, không alert, không backup | 9 ADR, dashboard + 8 alert rule, script backup/restore + 4 runbook |

---

## 🗺️ Bước tiếp theo

**Phase 1 đã đóng (2026-10-18)** — API chuyển tiền chạy end-to-end qua gateway, idempotent, có Saga và
read model. Tổng kết: [`docs/PR-phase-1.md`](docs/PR-phase-1.md).

**Phase 2** — nạp tiền qua một cổng thanh toán thật (sandbox). Định vị sản phẩm đã chốt ở
[ADR-015](docs/adr/ADR-015-product-positioning.md): ví đóng trước, kiến trúc Wallet-as-a-Service.
**Luồng rút tiền ra ngoài** (phần withdraw của 2.4, cùng 2.1 HOLD phục vụ nó) chờ kết luận pháp lý.
Chi tiết: [roadmap §Phase 2](docs/walletflow-production-roadmap.md).

Tồn đọng từ Phase 0:
- [x] Diễn tập restore và đo RTO — **4 giây**, 2026-10-16 ([`docs/runbooks/restore.md`](docs/runbooks/restore.md)); nên lặp lại định kỳ
- [x] Bật branch protection trên `master` — bắt buộc `Build & test`, `Guardrails`, `Secret scan`
- [ ] Thêm check `E2E (compose + DoD Phase 1)` vào danh sách bắt buộc của branch protection
