# WalletFlow — Ví điện tử Microservices

Hệ thống ví điện tử phân tán xây bằng **Java 21 · Spring Boot 3.5 · Spring Cloud 2025**, thiết kế
để giải các bài toán khó & phổ biến của hệ phân tán trong lĩnh vực fintech: **sổ cái double-entry**,
**idempotency (chống double-charge)**, **Saga orchestration**, **Transactional Outbox**,
**Kafka at-least-once + dedup**, **reconciliation**.

> 📚 Kế hoạch kỹ thuật đầy đủ: [`docs/walletflow-master-plan.md`](docs/walletflow-master-plan.md)
> · Quyết định kiến trúc: [`docs/adr/`](docs/adr/)

---

## 🧩 Thành phần (theo milestone)

| Thành phần | Cổng | Vai trò | Milestone |
|---|---|---|---|
| discovery-server (Eureka) | 8761 | Danh bạ service | ✅ M0 |
| config-server | 8888 | Cấu hình tập trung | ✅ M0 |
| api-gateway | 8080 | Cổng vào: routing, verify JWT, rate limit | ✅ M0 |
| auth-service | 8081 | Đăng ký, đăng nhập, cấp JWT | ✅ M0 |
| common-lib | — | Money, response, exception, event envelope, outbox, dedup | ✅ M0 |
| ledger-service | 8083 | Sổ cái double-entry (source of truth) | ✅ M1 |
| wallet-service | 8082 | Read model số dư (CQRS) | ⏳ M2 |
| transaction-service | 8084 | Orchestrator Saga + idempotency | ⏳ M3 |
| bank-adapter | 8085 | Mock cổng ngân hàng ngoài | ⏳ M4 |
| notification-service | 8086 | Thông báo kết quả | ⏳ M4 |

**Hạ tầng:** PostgreSQL (DB-per-service), Redis, Apache Kafka, Jaeger, Prometheus + Grafana.

---

## 🚀 Chạy (Docker)

```bash
cd walletflow
docker compose up -d --build
```

Lần đầu build lâu (Maven tải dependency). Kiểm tra sau khi lên:

| Thành phần | URL |
|---|---|
| Eureka | http://localhost:8761 |
| API Gateway | http://localhost:8080 |
| Auth Swagger | http://localhost:8081/swagger-ui.html |
| Jaeger | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana (admin/admin) | http://localhost:3000 |

**Thử nhanh auth (qua Gateway):**

```bash
# Đăng ký
curl -X POST http://localhost:8080/api/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"a@b.com","password":"secret1","fullName":"Nguyen Van A"}'

# Đăng nhập -> nhận accessToken
curl -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"a@b.com","password":"secret1"}'
```

---

## 🧪 Test

```bash
# Chạy toàn bộ test (cần Maven, hoặc dùng Docker maven image)
mvn -pl common-lib test          # Money value object

# Nếu không có mvn local:
docker run --rm -v "$PWD":/w -w /w maven:3.9-eclipse-temurin-21 \
  mvn -q -pl common-lib -am test
```

---

## 🗺️ Trạng thái

Đã xong **Milestone 1**: sổ cái double-entry là nguồn sự thật về tiền, nhận `PostJournalCommand`
qua Kafka và phát `LedgerPostedEvent`/`LedgerRejectedEvent` qua Transactional Outbox.

Được chứng minh bằng test (Testcontainers: Postgres + Kafka thật):

| Tính chất | Test |
|---|---|
| Bảo toàn tiền: `SUM(mọi posting) = 0` | `LedgerServiceIT` |
| Bút toán lệch / tràn số / thiếu vế đều bị từ chối trọn vẹn | `LedgerServiceIT` |
| Idempotent: lệnh trùng **trả về** bút toán cũ, không nhân đôi tiền | `LedgerServiceIT` |
| 100 luồng rút cùng một ví: đúng 10 lần thành công, không bao giờ âm | `LedgerServiceIT` |
| Sổ cái append-only: DB từ chối UPDATE/DELETE kể cả bằng SQL trực tiếp | `LedgerServiceIT` |
| Outbox thật sự publish được lên Kafka, quét lại không gửi lặp | `OutboxRelayIT` |
| Cả chuỗi: lệnh vào Kafka → ghi sổ → event đi ra; dedup message trùng | `LedgerCommandFlowIT` |

```bash
mvn -pl common-lib,services/ledger-service -am verify   # cần Docker cho Testcontainers
```

Bước tiếp theo: **Milestone 2 — wallet-service** (read model số dư, consume `LedgerPostedEvent`).
