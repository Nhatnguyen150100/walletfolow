# Runbook — `ServiceDown` / `HighErrorRate`

> **Mức: page.**

## Chẩn đoán — 2 phút

```bash
docker compose ps                        # container nào chết
docker logs wf-ledger --tail 200         # đổi tên theo service
curl -s localhost:8083/actuator/health | jq
```

| Service | Chết thì sao | Mức khẩn |
|---|---|---|
| **ledger-service** | Lệnh tiền dồn trong Kafka, **không mất** (retention 7 ngày). Giao dịch treo. | 🔴 Cao — nhưng không mất tiền |
| **auth-service** | Không ai đăng nhập/refresh được. Access token đang có vẫn dùng được tới khi hết hạn (15 phút). | 🟠 |
| **api-gateway** | Toàn hệ thống không truy cập được từ ngoài | 🔴 |
| **postgres** | Mọi thứ dừng. **Không ghi sổ được.** | 🔴 Cao nhất |
| **kafka** | Ledger không nhận lệnh; outbox dồn lại (bền vững, không mất) | 🟠 |
| discovery / config | Service đang chạy vẫn chạy; chỉ ảnh hưởng instance mới | 🟡 |

## Thứ tự khởi động lại

Phụ thuộc đã được khai bằng `depends_on: condition: service_healthy`, nhưng khi khởi động tay
phải theo đúng thứ tự:

```bash
docker compose up -d postgres kafka redis      # hạ tầng trước
docker compose up -d discovery-server config-server
docker compose up -d auth-service ledger-service
docker compose up -d api-gateway
```

## Nguyên nhân hay gặp trên máy dev

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| Container bị OOMKilled | Máy chỉ còn ~3GB RAM khả dụng | `docker stop edvpath-mysql edvpath-firebase-1`, hoặc dùng chế độ Lite (`docker-compose.infra.yml`) |
| `port is already allocated` | Port 8888 đang bị tiến trình node chiếm | Dùng chế độ Lite (không chạy config-server) |
| Service chết ngay khi khởi động, log báo thiếu `JWT_PRIVATE_KEY` | Chưa sinh khoá | `./scripts/gen-keys.sh` — đây là hành vi **đúng theo thiết kế**, không phải bug |
| Flyway báo "Found non-empty schema without schema history table" | `authdb` cũ do `ddl-auto` sinh ra | `DROP DATABASE authdb; CREATE DATABASE authdb;` |

## Sau sự cố

- [ ] `SELECT SUM(amount_minor) FROM postings;` = 0
- [ ] Outbox tồn đọng đã rút về 0
- [ ] DLT rỗng (hoặc đã xử lý — xem `dlt-not-empty.md`)
