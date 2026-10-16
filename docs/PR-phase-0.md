# Phase 0 — Siết nền móng: DLT thật, JWT RS256, Flyway, CI, quan sát được

> Nội dung mô tả PR. Dán vào GitHub rồi xoá file này sau khi PR đã tạo.
> Base: `master` · Head: `feat/phase-0-hardening`

## Mục tiêu

Phase 0 của [lộ trình production](walletflow-production-roadmap.md): **không thêm tính năng nào**. Biến thứ đã có thành thứ **tin được**, và dựng hàng rào để các phase sau không tích thêm nợ.

Lý do làm trước: cả 4 service sắp viết (wallet, transaction, bank-adapter, notification) đều dùng lại `common-lib`, cấu hình Kafka, mô hình bảo mật và CI. Sửa 1 chỗ bây giờ rẻ hơn sửa 5 chỗ sau.

---

## 🔴 Ba lỗ hổng nghiêm trọng được bịt

### 1. DLT chỉ tồn tại trong javadoc

`WalletTopics.DLT_SUFFIX` và javadoc của `LedgerCommandListener` đều nói message lỗi sẽ vào DLT. **Không có cấu hình nào thực hiện lời hứa đó.**

Mặc định của Spring Kafka là `DefaultErrorHandler` với `FixedBackOff(0, 9)`: thử 10 lần liên tiếp không nghỉ, rồi **ghi log và commit offset**. Nghĩa là một `PostJournalCommand` gặp sự cố kỹ thuật kéo dài hơn mười lần thử — chỉ cần Postgres restart vài giây — sẽ **biến mất vĩnh viễn**: giao dịch nằm lại ở `PENDING` mãi mãi, không ai biết, và không có gì để replay.

**Sửa:** `KafkaErrorHandlingAutoConfiguration` với `DeadLetterPublishingRecoverer` thật, backoff luỹ thừa 5 lần. Lỗi parse (`PoisonMessageException`, `DeserializationException`) là **non-retryable** → đi thẳng DLT. Retry một message hỏng không chỉ vô ích mà còn có hại: nó chặn cả partition, làm giao dịch của những ví khác phải chờ vì một message rác.

### 2. Secret JWT đối xứng, có giá trị mặc định trong repo

Secret HS256 được commit trong `auth-service/application.yml`, `api-gateway/application.yml` và `docker-compose.yml`. Hai vấn đề **độc lập nhau**:

- Ai đọc được repo cũng ký được token với `role=ADMIN`.
- HS256 đối xứng nghĩa là khoá ký và khoá verify là MỘT. Gateway cần verify nên phải cầm khoá ký — tức là **gateway, thành phần duy nhất lộ ra Internet, có khả năng phát hành token tuỳ ý**. Vấn đề này không sửa được bằng cách giấu secret kỹ hơn; nó là tính chất của thuật toán.

**Sửa:** RS256 + JWKS theo `kid` ([ADR-009](adr/ADR-009-asymmetric-jwt-jwks.md)). Gateway chỉ có public key. Xoay khoá không cần deploy đồng thời. Không có giá trị mặc định — thiếu env thì service **chết lúc khởi động**.

### 3. `ddl-auto: update` ở auth-service

Schema do Hibernate suy diễn thì không review được, không rollback được, và khác nhau giữa các môi trường tuỳ theo lịch sử deploy. **Sửa:** Flyway + `ddl-auto: validate`.

---

## 🐛 Hai bug thật do test mới phát hiện

Cùng một lớp lỗi: **tác dụng phụ về bảo mật nằm chung transaction với một luồng kết thúc bằng exception thì bị rollback theo.** Cả hai đều hỏng hoàn toàn im lặng — code trông đúng, log nói đã làm, thực tế không có gì xảy ra.

| Chỗ | Hậu quả |
|---|---|
| `RefreshTokenService.revokeFamily` | Phát hiện token bị đánh cắp → gọi thu hồi → ném 401 → **rollback luôn lệnh thu hồi**. Token vẫn sống nguyên trong khi log và response đều nói đã giết cả chuỗi. Cơ chế chống trộm token **không hề hoạt động**. |
| `LoginAttemptService` | Bộ đếm đăng nhập sai bị rollback cùng exception → luôn ở 0 → **chống brute-force vô hiệu**. |

Cả hai nay dùng `REQUIRES_NEW` và có test riêng (`AuthFlowIT`).

---

## Chi tiết theo hạng mục

<details>
<summary><b>DLT &amp; khai báo topic</b></summary>

- `KafkaErrorHandlingAutoConfiguration` — áp cho mọi `@KafkaListener` mà không phải khai gì ở từng service.
- `KafkaTopicsAutoConfiguration` khai báo 20 topic tường minh; tắt `auto.create.topics.enable`. Khi broker tự tạo topic, một lỗi gõ sai tên ở producer **không báo lỗi**: broker tạo topic mới, nhận message, không consumer nào đăng ký. Với topic lệnh tiền, đó là những giao dịch lặng lẽ không bao giờ được thực thi.
- Metric `walletflow.dlt.messages` + alert `DltNotEmpty`.
</details>

<details>
<summary><b>Outbox</b></summary>

- Gauge `pending` và **`oldest.age.seconds`**. Cái sau là chỉ số duy nhất phát hiện được relay chết: relay bắt mọi exception để thử lại (đúng thiết kế), nên hỏng hoàn toàn trông **y hệt** đang rảnh rỗi — khi tải thấp, số lượng hàng chờ vẫn nhỏ và không có gì bất thường.
- `OutboxCleanupJob` — chỉ đụng hàng `published = true`. Hàng chưa gửi là lệnh tiền chưa đến đích; xoá nó là mất tiền âm thầm. Có test riêng chỉ để bảo vệ tính chất này.
- `V3` — partial index `WHERE published = false` thay index phủ toàn bảng (ở trạng thái khoẻ mạnh, gần 100% hàng là đã gửi — index cũ dành gần hết dung lượng cho những hàng relay không bao giờ cần).
</details>

<details>
<summary><b>Auth</b></summary>

- `V1__init_auth.sql`: `users` unique theo `lower(email)` (unique thường cho đăng ký cả `A@b.com` lẫn `a@b.com` → hai tài khoản cùng danh tính nhưng khác ví), `refresh_tokens`, `audit_events` append-only có trigger.
- Refresh token **xoay vòng + phát hiện tái sử dụng**: dùng lại token đã thu hồi ⇒ giết cả `family`. Không có bước này thì rotation chỉ làm phiền kẻ trộm chứ không chặn được.
- Lưu hash chứ không lưu token; khoá tài khoản tạm 15 phút sau 5 lần sai (tạm chứ không vĩnh viễn — khoá vĩnh viễn biến chính cơ chế bảo vệ thành công cụ tấn công).
- Access token 24h → **15 phút**. Access token không thu hồi được vì verify là thao tác offline; thời hạn ngắn chính là cơ chế thu hồi duy nhất.
</details>

<details>
<summary><b>Gateway</b></summary>

- **Strip `X-User-*` trên MỌI path**, trước cả khi kiểm tra path công khai. Bản cũ thoát sớm cho path công khai — nghĩa là trên những path đó, header do client tự đặt đi thẳng xuống downstream mà không ai đụng vào. Chưa khai thác được vì chưa có endpoint tiền nào công khai, nhưng là mìn hẹn giờ cho ngày có người thêm route công khai mới.
- Tắt `discovery.locator` — khi bật, **mọi** service đăng ký Eureka tự có đường vào qua `/{service-id}/**`, kể cả `ledger-service`.
- Bỏ map cổng 8083 của ledger ra host (chuyển sang `docker-compose.dev.yml`).
</details>

<details>
<summary><b>CI &amp; vận hành</b></summary>

- `ci.yml`: `mvn verify` với Testcontainers, gitleaks, build image matrix → GHCR (tag theo SHA).
- **4 guardrail** chặn các quy tắc Phase 0 quay lại: `ddl-auto=update`, auto-create topic, secret mặc định, `kafkaTemplate.send` trực tiếp trong code nghiệp vụ.
- Coverage gate trên dữ liệu **integration test** (toàn bộ giá trị kiểm chứng của dự án nằm ở đó): ledger 85%, auth 70%.
- Dashboard Grafana + 8 alert rule, script `backup.sh`/`restore.sh`, 4 runbook, 9 ADR.
- Chế độ chạy local nhẹ `docker-compose.infra.yml` (~1,3GB thay vì ~7GB) + Maven wrapper.
</details>

---

## Kiểm chứng

```
./mvnw clean verify  →  BUILD SUCCESS, 34/34 test xanh
```

7 unit + **27 integration test chạy Postgres + Kafka thật** (Testcontainers). Coverage: ledger-service **94,7%**, auth-service **79,8%** — cả hai gate pass.

Test mới trong PR này:

| Tính chất | Test |
|---|---|
| Message hỏng vào DLT **nguyên văn**, không chặn message hợp lệ cùng partition | `KafkaDltIT` |
| Dọn outbox **không bao giờ** đụng hàng chưa gửi | `OutboxRetentionIT` |
| Gauge tuổi outbox phản ánh đúng tồn đọng, về 0 khi hết | `OutboxRetentionIT` |
| Token RS256 verify được bằng khoá từ JWKS; JWKS **không lộ** private key | `AuthFlowIT` |
| Refresh xoay vòng; dùng lại token đã thu hồi giết cả chuỗi | `AuthFlowIT` |
| Bộ đếm đăng nhập sai sống sót qua rollback | `AuthFlowIT` |

---

## ⚠️ Breaking — cần làm sau khi merge

```bash
# authdb cũ do ddl-auto sinh ra, không có flyway_schema_history -> Flyway sẽ từ chối
docker exec wf-postgres psql -U walletflow -c 'DROP DATABASE authdb'
docker exec wf-postgres psql -U walletflow -c 'CREATE DATABASE authdb'

# Sinh khoá JWT — không có bước này service KHÔNG khởi động (cố ý)
cp .env.example .env && ./scripts/gen-keys.sh
```

Thay đổi API: `AuthResponse` nay trả thêm `refreshToken` / `expiresInSeconds`; `sub` của JWT là `userId` thay vì email; mật khẩu tối thiểu 12 ký tự.

## Còn tồn đọng của Phase 0

- [ ] Diễn tập restore một lần, điền RTO đo thật vào [`docs/runbooks/restore.md`](runbooks/restore.md)
- [ ] Bật branch protection trên `master`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
