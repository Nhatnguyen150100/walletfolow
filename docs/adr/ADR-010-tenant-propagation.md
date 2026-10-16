# ADR-010 — Lan truyền tenant end-to-end: JWT → gateway → TenantContext → envelope

**Trạng thái:** Accepted · **Ngày:** 2026-10-16 · **Milestone:** Phase 1 (1.0, 1.0b)

## Context

Roadmap §4 chọn con đường B (Wallet-as-a-Service): một sổ cái phục vụ nhiều doanh nghiệp. PR #17
đưa `tenant_id` vào mọi bảng của ledger và vào `EventEnvelope`, nhưng tenant chỉ có **một** đường
vào: Kafka listener đọc `envelope.tenantId`. Phía HTTP không có gì — `users` không có tenant, JWT
không mang tenant, không ai đặt `TenantContext` cho request. API ghi tiền đầu tiên sẽ nổ ở
`TenantContext.require()`.

Câu hỏi thật không phải "truyền tenant thế nào", mà là **ai được quyền nói tenant là gì**. Mọi giá
trị client gửi lên đều có thể bị giả; một tenant tự khai lọt xuống service nội bộ là đọc/ghi được sổ
của khách hàng khác chỉ bằng một dòng curl.

## Decision

Tenant có đúng **một nguồn sự thật** ở mỗi chặng, và mỗi chặng chỉ tin chặng trước nó:

| Chặng | Nguồn tenant | Ai đặt |
|---|---|---|
| Đăng ký / đăng nhập | cấu hình `auth.tenant-id` | auth-service — **không** từ request |
| Token | claim `tid` (từ `users.tenant_id`) | auth-service ký RS256 |
| Gateway → service | header `X-Tenant-Id` | gateway, từ `tid` đã verify |
| Trong service (HTTP) | `TenantContext` | `TenantFilter` (auto-config của common-lib) |
| Service → Kafka | `envelope.tenantId` | `OutboxPublisher`, từ `TenantContext` |
| Kafka → service | `TenantContext` | listener, từ `envelope.tenantId` |

Quy tắc đi kèm:

- **Gateway xoá `X-Tenant-Id` trên MỌI path**, kể cả path công khai, trước khi làm bất cứ gì khác —
  cùng quy tắc đã áp cho `X-User-*` ở Phase 0.
- **Thiếu tenant thì từ chối, không bao giờ đoán.** Token thiếu `tid` → 401. Message thiếu
  `tenantId` → DLT. Code chạm dữ liệu khi chưa có tenant → `IllegalStateException`.
- **Request không có `X-Tenant-Id` vẫn đi qua `TenantFilter`** nhưng không có tenant. Path công khai
  hoạt động; chạm dữ liệu thì nổ. Không lặp lại danh sách path công khai ở service.
- **Email unique theo tenant**, không toàn cục: cùng một người có thể là khách của hai doanh nghiệp.
- Đặt tenant bằng `callWith`/`runWith`, hoặc `set` + `clear` trong `finally` ở filter — thread được
  tái sử dụng.

## Consequences

- ✅ Không có đường nào để client chọn tenant: nó chỉ đến từ chữ ký của auth-service.
- ✅ Một service mới tự động có `TenantFilter` (auto-config) — không có gì để quên.
- ✅ Token phát trước thay đổi này bị 401 — tối đa 15 phút, client chỉ cần refresh.
- ⚠️ Service nội bộ **tin header** — đúng như mô hình gateway-centric hiện tại. Lộ mạng nội bộ =
  bypass. Giải quyết ở Phase 4 bằng mTLS / NetworkPolicy (GAP-B5, B6).
- ⚠️ Endpoint công khai của auth chỉ phục vụ **một** tenant (từ cấu hình). Đủ cho giai đoạn một
  khách hàng; Phase 5 thay bằng tenant suy ra từ API key đã xác thực — **không bao giờ** bằng giá trị
  client tự khai.

## Alternatives rejected

- **Tenant trong header/body lúc đăng ký.** Ai cũng tự xưng được là khách của tenant bất kỳ.
- **Tenant theo subdomain (`acme.walletflow.io`).** Hợp lý cho UI B2C, nhưng phải có danh sách
  subdomain → tenant và vẫn cần JWT mang tenant cho các chặng sau. Có thể thêm ở Phase 5 như một
  cách *chọn* tenant lúc đăng nhập, không thay được claim `tid`.
- **Hibernate `@Filter` / Postgres RLS ngay bây giờ.** Là lớp phòng thủ thứ hai tốt, nhưng không giải
  quyết câu hỏi "tenant đến từ đâu" — RLS cũng phải đọc tenant từ một biến phiên mà ai đó đặt. Để
  Phase 4.

## Liên quan
`TenantContext`, `TenantFilter`, `TenantWebAutoConfiguration`, `JwtAuthenticationFilter`,
`JwtService`, `EnvelopeReader`, auth `V2__user_tenant.sql`, ledger `V4__multi_tenant.sql`,
`TenantIsolationIT`, `TenantFilterTest`, `JwtAuthenticationFilterTest`, `AuthFlowIT`
