# ADR-001 — Database riêng cho mỗi service

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 0

## Context

Nhiều service cần dữ liệu liên quan tới nhau (user, ví, bút toán, giao dịch). Hai lựa chọn: một
database dùng chung, hay mỗi service một database riêng.

Database dùng chung cho phép JOIN và transaction ACID xuyên nghiệp vụ — rất tiện lúc đầu. Nhưng nó
biến schema thành một hợp đồng ngầm giữa mọi service: đổi một cột là phải kiểm tra tất cả, và
không service nào thực sự *sở hữu* dữ liệu của mình.

## Decision

Mỗi service sở hữu một database riêng (`authdb`, `walletdb`, `ledgerdb`, `txndb`, `railsdb`).
**Không service nào đọc thẳng database của service khác.** Trao đổi dữ liệu chỉ qua API hoặc event.

Ở giai đoạn dev, năm database này nằm trong cùng một instance Postgres — nhưng đó là quyết định
**triển khai**, không phải kiến trúc. Vì không có truy vấn nào xuyên database, việc tách ra thành
các instance riêng ở Phase 4 là thao tác hạ tầng thuần tuý, không phải sửa code.

## Consequences

- ✅ Service tiến hoá schema độc lập; không có "ai đó join vào bảng của tôi".
- ✅ Tách instance riêng cho `ledgerdb` ở Phase 4 không cần sửa code.
- ⚠️ **Mất transaction ACID xuyên service** → phải dùng Saga (ADR-002) và Outbox (ADR-003).
- ⚠️ Mất khả năng JOIN → số dư trở thành read model được chiếu từ event (ADR-004), và phải có đối
  soát để phát hiện lệch (Phase 3).
- ⚠️ Nhất quán trở thành *eventual* giữa các service. Đây là cái giá thật, và là lý do tồn tại của
  job đối soát.

## Liên quan
ADR-002 (Saga), ADR-003 (Outbox), ADR-004 (CQRS), roadmap §4.2
