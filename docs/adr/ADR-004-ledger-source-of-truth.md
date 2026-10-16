# ADR-004 — Sổ cái double-entry là nguồn sự thật; số dư là read model

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 1

## Context

Cách hiển nhiên để lưu số dư là một cột `balance` trên bảng ví, cộng trừ mỗi lần giao dịch. Cách
này nhanh và dễ hiểu, nhưng nó **phá huỷ thông tin**: sau khi cộng trừ, không còn cách nào biết số
dư hiện tại được tạo thành từ những chuyển động nào. Một bug làm sai số dư sẽ không để lại dấu vết
và không thể sửa đúng, vì không biết đúng là bao nhiêu.

## Decision

**Sổ cái double-entry append-only là nguồn sự thật duy nhất về tiền.**

- Mỗi chuyển động tiền = một `journal_entry` + ≥2 `posting`, tổng bằng 0.
- Tiền từ thế giới ngoài đi vào qua tài khoản hệ thống (`EXTERNAL_BANK`) → **tổng mọi posting toàn
  hệ thống luôn = 0**. Đây là bất biến vàng: khác 0 nghĩa là tiền tự sinh hoặc bốc hơi.
- Số dư là **giá trị suy ra**: `SUM(postings)`.
- `wallet.balance_minor` ở wallet-service chỉ là **cache để đọc nhanh**, cập nhật từ event ledger —
  một dạng CQRS. Không bao giờ sửa trực tiếp bằng nghiệp vụ, và **phải dựng lại được** từ ledger.
- Append-only được cưỡng chế ở **ba tầng**: `ImmutableEntity` không có setter, repository không lộ
  `delete()`, và — tầng duy nhất không thể lách — **trigger ở DB chặn UPDATE/DELETE**.

Tầng thứ ba mới là tầng quan trọng. Hai tầng đầu chỉ là quy ước trong code Java, mà code Java không
phải thứ duy nhất chạm vào database này: còn migration về sau, script vận hành, và SQL viết tay lúc
2 giờ sáng để "sửa nhanh một giao dịch".

## Consequences

- ✅ Mọi số dư giải thích được bằng lịch sử bút toán; sổ cái chính là audit trail.
- ✅ Sai sót sửa bằng bút toán `REVERSAL` — vết sửa cũng được ghi lại, không bị xoá dấu.
- ✅ Đối soát khả thi: `SUM = 0` và `wallet.balance == SUM(postings)` là hai mệnh đề kiểm tra được.
- ⚠️ Đọc số dư bằng `SUM` tốn kém khi dữ liệu lớn → cần read model, và read model thì lệch được
  → bắt buộc có đối soát (Phase 3).
- ⚠️ Read model là *eventual consistent*. Người dùng có thể thấy số dư cũ trong khoảnh khắc.
- ⚠️ Không sửa được dữ liệu sai bằng UPDATE — kể cả khi "chỉ sửa một chút". Đây là tính năng, không
  phải hạn chế.

## Liên quan
ADR-001, `V2__append_only_and_event_id.sql`, `LedgerServiceIT`, roadmap §3.1
