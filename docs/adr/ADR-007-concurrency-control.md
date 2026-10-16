# ADR-007 — Khoá hàng có thứ tự cho ledger; optimistic lock cho read model

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 1

## Context

Hai giao dịch cùng trừ tiền một ví chạy song song. Nếu đọc số dư rồi mới trừ, cả hai đọc được cùng
giá trị cũ và cả hai đều thấy "đủ tiền" → lost update, và số dư âm.

Nhưng ledger có một đặc điểm khiến các giải pháp thông thường không dùng được: **số dư ở đây là giá
trị suy ra (`SUM(postings)`), không phải một cột**. Nên không có hàng nào để gắn `@Version`, và
cũng không viết được `UPDATE ... WHERE balance >= :amount`.

## Decision

**Ở ledger — khoá bi quan có thứ tự:** dùng chính hàng `accounts` làm mutex cho tài khoản đó
(`SELECT ... FOR UPDATE`). Giữa lúc đọc số dư và lúc ghi posting, không transaction nào chen vào được.

Khoá được lấy theo thứ tự `accountRef` **đã sắp xếp**. Mọi transaction xin khoá theo cùng một trình
tự → **không bao giờ deadlock chéo**. Đây là chi tiết dễ bỏ sót nhất và trả giá muộn nhất: deadlock
chéo chỉ xuất hiện khi có tải thật.

**Ở read model (wallet-service) — optimistic lock:** `@Version` + conditional update
(`UPDATE ... SET balance = balance + :delta WHERE id = :id`), không read-modify-write ở tầng ứng dụng.
Ở đây optimistic phù hợp vì tranh chấp thấp và đây không phải nguồn sự thật — sai thì rebuild được
từ ledger.

## Consequences

- ✅ Số dư không bao giờ âm; chứng minh bằng test 100 luồng song song (`LedgerServiceIT`).
- ✅ Không deadlock, kể cả khi hai bút toán chạm cùng tập tài khoản theo thứ tự ngược nhau.
- ⚠️ **Đánh đổi đã biết:** mọi top-up/withdraw đều khoá `EXTERNAL_BANK` → các luồng này bị tuần tự
  hoá với nhau. Transfer ví→ví không chạm tài khoản hệ thống nên vẫn song song tốt. Khi throughput
  nạp/rút thành nút thắt, lời giải là **tách nostro theo provider/shard** (roadmap §4.5) — không
  phải bỏ khoá.
- ⚠️ Khoá bi quan giữ kết nối lâu hơn → cần theo dõi connection pool.

## Liên quan
ADR-004, `LedgerService.resolveAccounts`, `LedgerServiceIT`, roadmap §4.5
