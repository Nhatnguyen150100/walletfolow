-- =====================================================================
-- V4 — Trạng thái REFUNDING của saga nạp tiền (roadmap 2.4b, ADR-020)
-- =====================================================================
-- Expand thuần: chỉ NỚI danh sách trạng thái hợp lệ. Bản app đang chạy không bao giờ ghi giá trị mới
-- (việc gửi lệnh hoàn nằm sau cờ walletflow.transaction.topup.refund-enabled, mặc định tắt), nên nó
-- chạy nguyên vẹn trên schema này.
--
-- THỨ TỰ TRIỂN KHAI (ADR-019 §6b — bài học của V3, và ADR-020 §5):
--   1. V4 + bản app đọc được REFUNDING lên TOÀN BỘ instance. Instance cũ không map được giá trị lạ qua
--      @Enumerated(STRING): GET /api/v1/transactions/{id} trả 500 và job quét saga của nó nổ mỗi lượt.
--   2. Mọi instance ledger đã lên V6 (hàng rào rejected_topups) — lời từ chối phải là lời cuối cùng,
--      nếu không ta vừa hoàn tiền vừa để một bản gửi lại ghi sổ thành công.
--   3. Chỉ khi đó mới bật cờ refund-enabled.

-- REFUNDING: đã thu được tiền, không ghi sổ được, lệnh hoàn đã gửi sang bank-adapter và đang chờ trả
--            lời. Tách riêng khỏi MANUAL_REVIEW vì đây là ca hệ thống TỰ xử lý được — gộp chung thì
--            alert "cần người can thiệp" kêu cho những ca không cần người nào.
--
-- KHOÁ: hai câu dưới giữ ACCESS EXCLUSIVE trên `transactions` tới hết migration, và câu ADD còn quét
-- toàn bảng để validate. Chấp nhận ở quy mô hiện tại (bảng còn nhỏ, cùng khuôn với V3). Mẫu NOT VALID
-- + VALIDATE KHÔNG giúp gì nếu để chung file: Flyway bọc mỗi script trong một transaction trên Postgres,
-- nên VALIDATE vẫn chạy dưới đúng khoá đó. Khi bảng đủ lớn để quan tâm thì phải tách VALIDATE sang một
-- migration riêng có `executeInTransaction=false` — đừng viết nó ở đây rồi tưởng là đã an toàn.
ALTER TABLE transactions DROP CONSTRAINT ck_transactions_status;
ALTER TABLE transactions ADD CONSTRAINT ck_transactions_status
    CHECK (status IN ('PROCESSING', 'AWAITING_PAYMENT', 'POSTING', 'REFUNDING',
                      'COMPLETED', 'FAILED', 'MANUAL_REVIEW'));
