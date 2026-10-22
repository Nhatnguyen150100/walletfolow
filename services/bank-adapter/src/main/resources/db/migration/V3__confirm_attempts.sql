-- ADR-017 Amendment 2: vòng hỏi XÁC NHẬN sau EXPIRED có trần số lần, để một hàng provider không bao giờ
-- trả lời được thôi bị hỏi lại mỗi max-recheck-after mãi mãi trong im lặng.
--
-- Cột RIÊNG chứ không dùng lại check_attempts (vốn đếm cho giai đoạn PENDING), vì ba lý do:
--
--  1. Hàng đã EXPIRED từ trước mang check_attempts tích luỹ của giai đoạn PENDING (backoff 30s → 10m qua
--     30 phút quá hạn + grace thường để lại 6–8 lượt). Dùng chung cột thì ngay lần hỏi xác nhận đầu tiên
--     chúng đã vượt trần — vừa mất cơ hội phát hiện tiền về muộn, vừa bắn alert giả. Cột mới mặc định 0 nên
--     hàng cũ nhận đủ hạn mức, không cần backfill.
--  2. Hàng cũ đã xác nhận xong có next_check_at NULL và check_attempts lớn: dùng chung cột thì không phân
--     biệt được "đã xác nhận" với "hết lượt".
--  3. Lúc rolling deploy, bản app cũ không bao giờ ghi cột này, nên nó không thể làm hỏng bộ đếm của bản mới.
--
-- EXPAND-only: thêm cột nullable-an-toàn (NOT NULL + DEFAULT 0 không rewrite bảng trên PG11+). Bản app đang
-- chạy không biết cột này và không cần biết.
ALTER TABLE provider_transactions
    ADD COLUMN confirm_attempts INT NOT NULL DEFAULT 0;

COMMENT ON COLUMN provider_transactions.confirm_attempts IS
    'Số lần đã hỏi XÁC NHẬN sau khi hàng sang FAILED(EXPIRED) — tách khỏi check_attempts của giai đoạn PENDING';
