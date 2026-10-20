-- =====================================================================
-- V3 — Saga nạp tiền: lệnh TOPUP không có ví nguồn + hai trạng thái chờ (roadmap 2.4a, ADR-019)
-- =====================================================================
-- Expand thuần: mọi thay đổi ở đây đều NỚI ra. Bản app đang chạy chỉ ghi TRANSFER với hai ví
-- không null và khác nhau, và chỉ đọc ba trạng thái cũ — nó chạy nguyên vẹn trên schema này.

-- ---------------------------------------------------------------------
-- Ví nguồn — null với TOPUP
-- ---------------------------------------------------------------------
-- Nạp tiền: tiền đến từ ngân hàng, không từ ví nào. Không bịa một giá trị (lặp lại ví đích, hay một
-- UUID "ví hệ thống" không tồn tại ở wallet-service) — cột sẽ nói dối, và mọi báo cáo đọc nó về sau
-- đều sai. Ràng buộc chuyển thành một CHECK theo LOẠI giao dịch, nên DB vẫn là nơi chặn hàng vô nghĩa.
ALTER TABLE transactions ALTER COLUMN from_wallet DROP NOT NULL;

ALTER TABLE transactions DROP CONSTRAINT ck_transactions_distinct_wallets;
ALTER TABLE transactions ADD CONSTRAINT ck_transactions_wallets CHECK (
    CASE type
        -- Chuyển tiền: đủ hai ví và phải khác nhau (giữ nguyên luật cũ)
        WHEN 'TRANSFER' THEN from_wallet IS NOT NULL AND from_wallet <> to_wallet
        -- Nạp tiền: KHÔNG được có ví nguồn. Có là bug producer, không phải dữ liệu hợp lệ.
        WHEN 'TOPUP' THEN from_wallet IS NULL
        -- WITHDRAW/REVERSAL chưa mở (2.1 còn chờ kết luận pháp lý) — để lọt, luật của chúng sẽ được
        -- thêm bằng migration của chính mục đó, không đoán trước ở đây.
        ELSE TRUE
    END
);

-- ---------------------------------------------------------------------
-- Hai trạng thái chờ của saga nạp tiền
-- ---------------------------------------------------------------------
-- AWAITING_PAYMENT: đã có trang thanh toán, đang chờ NGƯỜI DÙNG trả tiền. Có thể kéo dài hàng chục
--                   phút và kết thúc bằng "không trả" — khác hẳn PROCESSING (đang chờ MÁY trả lời).
-- POSTING:          provider đã xác nhận thu được tiền, lệnh ghi sổ đã gửi, đang chờ ledger. Tách
--                   riêng vì đây là cửa sổ nguy hiểm nhất: tiền ĐÃ ở chỗ ta mà chưa vào ví ai. Nhìn
--                   PROCESSING không phân biệt được ca này với ca chưa thu được đồng nào.
ALTER TABLE transactions DROP CONSTRAINT ck_transactions_status;
ALTER TABLE transactions ADD CONSTRAINT ck_transactions_status
    CHECK (status IN ('PROCESSING', 'AWAITING_PAYMENT', 'POSTING', 'COMPLETED', 'FAILED', 'MANUAL_REVIEW'));

-- ---------------------------------------------------------------------
-- Phiên thanh toán ở provider
-- ---------------------------------------------------------------------
-- Lưu ở đây để GET /transactions/{id} trả được trang thanh toán mà không phải gọi sang bank-adapter:
-- client gửi lệnh xong là poll chính nó. provider_ref còn là mối nối để đối soát ngược sang
-- bank-adapter khi có sự cố (runbook topup-stuck.md).
ALTER TABLE transactions ADD COLUMN provider     VARCHAR(32);
ALTER TABLE transactions ADD COLUMN provider_ref VARCHAR(128);
ALTER TABLE transactions ADD COLUMN payment_url  TEXT;
