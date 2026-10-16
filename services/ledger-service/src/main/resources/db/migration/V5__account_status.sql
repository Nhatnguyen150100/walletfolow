-- =====================================================================
-- V5 — Trạng thái tài khoản, do SỔ CÁI cưỡng chế
-- =====================================================================
-- Vì sao trạng thái nằm ở ledger chứ không chỉ ở wallet-service: ví bị đóng băng phải KHÔNG chi
-- được tiền, và nơi duy nhất quyết định được "lệnh này có được ghi nợ không" là nơi đang giữ khoá
-- FOR UPDATE trên tài khoản — tức ledger. Một trạng thái chỉ nằm ở read model sẽ bị vượt qua bởi
-- bất kỳ lệnh nào đến trước khi read model kịp cập nhật. wallet-service chỉ phản chiếu cột này.
--
--   ACTIVE  — bình thường
--   FROZEN  — chặn GHI NỢ, vẫn NHẬN tiền: hoàn tiền về một ví đang bị đóng băng phải chạy được,
--             nếu không bước bù trừ của saga thất bại và tiền kẹt ở tài khoản trung gian
--   CLOSED  — chặn cả hai chiều
--
-- Giữ DEFAULT 'ACTIVE' (khác với tenant_id ở V4): đây là giá trị nghiệp vụ hợp lý cho mọi tài khoản
-- mới, không phải một định danh mà việc quên khai báo sẽ gây rò rỉ.
ALTER TABLE accounts
    ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
        CONSTRAINT ck_accounts_status CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED'));
