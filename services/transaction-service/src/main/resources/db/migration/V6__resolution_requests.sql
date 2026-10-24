-- =====================================================================
-- V6 — Xử lý ca cần can thiệp: 4-eyes + dấu "đã có người xử lý" (roadmap 2.5 PR2, ADR-021)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) transactions.resolved_at — dấu BỀN "ca này đã được người xử lý"
-- ---------------------------------------------------------------------
-- Vì sao cần, và vì sao phải nằm trên chính hàng `transactions`:
--
--   Sau khi người vận hành hoàn tay qua portal và đánh dấu, giao dịch về FAILED. Một `LedgerPosted`
--   tới muộn sau đó rơi vào nhánh "ledger thắng" của SagaService (`case FAILED -> complete()`) và
--   đẩy giao dịch thành COMPLETED — ví được cộng TRONG KHI tiền đã trả lại qua portal. Người dùng
--   nhận tiền hai lần, và giao dịch trông như một lần nạp thành công nên không ai nhìn lại.
--
--   Ca này KHÔNG phải giả định: lời từ chối của ledger TRƯỚC V6 của ledger-service không có hàng rào
--   `rejected_topups`, nên lệnh ghi sổ gửi lại vẫn ghi được sau khi ví được mở lại.
--
--   Cùng khuôn với hàng rào của 2.4b (`saga_state.current_step = 'BANK_REFUND'`), nhưng đặt trên
--   `transactions` vì `SagaService` đã KHOÁ và đọc sẵn hàng này ở mọi đường — kiểm thêm một cột là
--   miễn phí, trong khi đọc một bảng khác là một lượt đi về DB cho cả đường chuyển tiền.
--
-- Không bao giờ bị xoá về NULL: "đã có người đụng vào" là sự thật không thu hồi được.
ALTER TABLE transactions ADD COLUMN resolved_at TIMESTAMPTZ;

COMMENT ON COLUMN transactions.resolved_at IS
    'Lúc một người vận hành chốt ca này (2.5 PR2). Khác NULL = ĐÃ có người xử lý: ledger ghi sổ sau '
        'thời điểm này là xung đột, không phải "ledger thắng". Chi tiết ai/vì sao ở audit_events.';

-- Dấu này là hàng rào TIỀN, nên nó phải chịu được cả một câu UPDATE gõ tay. Gỡ nó về NULL là mở lại
-- đúng cái cửa hoàn-tiền-hai-lần mà nó sinh ra để đóng — và "sửa bằng SQL tay, không để lại dấu vết"
-- chính là vấn đề mở đầu ADR-021. Cho phép đặt lần đầu, cấm xoá và cấm sửa sang giá trị khác.
CREATE OR REPLACE FUNCTION txn_resolved_at_is_final() RETURNS trigger AS
$$
BEGIN
    IF OLD.resolved_at IS NOT NULL AND NEW.resolved_at IS DISTINCT FROM OLD.resolved_at THEN
        RAISE EXCEPTION 'transactions.resolved_at là VĨNH VIỄN: không gỡ hay sửa được (giao dịch %).',
            OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_resolved_at_final
    BEFORE UPDATE OF resolved_at
    ON transactions
    FOR EACH ROW
EXECUTE FUNCTION txn_resolved_at_is_final();

-- ---------------------------------------------------------------------
-- 2) resolution_requests — 4-eyes cho thao tác đụng tiền (GAP-A4)
-- ---------------------------------------------------------------------
-- Chỉ thao tác ĐỤNG TIỀN mới qua đây. Mở lại một ca kẹt không tạo chuyển động tiền nào (ledger
-- idempotent theo transactionId nên lệnh gửi lại không ghi bút toán thứ hai), nên bắt nó chờ người
-- thứ hai lúc 2 giờ sáng là đổi một rủi ro thật lấy một thủ tục.
CREATE TABLE resolution_requests
(
    id             UUID PRIMARY KEY,
    tenant_id      UUID         NOT NULL,
    transaction_id UUID         NOT NULL REFERENCES transactions (id),
    action         VARCHAR(32)  NOT NULL
        CONSTRAINT ck_resolution_action CHECK (action IN ('MARK_REFUNDED_MANUALLY')),
    status         VARCHAR(16)  NOT NULL
        CONSTRAINT ck_resolution_status CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    -- Người ĐỀ XUẤT và người DUYỆT. Email lưu lại vì vai và tài khoản đổi theo thời gian; đọc lại từ
    -- auth-service về sau là đọc một sự thật khác.
    maker_id       UUID         NOT NULL,
    maker_email    VARCHAR(255),
    maker_role     VARCHAR(16)  NOT NULL,
    checker_id     UUID,
    checker_email  VARCHAR(255),
    checker_role   VARCHAR(16),
    -- Vì sao phải làm, và bằng chứng (mã lần hoàn ở portal của provider). Bắt buộc có lý do: một
    -- thao tác đụng tiền không giải thích được thì không nên xảy ra.
    reason         TEXT         NOT NULL,
    evidence       TEXT,
    decided_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- 4-EYES, cưỡng chế ở DB: người duyệt không bao giờ là người đề xuất. Đây là chốt chặn cuối —
    -- tầng app cũng kiểm, nhưng một lỗi ở đó không được phép là thứ duy nhất đứng giữa.
    CONSTRAINT ck_resolution_four_eyes CHECK (checker_id IS NULL OR checker_id <> maker_id),
    -- Đã quyết thì phải biết ai quyết và lúc nào; còn PENDING thì chưa ai quyết cả.
    CONSTRAINT ck_resolution_decided CHECK (
        (status = 'PENDING' AND checker_id IS NULL AND decided_at IS NULL)
            OR (status <> 'PENDING' AND checker_id IS NOT NULL AND decided_at IS NOT NULL))
);

-- Mỗi giao dịch tối đa MỘT yêu cầu đang chờ: không thì hai người đề xuất hai thứ khác nhau và người
-- thứ ba duyệt nhầm cái nào cũng được.
CREATE UNIQUE INDEX uk_resolution_one_open
    ON resolution_requests (tenant_id, transaction_id) WHERE status = 'PENDING';

-- Màn hình "chờ tôi duyệt" đọc theo tenant + trạng thái, cũ nhất trước.
CREATE INDEX idx_resolution_pending
    ON resolution_requests (tenant_id, created_at) WHERE status = 'PENDING';
