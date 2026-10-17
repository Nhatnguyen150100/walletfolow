-- =====================================================================
-- V2 — Saga hoàn tất: trạng thái cuối và người xử lý (roadmap 1.2b)
-- =====================================================================

-- MANUAL_REVIEW: ledger không trả lời sau đủ số lần gửi lại. Không đoán: không tự đánh FAILED (tiền
-- có thể ĐÃ đi — bút toán ghi xong mà event bị mất), không tự đánh COMPLETED (có thể chưa đi). Một
-- người phải nhìn sổ cái và quyết. Nếu LedgerPosted đến muộn, saga tự đóng ca này thành COMPLETED.
ALTER TABLE transactions DROP CONSTRAINT ck_transactions_status;
ALTER TABLE transactions ADD CONSTRAINT ck_transactions_status
    CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED', 'MANUAL_REVIEW'));

ALTER TABLE saga_state
    ADD CONSTRAINT ck_saga_state_step_status
        CHECK (step_status IN ('SENT', 'DONE', 'FAILED', 'MANUAL_REVIEW'));

-- Index quét saga treo (V1) chỉ phủ step_status = 'SENT' — đúng tập job quét cần, và gần như rỗng
-- khi hệ thống khoẻ. Không cần thêm gì.
