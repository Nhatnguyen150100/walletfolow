-- =====================================================================
-- V7 — Thao tác vận hành thứ hai: ĐẢO BÚT TOÁN (roadmap 2.5 PR4, ADR-022)
-- =====================================================================
-- PR2 chỉ có một thao tác (`MARK_REFUNDED_MANUALLY`) và CHECK khoá cứng đúng giá trị đó. PR4 thêm
-- `REVERSE_LEDGER_ENTRY` cho ca `REFUND_POSTED_CONFLICT`: ledger đã ghi bút toán nạp TRONG KHI tiền
-- cũng đã được hoàn — ví giữ một khoản người dùng đã được trả lại, và cách duy nhất lấy nó ra mà
-- không phá sổ cái là một bút toán REVERSAL (ADR-004, ADR-011 §6).
--
-- `target_entry_id` — bút toán sẽ bị đảo — chốt NGAY LÚC ĐỀ XUẤT, không phải lúc duyệt. Hai lý do:
--   1. Lấy nó phải gọi HTTP sang ledger, mà `decide` chạy trong transaction DB (CLAUDE.md cấm gọi
--      HTTP trong transaction).
--   2. Quan trọng hơn: người duyệt phải thấy ĐÚNG bút toán mà người đề xuất đã xem. Tra lại lúc
--      duyệt có thể ra một bút toán khác (ledger vừa ghi thêm) — và 4-eyes lúc đó chỉ là hai người
--      ký vào hai thứ khác nhau.
--
-- Tương thích ngược (expand-only):
--   * CHECK được NỚI, mọi hàng cũ vẫn thoả. Bản app đang chạy chỉ ghi MARK_REFUNDED_MANUALLY nên
--     không gặp lỗi mới; nó cũng không đọc cột mới.
--   * Cột mới NULL được, nên INSERT của bản app cũ (không nhắc tới cột này) vẫn chạy.
--   * `ck_resolution_target_entry` phát biểu tương đương hai chiều, nên nó cũng cấm luôn việc điền
--     `target_entry_id` cho một thao tác KHÔNG phải đảo bút toán. Đặt một id bút toán vào một yêu
--     cầu "đánh dấu đã hoàn tay" là vô nghĩa, và vô nghĩa ở bảng này về sau thành một căn cứ đối
--     soát sai (Phase 3).
ALTER TABLE resolution_requests
    DROP CONSTRAINT ck_resolution_action;

ALTER TABLE resolution_requests
    ADD CONSTRAINT ck_resolution_action
        CHECK (action IN ('MARK_REFUNDED_MANUALLY', 'REVERSE_LEDGER_ENTRY'));

ALTER TABLE resolution_requests
    ADD COLUMN target_entry_id UUID;

COMMENT ON COLUMN resolution_requests.target_entry_id IS
    'Bút toán ledger sẽ bị đảo (journal_entries.id), chốt lúc đề xuất. NULL với thao tác khác.';

ALTER TABLE resolution_requests
    ADD CONSTRAINT ck_resolution_target_entry
        CHECK ((action = 'REVERSE_LEDGER_ENTRY') = (target_entry_id IS NOT NULL));
