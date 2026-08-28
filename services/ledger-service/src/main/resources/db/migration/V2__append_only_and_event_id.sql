-- =====================================================================
-- V2 — Cưỡng chế APPEND-ONLY cho sổ cái + eventId cho outbox
-- (plan §3 "Ledger append-only", §5.2, §7.1, §11)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Bảng bất biến thì không có "lần sửa cuối"
-- ---------------------------------------------------------------------
-- Một cột updated_at trên bảng append-only là mâu thuẫn tự thân: nó ngụ ý hàng có thể đổi, trong khi
-- toàn bộ tính kiểm toán được của sổ cái dựa trên điều ngược lại. Sai sót được sửa bằng bút toán
-- REVERSAL, không phải bằng UPDATE.
ALTER TABLE journal_entries DROP COLUMN updated_at;
ALTER TABLE postings DROP COLUMN updated_at;

-- ---------------------------------------------------------------------
-- 2) REVERSAL phải trỏ về một bút toán thật
-- ---------------------------------------------------------------------
ALTER TABLE journal_entries
    ADD CONSTRAINT fk_journal_reference
        FOREIGN KEY (reference_id) REFERENCES journal_entries (id);

-- ---------------------------------------------------------------------
-- 3) Chốt chặn cuối: DB từ chối UPDATE/DELETE trên sổ cái
-- ---------------------------------------------------------------------
-- Entity không có setter và repository không lộ delete() — nhưng cả hai đều là quy ước trong code
-- Java, và code Java không phải thứ duy nhất chạm vào database này (migration về sau, script vận
-- hành, SQL viết tay lúc 2 giờ sáng để "sửa nhanh một giao dịch"). Trigger là nơi duy nhất mà lời
-- hứa "sổ cái không bao giờ bị sửa" trở thành không thể phá vỡ.
CREATE OR REPLACE FUNCTION ledger_forbid_mutation() RETURNS trigger AS
$$
BEGIN
    RAISE EXCEPTION 'Bảng % là APPEND-ONLY: không được %. Hãy ghi một bút toán REVERSAL thay vì sửa bút toán cũ.',
        TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_journal_entries_append_only
    BEFORE UPDATE OR DELETE
    ON journal_entries
    FOR EACH ROW
EXECUTE FUNCTION ledger_forbid_mutation();

CREATE TRIGGER trg_postings_append_only
    BEFORE UPDATE OR DELETE
    ON postings
    FOR EACH ROW
EXECUTE FUNCTION ledger_forbid_mutation();

-- Ghi chú: TRUNCATE không kích hoạt trigger FOR EACH ROW, nên test vẫn dọn được dữ liệu giữa các
-- ca test bằng TRUNCATE. Đây là chủ ý: chặn con đường mà code nghiệp vụ có thể vô tình đi
-- (UPDATE/DELETE từng hàng), không phải chặn thao tác quản trị tường minh.

-- ---------------------------------------------------------------------
-- 4) outbox: eventId ra cột riêng
-- ---------------------------------------------------------------------
-- eventId đã nằm trong JSON của envelope, nhưng "event X đã được phát chưa?" là câu hỏi thường trực
-- khi đối soát và debug — nó cần tra được bằng index, không phải bằng cách bới JSON.
ALTER TABLE outbox_event ADD COLUMN event_id UUID;
UPDATE outbox_event SET event_id = gen_random_uuid() WHERE event_id IS NULL;
ALTER TABLE outbox_event ALTER COLUMN event_id SET NOT NULL;
CREATE INDEX idx_outbox_event_id ON outbox_event (event_id);
