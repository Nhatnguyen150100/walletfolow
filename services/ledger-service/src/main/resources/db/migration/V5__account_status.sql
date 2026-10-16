-- =====================================================================
-- V5 — Trạng thái tài khoản, do SỔ CÁI cưỡng chế
-- =====================================================================
-- Vì sao trạng thái nằm ở ledger chứ không chỉ ở wallet-service: ví bị đóng băng phải KHÔNG chi
-- được tiền, và nơi duy nhất quyết định được "lệnh này có được ghi nợ không" là nơi đang giữ khoá
-- FOR UPDATE trên tài khoản — tức ledger. Một trạng thái chỉ nằm ở read model sẽ bị vượt qua bởi
-- bất kỳ lệnh nào đến trước khi read model kịp cập nhật. wallet-service chỉ phản chiếu cột này.
--
--   ACTIVE  — bình thường
--   FROZEN  — chặn GHI NỢ, vẫn NHẬN tiền: đóng băng là để tiền không rời ví trong lúc điều tra
--   CLOSED  — chặn cả hai chiều
--
-- REVERSAL không qua kiểm tra trạng thái (bù trừ phải luôn chạy được), nhưng chỉ được hoàn tác
-- ĐÚNG các vế của bút toán gốc — xem LedgerService.validateMirrorsOriginal.
--
-- Giữ DEFAULT 'ACTIVE' (khác với tenant_id ở V4): đây là giá trị nghiệp vụ hợp lý cho mọi tài khoản
-- mới, không phải một định danh mà việc quên khai báo sẽ gây rò rỉ.
ALTER TABLE accounts
    ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
        CONSTRAINT ck_accounts_status CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED'));

-- ---------------------------------------------------------------------
-- status_version — phiên bản của trạng thái, DB tự tăng
-- ---------------------------------------------------------------------
-- Event mang trạng thái (AccountOpened, sau này AccountStatusChanged) cần một định danh đổi MỖI
-- LẦN trạng thái đổi. Dùng chính giá trị trạng thái thì vỡ ở chuỗi ACTIVE -> FROZEN -> ACTIVE: event
-- cuối trùng eventId với event đầu, consumer dedup bỏ đi, read model kẹt ở FROZEN trong khi sổ cái
-- đã mở lại. Một số đếm chỉ tăng thì không bao giờ lặp lại.
--
-- Tăng bằng TRIGGER chứ không bằng code: trạng thái có thể bị đổi bởi vận hành chạy SQL tay trong
-- sự cố, và đó đúng là lúc không ai nhớ tăng version. Trigger cũng chặn việc sửa tay version.
ALTER TABLE accounts
    ADD COLUMN status_version INT NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION accounts_bump_status_version() RETURNS trigger AS
$$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        NEW.status_version := OLD.status_version + 1;
    ELSE
        NEW.status_version := OLD.status_version;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_accounts_status_version
    BEFORE UPDATE
    ON accounts
    FOR EACH ROW
EXECUTE FUNCTION accounts_bump_status_version();

-- ---------------------------------------------------------------------
-- Mỗi bút toán chỉ được hoàn tác MỘT lần
-- ---------------------------------------------------------------------
-- REVERSAL được miễn kiểm tra trạng thái (xem trên), nên phải chặn chặt hơn ở chỗ khác.
-- uk_journal_tenant_txn_type không chặn được hai REVERSAL mang hai transactionId khác nhau cho cùng
-- một bút toán gốc. Hoàn tiền một lệnh rút hai lần là tiền tự sinh, và SUM = 0 vẫn đúng nên không
-- phép kiểm bất biến nào bắt được. LedgerService.validateNotAlreadyReversed chặn ở tầng nghiệp vụ
-- (từ chối rõ ràng); index này là chốt chặn cuối nếu có đường nào đi vòng qua đó.
CREATE UNIQUE INDEX uk_journal_one_reversal
    ON journal_entries (tenant_id, reference_id)
    WHERE type = 'REVERSAL';
