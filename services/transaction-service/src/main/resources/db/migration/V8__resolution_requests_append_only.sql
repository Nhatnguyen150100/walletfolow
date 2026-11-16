-- =====================================================================
-- V8 — resolution_requests trở thành append-only (trả nợ ADR-021 A7, ADR-023 §8)
-- =====================================================================
-- A7 viết nguyên văn: "`resolution_requests` không append-only: APPROVED → PENDING bằng SQL tay vẫn
-- làm được. Khác audit_events (có trigger) và khác resolved_at (nay có trigger). Cân nhắc khi bảng
-- này trở thành căn cứ đối soát ở Phase 3."
--
-- Phase 3 tới rồi: bảng 4-eyes là căn cứ giải thích VÌ SAO một break đối soát được đóng. Một căn cứ
-- sửa được bằng một câu UPDATE không để lại dấu vết thì không phải căn cứ — và "sửa bằng SQL tay,
-- không ai biết" chính là vấn đề mở đầu ADR-021.
--
-- Chỉ EXPAND: không đổi cột nào, không đổi dữ liệu nào. Bản app đang chạy chỉ làm đúng hai việc mà
-- trigger dưới đây cho phép (tạo yêu cầu PENDING, rồi quyết một lần) nên nó không gặp lỗi mới.
-- Rolling deploy an toàn theo cả hai chiều.

-- ---------------------------------------------------------------------
-- 1) Siết hai chỗ CHECK còn hở mà A7 nêu kèm
-- ---------------------------------------------------------------------
-- ck_resolution_decided của V6 đòi checker_id và decided_at khi đã quyết, nhưng QUÊN checker_role —
-- một quyết định không biết người duyệt mang vai gì thì không tái lập được quyền lúc đó. Và nó
-- không cấm điền checker_* lúc còn PENDING: một hàng PENDING mang sẵn checker_id là 4-eyes bị rút
-- ruột, chỉ còn chờ một câu UPDATE status.
--
-- An toàn với dữ liệu đang có: mọi hàng do app ghi đều thoả (app luôn điền cả ba cùng lúc ở
-- `decide`, và không bao giờ điền checker_* lúc tạo).
ALTER TABLE resolution_requests
    DROP CONSTRAINT ck_resolution_decided;

ALTER TABLE resolution_requests
    ADD CONSTRAINT ck_resolution_decided CHECK (
        (status = 'PENDING'
            AND checker_id IS NULL AND checker_role IS NULL AND checker_email IS NULL
            AND decided_at IS NULL)
            OR (status <> 'PENDING'
                AND checker_id IS NOT NULL AND checker_role IS NOT NULL
                AND decided_at IS NOT NULL));

-- ---------------------------------------------------------------------
-- 2) Trigger: chỉ đi được MỘT chiều, PENDING -> quyết, rồi đóng băng
-- ---------------------------------------------------------------------
-- Cùng khuôn với txn_resolved_at_is_final() của V6. Ba luật:
--
--   a) Đã quyết là VĨNH VIỄN. OLD.status <> 'PENDING' ⇒ không UPDATE được gì nữa. Đây là luật đóng
--      đúng lỗ hổng A7 mô tả (APPROVED -> PENDING).
--   b) Lúc còn PENDING, mọi trường của NGƯỜI ĐỀ XUẤT là bất biến. Không chỉ là vệ sinh dữ liệu:
--      người duyệt ký vào một đề xuất cụ thể, và nếu đề xuất đó sửa được sau khi họ đọc thì 4-eyes
--      chỉ là hai người ký vào hai thứ khác nhau. Cùng lý lẽ với target_entry_id ở V7.
--   c) Cấm DELETE, luôn. Xoá một yêu cầu đụng tiền là xoá bằng chứng.
--
-- updated_at cố ý KHÔNG nằm trong danh sách bất biến: nó là siêu dữ liệu của hàng, không phải nội
-- dung đề xuất.
CREATE OR REPLACE FUNCTION resolution_request_is_append_only() RETURNS trigger AS
$$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'resolution_requests APPEND-ONLY: không xoá được yêu cầu % (thao tác đụng tiền phải để lại bằng chứng).',
            OLD.id;
    END IF;

    IF OLD.status <> 'PENDING' THEN
        RAISE EXCEPTION 'resolution_requests APPEND-ONLY: yêu cầu % đã ở trạng thái % — quyết định là vĩnh viễn, không sửa được.',
            OLD.id, OLD.status;
    END IF;

    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
        OR NEW.transaction_id IS DISTINCT FROM OLD.transaction_id
        OR NEW.action IS DISTINCT FROM OLD.action
        OR NEW.maker_id IS DISTINCT FROM OLD.maker_id
        OR NEW.maker_email IS DISTINCT FROM OLD.maker_email
        OR NEW.maker_role IS DISTINCT FROM OLD.maker_role
        OR NEW.reason IS DISTINCT FROM OLD.reason
        OR NEW.evidence IS DISTINCT FROM OLD.evidence
        OR NEW.target_entry_id IS DISTINCT FROM OLD.target_entry_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'resolution_requests APPEND-ONLY: không sửa được nội dung đề xuất % — người duyệt phải ký vào đúng thứ người đề xuất đã trình.',
            OLD.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_resolution_requests_append_only
    BEFORE UPDATE OR DELETE
    ON resolution_requests
    FOR EACH ROW
EXECUTE FUNCTION resolution_request_is_append_only();
