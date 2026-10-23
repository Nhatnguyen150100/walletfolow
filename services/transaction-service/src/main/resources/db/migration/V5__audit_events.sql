-- =====================================================================
-- V5 — Dấu vết thao tác vận hành + hàng đợi cần người can thiệp (roadmap 2.5, ADR-021)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) audit_events — mọi thao tác của người vận hành trên giao dịch
-- ---------------------------------------------------------------------
-- Cùng hình dạng với `audit_events` của auth-service (auth V1), KHÁC hai chỗ, cả hai đều có lý do:
--
--   * có `tenant_id`: bảng của auth ra đời trước multi-tenancy và tới giờ vẫn thiếu cột này. Ở đây
--     mọi hàng nghiệp vụ đều thuộc một tenant (ADR-010) và một câu hỏi kiểm toán luôn có dạng "ai
--     đã đụng vào dữ liệu của tenant nào" — thiếu cột thì câu hỏi đó không trả lời được.
--   * có `actor_role`: 4-eyes (PR2) cần biết người thao tác đang mang vai gì LÚC ĐÓ. Vai đổi theo
--     thời gian, nên đọc lại từ `users` về sau là đọc một sự thật khác.
--
-- Vì sao một bảng RIÊNG ở txndb thay vì gọi sang auth-service: audit mà đi qua mạng thì mạng hỏng
-- là mất dấu vết, đúng lúc cần nhất. Bảng nằm cùng DB với thứ nó ghi lại, nên nó vào cùng một
-- transaction (ADR-021).
--
-- `detail` là JSONB chứ không phải TEXT như bên auth: trước/sau của một thao tác là dữ liệu có cấu
-- trúc, và báo cáo đối soát (Phase 3) sẽ phải lọc theo nó.
CREATE TABLE audit_events
(
    id          UUID PRIMARY KEY,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    tenant_id   UUID         NOT NULL,
    actor_id    UUID         NOT NULL,
    actor_email VARCHAR(255),
    actor_role  VARCHAR(16)  NOT NULL,
    action      VARCHAR(64)  NOT NULL,
    target      VARCHAR(255),
    ip          VARCHAR(45),
    detail      JSONB
);

-- Hai câu hỏi của kiểm toán: "người này đã làm gì" và "giao dịch này đã bị ai đụng vào".
CREATE INDEX idx_audit_events_actor ON audit_events (tenant_id, actor_id, created_at DESC);
CREATE INDEX idx_audit_events_target ON audit_events (tenant_id, target, created_at DESC);

-- Giống sổ cái: giá trị của audit log nằm ở chỗ nó KHÔNG sửa được. Một audit log chỉnh sửa được
-- thì không trả lời được câu hỏi "chuyện gì đã thực sự xảy ra" — nó chỉ nói "chuyện gì còn được
-- để lại cho ta đọc". Trigger chặn ở DB, không phải ở tầng app: người sửa tay cũng không qua được.
CREATE OR REPLACE FUNCTION txn_forbid_mutation() RETURNS trigger AS
$$
BEGIN
    RAISE EXCEPTION 'Bảng % là APPEND-ONLY: không được %.', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_events_append_only
    BEFORE UPDATE OR DELETE
    ON audit_events
    FOR EACH ROW
EXECUTE FUNCTION txn_forbid_mutation();

-- ---------------------------------------------------------------------
-- 2) Index cho hàng đợi cần người can thiệp
-- ---------------------------------------------------------------------
-- `GET /api/v1/admin/transactions/manual-review` lọc theo (tenant, status). Index duy nhất đang có
-- trên bảng này là (tenant_id, initiated_by, created_at) — vô dụng cho câu đó, nên queue sẽ quét
-- toàn bảng `transactions`, thứ chỉ mọc thêm mãi.
--
-- RIÊNG PHẦN: chỉ đánh chỉ mục những trạng thái cần người nhìn. Đó là một phần rất nhỏ của bảng
-- (gần như mọi giao dịch kết thúc ở COMPLETED/FAILED), nên index bé và luôn nằm trong bộ nhớ.
-- REFUNDING có mặt ở đây vì nó là ca hệ thống đang tự xử lý nhưng người trực vẫn cần nhìn được —
-- đứng quá lâu ở đó là một sự cố (runbook topup-stuck mục E).
-- Thứ tự cột theo ĐÚNG `ORDER BY created_at ASC, id ASC` của hàng đợi, kể cả `id`: lệch chiều hay
-- thiếu cột chốt thì kế hoạch vẫn dùng index nhưng chồng thêm một bước sort — làm đúng bây giờ rẻ
-- hơn một migration nữa về sau.
CREATE INDEX idx_transactions_needs_review
    ON transactions (tenant_id, created_at, id)
    WHERE status IN ('MANUAL_REVIEW', 'REFUNDING');
