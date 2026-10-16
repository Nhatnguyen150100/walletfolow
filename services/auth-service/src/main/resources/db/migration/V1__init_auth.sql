-- =====================================================================
-- V1 — Schema auth, thay cho `ddl-auto: update`
--
-- ⚠️ authdb trước đây do Hibernate tự sinh (ddl-auto=update) nên KHÔNG có bảng
-- flyway_schema_history và thiếu các cột mới. Dữ liệu ở đó là user dùng thử,
-- bỏ đi được. Dựng lại bằng:
--     docker exec wf-postgres psql -U walletflow -c 'DROP DATABASE authdb'
--     docker exec wf-postgres psql -U walletflow -c 'CREATE DATABASE authdb'
--
-- Cố tình KHÔNG bật `baseline-on-migrate`: với schema sẵn có, nó sẽ lặng lẽ đánh
-- dấu V1 là "đã chạy" rồi để Hibernate validate vỡ ở một chỗ khó hiểu hơn nhiều.
-- Thà hỏng to và rõ ngay từ đầu.
-- =====================================================================

CREATE TABLE users
(
    id                  UUID PRIMARY KEY,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    email               VARCHAR(255) NOT NULL,
    password            VARCHAR(100) NOT NULL, -- BCrypt hash, 60 ký tự
    full_name           VARCHAR(255),
    role                VARCHAR(16)  NOT NULL DEFAULT 'USER',
    status              VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | LOCKED | DISABLED
    failed_login_count  INT          NOT NULL DEFAULT 0,
    locked_until        TIMESTAMPTZ,
    password_changed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Unique KHÔNG PHÂN BIỆT HOA THƯỜNG: 'A@b.com' và 'a@b.com' là cùng một người.
-- Ràng buộc UNIQUE thường trên cột email sẽ cho đăng ký cả hai, và từ đó có hai tài khoản
-- cùng danh tính nhưng khác ví — một dạng chiếm tài khoản rất dễ khai thác.
CREATE UNIQUE INDEX uk_users_email_ci ON users (lower(email));

-- ---------------------------------------------------------------------
-- Refresh token — xoay vòng và phát hiện tái sử dụng
-- ---------------------------------------------------------------------
-- Lưu HASH chứ không lưu token: nếu DB bị đọc trộm, kẻ tấn công vẫn không có token dùng được.
-- Cùng lý do với việc không lưu mật khẩu dạng thô.
--
-- family_id gom cả chuỗi token sinh ra từ một lần đăng nhập. Mỗi lần refresh tạo token mới và
-- thu hồi token cũ. Nếu một token ĐÃ THU HỒI được dùng lại, kết luận duy nhất hợp lý là nó đã
-- bị đánh cắp (người dùng thật đã có token mới rồi) -> thu hồi TOÀN BỘ family, buộc đăng nhập lại.
CREATE TABLE refresh_tokens
(
    id             UUID PRIMARY KEY,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id        UUID        NOT NULL REFERENCES users (id),
    token_hash     VARCHAR(64) NOT NULL, -- SHA-256 hex
    family_id      UUID        NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    revoked_at     TIMESTAMPTZ,
    revoked_reason VARCHAR(32),          -- ROTATED | LOGOUT | REUSE_DETECTED | PASSWORD_CHANGED
    user_agent     VARCHAR(255),
    ip             VARCHAR(45),
    CONSTRAINT uk_refresh_token_hash UNIQUE (token_hash)
);

CREATE INDEX idx_refresh_tokens_user ON refresh_tokens (user_id);
CREATE INDEX idx_refresh_tokens_family ON refresh_tokens (family_id);
-- Dọn token hết hạn: chỉ quan tâm hàng còn sống
CREATE INDEX idx_refresh_tokens_expiry ON refresh_tokens (expires_at) WHERE revoked_at IS NULL;

-- ---------------------------------------------------------------------
-- Audit log — append-only
-- ---------------------------------------------------------------------
-- Mọi hành động nhạy cảm (đăng nhập hỏng, khoá tài khoản, phát hiện tái dùng token, thao tác
-- admin) ghi vào đây. Giống sổ cái, giá trị của audit log nằm ở chỗ nó KHÔNG sửa được — một
-- audit log chỉnh sửa được thì không trả lời được câu hỏi "chuyện gì đã thực sự xảy ra".
CREATE TABLE audit_events
(
    id          UUID PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor_id    UUID,
    actor_email VARCHAR(255),
    action      VARCHAR(64) NOT NULL,
    target      VARCHAR(255),
    ip          VARCHAR(45),
    detail      TEXT
);

CREATE INDEX idx_audit_events_actor ON audit_events (actor_id, created_at);
CREATE INDEX idx_audit_events_action ON audit_events (action, created_at);

CREATE OR REPLACE FUNCTION auth_forbid_mutation() RETURNS trigger AS
$$
BEGIN
    RAISE EXCEPTION 'Bảng % là APPEND-ONLY: không được %.', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_events_append_only
    BEFORE UPDATE OR DELETE
    ON audit_events
    FOR EACH ROW
EXECUTE FUNCTION auth_forbid_mutation();
