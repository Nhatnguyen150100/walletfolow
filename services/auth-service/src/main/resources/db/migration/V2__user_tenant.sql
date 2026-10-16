-- =====================================================================
-- V2 — Người dùng thuộc về một tenant
--
-- Đây là nửa còn thiếu của multi-tenancy (roadmap 1.0b): tenant đã đi qua Kafka và vào sổ cái,
-- nhưng chưa có đường nào đi vào từ HTTP. Nguồn của tenant phía HTTP là claim `tid` trong JWT, và
-- nguồn của claim đó là cột này.
--
-- Cùng kỹ thuật với V4 của ledger: ADD COLUMN ... DEFAULT (chỉ sửa metadata, tức thì) rồi DROP
-- DEFAULT ngay — từ đây mọi INSERT phải tự khai tenant, quên là hỏng to thay vì lặng lẽ rơi vào
-- tenant mặc định.
-- =====================================================================

ALTER TABLE users
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE users ALTER COLUMN tenant_id DROP DEFAULT;

-- ---------------------------------------------------------------------
-- Email unique THEO TENANT, không phải toàn cục
-- ---------------------------------------------------------------------
-- Ở con đường B (roadmap §4), tenant là một doanh nghiệp, người dùng là khách của doanh nghiệp đó.
-- Cùng một người hoàn toàn có thể dùng ví của hai doanh nghiệp khác nhau. Unique toàn cục nghĩa là
-- người đó không đăng ký được ở doanh nghiệp thứ hai — và tệ hơn, thông báo "email đã được đăng ký"
-- cho tenant B biết người này là khách của một tenant khác.
--
-- Vẫn giữ lower(email): 'A@b.com' và 'a@b.com' trong CÙNG một tenant vẫn là một người.
DROP INDEX uk_users_email_ci;
CREATE UNIQUE INDEX uk_users_tenant_email_ci ON users (tenant_id, lower(email));
