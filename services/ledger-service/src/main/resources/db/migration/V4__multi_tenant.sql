-- =====================================================================
-- V4 — Thêm tenant_id vào sổ cái
--
-- Làm BÂY GIỜ, lúc bảng còn gần như rỗng. Thêm tenant_id sau khi đã có dữ liệu thật
-- là một dự án riêng: phải backfill hàng triệu posting, đổi mọi unique key đang được
-- tham chiếu, và làm việc đó trên một bảng append-only có trigger chặn UPDATE.
-- =====================================================================

-- ---------------------------------------------------------------------
-- ⚠️ Vì sao dùng ADD COLUMN ... DEFAULT rồi DROP DEFAULT, chứ không ALTER + UPDATE
-- ---------------------------------------------------------------------
-- journal_entries và postings có trigger BEFORE UPDATE chặn mọi lệnh sửa (V2). Cách làm
-- thông thường — thêm cột nullable rồi `UPDATE ... SET tenant_id = ...` để backfill — sẽ
-- bị chính trigger đó chặn đứng, và "giải pháp" hiển nhiên là DISABLE TRIGGER trong lúc
-- migrate: tức là tự tay tháo lá chắn của sổ cái, với rủi ro quên bật lại.
--
-- Từ Postgres 11, ADD COLUMN với DEFAULT không ghi lại từng hàng — nó chỉ sửa metadata.
-- Không có UPDATE nào xảy ra, nên trigger FOR EACH ROW không kích hoạt, và bảng triệu
-- hàng cũng xong tức thì. Lá chắn append-only không bị đụng tới một giây nào.
--
-- DROP DEFAULT ngay sau đó: từ đây mọi INSERT PHẢI tự khai tenant_id. Giữ default lại
-- nghĩa là một đường code quên tenant sẽ lặng lẽ ghi vào tenant mặc định — đúng kiểu rò
-- rỉ chéo tenant mà TenantContext.require() sinh ra để chặn.
-- ---------------------------------------------------------------------

ALTER TABLE accounts
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE accounts ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE journal_entries
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE journal_entries ALTER COLUMN tenant_id DROP DEFAULT;

-- postings mang tenant_id TRÙNG LẶP (suy ra được qua entry_id -> journal_entries).
-- Cố ý phi chuẩn hoá: bất biến vàng `SUM(amount_minor) = 0` phải kiểm được THEO TỪNG
-- TENANT, và đó là truy vấn chạy thường xuyên nhất trên bảng lớn nhất. Bắt nó JOIN sang
-- journal_entries mỗi lần là trả giá vĩnh viễn để tiết kiệm 16 byte mỗi hàng.
-- An toàn vì postings là append-only và chỉ được ghi bởi LedgerService, trong cùng
-- transaction với journal_entry cha.
ALTER TABLE postings
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE postings ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE outbox_event
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE outbox_event ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE processed_event
    ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE processed_event ALTER COLUMN tenant_id DROP DEFAULT;

-- ---------------------------------------------------------------------
-- Unique key phải có tenant_id, nếu không tenant này chặn mất tenant kia
-- ---------------------------------------------------------------------
-- uk_accounts_ref_currency cũ nghĩa là chỉ MỘT tenant trên toàn hệ thống được có ví tên
-- 'WALLET-001'. uk_journal_txn_type cũ còn nguy hiểm hơn: transactionId của khách A có thể
-- va vào khoá idempotency của khách B và khiến một lệnh tiền hợp lệ bị coi là trùng rồi
-- lặng lẽ không được thực thi.
ALTER TABLE accounts DROP CONSTRAINT uk_accounts_ref_currency;
ALTER TABLE accounts ADD CONSTRAINT uk_accounts_tenant_ref_currency
    UNIQUE (tenant_id, external_ref, currency);

ALTER TABLE journal_entries DROP CONSTRAINT uk_journal_txn_type;
ALTER TABLE journal_entries ADD CONSTRAINT uk_journal_tenant_txn_type
    UNIQUE (tenant_id, transaction_id, type);

-- ---------------------------------------------------------------------
-- Index: mọi truy vấn nghiệp vụ nay đều bắt đầu bằng tenant_id
-- ---------------------------------------------------------------------
DROP INDEX IF EXISTS idx_postings_account;
CREATE INDEX idx_postings_tenant_account ON postings (tenant_id, account_id);

-- Nền của phép kiểm bất biến vàng theo từng tenant
CREATE INDEX idx_postings_tenant ON postings (tenant_id);

CREATE INDEX idx_journal_tenant_created ON journal_entries (tenant_id, created_at DESC);

-- Partial index của outbox (V3) nay phải mang tenant_id để relay nhiều tenant không quét chéo
DROP INDEX IF EXISTS idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished
    ON outbox_event (created_at)
    WHERE published = false;
CREATE INDEX idx_outbox_tenant ON outbox_event (tenant_id, created_at DESC);

-- ---------------------------------------------------------------------
-- Tài khoản hệ thống là PER-TENANT
-- ---------------------------------------------------------------------
-- EXTERNAL_BANK của khách A và của khách B phải là hai tài khoản khác nhau. Dùng chung
-- nghĩa là số dư nostro của hai khách bị trộn vào nhau — không đối soát được, và tệ hơn,
-- bất biến `SUM = 0` vẫn đúng toàn cục trong khi sổ của từng khách đã sai.
--
-- Ba tài khoản seed ở V1 nay thuộc về tenant mặc định (đã backfill bởi DEFAULT ở trên).
-- Tenant mới sẽ được cấp tài khoản hệ thống lúc onboarding.
