-- =====================================================================
-- V1 — transaction-service: nhận lệnh chuyển tiền + trạng thái saga (roadmap 1.2)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Idempotency-Key — "gửi lại không trừ tiền lần hai"
-- ---------------------------------------------------------------------
-- Khoá theo (tenant, NGƯỜI DÙNG, key) — không theo (tenant, key) như bản đầu của roadmap. Hai người
-- tình cờ dùng cùng một chuỗi key không bao giờ đụng nhau, và không có câu trả lời nào ("key này đã
-- có người dùng") tiết lộ hoạt động của người khác. Xem ADR-013.
--
-- Cố ý KHÔNG có trạng thái IN_PROGRESS: transfer trả 202 rồi chạy bất đồng bộ, nên toàn bộ phần nhận
-- lệnh (hàng này + transactions + saga_state + outbox) là MỘT transaction cục bộ, và hàng được ghi
-- thẳng ở trạng thái "đã có câu trả lời". Hai request trùng song song: Postgres bắt cái thứ hai chờ ở
-- khoá chính tới khi cái đầu commit, rồi cái thứ hai thấy hàng đã có và trả lại đúng câu trả lời đó.
-- Không có cửa sổ nào để một request thấy "đang xử lý".
CREATE TABLE idempotency_keys
(
    tenant_id       UUID         NOT NULL,
    user_id         UUID         NOT NULL,
    idem_key        VARCHAR(64)  NOT NULL,
    -- SHA-256 của nội dung lệnh ở dạng chuẩn hoá — cùng key mà khác nội dung là bug phía client
    request_hash    VARCHAR(64)  NOT NULL,
    transaction_id  UUID         NOT NULL,
    response_status SMALLINT     NOT NULL,
    -- Nguyên văn response đã trả lần đầu: gửi lại nhận về TỪNG BYTE như cũ
    response_body   TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (tenant_id, user_id, idem_key)
);

-- Dọn key hết hạn (job sau này) mà không quét cả bảng
CREATE INDEX idx_idempotency_keys_expiry ON idempotency_keys (expires_at);

-- ---------------------------------------------------------------------
-- Giao dịch — trạng thái nghiệp vụ, KHÔNG phải nguồn sự thật về tiền
-- ---------------------------------------------------------------------
-- Tiền chỉ di chuyển khi ledger ghi bút toán. Bảng này trả lời "lệnh của tôi tới đâu rồi", và là
-- khoá idempotency của ledger: id ở đây là transactionId của PostJournalCommand.
CREATE TABLE transactions
(
    id             UUID PRIMARY KEY,
    tenant_id      UUID        NOT NULL,
    type           VARCHAR(16) NOT NULL,                -- TRANSFER (TOPUP/WITHDRAW ở Phase 2)
    -- PROCESSING: đã nhận, đã gửi lệnh ghi sổ, chờ ledger trả lời (1.2b chuyển sang COMPLETED/FAILED)
    status         VARCHAR(20) NOT NULL
        CONSTRAINT ck_transactions_status CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
    initiated_by   UUID        NOT NULL,
    from_wallet    UUID        NOT NULL,
    to_wallet      UUID        NOT NULL,
    amount_minor   BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency       VARCHAR(3)  NOT NULL,
    note           VARCHAR(140),
    failure_code   VARCHAR(32),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_transactions_distinct_wallets CHECK (from_wallet <> to_wallet)
);

CREATE INDEX idx_transactions_tenant_initiator ON transactions (tenant_id, initiated_by, created_at DESC);

-- ---------------------------------------------------------------------
-- Trạng thái saga — lưu sau MỖI bước, để crash giữa chừng vẫn đi tiếp được
-- ---------------------------------------------------------------------
CREATE TABLE saga_state
(
    transaction_id   UUID PRIMARY KEY REFERENCES transactions (id),
    tenant_id        UUID        NOT NULL,
    current_step     VARCHAR(32) NOT NULL,              -- POST_JOURNAL
    step_status      VARCHAR(16) NOT NULL,              -- SENT | DONE | FAILED
    attempt_count    INT         NOT NULL DEFAULT 0,
    -- eventId của lần gửi lệnh gần nhất. Lần gửi lại (1.2b) PHẢI dùng eventId MỚI: ledger bỏ qua
    -- eventId đã xử lý mà không phát lại kết quả — gửi lại đúng eventId cũ là saga treo vĩnh viễn.
    command_event_id UUID        NOT NULL,
    next_attempt_at  TIMESTAMPTZ,
    -- Quá hạn mà chưa xong = saga treo -> job quét (1.2b) gửi lại hoặc chuyển người xử lý
    deadline_at      TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_state_open_deadline ON saga_state (deadline_at) WHERE step_status = 'SENT';

-- ---------------------------------------------------------------------
-- Outbox + processed_event — cùng hình dạng với các service khác (entity ở common-lib)
-- ---------------------------------------------------------------------
CREATE TABLE outbox_event
(
    id           UUID PRIMARY KEY,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    tenant_id    UUID         NOT NULL,
    topic        VARCHAR(255) NOT NULL,
    msg_key      VARCHAR(255),
    event_id     UUID         NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    payload      TEXT         NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT false,
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_event (created_at) WHERE published = false;
CREATE INDEX idx_outbox_published_cleanup ON outbox_event (published_at) WHERE published = true;
CREATE INDEX idx_outbox_event_id ON outbox_event (event_id);
CREATE INDEX idx_outbox_tenant ON outbox_event (tenant_id, created_at DESC);

CREATE TABLE processed_event
(
    event_id     UUID        NOT NULL,
    consumer     VARCHAR(64) NOT NULL,
    tenant_id    UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
