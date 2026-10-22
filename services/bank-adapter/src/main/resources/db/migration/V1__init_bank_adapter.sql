-- =====================================================================
-- V1 — bank-adapter: giao dịch với cổng thanh toán (roadmap 2.2a, ADR-016)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Mỗi hàng = MỘT thao tác với provider (2.2a: chỉ CHARGE — thu tiền nạp ví)
-- ---------------------------------------------------------------------
-- Bảng này KHÔNG phải nguồn sự thật về tiền (ledger mới là). Nó trả lời hai câu:
--   1. "Đã bảo provider thu tiền cho giao dịch này chưa?" — để retry không thu hai lần;
--   2. "Provider nói gì?" — bằng chứng thô khi đối soát (Phase 3).
--
-- Vòng đời:  NEW --(provider tạo phiên)--> PENDING --(getStatus xác nhận)--> SUCCEEDED | FAILED
--            NEW --(provider từ chối ngay)--> FAILED
-- NEW nghĩa là "đã giành quyền, CHƯA BIẾT provider đã nhận hay chưa" (ví dụ timeout). Lần thử sau gửi
-- lại ĐÚNG idempotency_key cũ, nên provider trả về phiên đã tạo thay vì tạo phiên thứ hai.
CREATE TABLE provider_transactions
(
    id              UUID PRIMARY KEY,
    tenant_id       UUID         NOT NULL,
    -- Giao dịch ở transaction-service — khoá idempotency nghiệp vụ của lệnh
    transaction_id  UUID         NOT NULL,
    operation       VARCHAR(16)  NOT NULL
        CONSTRAINT ck_provider_txn_operation CHECK (operation IN ('CHARGE')),
    provider        VARCHAR(32)  NOT NULL,
    -- Khoá gửi cho PROVIDER — tất định theo (tenant, giao dịch, thao tác), không bao giờ sinh lại
    idempotency_key VARCHAR(64)  NOT NULL,
    -- Mã phía provider; NULL khi chưa tạo được phiên
    provider_ref    VARCHAR(128),
    wallet_id       UUID         NOT NULL,
    amount_minor    BIGINT       NOT NULL CHECK (amount_minor > 0),
    currency        VARCHAR(3)   NOT NULL,
    status          VARCHAR(16)  NOT NULL
        CONSTRAINT ck_provider_txn_status CHECK (status IN ('NEW', 'PENDING', 'SUCCEEDED', 'FAILED')),
    payment_url     TEXT,
    failure_code    VARCHAR(32),
    raw_request     JSONB        NOT NULL,
    raw_response    JSONB,
    -- Lần hỏi lại trạng thái kế tiếp (chỉ có nghĩa khi PENDING) — job đối chiếu dùng
    next_check_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_provider_txn_tenant_txn_op UNIQUE (tenant_id, transaction_id, operation),
    CONSTRAINT uk_provider_txn_idem_key UNIQUE (provider, idempotency_key),
    CONSTRAINT uk_provider_txn_provider_ref UNIQUE (provider, provider_ref),
    -- Thất bại thì phải có mã lỗi; đã qua NEW mà không thất bại thì phải có mã phía provider
    CONSTRAINT ck_provider_txn_failed_has_code CHECK (status <> 'FAILED' OR failure_code IS NOT NULL),
    CONSTRAINT ck_provider_txn_ref_after_new CHECK (status IN ('NEW', 'FAILED') OR provider_ref IS NOT NULL)
);

-- Job đối chiếu quét hàng PENDING tới hạn hỏi lại, xuyên tenant
CREATE INDEX idx_provider_txn_pending_check ON provider_transactions (next_check_at) WHERE status = 'PENDING';

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

-- Chưa dùng ở 2.2a (lệnh charge idempotent theo khoá nghiệp vụ, không theo eventId — xem ADR-016),
-- nhưng entity ProcessedEvent của common-lib được nạp qua @EntityScan nên bảng phải tồn tại.
CREATE TABLE processed_event
(
    event_id     UUID        NOT NULL,
    consumer     VARCHAR(64) NOT NULL,
    tenant_id    UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
