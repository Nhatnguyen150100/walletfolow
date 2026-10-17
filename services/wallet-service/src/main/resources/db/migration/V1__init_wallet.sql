-- =====================================================================
-- V1 — wallet-service: READ MODEL suy ra từ sổ cái (roadmap 1.1)
--
-- Không bảng nào ở đây là nguồn sự thật về tiền (roadmap §5.1). Mọi số trong service này phải
-- DỰNG LẠI ĐƯỢC từ ledger — xem WalletRebuildService.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Ví
-- ---------------------------------------------------------------------
-- Cố ý KHÔNG có:
--   held_minor   — tiền bị giữ nằm ở tài khoản HOLD_* trong sổ cái (roadmap 2.1). Một cột held ở
--                  đây là nguồn sự thật thứ hai.
--   version      — số dư chỉ đổi bằng `UPDATE ... SET balance_minor = balance_minor + :delta`, vốn
--                  đã atomic; không có read-modify-write nào để khoá lạc quan bảo vệ.
CREATE TABLE wallets
(
    id             UUID PRIMARY KEY,        -- CŨNG LÀ accountRef bên sổ cái
    tenant_id      UUID        NOT NULL,
    user_id        UUID        NOT NULL,
    currency       VARCHAR(3)  NOT NULL,
    balance_minor  BIGINT      NOT NULL DEFAULT 0,
    -- PENDING: đã gửi lệnh mở tài khoản, ledger chưa xác nhận. Chưa nhận/chi được tiền.
    status         VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        CONSTRAINT ck_wallets_status CHECK (status IN ('PENDING', 'ACTIVE', 'FROZEN', 'CLOSED')),
    -- Bản sao status_version của ledger. -1 = chưa nhận được gì. Chỉ áp trạng thái có version
    -- LỚN HƠN: event cũ đến muộn không ghi đè được trạng thái mới.
    status_version INT         NOT NULL DEFAULT -1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Một người một ví cho mỗi loại tiền, trong phạm vi một tenant. Là khoá để POST /wallets
    -- idempotent: gọi lại trả về ví đã có thay vì tạo ví thứ hai.
    CONSTRAINT uk_wallets_tenant_user_currency UNIQUE (tenant_id, user_id, currency)
);

-- ---------------------------------------------------------------------
-- Lịch sử giao dịch của ví — ĐỒNG THỜI là bảng khử trùng
-- ---------------------------------------------------------------------
-- Mỗi bút toán chạm ví đúng MỘT hàng, khoá bởi (wallet_id, entry_id). Áp một LedgerPostedEvent
-- = INSERT ... ON CONFLICT DO NOTHING; chỉ khi chèn được hàng mới thì mới cộng số dư.
--
-- Vì sao khử trùng THEO VÍ chứ không theo event (bảng processed_event): một lệnh chuyển A -> B
-- là MỘT event chạm HAI ví. Dựng lại ví A phải đánh dấu "bút toán E đã áp cho A" — nếu đánh dấu
-- theo event, việc đó cũng chặn luôn E áp cho B, và B mất tiền vĩnh viễn trong read model.
--
-- Hệ quả tốt: SUM(amount_minor) của một ví PHẢI bằng balance_minor của nó — một phép đối soát
-- nội bộ miễn phí, không cần hỏi ledger.
CREATE TABLE wallet_entries
(
    -- Khoá cursor của API lịch sử. Tăng theo thứ tự GHI VÀO READ MODEL: hàng mới luôn có seq lớn
    -- hơn, nên trang đã đọc không bao giờ bị xô lệch bởi giao dịch mới — điều phân trang offset
    -- không làm được.
    seq            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id      UUID        NOT NULL,
    wallet_id      UUID        NOT NULL REFERENCES wallets (id),
    entry_id       UUID        NOT NULL,   -- id bút toán ở ledger = eventId của LedgerPosted
    transaction_id UUID        NOT NULL,
    type           VARCHAR(16) NOT NULL,
    amount_minor   BIGINT      NOT NULL,   -- dương = tiền vào ví, âm = tiền ra
    posted_at      TIMESTAMPTZ,            -- thời điểm ghi sổ ở ledger (nếu biết)
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_wallet_entries_wallet_entry UNIQUE (wallet_id, entry_id)
);

CREATE INDEX idx_wallet_entries_wallet_seq ON wallet_entries (wallet_id, seq DESC);

-- ---------------------------------------------------------------------
-- Outbox — lệnh mở tài khoản đi ra cùng transaction với việc tạo ví
-- ---------------------------------------------------------------------
-- Cùng hình dạng với outbox của ledger sau V1..V4 (entity OutboxEvent ở common-lib).
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

-- ---------------------------------------------------------------------
-- processed_event — cho các consumer dùng EventDeduplicator của common-lib
-- ---------------------------------------------------------------------
-- Projection số dư KHÔNG dùng bảng này (xem wallet_entries). Có mặt vì entity ProcessedEvent được
-- nạp cho mọi service dùng common-lib, và để consumer sau này của service dùng được ngay.
CREATE TABLE processed_event
(
    event_id     UUID        NOT NULL,
    consumer     VARCHAR(64) NOT NULL,
    tenant_id    UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
