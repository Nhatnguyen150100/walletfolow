-- ===== Ledger domain (plan §5.2) =====
CREATE TABLE accounts
(
    id           UUID PRIMARY KEY,
    external_ref VARCHAR(64) NOT NULL,
    type         VARCHAR(16) NOT NULL, -- USER | SYSTEM
    currency     VARCHAR(3)  NOT NULL, -- VARCHAR chứ không CHAR: CHAR đệm khoảng trắng gây so sánh hụt
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_accounts_ref_currency UNIQUE (external_ref, currency)
);

CREATE TABLE journal_entries
(
    id             UUID PRIMARY KEY,
    transaction_id UUID        NOT NULL,
    type           VARCHAR(16) NOT NULL, -- TRANSFER | TOPUP | WITHDRAW | REVERSAL
    reference_id   UUID,                 -- trỏ về journal gốc nếu là REVERSAL
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_journal_txn_type UNIQUE (transaction_id, type)
);

CREATE TABLE postings
(
    id           UUID PRIMARY KEY,
    entry_id     UUID        NOT NULL REFERENCES journal_entries (id),
    account_id   UUID        NOT NULL REFERENCES accounts (id),
    amount_minor BIGINT      NOT NULL CHECK (amount_minor <> 0), -- âm=debit, dương=credit; vế 0 là vô nghĩa
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_postings_account ON postings (account_id);

INSERT INTO accounts (id, external_ref, type, currency, created_at, updated_at)
VALUES (gen_random_uuid(), 'EXTERNAL_BANK', 'SYSTEM', 'VND', now(), now()),
       (gen_random_uuid(), 'FEE_INCOME', 'SYSTEM', 'VND', now(), now()),
       (gen_random_uuid(), 'SETTLEMENT', 'SYSTEM', 'VND', now(), now());

CREATE TABLE outbox_event
(
    id           UUID PRIMARY KEY,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    topic        VARCHAR(255) NOT NULL,
    msg_key      VARCHAR(255),
    event_type   VARCHAR(255) NOT NULL,
    payload      TEXT         NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT false,
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_event (published, created_at);

CREATE TABLE processed_event
(
    event_id     UUID        NOT NULL,
    consumer     VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
