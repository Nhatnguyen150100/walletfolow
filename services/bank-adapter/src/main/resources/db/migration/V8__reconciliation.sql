-- =====================================================================
-- V8 — Đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024, phần tầng 3 không cần sao kê)
-- =====================================================================
-- Tầng 1 (ledger V8) và tầng 2 (wallet V2) chỉ so DỮ LIỆU NỘI BỘ của sổ cái. Không tầng nào thấy được
-- ca nghiêm trọng nhất ở biên ngoài: provider đã thu tiền của khách mà sổ cái không có bút toán, hoặc
-- sổ cái ghi tiền vào mà không cổng nào thu. Hai bảng này giữ kết quả của phép so đó.
--
-- HÌNH DẠNG GIỐNG HỆT ledger-service V8 và wallet-service V2 — cố ý, để báo cáo 3.2 gộp ba nguồn mà
-- không phải dịch (ADR-023 §1). Đổi một bên thì phải đổi cả ba. Chú thích đầy đủ về từng quyết định
-- (giành lượt, break không tự đóng) nằm ở V8 của ledger; ở đây chỉ ghi những chỗ KHÁC.
--
-- Nằm ở railsdb (DB của bank-adapter) chứ không ở ledgerdb: bank-adapter là bên giữ sự thật phía provider, và ADR-001 cấm
-- một service ghi chéo vào DB của service khác (ADR-024 §1).
--
-- Chỉ EXPAND: hai bảng mới, không đụng provider_transactions. Bản app đang chạy không biết chúng tồn tại.

CREATE TABLE reconciliation_runs
(
    id              UUID PRIMARY KEY,
    -- Ở railsdb: PROVIDER_LEDGER (gộp cả hai chiều quét trong một lượt — cùng một câu hỏi "tiền qua
    -- cửa ngoài có khớp sổ cái không", tách hai lượt thì một chiều có thể chạy mà chiều kia không).
    kind            VARCHAR(32) NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    status          VARCHAR(16) NOT NULL
        CONSTRAINT ck_recon_run_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    checked_count   BIGINT      NOT NULL DEFAULT 0,
    break_count     INT         NOT NULL DEFAULT 0,
    failure_message TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_recon_run_window UNIQUE (kind, window_start),
    CONSTRAINT ck_recon_run_finished CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
            OR (status <> 'RUNNING' AND finished_at IS NOT NULL))
);

CREATE INDEX idx_recon_run_success
    ON reconciliation_runs (kind, window_start DESC) WHERE status = 'SUCCEEDED';

CREATE TABLE reconciliation_breaks
(
    id             UUID PRIMARY KEY,
    run_id         UUID        NOT NULL REFERENCES reconciliation_runs (id),
    tenant_id      UUID        NOT NULL,
    -- PROVIDER_MONEY_NOT_POSTED: provider đã thu (ròng) NHIỀU hơn sổ cái ghi vào — khách đã trả mà ví
    --   chưa nhận đủ. Gồm cả ca hoàn tay qua portal mà bank-adapter không có hàng REFUND (ADR-024 §5).
    -- POSTED_WITHOUT_PROVIDER_MONEY: sổ cái ghi vào NHIỀU hơn provider thu — tiền vào ví mà không ai
    --   trả, và tiêu được ngay. Nguyên nhân đã biết duy nhất là REFUND_POSTED_CONFLICT (lệnh nạp tới
    --   ledger sau khi đã hoàn); gọi người (alert page).
    type           VARCHAR(32) NOT NULL,
    -- Ở đây luôn là '<transactionId>:<currency>'. Có loại tiền trong khoá vì EXTERNAL_BANK là một tài
    -- khoản cho mỗi loại tiền: một lệnh lệch ở hai loại tiền cùng chiều là hai chỗ lệch, và chỉ có
    -- transactionId thì uk_recon_break_open gộp chúng thành một hàng, cái sau ghi đè cái trước.
    subject        VARCHAR(128) NOT NULL,
    currency       VARCHAR(3),
    -- Quy ước: expected = tiền ròng phía PROVIDER theo provider_transactions (thu thành công - hoàn
    -- thành công), actual = tiền ròng sổ cái ghi qua EXTERNAL_BANK. delta = actual - expected: âm là
    -- tiền chưa vào sổ, dương là sổ ghi thừa. Phía nào vắng mặt thì là 0.
    expected_minor BIGINT      NOT NULL,
    actual_minor   BIGINT      NOT NULL,
    delta_minor    BIGINT      NOT NULL,
    status         VARCHAR(16) NOT NULL DEFAULT 'OPEN'
        CONSTRAINT ck_recon_break_status CHECK (status IN ('OPEN', 'CLOSED')),
    -- Ở railsdb luôn false: không tồn tại "số đúng" để máy chép về. Sổ cái append-only, và việc nạp bù
    -- hay đảo bút toán là quyết định nghiệp vụ có 4-eyes (ADR-021/022), không phải việc của job.
    auto_repaired  BOOLEAN     NOT NULL DEFAULT false,
    times_seen     INT         NOT NULL DEFAULT 1,
    detail         TEXT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at      TIMESTAMPTZ,
    closed_by      UUID,
    closed_note    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_recon_break_closed CHECK (
        (status = 'OPEN' AND closed_at IS NULL AND closed_by IS NULL)
            OR (status = 'CLOSED' AND closed_at IS NOT NULL AND closed_by IS NOT NULL)),
    CONSTRAINT ck_recon_break_delta CHECK (delta_minor = actual_minor - expected_minor)
);

CREATE UNIQUE INDEX uk_recon_break_open
    ON reconciliation_breaks (tenant_id, type, subject) WHERE status = 'OPEN';

CREATE INDEX idx_recon_break_open
    ON reconciliation_breaks (tenant_id, first_seen_at) WHERE status = 'OPEN';

CREATE INDEX idx_recon_break_run ON reconciliation_breaks (run_id);

-- Không cần index mới trên provider_transactions: chiều xuôi quét theo khoá chính (id) và tra lần hoàn
-- theo (tenant_id, transaction_id, operation) — đúng tiền tố của uk_provider_txn_tenant_txn_op (V1).
