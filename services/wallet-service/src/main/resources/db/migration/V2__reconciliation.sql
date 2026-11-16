-- =====================================================================
-- V2 — Đối soát tầng 2: read model so với sổ cái (roadmap 3.1, ADR-023, GAP-A3)
-- =====================================================================
-- V1 đã viết sẵn tính chất mà tầng 2a kiểm: "SUM(amount_minor) của một ví PHẢI bằng balance_minor
-- của nó — một phép đối soát nội bộ miễn phí, không cần hỏi ledger". Tới giờ nó chỉ được assert
-- trong WalletProjectionIT/WalletRebuildIT, chưa bao giờ chạy thật.
--
-- Tầng 2b là phép so mà roadmap thật sự đòi: balance_minor so với SUM(postings) bên ledger. 2a
-- KHÔNG thay thế được 2b — projector có thể bỏ sót CẢ hàng lịch sử lẫn số dư, lúc đó 2a vẫn khớp
-- trong khi ví thiếu tiền (ADR-023 §3).
--
-- HÌNH DẠNG HAI BẢNG NÀY GIỐNG HỆT ledger-service V8. Cố ý — báo cáo tổng hợp của 3.2 gộp hai
-- nguồn mà không phải dịch. Đổi một bên thì phải đổi cả hai. Chú thích đầy đủ về từng quyết định
-- (giành lượt, break không tự đóng, vì sao không có trigger append-only) nằm ở V8 của ledger; ở đây
-- chỉ ghi những chỗ KHÁC.
--
-- Chỉ EXPAND: hai bảng mới, không đụng wallets/wallet_entries. Bản app đang chạy không biết chúng
-- tồn tại.

CREATE TABLE reconciliation_runs
(
    id              UUID PRIMARY KEY,
    -- Ở walletdb: WALLET_PROJECTION (gộp cả 2a lẫn 2b trong một lượt — cùng một lần quét ví, tách
    -- ra thành hai lượt nghĩa là quét bảng wallets hai lần cho cùng một câu hỏi).
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
    -- PROJECTION_SELF_MISMATCH: balance_minor <> SUM(wallet_entries) — lỗi của projector hoặc một
    --   câu UPDATE gõ tay, thấy được không cần hỏi ai (tầng 2a).
    -- LEDGER_MISMATCH: balance_minor <> SUM(postings) bên ledger — ví lệch khỏi NGUỒN SỰ THẬT
    --   (tầng 2b). Nặng hơn hẳn loại trên.
    -- LEDGER_ACCOUNT_MISSING: ví đã ACTIVE và có số dư nhưng ledger không có tài khoản nào cho nó.
    --   Không phải lệch số — là một ví đang giữ tiền không có trong sổ cái.
    type           VARCHAR(32) NOT NULL,
    -- Ở đây luôn là id ví.
    subject        VARCHAR(128) NOT NULL,
    currency       VARCHAR(3),
    expected_minor BIGINT      NOT NULL,
    actual_minor   BIGINT      NOT NULL,
    delta_minor    BIGINT      NOT NULL,
    status         VARCHAR(16) NOT NULL DEFAULT 'OPEN'
        CONSTRAINT ck_recon_break_status CHECK (status IN ('OPEN', 'CLOSED')),
    -- Ở tầng 2 cột này THẬT SỰ được dùng: ledger luôn thắng nên job gọi WalletRebuildService ngay,
    -- số dư đúng lại trước sáng. Break vẫn OPEN — rebuild chép lại con số đúng, nó không sửa cái
    -- bug đã làm lệch (ADR-023 §5).
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

-- `type` nằm TRONG khoá, nên một ví có thể mang hai break đang mở thuộc hai loại khác nhau — hai
-- triệu chứng khác nhau thì người trực cần thấy cả hai.
--
-- Lưu ý về hành vi thật: trong MỘT lượt, một ví bị sửa tay thường chỉ sinh ĐÚNG MỘT break. Job xử
-- lý ứng viên 2a trước, rebuild ngay tại đó, nên tới lượt ứng viên 2b thì lần đọc thứ hai đã thấy
-- khớp và không ghi gì nữa. Đó là chủ ý — một nguyên nhân, một break — chứ không phải bỏ sót.
-- Hai hàng cùng ví xuất hiện khi hai loại lệch đến từ hai lượt khác nhau.
CREATE UNIQUE INDEX uk_recon_break_open
    ON reconciliation_breaks (tenant_id, type, subject) WHERE status = 'OPEN';

CREATE INDEX idx_recon_break_open
    ON reconciliation_breaks (tenant_id, first_seen_at) WHERE status = 'OPEN';

CREATE INDEX idx_recon_break_run ON reconciliation_breaks (run_id);
