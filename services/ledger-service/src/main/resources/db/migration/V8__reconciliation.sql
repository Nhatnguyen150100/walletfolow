-- =====================================================================
-- V8 — Đối soát tầng 1: bất biến nội tại sổ cái (roadmap 3.1, ADR-023, GAP-A3)
-- =====================================================================
-- Tới trước V8, bất biến vàng `SUM(postings) = 0` chỉ được kiểm TRONG TEST
-- (LedgerCommandFlowIT, TenantIsolationIT). Khi chạy thật không có gì phát hiện được tiền tự sinh
-- hay tiền bốc hơi — đó là nguyên văn GAP-A3.
--
-- Hai bảng dưới đây KHÔNG phải nguồn sự thật về tiền (roadmap §5.1): chúng là ghi chép về việc ĐÃ
-- KIỂM, xoá đi thì sổ cái vẫn nguyên vẹn. Vì vậy chúng KHÔNG có trigger append-only như
-- journal_entries/postings — người trực phải đóng được break, tức là phải UPDATE được.
--
-- Hình dạng hai bảng này giống hệt bên walletdb (wallet-service V2), cố ý: báo cáo đối soát tổng
-- hợp của 3.2 gộp hai nguồn mà không phải dịch. Đổi một bên thì phải đổi cả hai.
--
-- Chỉ EXPAND: hai bảng mới, không đụng bảng nào đang có. Bản app đang chạy không biết chúng tồn tại.

-- ---------------------------------------------------------------------
-- 1) reconciliation_runs — mỗi LƯỢT quét một hàng
-- ---------------------------------------------------------------------
-- ĐỒNG THỜI là cách giành lượt giữa nhiều instance (ADR-023 §6). Repo không có ShedLock và không có
-- advisory lock; idiom `FOR UPDATE SKIP LOCKED` thì dành cho job lấy LÔ, còn đối soát là MỘT lượt
-- quét duy nhất — hai instance cùng quét sẽ ghi hai bộ break trùng nhau.
--
-- Mỗi lượt chốt vào một cửa sổ tất định (làm tròn xuống theo chu kỳ). Instance nào INSERT được thì
-- chạy; instance thua nổ 23505 ở uk_recon_run_window và bỏ qua im lặng.
--
-- Một lượt chết giữa chừng để lại hàng RUNNING và cửa sổ đó KHÔNG chạy lại. Đó là chủ ý: khác với
-- một cái khoá vô hình bị rò rỉ, hàng RUNNING nhìn thấy được, và metric last_success sẽ già đi cho
-- tới khi alert bắn. Job đối soát chết âm thầm còn tệ hơn không có job (roadmap §3.1).
CREATE TABLE reconciliation_runs
(
    id              UUID PRIMARY KEY,
    -- Loại phép kiểm. Ở ledgerdb hiện chỉ có LEDGER_INVARIANT; cột vẫn rộng vì khoá giành lượt
    -- phải tách theo loại — hai phép kiểm khác nhau không được tranh lượt của nhau.
    kind            VARCHAR(32) NOT NULL,
    -- Đầu cửa sổ, đã làm tròn xuống. KHÔNG phải thời điểm thật sự bắt đầu chạy (started_at).
    window_start    TIMESTAMPTZ NOT NULL,
    status          VARCHAR(16) NOT NULL
        CONSTRAINT ck_recon_run_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    -- Đã soi bao nhiêu đối tượng (nhóm tenant+currency ở tầng 1, ví ở tầng 2). 0 với một lượt chạy
    -- trên hệ thống rỗng là HỢP LỆ và khác hẳn "lượt chạy hỏng" — status nói điều đó.
    checked_count   BIGINT      NOT NULL DEFAULT 0,
    break_count     INT         NOT NULL DEFAULT 0,
    -- Vì sao lượt này FAILED. Lỗi kỹ thuật (ledger không trả lời, DB đứt) — không phải lệch tiền.
    failure_message TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_recon_run_window UNIQUE (kind, window_start),
    -- Đã kết thúc thì phải biết lúc nào; còn RUNNING thì chưa.
    CONSTRAINT ck_recon_run_finished CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
            OR (status <> 'RUNNING' AND finished_at IS NOT NULL))
);

-- Metric walletflow.reconciliation.last_success_timestamp đọc theo đúng đường này: lượt SUCCEEDED
-- mới nhất của một kind.
CREATE INDEX idx_recon_run_success
    ON reconciliation_runs (kind, window_start DESC) WHERE status = 'SUCCEEDED';

-- ---------------------------------------------------------------------
-- 2) reconciliation_breaks — mỗi chỗ LỆCH một hàng
-- ---------------------------------------------------------------------
-- "Break phải được đóng bởi người, không tự biến mất" (roadmap §3.1). Kể cả khi máy đã sửa được
-- hậu quả (tầng 2 tự rebuild read model), hàng break vẫn OPEN: rebuild chép lại con số đúng, nó
-- KHÔNG sửa cái bug đã làm lệch. Một break tự sinh tự đóng mỗi đêm là một bug ẩn chạy mãi.
CREATE TABLE reconciliation_breaks
(
    id             UUID PRIMARY KEY,
    run_id         UUID        NOT NULL REFERENCES reconciliation_runs (id),
    -- "Sổ của khách nào sai" là câu hỏi đầu tiên người trực hỏi, nên cột này NOT NULL.
    -- Break không thuộc về khách nào (LEDGER_IMBALANCE_GLOBAL) mang tenant sentinel toàn số 0 —
    -- xem ReconciliationScope.SYSTEM_TENANT. Để NULL thì uk_recon_break_open mất tác dụng với đúng
    -- loại break cần dedup nhất, vì hai NULL là KHÁC nhau trong index UNIQUE của Postgres.
    tenant_id      UUID        NOT NULL,
    -- LEDGER_IMBALANCE: một nhóm (tenant, currency) có tổng khác 0.
    -- LEDGER_IMBALANCE_GLOBAL: tổng toàn hệ thống khác 0 (bắt ca ghi lệch tenant mà từng nhóm vẫn
    -- cân — xem javadoc PostingRepository.totalOfSystem).
    type           VARCHAR(32) NOT NULL,
    -- Đối tượng lệch, ở dạng người đọc được: mã loại tiền ở tầng 1, id ví ở tầng 2.
    subject        VARCHAR(128) NOT NULL,
    currency       VARCHAR(3),
    -- Con số ĐÚNG theo nguồn sự thật, và con số đang có. delta = actual - expected, lưu sẵn để báo
    -- cáo tổng chênh lệch không phải tính lại (và để một hàng đọc một mình vẫn hiểu được).
    expected_minor BIGINT      NOT NULL,
    actual_minor   BIGINT      NOT NULL,
    delta_minor    BIGINT      NOT NULL,
    status         VARCHAR(16) NOT NULL DEFAULT 'OPEN'
        CONSTRAINT ck_recon_break_status CHECK (status IN ('OPEN', 'CLOSED')),
    -- true = máy đã sửa được HẬU QUẢ (tầng 2 rebuild read model). Không bao giờ true ở tầng 1:
    -- sổ cái append-only và không tồn tại "số đúng" nào để chép về — imbalance thì gọi người.
    auto_repaired  BOOLEAN     NOT NULL DEFAULT false,
    -- Đã thấy lại bao nhiêu lượt. Một break lệch mãi KHÔNG sinh hàng mới mỗi đêm (xem
    -- uk_recon_break_open): nó cập nhật hàng đang mở và tăng số này. Thiếu nó thì sau một tháng một
    -- break duy nhất thành 30 hàng và hàng đợi của người trực vô dụng.
    times_seen     INT         NOT NULL DEFAULT 1,
    detail         TEXT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at      TIMESTAMPTZ,
    closed_by      UUID,
    closed_note    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Đóng rồi thì phải biết ai đóng và lúc nào — cùng tinh thần ck_resolution_decided của txndb.
    CONSTRAINT ck_recon_break_closed CHECK (
        (status = 'OPEN' AND closed_at IS NULL AND closed_by IS NULL)
            OR (status = 'CLOSED' AND closed_at IS NOT NULL AND closed_by IS NOT NULL)),
    CONSTRAINT ck_recon_break_delta CHECK (delta_minor = actual_minor - expected_minor)
);

-- Mỗi đối tượng tối đa MỘT break đang mở. Đây là thứ biến "ghi break" thành một thao tác upsert
-- idempotent: lượt sau gặp lại đúng chỗ lệch đó thì cập nhật, không nhân bản.
CREATE UNIQUE INDEX uk_recon_break_open
    ON reconciliation_breaks (tenant_id, type, subject) WHERE status = 'OPEN';

-- Hàng đợi của người trực: break đang mở, cũ nhất trước (cùng quy ước với hàng đợi manual-review).
CREATE INDEX idx_recon_break_open
    ON reconciliation_breaks (tenant_id, first_seen_at) WHERE status = 'OPEN';

CREATE INDEX idx_recon_break_run ON reconciliation_breaks (run_id);
