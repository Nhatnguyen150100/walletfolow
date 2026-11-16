-- =====================================================================
-- V9 — Index cho đường quét chiều ngược của đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024)
-- =====================================================================
-- GET /internal/entries/topups quét XUYÊN tenant mọi bút toán TOPUP theo con trỏ (created_at, id).
-- idx_journal_tenant_created (V4) bắt đầu bằng tenant_id nên không phục vụ được thứ tự xuyên tenant;
-- không có index này thì mỗi trang là một lượt quét toàn bảng journal_entries — bảng lớn thứ hai của
-- ledger, và phần lớn hàng là TRANSFER không liên quan.
--
-- PARTIAL theo type = 'TOPUP': chỉ chứa đúng các hàng đường quét cần, nên rẻ để giữ ở đường ghi nóng
-- (một lệnh chuyển tiền không chạm tới index này).
--
-- Chỉ EXPAND: không đụng dữ liệu, không đổi cột. journal_entries có trigger chặn UPDATE/DELETE (V2) —
-- CREATE INDEX không phải hai thứ đó.
--
-- ============================ KHOÁ — ĐỌC TRƯỚC KHI DEPLOY ============================
-- Cùng cảnh báo với V7: `CREATE INDEX` không CONCURRENTLY giữ SHARE trên `journal_entries` suốt lúc build
-- — chặn INSERT, tức là cả hệ thống KHÔNG GHI SỔ được trong cửa sổ đó (SELECT vẫn chạy). Partial index nên
-- build nhanh hơn index toàn bảng, nhưng vẫn phải quét cả bảng. CONCURRENTLY trong Flyway treo vĩnh viễn —
-- lý do đầy đủ ở V7. Khi bảng đủ lớn: chạy `CREATE INDEX CONCURRENTLY idx_journal_topup_created ...` bằng
-- psql trên DB đang chạy TRƯỚC khi deploy; `IF NOT EXISTS` dưới đây khi đó thành no-op.

CREATE INDEX IF NOT EXISTS idx_journal_topup_created
    ON journal_entries (created_at, id) WHERE type = 'TOPUP';
