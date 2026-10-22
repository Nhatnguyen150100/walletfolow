-- =====================================================================
-- V6 — Hàng rào: lệnh NẠP đã bị từ chối thì bị từ chối VĨNH VIỄN (roadmap 2.4b, ADR-020)
-- =====================================================================
-- Trước V6, ledger chỉ dedup lệnh ghi sổ theo eventId. Một lệnh nạp bị từ chối (ví đóng, EXTERNAL_BANK
-- đóng băng...) thì không để lại gì gắn với transactionId — nên một bản GỬI LẠI của chính lệnh đó (job quét
-- của saga gửi lại với eventId mới) tới sau khi tài khoản được mở lại vẫn ghi sổ được.
--
-- Với chuyển tiền, đó là chuyện đã chấp nhận ("ledger thắng", ADR-014). Với nạp tiền từ 2.4b thì KHÔNG: lời
-- từ chối kích hoạt HOÀN TIỀN qua cổng thanh toán. Từ chối + hoàn tiền + một bản gửi lại ghi sổ thành công =
-- người dùng nhận tiền hai lần (ví được cộng VÀ tiền về thẻ). Bảng này là hàng rào: một khi ledger đã nói
-- "không" với lệnh nạp của một giao dịch, nó không bao giờ nói "có" nữa — nên saga mới được phép hoàn.
--
-- Chỉ áp cho TOPUP: luật của chuyển tiền giữ nguyên (ADR-014).
--
-- Chỉ EXPAND: bảng mới, bản app đang chạy không biết tới nó và không cần biết. Bản cũ chạy song song trong
-- lúc rolling deploy không ghi hàng rào — nên PR bù trừ (transaction-service gửi lệnh hoàn) chỉ được bật SAU
-- KHI mọi instance ledger đã ở bản mới (ADR-020 §Triển khai).
CREATE TABLE rejected_topups
(
    tenant_id      UUID         NOT NULL,
    -- Giao dịch nạp ở transaction-service — cùng khoá idempotency với journal_entries.transaction_id
    transaction_id UUID         NOT NULL,
    -- Mã + lời từ chối LẦN ĐẦU. Mọi lần từ chối lại phát đúng hai giá trị này, để event từ chối (eventId tất
    -- định theo giao dịch) luôn mang cùng nội dung — quy tắc của dedup: cùng eventId thì cùng nội dung.
    reason_code    VARCHAR(64)  NOT NULL,
    reason         TEXT         NOT NULL,
    rejected_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_rejected_topups PRIMARY KEY (tenant_id, transaction_id)
);

-- Hàng rào chỉ có nghĩa nếu không gỡ được: append-only như sổ cái (V2), nhưng hàm riêng vì lời nhắn của
-- ledger_forbid_mutation ("hãy ghi REVERSAL") sai với bảng này. Muốn ghi sổ cho một giao dịch đã bị từ chối thì
-- đó là một GIAO DỊCH MỚI, không phải mở khoá giao dịch cũ.
CREATE OR REPLACE FUNCTION ledger_forbid_fence_mutation() RETURNS trigger AS
$$
BEGIN
    RAISE EXCEPTION 'Bảng % là APPEND-ONLY: không được %. Lệnh nạp đã bị từ chối thì không ghi sổ được nữa — có thể tiền đã được hoàn (ADR-020). Hoàn tay rồi để người dùng nạp lại bằng giao dịch mới.',
        TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_rejected_topups_append_only
    BEFORE UPDATE OR DELETE
    ON rejected_topups
    FOR EACH ROW
EXECUTE FUNCTION ledger_forbid_fence_mutation();
