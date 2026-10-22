-- =====================================================================
-- V6 — bank-adapter: thao tác HOÀN TIỀN (roadmap 2.4b, ADR-016 "giới hạn đã chấp nhận", ADR-020)
-- =====================================================================
-- Hàng REFUND sống cùng bảng với hàng CHARGE: cùng vòng đời giành-quyền → gọi provider → ghi câu trả lời,
-- cùng khoá idempotency (tenant, transaction_id, operation), cùng outbox. Một giao dịch có tối đa MỘT lần
-- thu và MỘT lần hoàn (chỉ hoàn trọn).
--
-- Vòng đời của hàng REFUND:  NEW --(provider hoàn xong)--> SUCCEEDED   (provider_ref = mã của LẦN HOÀN)
--                            NEW --(provider từ chối)----> FAILED
-- Không có PENDING: provider hiện tại trả lời hoàn tiền đồng bộ. Provider nào hoàn bất đồng bộ thì thêm sau.

-- 1) Cho phép operation = REFUND. Đổi CHECK = DROP + ADD trong cùng transaction của Flyway. Chỉ NỚI tập giá
--    trị nên hàng cũ luôn thoả; ADD CONSTRAINT vẫn quét bảng một lần dưới ACCESS EXCLUSIVE — bảng này nhỏ
--    (một hàng mỗi lần nạp). Bản app đang chạy không bao giờ ghi REFUND nên không bị ảnh hưởng.
ALTER TABLE provider_transactions DROP CONSTRAINT ck_provider_txn_operation;
ALTER TABLE provider_transactions
    ADD CONSTRAINT ck_provider_txn_operation CHECK (operation IN ('CHARGE', 'REFUND'));

-- 2) Lần hoàn trỏ về ĐÚNG lần thu nó hoàn — đối soát (Phase 3) đi từ đây: tổng hoàn của một lần thu không
--    bao giờ được vượt số đã thu. NULL với mọi hàng CHARGE.
ALTER TABLE provider_transactions
    ADD COLUMN refund_of UUID
        CONSTRAINT fk_provider_txn_refund_of REFERENCES provider_transactions (id);

-- Hàng REFUND phải có refund_of, hàng khác thì không. Thêm CHECK lúc bảng chưa có hàng REFUND nào nên hàng
-- cũ (đều CHARGE, refund_of NULL) luôn thoả.
ALTER TABLE provider_transactions
    ADD CONSTRAINT ck_provider_txn_refund_of CHECK ((operation = 'REFUND') = (refund_of IS NOT NULL));

-- FK mới cần index (CLAUDE.md) — partial: chỉ hàng REFUND có giá trị
CREATE INDEX idx_provider_txn_refund_of ON provider_transactions (refund_of) WHERE refund_of IS NOT NULL;
