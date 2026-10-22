-- =====================================================================
-- V2 — bank-adapter: vòng đời của lần thu PENDING (roadmap 2.2b, ADR-017)
-- =====================================================================
-- Ở V1 một lần thu PENDING không có trạng thái kết thúc: phiên bị người dùng bỏ dở được hỏi lại mỗi
-- 30 giây, mãi mãi (ADR-016, mục "còn mở"). V2 thêm đủ dữ liệu để:
--   1. kết thúc nó bằng FAILED(EXPIRED) khi đã quá hạn phiên + grace VÀ provider khẳng định chưa thu;
--   2. giãn nhịp hỏi lại theo cấp số thay vì dội đều đặn;
--   3. đo được hàng PENDING quá hạn mà vẫn chưa kết luận được (alert PendingChargeOverdue);
--   4. không bao giờ hỏi phiên bằng tài khoản merchant khác tài khoản đã tạo nó;
--   5. hỏi xác nhận một lần sau EXPIRED để phát hiện tiền về muộn (alert ExpiredChargeNeedsReview).
--
-- Chỉ EXPAND: mọi cột mới nullable hoặc có default, nên bản 2.2a đang chạy (không biết các cột này) vẫn
-- INSERT/UPDATE được trong lúc rolling deploy. Hàng cũ có expires_at NULL = không bao giờ tự hết hạn —
-- đúng phía an toàn (chỉ có hàng của MockProvider từ 2.2a).

ALTER TABLE provider_transactions
    -- Lúc provider tạo phiên, theo lời provider. VNPay cần đúng giá trị này (vnp_TransactionDate) để hỏi
    -- lại trạng thái: sai một giây là "không tìm thấy giao dịch".
    ADD COLUMN provider_created_at TIMESTAMPTZ,
    -- Hạn phiên thanh toán đã gửi cho provider (VNPay: vnp_ExpireDate). Sau mốc này + grace mà provider
    -- vẫn nói "chưa thu" thì lần thu kết thúc FAILED(EXPIRED).
    ADD COLUMN expires_at          TIMESTAMPTZ,
    -- Số lần đã hỏi lại — cơ số của backoff cấp số. Reset khi hàng sang PENDING.
    ADD COLUMN check_attempts      INT NOT NULL DEFAULT 0
        CONSTRAINT ck_provider_txn_check_attempts CHECK (check_attempts >= 0),
    -- Tài khoản merchant đã tạo phiên (VNPay: vnp_TmnCode). Phiên chỉ tra được dưới ĐÚNG tài khoản đó: hỏi
    -- bằng tài khoản khác (sau khi đổi VNPAY_TMN_CODE) nhận "không tìm thấy" — câu trả lời đúng chữ ký, sẽ
    -- biến một lần thu ĐÃ TRẢ thành EXPIRED. NULL = hàng trước 2.2b (chỉ của mock).
    ADD COLUMN provider_account    VARCHAR(64);

-- Metric hàng PENDING quá hạn (quét mỗi lượt của job đối chiếu, xuyên tenant)
CREATE INDEX idx_provider_txn_pending_expiry ON provider_transactions (expires_at) WHERE status = 'PENDING';

-- Lần hỏi XÁC NHẬN sau khi hết hạn (ADR-017): hàng FAILED(EXPIRED) giữ next_check_at tới khi provider xác
-- nhận lần nữa rằng không có tiền nào về muộn. Chỉ phát hiện + kêu, không bao giờ tự đổi trạng thái.
CREATE INDEX idx_provider_txn_expired_confirm ON provider_transactions (next_check_at)
    WHERE status = 'FAILED' AND next_check_at IS NOT NULL;
