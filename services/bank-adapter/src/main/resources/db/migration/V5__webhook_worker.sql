-- =====================================================================
-- V5 — bank-adapter: lịch thử lại cho worker xử lý webhook (roadmap 2.3b, ADR-018)
-- =====================================================================
-- V4 ghi webhook đã qua chữ ký rồi để đó; 2.3b thêm worker đọc hàng đó và hỏi lại provider. Việc hỏi lại
-- là một lời gọi mạng ra ngoài, nên nó THẤT BẠI ĐƯỢC — và phải thất bại được nhiều lần mà không làm hỏng
-- gì. Hai cột dưới đây là toàn bộ phần trạng thái cần cho chuyện đó.
--
-- Không có chúng thì một webhook mà provider không trả lời được sẽ nằm mãi ở đầu lô (ORDER BY received_at),
-- bị hỏi lại mỗi lượt quét, và chặn mọi webhook tới sau nó — đúng lúc provider đang chập chờn, tức là đúng
-- lúc tồn đọng lớn nhất.

ALTER TABLE webhook_events
    -- Số lần worker ĐÃ lấy hàng này ra xử lý. Tăng NGAY khi lấy lô, trước khi gọi provider: lượt nào cũng
    -- phải tính, kể cả lượt làm process chết giữa chừng.
    ADD COLUMN attempts        INT         NOT NULL DEFAULT 0,

    -- Mốc được phép thử lại. now() cho hàng cũ: mọi webhook nhận trong 2.3a đều tới hạn ngay khi worker lên,
    -- đúng ý — chúng là tồn đọng chưa ai đọc.
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Vì sao hàng này ngừng được xử lý. Chỉ để vận hành đọc, không có luồng nghiệp vụ nào rẽ theo nó:
    --   RESOLVED       — đã tra ra lần thu và hỏi lại provider xong
    --   UNRESOLVED_REF — chữ ký hợp lệ nhưng mã tham chiếu không thuộc hệ thống này (xem bên dưới)
    --   ABANDONED      — hết trần số lần thử, thôi hỏi; cần người đối soát
    ADD COLUMN outcome         VARCHAR(24);

-- Lô việc của worker: chưa xử lý VÀ tới hạn thử lại, của ĐÚNG provider đang chạy, cũ trước. Thay index theo
-- received_at của V4 vì worker sắp theo next_attempt_at — giữ cả hai là trả giá ghi hai lần trên một bảng
-- chỉ có INSERT, cho một index không ai đọc. Gauge walletflow.bank.webhook.unprocessed
-- (count WHERE processed_at IS NULL) vẫn dùng được index mới.
--
-- provider đứng TRƯỚC trong khoá: câu lấy lô lọc theo provider đang chạy, nên sau khi đổi BANK_PROVIDER,
-- hàng chết của provider cũ không bị quét lại mỗi 2 giây mãi mãi (xem runbook payment-provider-down.md).
--
-- Tương thích ngược: ADD COLUMN với DEFAULT hằng/STABLE là fast-path của PG 11+, không rewrite bảng; bản
-- 2.3a đang chạy không liệt kê cột mới khi INSERT nên không sai. CHÚ Ý khi tồn đọng lớn (≳10^5 hàng):
-- CREATE INDEX không CONCURRENTLY giữ SHARE lock chặn INSERT vào webhook_events suốt lúc build, tức chặn
-- luôn endpoint /webhooks/** của bản đang chạy → provider timeout rồi gửi lại. Lúc đó phải tách thành một
-- migration riêng dùng CREATE INDEX CONCURRENTLY (ngoài transaction của Flyway).
DROP INDEX idx_webhook_events_unprocessed;
CREATE INDEX idx_webhook_events_due ON webhook_events (provider, next_attempt_at)
    WHERE processed_at IS NULL;
