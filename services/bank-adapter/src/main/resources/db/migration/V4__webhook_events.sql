-- =====================================================================
-- V4 — bank-adapter: nhận webhook/IPN của cổng thanh toán (roadmap 2.3, ADR-018)
-- =====================================================================
-- Cổng thanh toán báo kết quả bằng webhook, và webhook thì TRÙNG LẶP, ĐẾN MUỘN và ĐẾN SAI THỨ TỰ.
-- Bảng này là nơi ghi lại nguyên văn thứ đã nhận TRƯỚC khi xử lý, để:
--   1. trả 200 cho provider ngay (xử lý đồng bộ là nguyên nhân kinh điển của "provider retry 5 lần
--      vì bạn trả lời chậm") — việc xử lý do worker ở 2.3b làm;
--   2. chống phát lại: (provider, provider_event_id) UNIQUE;
--   3. còn bằng chứng đối soát khi sau này phải giải thích vì sao một lần thu kết thúc như thế.
--
-- CHỈ ghi webhook ĐÃ QUA KIỂM CHỮ KÝ. Chữ ký sai thì trả 401 và không ghi gì: đây là endpoint công
-- khai không có JWT, ghi cả thứ chưa xác thực là biến nó thành kênh bơm dữ liệu rác vào DB.

CREATE TABLE webhook_events
(
    id                UUID PRIMARY KEY,
    provider          VARCHAR(32)  NOT NULL,

    -- Khoá chống phát lại. VNPay KHÔNG gửi event id riêng, nên khoá là SHA-256 (64 ký tự hex) của
    -- chính chuỗi đã ký. Hệ quả có chủ ý: IPN phát lại y hệt thì trùng và bị bỏ qua, còn IPN mang
    -- trạng thái MỚI (payload khác đi) là một hàng khác và vẫn được xử lý. Băm payload thay vì chọn
    -- một trường của provider làm khoá, vì không trường nào của VNPay được bảo đảm là duy nhất.
    provider_event_id VARCHAR(64)  NOT NULL,

    -- Mã tham chiếu phía provider đọc được từ payload (VNPay: vnp_TxnRef). NULL khi payload hợp lệ
    -- chữ ký nhưng không mang mã nào — vẫn ghi lại để còn điều tra.
    provider_ref      VARCHAR(128),

    -- CỐ Ý nullable, khác mọi bảng nghiệp vụ khác (ADR-010). Webhook tới từ Internet, không có
    -- TenantContext và không được phép tin bất cứ tenant nào do bên ngoài gửi. Tenant chỉ suy ra
    -- được bằng cách tra provider_ref trong provider_transactions, và việc đó thuộc về worker 2.3b.
    -- Ghi raw trước, phân giải tenant sau.
    tenant_id         UUID,

    -- Nguyên văn thứ đã nhận (query string của IPN). Là bằng chứng, không bao giờ bị sửa.
    raw_payload       TEXT         NOT NULL,

    received_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- NULL = worker 2.3b chưa xử lý. Đặt khi đã xử lý xong, dù kết quả ra sao.
    processed_at      TIMESTAMPTZ,

    CONSTRAINT uk_webhook_events_provider_event UNIQUE (provider, provider_event_id)
);

-- Lô việc của worker 2.3b: hàng chưa xử lý, cũ trước. Partial index nên về lâu dài chỉ to bằng tồn đọng
-- thật, không bằng toàn bộ lịch sử webhook. LƯU Ý trong 2.3a: worker chưa tồn tại nên MỌI hàng đều thoả
-- điều kiện partial — index tạm thời bằng cả bảng. Gauge walletflow.bank.webhook.unprocessed đo đúng
-- con số đó; nó phải về gần 0 sau khi 2.3b lên.
CREATE INDEX idx_webhook_events_unprocessed ON webhook_events (received_at)
    WHERE processed_at IS NULL;

-- Truy vết khi điều tra một lần thu: "giao dịch này đã nhận những webhook nào".
CREATE INDEX idx_webhook_events_provider_ref ON webhook_events (provider, provider_ref)
    WHERE provider_ref IS NOT NULL;
