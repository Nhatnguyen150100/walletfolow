-- =====================================================================
-- V3 — Index outbox đúng với cách bảng này thực sự được dùng
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Index tìm hàng chưa gửi: đổi sang PARTIAL INDEX
-- ---------------------------------------------------------------------
-- Index cũ (published, created_at) phủ MỌI hàng. Nhưng ở trạng thái khoẻ mạnh, gần như 100% hàng
-- trong bảng này là `published = true` — relay gửi xong trong vài giây, còn hàng đã gửi thì nằm lại
-- cho tới khi job dọn chạy. Nghĩa là index cũ dành gần như toàn bộ dung lượng cho những hàng mà
-- truy vấn của relay KHÔNG BAO GIỜ cần tới.
--
-- Partial index chỉ chứa hàng chưa gửi: ở hệ thống khoẻ mạnh nó gần như rỗng, nằm trọn trong cache,
-- và không phình theo lịch sử giao dịch. Câu truy vấn của relay
-- (WHERE published = false ORDER BY created_at) khớp chính xác với nó.
DROP INDEX IF EXISTS idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished
    ON outbox_event (created_at)
    WHERE published = false;

-- ---------------------------------------------------------------------
-- 2) Index cho job dọn dẹp
-- ---------------------------------------------------------------------
-- OutboxCleanupJob tìm `published = true AND published_at < cutoff ORDER BY published_at LIMIT n`.
-- Không có index này, mỗi vòng dọn là một lần quét toàn bảng — đúng cái bảng đang lớn dần mà ta
-- muốn dọn, nên càng tồn đọng thì càng chậm.
CREATE INDEX idx_outbox_published_cleanup
    ON outbox_event (published_at)
    WHERE published = true;

-- ---------------------------------------------------------------------
-- 3) Tra cứu theo eventId khi đối soát
-- ---------------------------------------------------------------------
-- idx_outbox_event_id đã tạo ở V2, giữ nguyên.
