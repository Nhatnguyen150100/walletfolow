# ADR-003 — Transactional Outbox cho dual-write

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 1

## Context

Nghiệp vụ cần làm hai việc: (a) ghi dữ liệu vào database, (b) phát event lên Kafka. Hai hệ thống
khác nhau, không có transaction chung.

Làm tuần tự thì luôn có một khe hở: commit DB xong rồi crash trước khi gửi Kafka → **event mất**,
giao dịch treo vĩnh viễn. Gửi Kafka trước rồi DB rollback → **event ma**, downstream xử lý một việc
chưa từng xảy ra.

Khe hở này không thu nhỏ được bằng cách viết cẩn thận hơn — nó là tính chất của việc ghi vào hai
hệ thống không có giao thức chung.

## Decision

Ghi event vào bảng `outbox_event` **trong cùng transaction** với dữ liệu nghiệp vụ. Một tiến trình
nền (`OutboxRelay`) quét hàng chưa gửi, publish lên Kafka, rồi đánh dấu đã gửi.

Chi tiết quan trọng:
- `OutboxPublisher` dùng `Propagation.MANDATORY` → gọi ngoài transaction là **lỗi lúc chạy**, không
  phải lỗi âm thầm. Đây là thứ biến nguyên tắc thành ràng buộc kỹ thuật.
- Cột `payload` lưu **đúng chuỗi JSON sẽ nằm trên Kafka**. Relay không deserialize về kiểu Java —
  nên đổi tên một class event không biến các hàng đang chờ thành rác không gửi được mãi mãi.
- Relay đọc bằng `SELECT ... FOR UPDATE SKIP LOCKED` → chạy nhiều instance song song an toàn.
- CI có guardrail chặn `kafkaTemplate.send` trực tiếp trong code nghiệp vụ.

## Consequences

- ✅ Không bao giờ mất event, không bao giờ có event ma.
- ✅ Nội dung trên Kafka trùng khít nội dung trong DB → đối soát và replay được.
- ⚠️ Giao hàng là **at-least-once** → consumer **bắt buộc** idempotent (ADR-008).
- ⚠️ Thêm độ trễ bằng chu kỳ quét (mặc định 2 giây).
- ⚠️ Relay bắt mọi exception để thử lại → **hỏng hoàn toàn trông giống đang rảnh**. Bắt buộc phải
  có metric `walletflow.outbox.oldest.age.seconds` và alert `OutboxStalled`; không có chúng thì
  outbox là một điểm mù.
- ⚠️ Bảng phình vô hạn nếu không dọn → `OutboxCleanupJob`, chỉ đụng hàng đã gửi.

## Liên quan
ADR-008, `OutboxRelayIT`, `OutboxRetentionIT`, runbook `outbox-stalled.md`
