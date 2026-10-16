# ADR-008 — At-least-once + inbox dedup + partition theo ví

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 1

## Context

Outbox (ADR-003) giao hàng **at-least-once**: relay có thể gửi lại sau khi crash giữa chừng. Kafka
cũng giao at-least-once khi consumer rebalance hoặc retry. Nghĩa là consumer **sẽ** nhận message
trùng — không phải "có thể", mà là chắc chắn, chỉ là chưa biết lúc nào.

Thêm nữa, Kafka chỉ đảm bảo thứ tự **trong một partition**, không đảm bảo thứ tự toàn cục.

## Decision

**1. Dedup bằng inbox table.** Trước khi xử lý, consumer ghi `(event_id, consumer)` vào bảng
`processed_event` **trong cùng transaction** với xử lý nghiệp vụ. Trùng thì khoá chính vỡ, hoặc
kiểm tra trước thấy đã có → bỏ qua. Xử lý và đánh dấu cùng sống hoặc cùng chết.

**2. `eventId` phải TẤT ĐỊNH.** Đây là điểm dễ sai và hậu quả thì câm. Event "đã ghi bút toán" lấy
`eventId = journalEntry.id`. Nếu lệnh bị Kafka giao lại và ledger phát lại event (đúng theo ngữ
nghĩa idempotent ở ADR-006), consumer sẽ thấy **cùng một** `eventId` nên dedup được. Dùng
`UUID.randomUUID()` ở đây là cách chắc chắn nhất để cộng đôi số dư read model — và không có gì báo
lỗi cả. Event từ chối, vốn không có hàng nào trong DB để lấy id, sinh UUID theo tên từ
`(transactionId, type)` để giữ cùng tính chất.

**3. Partition key = walletId.** Mọi message của một ví vào cùng partition → thứ tự được đảm bảo
cho từng ví, đúng thứ thực sự cần, trong khi các ví khác nhau vẫn xử lý song song. Không phụ thuộc
thứ tự toàn cục.

**4. Poison message đi thẳng DLT.** Lỗi parse không bao giờ tự khỏi khi thử lại, và retry nó sẽ
chặn cả partition — làm giao dịch của những ví khác phải chờ vì một message rác.

## Consequences

- ✅ Nhận trùng là vô hại; đây là điều kiện để toàn bộ chuỗi outbox→Kafka→consumer an toàn.
- ✅ Thứ tự đúng ở mức cần thiết mà vẫn song song hoá được.
- ⚠️ `processed_event` cần job dọn theo thời gian.
- ⚠️ Mỗi consumer phải có tên riêng trong khoá dedup — hai service tiêu thụ cùng topic đều phải
  được xử lý event đó.
- ⚠️ Số partition quyết định mức song song tối đa; đổi số partition sau này sẽ **thay đổi ánh xạ
  ví→partition** và phá vỡ đảm bảo thứ tự trong giai đoạn chuyển tiếp.

## Liên quan
ADR-003, ADR-006, `EventDeduplicator`, `KafkaDltIT`, `LedgerCommandFlowIT`
