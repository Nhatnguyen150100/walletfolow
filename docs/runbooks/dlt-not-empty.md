# Runbook — `DltNotEmpty`

> **Mức: page (gọi người).** Mỗi message ở DLT là **một lệnh tiền chưa được thực thi**.
> Giao dịch tương ứng đang treo và sẽ treo mãi nếu không có người xử lý.

## Triệu chứng

- Alert `DltNotEmpty` bắn, metric `walletflow_dlt_messages_total{topic}` tăng.
- Log có dòng `Kafka: ĐẨY SANG DLT <topic>.DLT — offset ...`.
- Người dùng báo giao dịch "đang xử lý" mãi không xong.

## Chẩn đoán — 3 phút

**1. Xem message trong DLT và nguyên nhân:**

```bash
docker exec wf-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic txn.command.post-journal.v1.DLT \
  --from-beginning --property print.headers=true --timeout-ms 10000
```

Header quan trọng: `kafka_dlt-exception-cause-fqcn` — **nguyên nhân gốc**.
(`kafka_dlt-exception-fqcn` luôn là `ListenerExecutionFailedException`, lớp bọc của Spring, không
nói lên điều gì.)

**2. Phân loại theo nguyên nhân gốc:**

| Nguyên nhân | Ý nghĩa | Hướng xử lý |
|---|---|---|
| `PoisonMessageException` | JSON hỏng / sai kiểu. Producer gửi sai định dạng. | Sửa producer. Message này **không replay được** — phải dựng lại lệnh. → mục A |
| `JDBCConnectionException`, `CannotAcquireLock` | DB chết hoặc quá tải lúc đó. Message **vẫn hợp lệ**. | → mục B (replay) |
| `DataIntegrityViolationException` | Vi phạm ràng buộc — thường là đua giữa hai lệnh trùng | Kiểm tra xem bút toán đã tồn tại chưa (mục C) rồi mới quyết định |
| Khác | Đọc stack trace trong log service | — |

**3. Lệnh đó đã vào sổ chưa?** — câu hỏi quyết định, hỏi TRƯỚC khi làm bất cứ gì:

```bash
# transactionId lấy từ payload message trong DLT
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT id, type, created_at FROM journal_entries WHERE transaction_id = '<txnId>';"
```

- **Có kết quả** → bút toán ĐÃ ghi. Message trong DLT là bản giao lại. **Không replay.**
  Chỉ cần xoá/đánh dấu đã xử lý.
- **Rỗng** → lệnh chưa được thực thi, phải xử lý tiếp.

## Xử lý

### A. Poison message (producer gửi sai)

1. Ghi lại payload vào ticket.
2. Sửa producer.
3. Dựng lại lệnh đúng định dạng và gửi lại với **cùng `transactionId`** — ledger idempotent theo
   `(transactionId, type)` nên gửi lại an toàn kể cả khi không chắc.

### B. Lỗi tạm thời → replay

Xác nhận nguyên nhân gốc đã hết (DB đã lên lại), rồi đẩy message từ DLT về topic gốc:

```bash
docker exec wf-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic txn.command.post-journal.v1.DLT \
  --from-beginning --timeout-ms 5000 > /tmp/dlt.txt

# Kiểm tra nội dung TRƯỚC khi gửi lại
cat /tmp/dlt.txt

docker exec -i wf-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic txn.command.post-journal.v1 < /tmp/dlt.txt
```

Replay an toàn vì cả hai tầng chống trùng đều hoạt động: dedup theo `eventId` và idempotency theo
`(transactionId, type)`.

> Công cụ replay có kiểm soát (chọn lọc, có audit) là hạng mục của **Phase 3** — xem
> `walletflow-production-roadmap.md` §3.3. Cho tới lúc đó, quy trình thủ công này là cách duy nhất,
> và nó **bắt buộc phải có người đọc payload trước khi gửi lại**.

### C. Nghi ngờ trùng

Nếu bút toán đã tồn tại và message vẫn ở DLT: đây là message trùng đến muộn. Không làm gì với sổ
cái; chỉ cần ghi nhận và bỏ qua message.

## Sau sự cố

- [ ] `SELECT SUM(amount_minor) FROM postings;` phải **= 0**
- [ ] Số dư ví liên quan khớp với kỳ vọng của người dùng
- [ ] Ghi lại nguyên nhân gốc vào ticket
- [ ] Nếu là lỗi lặp lại: thêm test chặn nó, đừng chỉ sửa dữ liệu

## Phòng ngừa

Nguyên nhân phổ biến nhất của alert này là **nhập nhằng giữa lỗi nghiệp vụ và lỗi kỹ thuật**.
Từ chối nghiệp vụ (không đủ số dư, bút toán lệch) **không được** đi vào DLT — nó phải thành
`LedgerRejectedEvent`. Xem `LedgerCommandListener`: nếu một `AppException` lọt tới error handler,
đó là bug phân loại lỗi, không phải sự cố hạ tầng.
