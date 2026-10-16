# Runbook — `OutboxStalled` / `OutboxBacklogHigh` / `OutboxPublishFailing`

> **Mức: page.** Bút toán **đã ghi vào sổ** nhưng event chưa đi ra. Ledger đúng, còn mọi service
> downstream (số dư, trạng thái giao dịch, thông báo) đang lệch so với sự thật.
>
> Tiền không mất — outbox là bền vững. Nhưng hệ thống đang nói dối người dùng về trạng thái.

## Vì sao alert này dùng *tuổi* chứ không dùng *số lượng*

`OutboxRelay` cố tình bắt **mọi** exception để thử lại ở vòng sau — đúng theo thiết kế at-least-once.
Hệ quả: một relay hỏng hoàn toàn (Kafka không tới được, sai serializer, sai tên topic) trông **y hệt**
một relay đang rảnh rỗi. Khi tải thấp, số lượng hàng chờ vẫn nhỏ và không có gì bất thường.

`walletflow_outbox_oldest_age_seconds` thì tăng tuyến tính và không nói dối.

## Chẩn đoán — 2 phút

```bash
# 1. Tồn đọng bao nhiêu, cũ nhất bao lâu
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT count(*) AS cho_gui,
         min(created_at) AS cu_nhat,
         now() - min(created_at) AS da_cho
  FROM outbox_event WHERE published = false;"

# 2. Relay có đang chạy không? (log nhịp mỗi 2 giây ở mức DEBUG)
docker logs wf-ledger --tail 100 | grep -i "outbox"

# 3. Kafka có sống không
docker exec wf-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Nguyên nhân thường gặp

| Dấu hiệu | Nguyên nhân | Xử lý |
|---|---|---|
| Log `publish eventId=... thất bại` lặp lại | Kafka không tới được | Khởi động lại Kafka; relay tự gửi hết sau khi Kafka lên |
| Không có log outbox nào | Scheduler chết, hoặc `@EnableScheduling` bị mất | Restart service; kiểm tra `OutboxAutoConfiguration` có nạp không |
| `UnknownTopicOrPartitionException` | Topic chưa tồn tại và auto-create đã tắt | Kiểm tra `KafkaTopicsAutoConfiguration` có chạy; `walletflow.kafka.declare-topics` |
| `SerializationException` | Sai serializer | Phải là `StringSerializer` — xem `StringKafkaTemplates` |
| Tồn đọng tăng đều, relay vẫn chạy | Throughput không theo kịp | Tăng batch size / số instance (relay dùng `SKIP LOCKED` nên scale ngang an toàn) |
| Connection pool cạn | DB quá tải | Xem `HikariPool` trong log |

## Xử lý

Trong phần lớn trường hợp, **sửa nguyên nhân gốc là đủ** — relay tự gửi hết phần tồn đọng.

```bash
# Theo dõi tồn đọng rút xuống
watch -n 2 "docker exec wf-postgres psql -U walletflow -d ledgerdb -tc \
  'SELECT count(*) FROM outbox_event WHERE published = false;'"
```

> ⛔ **TUYỆT ĐỐI KHÔNG** xoá hàng outbox chưa gửi để "làm sạch alert". Mỗi hàng là một event chưa
> đến đích; xoá nó là làm mất vĩnh viễn một chuyển động tiền khỏi tầm nhìn của các service khác,
> và không có cách nào biết điều đó đã xảy ra. `OutboxCleanupJob` chỉ đụng tới hàng
> `published = true` — có test riêng (`OutboxRetentionIT`) chỉ để bảo vệ tính chất này.

## Sau sự cố

- [ ] `walletflow_outbox_pending` về gần 0, `oldest_age_seconds` = 0
- [ ] Đối chiếu số dư read-model với ledger cho các ví liên quan
- [ ] `SELECT SUM(amount_minor) FROM postings;` = 0
