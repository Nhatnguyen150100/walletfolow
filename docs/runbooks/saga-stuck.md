# Runbook — `SagaManualReview` / `SagaRetriesHigh` / `SagaScanFailing` / `SagaStatusConflict`

> **`SagaManualReview` — mức page.** Một lệnh chuyển tiền đã được nhận (202), đã gửi lệnh ghi sổ
> nhiều lần, và ledger **không trả lời lần nào**. Giao dịch đứng ở `MANUAL_REVIEW`.
>
> Tiền **có thể đã đi** (bút toán ghi xong, event trả lời bị mất) **hoặc chưa**. Không đoán.

## Nguyên tắc vàng

**Sổ cái là sự thật.** Trước khi làm gì, hỏi ledger: giao dịch này đã có bút toán chưa?

**KHÔNG** gửi lại lệnh bằng tay với `transactionId` mới — đó là một giao dịch khác, và nếu bút toán
gốc đã ghi thì tiền bị trừ hai lần. Mọi lần gửi lại phải giữ nguyên `transactionId` (ledger idempotent
theo nó) và dùng `eventId` mới.

## Chẩn đoán — 3 phút

```bash
# 1. Các giao dịch đang chờ người xử lý
docker exec wf-postgres psql -U walletflow -d txndb -c "
  SELECT t.id, t.tenant_id, t.from_wallet, t.to_wallet, t.amount_minor, t.currency,
         t.created_at, s.attempt_count, s.command_event_id
  FROM transactions t JOIN saga_state s ON s.transaction_id = t.id
  WHERE t.status = 'MANUAL_REVIEW' ORDER BY t.created_at;"

# 2. Với TỪNG giao dịch: ledger đã ghi sổ chưa? (thay <txn-id>, <tenant-id>)
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT id, type, created_at FROM journal_entries
  WHERE tenant_id = '<tenant-id>' AND transaction_id = '<txn-id>';"

# 3. Lệnh có ra khỏi outbox không? (relay chết -> OutboxStalled cũng phải đang kêu)
docker exec wf-postgres psql -U walletflow -d txndb -c "
  SELECT published, published_at, created_at FROM outbox_event
  WHERE topic = 'txn.command.post-journal.v1' AND payload LIKE '%<txn-id>%' ORDER BY created_at;"

# 4. Lệnh hoặc câu trả lời có nằm ở DLT không? (DltNotEmpty cũng phải đang kêu)
```

## Xử lý theo kết quả bước 2

| Ledger | Ý nghĩa | Xử lý |
|---|---|---|
| **Có** bút toán `TRANSFER` | Tiền đã đi, câu trả lời bị mất | **Mở lại saga** (bên dưới). Job quét gửi lại lệnh cùng `transactionId`; ledger gặp bút toán đã có, trả lại nó và phát lại `LedgerPosted`; saga tự đóng thành `COMPLETED`. |
| **Không** có bút toán | Lệnh chưa tới được ledger | Tìm và sửa nguyên nhân ở bước 3–4 trước (relay, DLT, ledger chết). Sau đó **mở lại saga**. |
| Ledger đang chết | — | Xem `service-down.md`. Không làm gì với giao dịch cho tới khi ledger sống lại — mở lại lúc này chỉ đốt hết lượt thử và quay về `MANUAL_REVIEW`. |

### Mở lại saga — câu lệnh duy nhất được dùng

Đặt lại **cả hai** bảng, trong **một** transaction. Chỉ đặt lại `saga_state` là không đủ: job quét thấy
giao dịch không còn `PROCESSING` và đóng saga thành `DONE` mà không gửi gì (có test cho đúng câu lệnh
này: `StuckSagaIT.mo_lai_manual_review_theo_runbook`).

```sql
-- txndb — thay <txn-id>
BEGIN;
UPDATE transactions SET status = 'PROCESSING', failure_code = NULL, failure_reason = NULL, updated_at = now()
 WHERE id = '<txn-id>' AND status = 'MANUAL_REVIEW';
UPDATE saga_state SET step_status = 'SENT', attempt_count = 0, deadline_at = now(), updated_at = now()
 WHERE transaction_id = '<txn-id>' AND step_status = 'MANUAL_REVIEW';
COMMIT;   -- mỗi câu phải báo UPDATE 1; nếu là UPDATE 0 thì ROLLBACK và xem lại trạng thái
```

Job quét nhặt saga ở lượt kế tiếp (mặc định 15 giây), gửi lại với eventId mới và **cùng
`transactionId`** — an toàn kể cả khi bút toán đã có: ledger không bao giờ ghi bút toán thứ hai cho
cùng `transactionId`.

**Tuyệt đối không** tự chèn lệnh vào `outbox_event` hay tạo lệnh mới với `transactionId` khác.

## `SagaScanFailing` — mức ticket

Job quét gặp lỗi khi xử lý một saga cụ thể (thường là dữ liệu hỏng ở hàng giao dịch). Saga đó bị đẩy
lùi hạn chót để không chặn các saga khác, nhưng sẽ lỗi lại ở lượt sau. Tìm `transactionId` trong log
(`xử lý saga ... lỗi`), sửa dữ liệu hoặc xử lý tay như một ca `MANUAL_REVIEW`.

## `SagaRetriesHigh` — mức ticket

Nhiều lần gửi lại trong 15 phút: ledger chậm (xem consumer lag), relay chập chờn, hoặc message đang
vào DLT. Đây là tín hiệu **trước** khi giao dịch thành `MANUAL_REVIEW` — xử lý nguyên nhân gốc ngay.

## `SagaStatusConflict` — mức ticket

Một giao dịch đã báo `FAILED` (ledger từ chối lệnh gốc, ví dụ thiếu tiền), rồi sau đó ledger **ghi
sổ** một lần gửi lại (tiền đã về trong lúc chờ). Saga chuyển sang `COMPLETED` vì sổ cái thắng.

- Tiền **đúng** theo sổ cái — không có gì cần sửa ở số dư.
- Người dùng đã nhận thông báo thất bại rồi thông báo thành công. Nếu họ đã **gửi lại lệnh mới** dựa
  trên thông báo thất bại, có thể họ đã chuyển hai lần — liên hệ người dùng.
- Tần suất cao = hạn chót saga quá ngắn so với độ trễ thật của ledger. Cân nhắc tăng
  `walletflow.transaction.saga-deadline`.

## Liên quan
ADR-014, `SagaService`, `StuckSagaScanner`, `outbox-stalled.md`, `dlt-not-empty.md`
