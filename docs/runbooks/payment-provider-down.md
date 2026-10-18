# Runbook — `PaymentProviderDown` / `BankStatusCheckFailing` / `ProviderAmountMismatch`

> **`PaymentProviderDown` — mức page.** Circuit breaker của bank-adapter đang mở: cổng thanh toán lỗi
> liên tục (timeout, 5xx, mất kết nối) nên adapter ngừng gọi ra ngoài.
>
> **Không mất tiền**, nhưng mọi lần nạp tiền đang đứng, và người đã trả tiền chưa được xác nhận.

## Nguyên tắc vàng

**Lỗi kỹ thuật không phải là "thất bại".** Timeout nghĩa là *không biết* — provider có thể đã tạo phiên,
người dùng có thể đã trả tiền. Vì vậy:

- **KHÔNG** sửa `provider_transactions.status` thành `FAILED` bằng tay. Chỉ provider (qua `getStatus`) được
  kết luận một lần thu (ADR-016).
- **KHÔNG** sinh lệnh thu mới với `transactionId` khác cho cùng một lần nạp — đó là lần thu thứ hai.
- Lệnh thu gửi lại luôn dùng **cùng idempotency key** (tất định theo tenant + giao dịch), nên provider trả
  lại phiên cũ. Thử lại luôn an toàn.

## Chẩn đoán — 3 phút

```bash
# 1. Mạch nào đang mở, từ lúc nào (Prometheus)
#    resilience4j_circuitbreaker_state{state="open"} == 1
#    resilience4j_circuitbreaker_failure_rate

# 2. Log của adapter — lý do cụ thể (timeout / 5xx / "circuit breaker đang mở")
docker logs --since 15m wf-bank-adapter | grep -E "không trả lời được|thử lại|DLT"

# 3. Các lần thu đang dở (railsdb)
docker exec wf-postgres psql -U walletflow -d railsdb -c "
  SELECT status, count(*), min(created_at) AS oldest
  FROM provider_transactions GROUP BY status;"

# 4. Lệnh thu có vào DLT không? (DltNotEmpty cũng phải đang kêu)
#    topic: txn.command.bank-charge.v1.DLT
```

## Xử lý

| Tình huống | Ý nghĩa | Xử lý |
|---|---|---|
| Trang trạng thái của provider báo sự cố | Provider chết | Chờ. Mạch tự thử lại sau `open-wait` (30 giây). Lệnh thu Kafka thử lại có backoff; hết lượt thì vào DLT. |
| Provider sống, adapter vẫn lỗi | Mạng/DNS/chứng chỉ/khoá sai phía mình | Kiểm kết nối từ container `wf-bank-adapter` tới provider; xem lỗi cụ thể ở bước 2. |
| Hàng `NEW` lâu | Lệnh thu chưa hỏi được provider lần nào (kể cả có thể đã tạo phiên mà mất câu trả lời) | Sau khi provider sống lại: **replay** message từ DLT `txn.command.bank-charge.v1.DLT` (xem `dlt-not-empty.md`). Adapter gửi lại đúng khoá cũ, không tạo phiên thứ hai. |
| Hàng `PENDING` lâu | Phiên đã tạo, chưa chốt kết quả | Không cần làm gì: job đối chiếu tự hỏi lại mỗi `recheck-after` (30 giây) khi provider sống lại. |

## `BankStatusCheckFailing` — mức ticket

Job đối chiếu (`PendingChargeReconciler`) không hỏi được trạng thái một số lần thu. Mỗi hàng lỗi đã được
lùi hạn nên không chặn hàng khác, và sẽ được hỏi lại. Thường đi cùng `PaymentProviderDown`. Nếu kêu một
mình: tìm `providerRef` trong log (`chưa hỏi được trạng thái lần thu`) — provider không nhận ra mã đó
(ví dụ dùng `mock` rồi khởi động lại: phiên mock chỉ nằm trong bộ nhớ).

## `ProviderAmountMismatch` — mức page

Provider báo một lần thu đã **thành công**, nhưng **số tiền hoặc loại tiền** provider thực thu khác số đã
yêu cầu. Adapter **không** báo thành công (event thành công mang số mà ledger sẽ cộng), nên lần thu đứng
ở `PENDING` và được hỏi lại mỗi 30 giây — alert kêu lại mỗi lần.

- Người dùng **đã trả tiền**; ví **chưa** được cộng.
- Tìm lần thu trong log (`provider báo đã thu ... nhưng yêu cầu là ...`), đối chiếu với trang quản trị của
  provider.
- **KHÔNG** sửa tay `status` sang `SUCCEEDED` — saga sẽ cộng số tiền YÊU CẦU, không phải số đã thu.
  Hướng xử lý (hoàn tiền phần lệch, ghi bút toán theo số thực thu) là việc của admin API 2.5; cho tới lúc
  đó xử lý với provider và ghi lại trong ticket.

## Liên quan
ADR-016, `ResilientPaymentProvider`, `ChargeService`, `ChargeStatusService`, `PendingChargeReconciler`,
`dlt-not-empty.md`
