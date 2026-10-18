# Runbook — `PaymentProviderDown` / `BankStatusCheckFailing` / `ProviderAmountMismatch` / `PendingChargeOverdue` / `ExpiredChargeNeedsReview`

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
| Hàng `PENDING` lâu | Phiên đã tạo, chưa chốt kết quả | Không cần làm gì: job đối chiếu tự hỏi lại, nhịp giãn cấp số 30 s → 1 m → 2 m → … → tối đa 10 m (`check_attempts`, ADR-017). Quá hạn phiên + grace (15 + 15 phút) mà vẫn đứng: xem `PendingChargeOverdue` bên dưới. |
| Hàng `FAILED` với `failure_code = EXPIRED` | Phiên hết hạn và provider **khẳng định** chưa thu đồng nào (ADR-017) | Bình thường — người dùng bỏ dở. Không bao giờ do provider chết mà ra: provider không trả lời thì hàng giữ `PENDING`. |

## `BankStatusCheckFailing` — mức ticket

Job đối chiếu (`PendingChargeReconciler`) không hỏi được trạng thái một số lần thu. Mỗi hàng lỗi đã được
lùi hạn nên không chặn hàng khác, và sẽ được hỏi lại. Vì nhịp hỏi giãn cấp số (ADR-017), một hàng lỗi
vĩnh viễn (ví dụ chữ ký sai) chỉ sinh vài lỗi mỗi 30 phút — alert này bắt lỗi diện rộng; hàng kẹt lẻ do
`PendingChargeOverdue` bắt. Thường đi cùng `PaymentProviderDown`. Nếu kêu một
mình: tìm `providerRef` trong log (`chưa hỏi được trạng thái lần thu`) — provider không nhận ra mã đó
(ví dụ dùng `mock` rồi khởi động lại: phiên mock chỉ nằm trong bộ nhớ).

## `ProviderAmountMismatch` — mức page

Provider báo một lần thu đã **thành công**, nhưng **số tiền hoặc loại tiền** provider thực thu khác số đã
yêu cầu. Adapter **không** báo thành công (event thành công mang số mà ledger sẽ cộng), nên lần thu đứng
ở `PENDING` và được hỏi lại theo nhịp giãn cấp số (tối đa mỗi 10 phút) — alert kêu lại mỗi lần hỏi. Hàng
này không bao giờ tự hết hạn (provider đã báo thu), nên sau hạn phiên + grace nó còn làm
`PendingChargeOverdue` kêu.

- Người dùng **đã trả tiền**; ví **chưa** được cộng.
- Tìm lần thu trong log (`provider báo đã thu ... nhưng yêu cầu là ...`), đối chiếu với trang quản trị của
  provider.
- **KHÔNG** sửa tay `status` sang `SUCCEEDED` — saga sẽ cộng số tiền YÊU CẦU, không phải số đã thu.
  Hướng xử lý (hoàn tiền phần lệch, ghi bút toán theo số thực thu) là việc của admin API 2.5; cho tới lúc
  đó xử lý với provider và ghi lại trong ticket.

## `PendingChargeOverdue` — mức ticket

Có lần thu `PENDING` đã quá hạn phiên + grace (gauge `walletflow_bank_pending_overdue`). Lẽ ra nó đã kết
thúc: thành công, hoặc `EXPIRED` nếu provider khẳng định chưa thu. Còn đứng tức là adapter **không kết luận
được**, và tiền của người dùng **có thể đã bị trừ**.

```bash
docker exec wf-postgres psql -U walletflow -d railsdb -c "
  SELECT id, tenant_id, transaction_id, provider, provider_ref, amount_minor, expires_at, check_attempts,
         raw_response
  FROM provider_transactions
  WHERE status = 'PENDING' AND expires_at <= now() - interval '15 minutes'
  ORDER BY expires_at;"
```

| `raw_response` / log | Ý nghĩa | Xử lý |
|---|---|---|
| Log `chưa hỏi được trạng thái lần thu` (kèm `BankStatusCheckFailing`) | Provider không trả lời / chữ ký câu trả lời sai | Xem phần `PaymentProviderDown`. Chữ ký sai kéo dài: kiểm `VNPAY_HASH_SECRET` đúng môi trường. |
| VNPay `vnp_TransactionStatus` = `04`, `07`, `05`, `06`, `09` (log `trạng thái lơ lửng`) | Đảo giao dịch, nghi gian lận, đang hoàn — **không** tự hết hạn | Đối chiếu trên trang quản trị VNPay; ghi lại trong ticket. Xử lý chính thức qua admin API (2.5). |
| Kèm `ProviderAmountMismatch` | Lệch số tiền | Xem mục trên. |
| `provider` khác `BANK_PROVIDER` hiện tại | Phiên của provider cũ, không ai hỏi nữa | Chạy tạm adapter với provider cũ để chốt, hoặc đối chiếu tay với provider đó. |

**KHÔNG** sửa tay `status` — kể cả sang `FAILED`: người dùng có thể đã trả tiền.

### Trước khi đổi tài khoản merchant (`VNPAY_TMN_CODE`) hoặc `BANK_PROVIDER`

Phiên chỉ tra được dưới đúng tài khoản đã tạo nó (cột `provider_account`). Sau khi đổi, hàng của tài khoản
cũ **không** còn được hỏi: chúng không bao giờ hết hạn sai, nhưng đứng `PENDING` và làm `PendingChargeOverdue`
kêu. Vì vậy:

1. Ngừng nhận lệnh nạp mới (dừng saga top-up / tạm dừng consumer `txn.command.bank-charge.v1`).
2. Chờ hàng `PENDING` của tài khoản cũ về 0 — tối đa hạn phiên + grace (30 phút) + một nhịp hỏi (10 phút):
   `SELECT count(*) FROM provider_transactions WHERE status = 'PENDING' AND provider_account = '<cũ>';`
3. Đổi cấu hình, khởi động lại, mở lại luồng nạp.

## `ExpiredChargeNeedsReview` — mức page

Một lần thu đã kết thúc `FAILED(EXPIRED)` (event `bank.event.failed` đã phát, người dùng thấy "thất bại"),
nhưng lần hỏi xác nhận 1 giờ sau đó thấy provider báo **đã thu** hoặc **lơ lửng**. Tiền có thể đã rời tài
khoản người dùng mà ví không được cộng.

- Tìm trong log: `đã báo EXPIRED nhưng provider nay báo` (kèm id lần thu và giao dịch).
- Đối chiếu trên trang quản trị của provider bằng `provider_ref` (`vnp_TxnRef`).
- Adapter **không** tự sửa trạng thái; **KHÔNG** sửa tay sang `SUCCEEDED` (saga đã kết thúc giao dịch).
  Hướng xử lý: hoàn tiền cho người dùng qua provider, hoặc ghi bút toán nạp bù qua admin API (2.5) — ghi lại
  trong ticket cho tới lúc đó.

## Liên quan
ADR-016, ADR-017, `ResilientPaymentProvider`, `ChargeService`, `ChargeStatusService`, `PendingChargeReconciler`,
`VnPayProvider`, `dlt-not-empty.md`
