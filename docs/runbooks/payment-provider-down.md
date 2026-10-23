# Runbook — `PaymentProviderDown` / `PaymentRefundProviderDown` / `BankStatusCheckFailing` / `ProviderAmountMismatch` / `PendingChargeOverdue` / `ExpiredChargeNeedsReview` / `ExpiredChargeUnconfirmed` / `BankWebhookBacklog` / `BankWebhookAbandoned` / `BankWebhookForeignAccount` / `BankWebhookUnresolvedRef`

> **`PaymentProviderDown` — mức page.** Circuit breaker của **đường THU** (`provider-<tên>`) đang mở: cổng
> thanh toán lỗi liên tục (timeout, 5xx, mất kết nối) nên adapter ngừng gọi ra ngoài.
>
> **Không mất tiền**, nhưng mọi lần nạp tiền đang đứng, và người đã trả tiền chưa được xác nhận.
>
> **`PaymentRefundProviderDown` — mức page.** Mạch của **đường HOÀN** (`provider-<tên>-refund`) đang mở.
> Từ 2.4c hai đường có hai khoang riêng ([ADR-020](../adr/ADR-020-topup-compensation-refund.md)
> *Amendments* B2), nên alert này KHÔNG kéo theo alert kia — xem mục R.

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

## R — `PaymentRefundProviderDown` (mạch đường HOÀN mở)

Khác `PaymentProviderDown` ở một điểm quyết định: **đường thu vẫn chạy**. Lần thu vẫn được xác nhận, ví
vẫn được cộng, người dùng vẫn nạp được. Thứ đang đứng là các lệnh **hoàn** — và sau mỗi lệnh đó là tiền
đã bị trừ của một người dùng mà ví không được cộng.

```bash
# Lệnh hoàn đang ở đâu
docker exec wf-postgres psql -U walletflow -d railsdb -c "
  SELECT transaction_id, status, provider_ref, failure_code, created_at, updated_at
  FROM provider_transactions WHERE operation = 'REFUND' ORDER BY created_at DESC LIMIT 20;"
```

| Hàng `REFUND` ở | Nghĩa | Xử lý |
|---|---|---|
| `NEW` | Chưa gọi được provider lần nào, hoặc đã gọi mà mất câu trả lời | **Không hoàn tay.** Chờ provider sống lại rồi replay DLT `txn.command.bank-refund.v1.DLT`; khoá gửi provider là tất định nên không hoàn hai lần. |
| `SUCCEEDED` | Tiền đã về thật | Không làm gì. Saga nhận `bank.event.completed` và tự đóng. |
| `FAILED` | Provider từ chối dứt khoát | Mạch không liên quan tới ca này — xem [`topup-stuck.md`](topup-stuck.md) mục E. |

**Nguy hiểm nhất ở đây là hoàn tay song song.** Mạch mở không nói lệnh hoàn chưa đi — nó chỉ nói ta không
*hỏi* được. Một hàng `NEW` có thể đã hoàn xong ở phía provider. Hoàn tay thêm lần nữa là mất tiền của
công ty và không có đường lấy lại. Đối chiếu portal theo `provider_ref` của **lần thu** trước, luôn.

Saga ở transaction-service tự lo phần còn lại: job quét gửi lại lệnh hoàn, hết lượt thì chuyển
`REFUND_NO_RESPONSE` + `MANUAL_REVIEW` (alert `TopupRefundFailed`). Không cần đụng vào `transactions`.

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

## `ExpiredChargeUnconfirmed` — mức ticket

Một lần thu đã kết thúc `FAILED(EXPIRED)`, rồi adapter hỏi lại provider **hết số lượt cho phép**
(`walletflow.bank.status-check.max-confirm-attempts`, mặc định 5) mà **không lần nào** có câu trả lời đọc
được. Khác `ExpiredChargeNeedsReview`: ở đó ta biết provider nói có tiền; ở đây ta **không biết gì cả**, và
adapter đã thôi hỏi — sẽ không có gì tự giải quyết nữa.

```sql
SELECT id, tenant_id, transaction_id, provider_ref, amount_minor, provider_created_at, updated_at
  FROM provider_transactions
 WHERE status = 'FAILED' AND failure_code = 'EXPIRED'
   AND next_check_at IS NULL AND confirm_attempts >= 5    -- = max-confirm-attempts
 ORDER BY updated_at DESC;
```

- Alert là **counter** (`increase(...[1h])`), nên nó tự tắt sau một giờ không có ca mới. Kêu lại nghĩa là có
  ca MỚI, không phải ca cũ chưa đóng — đừng mute.
- Nguyên nhân hay gặp: provider đổi hợp đồng câu trả lời, `provider_created_at` sai nên VNPay luôn nói
  "không tìm thấy" mà không ký đúng, hoặc mạng ra provider hỏng suốt cả cửa sổ hỏi (~31 giờ với cấu hình
  mặc định).
- Đối soát **tay** với sao kê/settlement của provider theo `provider_ref`. Đây là đúng việc mà đối soát tầng
  3 (roadmap 3.1) sẽ tự động hoá; tới lúc đó phải làm tay.
- Nếu sao kê cho thấy tiền ĐÃ về: xử lý như `ExpiredChargeNeedsReview` (hoàn tiền, hoặc nạp bù qua admin API
  2.5). **KHÔNG** sửa tay `status`.
- Nếu sao kê cho thấy không có giao dịch: kết luận `EXPIRED` là đúng, đóng ticket. Không cần sửa gì trong DB
  — counter đã kêu xong và không kêu lại cho hàng này nữa.

> **Hàng của tài khoản merchant CŨ không vào alert này.** `lockDueExpiredConfirmations` lọc theo
> `provider_account`, nên hàng của `VNPAY_TMN_CODE` cũ không bao giờ được hỏi, `confirm_attempts` đứng ở 0 và
> chúng cũng không vào `PendingChargeOverdue` (đã `FAILED`). Nhóm này hiện chỉ được chặn bằng quy trình xả
> hàng trước khi đổi tài khoản (mục trên) — chưa có alert. Nếu vừa đổi tài khoản, phải tự rà bằng tay:
> `SELECT * FROM provider_transactions WHERE provider_account <> '<tmn hiện tại>' AND status IN ('PENDING','FAILED');`

## `BankWebhookBacklog` — mức ticket

Webhook đã nhận mà `WebhookWorker` chưa xử lý (`walletflow_bank_webhook_unprocessed` > 20 trong 10 phút).
Nhớ: ta **đã trả `RspCode=00`** cho provider, tức "đã ghi nhận, đừng gửi lại" — hàng tồn ở đây là những tin
báo sẽ **không bao giờ được gửi lại**.

```sql
SELECT provider, count(*), min(received_at), max(attempts)
  FROM webhook_events WHERE processed_at IS NULL GROUP BY provider;
```

- Tiền vẫn an toàn: job đối chiếu (`PendingChargeReconciler`) vẫn chốt mọi lần thu. Cái mất là **tốc độ** —
  người dùng chờ lâu hơn. Đừng xử lý như sự cố mất tiền.
- `max(attempts) = 0` ⇒ worker **không chạy**: kiểm `walletflow.bank.webhook.enabled`, log khởi động, và
  thread pool `spring.task.scheduling.pool.size` (job đối chiếu chiếm hết thread thì worker đói).
- `attempts` tăng đều mà không đóng ⇒ provider không trả lời: xem `BankStatusCheckFailing` /
  `PaymentProviderDown` bên trên, cùng nguyên nhân.
- Hàng của **provider cũ** không bao giờ được nhặt (`lockDueUnprocessed` lọc theo provider đang chạy) — nếu
  vừa đổi `BANK_PROVIDER`, tồn đọng này là hàng chết, đóng tay:
  `UPDATE webhook_events SET processed_at = now(), outcome = 'ABANDONED' WHERE provider = '<provider cũ>' AND processed_at IS NULL;`

## `BankWebhookAbandoned` — mức ticket

Webhook dùng hết `walletflow.bank.webhook.max-attempts` lượt mà chưa hỏi được provider lần nào. Adapter đã
thôi thử — không có gì tự giải quyết nữa.

```sql
SELECT id, provider_ref, received_at, attempts, raw_payload
  FROM webhook_events WHERE outcome = 'ABANDONED' ORDER BY received_at DESC;
```

- Tra `provider_ref` sang `provider_transactions`. Hàng đã có kết quả cuối ⇒ job đối chiếu đã làm thay, đóng
  ticket. Hàng còn `PENDING` ⇒ xử lý như `PendingChargeOverdue`.
- Muốn thử lại sau khi provider hồi: `UPDATE webhook_events SET processed_at = NULL, outcome = NULL,
  attempts = 0, next_attempt_at = now() WHERE id = '<id>';` — an toàn vì worker không bao giờ tự ghi tiền,
  nó chỉ hỏi lại provider.

## `BankWebhookForeignAccount` — mức ticket

Webhook của một lần thu thuộc **tài khoản merchant cũ** (`provider_account` khác `VNPAY_TMN_CODE` đang
chạy). Worker đóng ngay với `outcome = 'FOREIGN_ACCOUNT'` thay vì thử lại: chỉ tài khoản đã tạo phiên mới
tra được phiên đó.

```sql
SELECT p.id, p.provider_account, p.status, p.amount_minor, w.received_at
  FROM webhook_events w JOIN provider_transactions p ON p.provider_ref = w.provider_ref
 WHERE w.outcome = 'FOREIGN_ACCOUNT' ORDER BY w.received_at DESC;
```

Đây là dấu hiệu một lần xoay `VNPAY_TMN_CODE` **còn bỏ dở** — xem "Trước khi đổi tài khoản merchant" bên
trên. Những lần thu này không có đường tự chốt nào nữa (job đối chiếu cũng lọc theo tài khoản): phải đối
soát tay với sao kê của **tài khoản cũ**.

## `BankWebhookUnresolvedRef` — mức ticket

Webhook có **chữ ký hợp lệ** nhưng `provider_ref` không có trong `provider_transactions`. Khác hẳn counter
`webhook.received{outcome=rejected}` (người lạ gõ cửa — chuyện thường ở endpoint công khai): tới được đây
nghĩa là ai đó **ký được bằng khoá của ta**.

Không phải ca đua: hàng `provider_transactions` luôn tồn tại trước khi người dùng được gửi sang provider.

1. Có môi trường nào khác (staging, máy dev) dùng chung `VNPAY_TMN_CODE` / `VNPAY_HASH_SECRET` không? Đây là
   nguyên nhân thường gặp nhất — tách tài khoản merchant ra.
2. Nếu không: coi như **khoá đã lộ**. Xoay `VNPAY_HASH_SECRET` (hoặc `BANK_MOCK_WEBHOOK_SECRET`) theo
   `docs/runbooks/` quy trình bí mật, và rà `raw_payload` của các hàng `UNRESOLVED_REF` xem kẻ gửi biết gì.

## Liên quan
ADR-016, ADR-017, ADR-018, `ResilientPaymentProvider`, `ChargeService`, `ChargeStatusService`,
`PendingChargeReconciler`, `WebhookWorker`, `WebhookProcessor`, `VnPayProvider`, `dlt-not-empty.md`
