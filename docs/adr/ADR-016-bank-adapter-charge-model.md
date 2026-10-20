# ADR-016 — bank-adapter: thu tiền theo luồng chuyển hướng + xác nhận bất đồng bộ

**Trạng thái:** Accepted · **Ngày:** 2026-10-18 · **Milestone:** Phase 2 — 2.2a

## Context

Phase 2 nạp tiền qua một cổng thanh toán có giấy phép (ADR-015). Trước khi viết bank-adapter có ba nguồn
nói ba điều khác nhau về cách "thu tiền" diễn ra:

- **Master plan §8–9:** charge **đồng bộ** — gọi `charge`, nhận kết quả ngay (kiểu thẻ đã lưu).
- **Roadmap 2.3/2.4:** kết quả đến qua **webhook**; saga top-up có bước `AWAITING_PAYMENT`.
- **Code:** `WalletTopics` đã khai `txn.command.bank-charge.v1` và `bank.event.completed/failed.v1` theo
  kiểu lệnh/trả lời, chưa ai dùng, chưa có schema.

Thực tế của cổng nội địa (VNPay, MoMo): người dùng được **chuyển hướng** tới trang của provider, trả tiền
ở đó, rồi provider báo kết quả qua IPN/webhook — và cho phép hỏi lại trạng thái. Không có kết quả đồng bộ.

Thêm hai bài học từ Phase 1 phải áp vào đây:
- **ADR-014:** dedup theo `eventId` rồi bỏ qua im lặng làm saga treo, khi câu trả lời đầu tiên bị mất.
- **Roadmap §5.7:** nhập nhằng lỗi kỹ thuật với lỗi nghiệp vụ là cách mất tiền phổ biến nhất. Với
  provider, đây là chuyện thật: timeout **không** có nghĩa là chưa thu tiền.

## Decision

### 1. Mô hình thu: chuyển hướng + xác nhận bất đồng bộ

```
saga ──txn.command.bank-charge.v1──▶ bank-adapter ──charge(key)──▶ provider
     ◀─bank.event.charge-pending.v1── (providerRef, paymentUrl)        │ người dùng trả tiền
                                     ◀── webhook (2.3) = gợi ý ─────────┤
                                     ── getStatus(ref) ────────────────▶│
     ◀─bank.event.completed.v1 / bank.event.failed.v1── (kết quả CUỐI)
```

- `bank.event.charge-pending.v1` là **topic mới**. Nó mang URL thanh toán, và **không** phải kết quả.
- Kết quả cuối chỉ được phát sau khi chính adapter hỏi lại provider (`getStatus`).
  - Webhook (2.3) chỉ kích hoạt việc hỏi lại, không tự ghi kết quả.
  - Job đối chiếu (`PendingChargeReconciler`) hỏi lại định kỳ. Đây là lưới an toàn khi webhook không bao
    giờ tới.

### 2. Idempotency theo khoá của chính adapter, không theo eventId

- **Khoá nghiệp vụ:**
  - Mỗi `(tenant, transactionId, operation)` có đúng một hàng `provider_transactions`. Hàng được giành
    bằng `INSERT … ON CONFLICT DO NOTHING`.
  - Cùng `transactionId` mà khác ví/số tiền/loại tiền là bug producer → DLT.
- **Khoá gửi cho provider:**
  - Tất định theo `(tenant, transactionId, operation)` và không bao giờ sinh lại.
  - Có tenant trong khoá vì ở provider chỉ có một không gian khoá.
- **Lệnh gửi lại sau khi đã có câu trả lời:** phát **lại** câu trả lời đó. Không bỏ qua theo eventId như
  ledger, vì saga gửi lại chính là vì nó chưa nghe được câu trả lời.
- **eventId:** tất định theo `(hàng, trạng thái)`, nên phát lại thì consumer vẫn dedup được.

### 3. Gọi provider NGOÀI transaction DB, trạng thái `NEW` nghĩa là "không biết"

Thứ tự: giành hàng (commit) → gọi provider → ghi câu trả lời + outbox (commit).

- Crash hoặc timeout ở giữa để lại hàng `NEW`.
- Lần thử sau gửi lại **đúng khoá cũ**, nên provider trả lại phiên đã tạo. Không có phiên thứ hai.
- Đây là tính chất mà `ChargeIdempotencyIT` chứng minh (ca "mất câu trả lời").

### 4. Phân loại lỗi

| Loại | Ví dụ | Đường đi |
|---|---|---|
| Provider trả lời dứt khoát | từ chối, phiên hết hạn | giá trị trả về → `bank.event.failed.v1` |
| Lệnh hỏng | số tiền ≤ 0, gửi lại khác số tiền | `PoisonMessageException` → DLT ngay |
| Kỹ thuật | timeout, 5xx, mạch mở, bulkhead đầy | `ProviderUnavailableException` → Kafka thử lại → DLT. **Không bao giờ** thành `failed` |

### 5. Resilience4j dựng bằng code

`ResilientPaymentProvider` bọc mọi lời gọi theo thứ tự bulkhead (thread pool riêng) → timeout → circuit
breaker.

- Dựng bằng code, không qua annotation/AOP, để thứ tự bọc nằm tường minh ở một chỗ.
- Câu trả lời dứt khoát (từ chối) không tính là lỗi cho breaker: người dùng nhập sai số tiền không được
  làm mở mạch cho người khác.

### 6. Chọn provider: không có mặc định

- `walletflow.bank.provider: ${BANK_PROVIDER}`.
- Thiếu biến hoặc giá trị lạ → service **không khởi động**.
- `mock` cần **hai** xác nhận: `provider=mock` và `walletflow.bank.mock.allowed=true`
  (`BANK_MOCK_ALLOWED`, mặc định `false`).
  - Chỉ stack dev/E2E và test bật cờ thứ hai.
  - Kể cả bật cờ, profile `prod` vẫn bị từ chối.
  - Lý do: repo không có môi trường nào đặt profile `prod`, nên chỉ chặn theo profile là không bảo vệ
    được gì.
- `MockProvider` không thu tiền của ai, nhưng saga vẫn sẽ cộng tiền thật vào ví. Lọt lên môi trường thật
  là tạo tiền từ không khí.

### 6b. Tin câu trả lời "thành công" có điều kiện

- **Chỉ hỏi provider đã tạo phiên.** Job đối chiếu lọc `provider = <provider hiện tại>`, và `refresh` từ
  chối hàng của provider khác.
  - Lý do: sau khi đổi `BANK_PROVIDER`, provider mới trả "không tìm thấy" có thể bị hiểu thành thất bại
    cho một lần thu người dùng đã trả.
- **Đối chiếu số tiền.**
  - `getStatus` trả số tiền và loại tiền provider **thực thu**.
  - Lệch với số yêu cầu thì không báo thành công: giữ `PENDING`, tăng metric
    `walletflow.bank.amount_mismatch`, alert `ProviderAmountMismatch`.
  - Lý do: event thành công mang số mà ledger sẽ cộng.

### 7. Provider thật: VNPay sandbox (2.2b)

Chốt ngày 2026-10-18:
- VND, luồng redirect + IPN ký HMAC-SHA512, sandbox miễn phí, không cần SDK.
- Làm ở PR riêng (2.2b), kèm khoá sandbox qua `.env` và masking log.

## Hệ quả

**Cho 2.4 (saga top-up) — consumer của các event này phải:**
- Lưu `paymentUrl` từ `charge-pending` để client lấy qua `GET /transactions/{id}`. Client chuyển người
  dùng tới đó.
- Dedup theo eventId: event được giao at-least-once và phát lại khi lệnh được gửi lại.
- Gửi lại lệnh thu khi treo bằng **cùng `transactionId`** và eventId mới. An toàn: adapter trả lại câu trả
  lời đã có, hoặc gửi lại đúng khoá cũ cho provider.
- Ghi sổ nạp tiền chỉ sau `bank.event.completed.v1`. Tiền vào ví khi ledger ghi xong (roadmap 2.4).
- Chịu được `charge-pending` tới **sau** `completed`/`failed`. Đây là ba topic khác nhau, nên không có thứ
  tự giữa chúng.
  - Event pending muộn không được kéo giao dịch đã kết thúc lùi lại.

> **Đã chốt ở [ADR-017](ADR-017-vnpay-session-expiry-log-masking.md) (2.2b):** hạn phiên 15′ + grace 15′,
> `FAILED(EXPIRED)` chỉ khi provider khẳng định chưa thu, backoff cấp số cho mọi hàng `PENDING` (kể cả
> hàng lệch số tiền), gauge `walletflow.bank.pending_overdue`. Phần dưới giữ nguyên làm bối cảnh.

**Còn mở, phải chốt trước/trong 2.2b — lần thu `PENDING` chưa có trạng thái kết thúc:**
- Hiện một phiên bị bỏ dở được hỏi lại mỗi `recheck-after`, mãi mãi.
- VNPay không có phiên phía server cho tới khi người dùng mở URL, nên "không tìm thấy" là câu trả lời bình
  thường cho phiên bị bỏ.
- Cần chốt (quy tắc nghiệp vụ theo hạn phiên của provider, không đoán ở 2.2a):
  - khi nào hạn phiên của provider biến câu trả lời thành `FAILED(EXPIRED)` dứt khoát — chỉ dựa trên câu
    trả lời của provider;
  - lưu `expires_at`;
  - giãn `next_check_at` theo cấp số;
  - metric cho hàng `PENDING` quá hạn.
- Hàng lệch số tiền (`ProviderAmountMismatch`) cũng đứng `PENDING` và bị hỏi lại mỗi `recheck-after`, kèm
  một dòng log lỗi mỗi lần.
  - Chấp nhận được khi chưa có luồng nào gửi lệnh thu thật.
  - Trước khi bật saga top-up (2.4) cần đánh dấu hàng chờ người xử lý (2.5) để ngừng hỏi lại, hoặc giãn
    cấp số như trên.

**Những chỗ code hiện tại sẽ phải đổi ở 2.4** (phát hiện khi lập bản đồ code cho 2.2a):
- `StuckSagaScanner` gửi lại cứng `TRANSFER`.
- `transactions.from_wallet` NOT NULL.
- `txn.event.completed/failed.v1` bắt buộc `fromWalletId`, nên cần `.v2`.
- Ledger đặt partition key của event từ chối theo vế ghi nợ đầu tiên. Với TOPUP, vế đó là `EXTERNAL_BANK`.

**Vận hành:**
- Alert `PaymentProviderDown` (mạch mở), `BankStatusCheckFailing` và `ProviderAmountMismatch`.
- Job `@Scheduled` chạy trên `taskScheduler` 4 thread của Spring Boot (`spring.task.scheduling.pool.size`),
  để job đối chiếu đang chờ provider không giữ thread của outbox relay.
  - Executor hẹn giờ timeout của Resilience4j cố ý **không** là bean. Một bean `ScheduledExecutorService`
    làm Boot bỏ qua `taskScheduler`, khiến `@Scheduled` rơi vào đúng thread hẹn giờ đó, và timeout của chính
    lời gọi đang chờ không bao giờ nổ.
  - Review độc lập bắt được lỗi này; `ProviderResilienceIT` chứng minh.
- Runbook `payment-provider-down.md`.
- Hàng `NEW` lâu = lệnh đang ở DLT, replay sau khi provider sống lại.

**Giới hạn đã chấp nhận:**
- `MockProvider` giữ phiên trong bộ nhớ. Khởi động lại thì lần thu đang chờ sẽ bị hỏi lại mãi (cảnh báo
  `BankStatusCheckFailing`). Chỉ là máy dev.
- Chưa có `REFUND` qua Kafka: interface và Mock đã có, lệnh/hàng `REFUND` thêm ở 2.4 (mở rộng CHECK
  `operation` bằng migration). → *Đã có ở 2.4b: `txn.command.bank-refund.v1`, bank-adapter V6
  ([ADR-020](ADR-020-topup-compensation-refund.md)).*

## Alternatives rejected

- **Charge đồng bộ (master plan):** không khớp với cổng nội địa, vốn không có kết quả đồng bộ. Ép vào
  thì phải chờ người dùng trả tiền trong một lời gọi.
- **Webhook là sự thật:** webhook trùng, muộn, sai thứ tự và có thể giả (roadmap 2.3). Ghi kết quả theo
  nó là để một request HTTP từ bên ngoài quyết định cộng tiền.
- **Dedup lệnh theo eventId như ledger:** lặp lại lỗi saga treo của ADR-014.
- **Giữ transaction DB trong lúc gọi provider:** provider chậm thì cạn connection pool. Cạn pool đã
  xảy ra ở load test Phase 1 (transaction-service, `benchmarks/phase1.md`), dù khi đó không có lời gọi
  ra ngoài nào.
- **Có provider mặc định = mock cho tiện dev:** đúng loại "default theo lên production" mà bất biến §5.6
  cấm.

## Liên quan

- Roadmap Phase 2 (2.2, 2.3, 2.4), §5.3, §5.6, §5.7.
- ADR-003 (outbox), ADR-008 (at-least-once), ADR-014 (saga), ADR-015 (định vị).
- Runbook `payment-provider-down.md`, `dlt-not-empty.md`.
