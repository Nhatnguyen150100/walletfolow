# ADR-019 — Saga nạp tiền: thu được tiền trước, ghi sổ sau

**Trạng thái:** Accepted · **Ngày:** 2026-10-20 · **Milestone:** Phase 2 — 2.4a

## Context

ADR-016 chốt mô hình thu tiền ở bank-adapter và ADR-018 chốt đường webhook. Tới 2.3b, bank-adapter đã
thu được tiền thật và phát `bank.event.completed.v1` / `bank.event.failed.v1` — nhưng **không ai nghe**.
Tiền vào được tài khoản merchant mà không vào được ví ai. 2.4a nối mắt xích cuối: transaction-service
nhận lệnh nạp, điều phối saga, và ra lệnh cho ledger ghi bút toán.

Khác biệt cốt lõi với saga chuyển tiền (ADR-014): chuyển tiền chỉ có **một** bước (ghi sổ), và ledger là
nơi duy nhất quyết định. Nạp tiền có **hai** bước, bước đầu nằm ở một hệ thống ta không kiểm soát, mất
hàng chục phút, và có thể kết thúc bằng "người dùng đổi ý".

## Decision

### 1. Thứ tự là bất biến: thu được tiền TRƯỚC, ghi sổ SAU

```
PROCESSING ──charge-pending──▶ AWAITING_PAYMENT ──bank-completed──▶ POSTING ──posted──▶ COMPLETED
```

Lệnh ghi sổ (`txn.command.post-journal.v1`, type `TOPUP`) **chỉ** được xếp vào outbox khi nhận
`bank.event.completed.v1` — tức là sau khi bank-adapter đã tự `getStatus` và provider xác nhận
(ADR-016 §1). Lời báo tạo phiên thanh toán và webhook đều **không** đủ.

Đảo thứ tự này là cộng vào ví số tiền chưa ai trả. Test `TopupSagaIT.chua_thu_duoc_tien_thi_khong_ghi_so`
giữ bất biến đó.

Bút toán: `EXTERNAL_BANK −N | ví +N`. Dùng tài khoản hệ thống per-tenant có sẵn thay vì một đường ghi
riêng, nên `SUM = 0` vẫn đúng và "hệ thống đang giữ bao nhiêu tiền của ngân hàng" đọc được từ chính sổ cái.

### 2. Hai trạng thái chờ, không gộp vào PROCESSING

`AWAITING_PAYMENT` (chờ **người** trả tiền) và `POSTING` (đã thu được tiền, chờ **ledger**) là hai thứ
khác nhau về bản chất vận hành:

- `AWAITING_PAYMENT` kéo dài hàng chục phút là **bình thường**; job quét saga treo không được coi nó là
  treo. Kết thúc bằng "không trả tiền" cũng là một kết cục hợp lệ.
- `POSTING` là cửa sổ **nguy hiểm nhất** của hệ thống: tiền đã ở chỗ ta mà chưa vào ví ai. Mọi phút ở
  đây đều phải đếm được.

Gộp cả hai vào `PROCESSING` thì không báo cáo nào phân biệt được "chưa thu được đồng nào" với "đã cầm
tiền của khách mà chưa ghi sổ".

### 3. Máy trạng thái chỉ TIẾN một chiều

Ba producer (bank-adapter, ledger, chính nó) trên bốn topic, không có thứ tự chung. Quy tắc: một event
gặp trạng thái đã đi xa hơn thì **không làm gì**, không bao giờ kéo giao dịch lùi.

Đó là lý do `markAwaitingPayment` có điều kiện `status = 'PROCESSING'` và `advanceStep` có điều kiện
`current_step = 'BANK_CHARGE'`. Nhờ vậy không cần bảng dedup cho ba topic của bank-adapter: khoá hàng
giao dịch + máy trạng thái một chiều đã khiến mọi bản lặp trở thành no-op (giống `LedgerResultListener`).

### 4. Ledger từ chối lệnh nạp ⇒ MANUAL_REVIEW, KHÔNG phải FAILED

Với chuyển tiền, `LedgerRejected` là kết cục sạch (thiếu tiền) → `FAILED`. Với nạp tiền thì **không**:
ta chỉ gửi lệnh ghi sổ sau khi đã thu được tiền, nên từ chối ở đây nghĩa là ngân hàng đã trừ người dùng
mà ví không được cộng. `FAILED` nói với họ "bạn không mất tiền" — một lời nói dối.

Nên: `MANUAL_REVIEW` + counter `walletflow.topup.manual_review` + log mang `provider_ref` để người xử lý
đối chiếu.

> **Sửa đổi (2.4b, [ADR-020](ADR-020-topup-compensation-refund.md)):** ca này không còn dừng ở
> `MANUAL_REVIEW` khi cờ `TOPUP_REFUND_ENABLED` bật — saga chuyển `REFUNDING`, gửi lệnh hoàn, và chỉ về
> `FAILED` sau khi tiền đã về người dùng. Lúc đó `FAILED` nói đúng sự thật. Cờ tắt ⇒ y như trên.

### 5. Xác nhận lệch với lệnh đã gửi thì KHÔNG ghi sổ

`BankCompleted` mang `amountMinor`/`currency`/`walletId`. Lệch với giao dịch ⇒ `MANUAL_REVIEW`, không ghi
sổ. Ghi theo con số của ta thì lệch sổ với ngân hàng; ghi theo con số của họ thì ví được cộng số tiền
người dùng không yêu cầu. Cả hai đều sai, nên không chọn cái nào.

### 6. Job quét gửi lại lệnh của ĐÚNG bước saga đang đứng

`saga_state.current_step` quyết định gửi lại `CMD_BANK_CHARGE` hay `CMD_POST_JOURNAL`. Gửi nhầm là hỏng
nặng theo cả hai chiều. Cả hai lệnh đều an toàn khi gửi lại — ledger idempotent theo `transactionId`,
bank-adapter idempotent theo `(tenant, transactionId)` và **phát lại câu trả lời hiện tại** (ADR-016),
nên gửi lại còn là cách duy nhất lấy lại một kết quả đã mất trên đường.

Hạn chót theo bước: `topup.charge-deadline` mặc định 5′ (chờ máy tạo phiên), nới lên
`topup.payment-deadline` 45′ ngay khi vào `AWAITING_PAYMENT` (phủ hạn phiên 15′ + grace 15′ của ADR-017).

**Đánh đổi đã biết:** sau lần gửi lại đầu tiên, nhịp giãn của `StuckSagaScanner` quay về
`saga-deadline` (2′ × số lần thử) chứ không theo `payment-deadline` — `recordRetry` không biết bước.
Trong luồng bình thường không chạm tới (bank-adapter đã phát `BankFailed` ở ~30′), nhưng nếu nới
`payment-deadline` lên nhiều thì phải cho `recordRetry` nhận hạn chót theo bước.

### 6b. Thứ tự triển khai: V3 phải đi trước, `POST /topup` đi sau

Migration V3 tương thích ngược ở mức **schema** nhưng không ở mức **dữ liệu** khi hai phiên bản app
chạy song song (rolling deploy — và `StuckSagaScanner` vốn được thiết kế cho nhiều instance). Ngay khi
một instance mới ghi hàng `status = 'AWAITING_PAYMENT'`, instance cũ không map được giá trị đó qua
`@Enumerated(EnumType.STRING)`: `GET /api/v1/transactions/{id}` trả 500, và job quét của nó nổ mỗi lượt.

Nên: **deploy V3 + bản app đọc được hai trạng thái mới cho TOÀN BỘ instance trước**, rồi mới cho lệnh
nạp đầu tiên đi qua. Expand/contract ở đây phải tính cả enum của app, không chỉ CHECK của DB.

### 7. `from_wallet` null với TOPUP; `fromWalletId` của event kết quả được nới thành nullable

Nạp tiền không có ví nguồn. Migration V3 nới `transactions.from_wallet` thành nullable và thay
`CHECK (from_wallet <> to_wallet)` bằng một CHECK theo `type`, nên DB vẫn chặn được hàng vô nghĩa.

Kéo theo: `txn.event.completed.v1` và `txn.event.failed.v1` đang bắt buộc `fromWalletId` là UUID
không-null. Hai lựa chọn: đẻ `.v2`, hoặc nới `.v1`.

**Chọn nới `.v1`** — `oneOf [uuid, null]`, **vẫn giữ trong `required`** (vắng mặt field vẫn là message
hỏng; chỉ giá trị được nới). Lý do: hai topic này hiện **không có consumer nào** — notification-service
thuộc 1.4, chưa tồn tại; chỉ transaction-service phát. Đẻ `.v2` cho một topic chưa ai đọc là nghi thức
suông, còn `.v1` nới thì mô tả đúng nghiệp vụ. Test
`EventContractTest.schema_vi_nguon_null_duoc_vang_mat_thi_khong` giữ phần "không mất răng".

**Hệ quả phải nhớ:** khi notification-service ra đời, nó PHẢI coi `fromWalletId` là nullable ngay từ đầu.

## Consequences

**Được:**
- DoD Phase 2 ô "nạp tiền qua sandbox → số dư tăng đúng" đi được hết đường.
- Mọi ca mất tiền đều dừng ở `MANUAL_REVIEW` với đủ ngữ cảnh (`provider`, `provider_ref`, `failure_code`),
  không ca nào lặng lẽ biến mất.
- Không bảng dedup mới, không topic mới, không schema mới.

**Mất / còn mở:**
- ~~**Chưa có hoàn tiền tự động.**~~ Đã trả ở 2.4b ([ADR-020](ADR-020-topup-compensation-refund.md)):
  ca §4 (ledger từ chối) và "hai kết quả trái nhau" được hoàn tự động qua `REFUNDING → FAILED` khi cờ
  `TOPUP_REFUND_ENABLED` bật. **Ca §5 (xác nhận lệch số tiền) vẫn chờ người** — lệch số tiền nghĩa là
  không biết đã thu bao nhiêu, nên không biết phải hoàn bao nhiêu.
- **`clientIp` gửi đi là null.** Sau gateway, IP mà transaction-service nhìn thấy là IP của gateway. Lấy
  đúng IP người dùng cần chốt số proxy tin cậy trước (mục còn mở của ADR-018). Tới lúc đó bank-adapter
  dùng IP máy chủ — đúng hợp đồng v1 nhưng làm yếu chống gian lận phía VNPay.
- **`COMPLETED` vẫn là trạng thái cuối** kể cả khi provider sau đó báo thất bại: ledger đã ghi bút toán,
  và sửa nó phải bằng một `REVERSAL`, không phải bằng cách viết lại trạng thái. Ca này chỉ tăng counter
  `walletflow.topup.result_conflict` + log; chưa có alert.
- **Mọi lần nạp khoá `EXTERNAL_BANK`** của tenant dưới `FOR UPDATE` nên bị tuần tự hoá (đánh đổi đã ghi
  ở `LedgerService`). Chưa đo dưới tải; nếu thành nút cổ chai thì tách tài khoản theo shard là một ADR riêng.

## Alternatives rejected

- **Ghi sổ ngay khi nhận webhook/lời báo tạo phiên:** nhanh hơn, và sai. Webhook là gợi ý (ADR-016 §1);
  một IPN thật bị chụp lại và phát lại vẫn là một IPN thật.
- **Cột `topup_status` riêng thay vì mở rộng `ck_transactions_status`:** hai nguồn sự thật về trạng thái
  một giao dịch, và mọi truy vấn vận hành phải biết nhìn cột nào theo loại.
- **Bảng `topup_requests` riêng:** nhân đôi idempotency + saga + job quét, và phá thể thống nhất của
  `GET /api/v1/transactions/{id}`.
- **Dùng `EventDeduplicator` cho ba topic của bank-adapter:** thừa. Khoá hàng + máy trạng thái một chiều
  đã cho tính idempotent, và một bảng dedup nữa chỉ thêm một thứ phải dọn.
