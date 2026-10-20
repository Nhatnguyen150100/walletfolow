# ADR-020 — Bù trừ saga nạp tiền: hàng rào ở ledger trước, hoàn tiền sau

**Trạng thái:** Accepted · **Ngày:** 2026-10-20 · **Milestone:** Phase 2 — 2.4b
(PR1: hàng rào ledger + lệnh hoàn ở bank-adapter · PR2: saga bù trừ ở transaction-service)

## Context

ADR-019 §4: ledger từ chối lệnh nạp ⇒ `MANUAL_REVIEW`, vì tiền đã bị thu của người dùng. 2.4b thay chỗ chờ
người đó bằng **hoàn tiền tự động** qua cổng thanh toán. Đã chốt (2026-10-20):

- Hoàn tự động cho hai ca **chắc chắn đã thu tiền mà ví chưa được cộng**: ledger từ chối ghi sổ, và
  `BANK_RESULT_CONFLICT` (provider xác nhận đã thu sau khi giao dịch đã đóng thất bại). `BANK_AMOUNT_MISMATCH`,
  `BANK_NO_RESPONSE`, `SAGA_STATE_DESYNC` vẫn chờ người — không biết chắc phải hoàn bao nhiêu, hay đã thu chưa.
- Kết cục sau khi hoàn xong: `REFUNDING → FAILED` (phát `txn.event.failed.v1`), không thêm trạng thái cuối mới.
- Hoàn thật qua VNPay (`vnp_Command=refund`) là 2.4c.

Khảo sát code trước khi làm lộ ra một lỗ hổng làm mọi thiết kế "nghe từ chối rồi hoàn" trở nên nguy hiểm:

**Ledger không nhớ lời từ chối theo giao dịch.** Nó chỉ dedup theo `eventId` (`LedgerCommandHandler`). Job quét
saga gửi lại lệnh ghi sổ với `eventId` MỚI (ADR-019 §6). Chuỗi sự kiện:

1. Lệnh ghi sổ lần 1 bị từ chối (ví vừa bị đóng, `EXTERNAL_BANK` bị đóng băng…).
2. Saga hoàn tiền về thẻ người dùng.
3. Tài khoản được mở lại; một bản gửi lại (đã nằm sẵn trên topic, hoặc do runbook "mở lại saga") tới nơi → ledger
   ghi sổ thành công.

Kết quả: ví được cộng **và** tiền đã về thẻ. Với chuyển tiền đây là hành vi đã chấp nhận ("ledger thắng",
ADR-014 — không có tiền nào rời hệ thống). Với nạp tiền có bù trừ thì đó là tạo ra tiền.

## Decision

### 1. Hàng rào: lệnh NẠP đã bị từ chối thì bị từ chối vĩnh viễn

Bảng `rejected_topups(tenant_id, transaction_id)` ở ledger (V6), append-only bằng cùng trigger với sổ cái.

- Đường từ chối (`LedgerCommandHandler.reject`) với TOPUP dựng hàng rào **trong cùng transaction** với event
  `ledger.event.rejected.v1` — không có lúc nào saga nghe được lời từ chối mà hàng rào chưa tồn tại.
- `postJournal` với TOPUP: có bút toán rồi thì trả bút toán (idempotent như cũ); chưa có mà có hàng rào thì từ
  chối lại với **mã + lời của lần đầu**. eventId của lời từ chối vốn tất định theo `(transactionId, type)`, nên
  từ chối lại luôn là cùng một event — consumer dedup được.
- Chỉ áp cho TOPUP. Chuyển tiền giữ đúng ADR-014.

Hàng rào là điều kiện **cần** để saga được phép hoàn: lời từ chối mà saga nghe được là lời cuối cùng.

### 2. Khoá theo giao dịch: hai đường gặp nhau ở một chỗ

Đường ghi sổ khoá tài khoản (`FOR UPDATE`), đường từ chối thì không khoá gì. Lúc rebalance, hai consumer có thể
cùng xử lý hai bản của một lệnh: một bản bị từ chối (ví đang đóng), bản kia tới sau khi trạng thái tài khoản vừa
đổi và ghi sổ thành công — cả hai cùng commit thì vừa có bút toán vừa có hàng rào, và saga sẽ hoàn tiền cho một
giao dịch đã vào ví.

Nên cả hai đường lấy `pg_advisory_xact_lock(hash("ledger-topup:" + tenant + ":" + transactionId))` trước khi
quyết định, và đường từ chối kiểm lại dưới khoá: **đã có bút toán nạp thì lời từ chối là cũ — không dựng hàng
rào, không phát gì**. Đụng hash chỉ làm hai giao dịch chờ nhau một nhịp.
`TopupRejectionFenceIT.ghi_so_va_tu_choi_dong_thoi_chi_mot_ket_qua_thang` (40 vòng đua) đỏ khi bỏ khoá.

### 3. Lệnh hoàn ở bank-adapter: cùng khuôn với lệnh thu

`txn.command.bank-refund.v1` (`BankRefundCommand`). Kết quả đi trên `bank.event.completed.v1` /
`bank.event.failed.v1` với `operation = REFUND` — schema đã cho phép từ 2.2a, nên không có topic kết quả mới.

- Hàng `REFUND` trong `provider_transactions` (V6: CHECK `operation` thêm `REFUND`, cột `refund_of` trỏ về lần
  thu, CHECK `(operation = 'REFUND') = (refund_of IS NOT NULL)`). Idempotent theo `(tenant, transactionId,
  REFUND)`, khoá gửi provider tất định, lệnh gửi lại thì phát lại câu trả lời (ADR-016).
- **Chỉ hoàn trọn.** Lệnh lệch ví/số tiền/loại tiền với lần thu, không có lần thu, hoặc lần thu thuộc
  provider/tài khoản merchant khác → bug producer → DLT. Hoàn theo con số của lệnh là trả người dùng số tiền họ
  chưa từng trả.
- **Lần thu chưa `SUCCEEDED` → cũng DLT, không ghi hàng nào.** Saga chỉ gửi lệnh hoàn sau khi đã nghe
  `bank.event.completed` của lần thu, nên ca này là bug. Không trả lời "từ chối": `PENDING` là *chưa biết*, không
  phải *chưa thu* (roadmap §5.7), và một hàng REFUND `FAILED` là vĩnh viễn — mọi lệnh gửi lại sau khi lần thu
  thành công chỉ phát lại lời từ chối đó (review độc lập của PR1).
- Lỗi kỹ thuật → hàng ở `NEW`, thử lại → DLT; lần sau gửi lại đúng khoá cũ nên provider không hoàn hai lần.
- Hai luồng đua nhau mà nhận hai câu trả lời trái nhau cho cùng một khoá (provider thật, 2.4c): câu ghi trước
  thắng, câu sau không lật hàng nhưng tăng counter `walletflow.bank.refund_conflict` + log ERROR — một lần hoàn
  thật có thể đang bị bỏ sót khỏi sổ của adapter.

### 4. VNPay tới 2.4c: từ chối dứt khoát, không ném lỗi

`VnPayProvider.refund` trả `Declined(REFUND_UNSUPPORTED)` mà không gọi mạng. Ném exception (như trước) thì lớp
resilience coi là lỗi kỹ thuật: thử lại vô ích → DLT, và tính là lỗi của circuit breaker — mạch mở chặn luôn
`querydr` của mọi lần thu đang chờ. Từ chối thì saga (PR2) chuyển `MANUAL_REVIEW` để người hoàn tay qua portal.

### 5. Thứ tự triển khai

1. **PR1** (hàng rào + lệnh hoàn) lên **mọi** instance ledger và bank-adapter. Tự nó không đổi hành vi nào của
   luồng đang chạy: chưa ai gửi lệnh hoàn; transaction-service đang bỏ qua event kết quả `operation ≠ CHARGE`.
2. **PR2** (transaction-service gửi lệnh hoàn) chỉ bật sau bước 1. Một instance ledger cũ còn sống không dựng
   hàng rào → lời từ chối của nó không an toàn để hoàn.

## Consequences

**Được:**
- Lời từ chối lệnh nạp trở thành sự thật cuối cùng — điều kiện để hoàn tiền tự động mà không tạo ra tiền.
- Đường hoàn tiền có đủ idempotency, phân loại lỗi và DLT như đường thu; kiểm bằng `RefundFlowIT`.

**Mất / còn mở:**
- **Runbook "mở lại ví rồi ghi sổ lại" chết với lời từ chối sau V6.** Giao dịch đã bị ledger từ chối không bao
  giờ ghi sổ được nữa, kể cả khi người vận hành muốn — phải hoàn tiền (tay, hoặc tự động từ PR2) và để người dùng
  nạp lại bằng giao dịch mới. Đây là chủ ý: hàng rào gỡ được thì không còn là hàng rào. Lời từ chối **trước** V6
  không có hàng rào, nên vẫn mở lại được theo runbook cũ ([`topup-stuck.md`](../runbooks/topup-stuck.md) mục A).
- `provider_ref` vẫn UNIQUE theo `(provider, provider_ref)` chung cho mọi thao tác. Provider nào trả mã của lần
  hoàn trùng mã của lần thu (2.4c phải kiểm với VNPay) thì phải tách khoá theo `operation` trước.
- **Hàng REFUND `FAILED` là vĩnh viễn**, gồm cả `REFUND_UNSUPPORTED` của VNPay trước 2.4c: lệnh hoàn gửi lại
  sau khi 2.4c lên vẫn chỉ phát lại lời từ chối cũ, nên những giao dịch đó KHÔNG bao giờ được hoàn tự động — phải
  hoàn tay qua portal. Lần hoàn tay đó không để lại dấu vết nào trong `provider_transactions`; 2.4c và 2.5 (admin
  API, `audit_events`) phải có đường ghi nhận nó.
- Chưa có hoàn một phần, chưa có lần hoàn `PENDING` (provider hoàn bất đồng bộ) — thêm khi có provider cần.
- PR2 nên đặt việc gửi lệnh hoàn sau một cờ cấu hình, để quy tắc "chỉ bật sau khi mọi instance ledger lên V6"
  (§5) được cưỡng chế chứ không chỉ được viết ra.
- `MockProvider` giữ lần hoàn trong bộ nhớ như phiên thu (ADR-016): khởi động lại thì lệnh hoàn của một lần thu cũ
  báo lỗi kỹ thuật → DLT. Chỉ là máy dev.

## Alternatives rejected

- **Hoàn ngay khi nghe lời từ chối, không có hàng rào:** đúng ở ca thường, tạo ra tiền ở ca gửi lại muộn — đúng
  loại lỗi chỉ xảy ra một lần mỗi vài tháng và không ai tái hiện được.
- **Một lệnh `VOID` riêng gửi ledger trước khi hoàn** (saga: từ chối → VOID → hoàn): đúng, nhưng thêm một topic,
  một schema, một bước saga và một cửa sổ chờ nữa, để làm đúng việc mà lời từ chối đã có thể làm nếu nó là lời
  cuối cùng.
- **Hàng rào ở transaction-service** (saga tự nhớ "đã hoàn" và bỏ qua `LedgerPosted` tới sau): quá muộn — khi
  `LedgerPosted` tới thì ví đã được cộng. Chỉ ledger chặn được việc ghi sổ.
- **Áp hàng rào cho mọi loại bút toán:** đổi ngữ nghĩa chuyển tiền (ADR-014) mà không có lý do về tiền.
