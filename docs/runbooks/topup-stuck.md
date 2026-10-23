# Runbook — `TopupManualReview` / `TopupResultConflict` / `TopupRefundFailed`

> **`TopupManualReview` — mức page.** Một lệnh nạp tiền đã **thu được tiền của người dùng** nhưng
> không ghi vào ví được. Giao dịch đứng ở `MANUAL_REVIEW` (ADR-019 §4, §5).
>
> **`TopupResultConflict` — mức cảnh báo.** bank-adapter nói hai điều trái nhau về cùng một lần thu,
> hoặc lời xác nhận lệch với lệnh ta gửi. Có thể kèm theo `TopupManualReview`, có thể không.
>
> **`TopupRefundFailed` — mức page.** Đã thử hoàn tiền tự động và KHÔNG hoàn được (2.4b, ADR-020).
> Tiền vẫn ở chỗ provider — mục E.

## Nguyên tắc vàng

**Tiền đã ra khỏi tài khoản của người dùng.** Khác hẳn `SagaManualReview` của chuyển tiền (ở đó tiền
*có thể* chưa đi): ở đây lệnh ghi sổ chỉ được gửi SAU khi provider xác nhận, nên tới được trạng thái
này nghĩa là ngân hàng **đã** trừ họ. Mỗi phút ở đây là một phút khách hàng mất tiền mà không có gì.

**Hai nguồn phải khớp trước khi quyết:** sổ cái của ta (`ledger`) và cổng thanh toán (`provider`).
Không quyết dựa trên một nguồn.

**Hoàn tiền tự động phụ thuộc một cờ** (2.4b, [ADR-020](../adr/ADR-020-topup-compensation-refund.md)).
`TOPUP_REFUND_ENABLED` ở transaction-service:

- **Tắt** (mặc định) → mọi ca kết thúc bằng thao tác tay như trước: hoặc ghi sổ được (chỉ khi ledger chưa
  dựng hàng rào — mục A), hoặc hoàn tiền qua portal của provider.
- **Bật** → hai ca *biết chắc đã thu tiền mà ví chưa được cộng* (ledger từ chối, `BANK_RESULT_CONFLICT`)
  tự chuyển sang `REFUNDING` và gửi lệnh hoàn; hoàn xong thì về `FAILED`. Những ca "không biết chắc"
  (`BANK_AMOUNT_MISMATCH`, `BANK_NO_RESPONSE`, `SAGA_STATE_DESYNC`) vẫn dừng ở `MANUAL_REVIEW` —
  không biết đã thu bao nhiêu thì không biết phải hoàn bao nhiêu.

**Chỉ bật cờ khi MỌI instance ledger đã lên V6** (hàng rào `rejected_topups`) và mọi instance
transaction-service đã lên V4. Bật sớm hơn thì một bản gửi lại của lệnh ghi sổ vẫn có thể được ghi SAU khi
đã hoàn tiền: ví được cộng **và** tiền đã về thẻ.

Với **VNPay**, hoàn tự động chưa có cả khi cờ bật (2.4c): bank-adapter trả `REFUND_UNSUPPORTED`, saga
chuyển `MANUAL_REVIEW` với `failure_code = 'REFUND_DECLINED'` và ca đó phải hoàn tay qua portal. Hàng hoàn
`FAILED` ở bank-adapter là **vĩnh viễn** — gửi lại lệnh hoàn sau khi 2.4c lên chỉ phát lại lời từ chối cũ.

## Chẩn đoán — 3 phút

> Từ 2.5 PR1 có API thay cho bước 1 bên dưới, và nó **để lại dấu vết** (`audit_events`) trong khi
> `psql` thì không: `GET /api/v1/admin/transactions/manual-review` (vai `ADMIN`/`OPS`/`AUDITOR`), và
> `GET /api/v1/admin/transactions/{id}` cho một ca kèm trạng thái saga. Bước 2 và 3 vẫn phải dùng
> `psql` cho tới 2.5 PR3.

```bash
# 1. Các lệnh nạp đang chờ người xử lý — kèm mã tham chiếu phía provider
docker exec wf-postgres psql -U walletflow -d txndb -c "
  SELECT t.id, t.tenant_id, t.to_wallet, t.amount_minor, t.currency, t.failure_code,
         t.provider, t.provider_ref, t.created_at, s.current_step, s.attempt_count
  FROM transactions t LEFT JOIN saga_state s ON s.transaction_id = t.id
  WHERE t.type = 'TOPUP' AND t.status IN ('MANUAL_REVIEW', 'REFUNDING') ORDER BY t.created_at;"
# REFUNDING = máy đang tự xử lý (lệnh hoàn đã gửi, chờ bank-adapter). Chỉ đáng lo nếu nó đứng đó quá
# lâu — job quét gửi lại lệnh hoàn mỗi lượt, hết lượt thì tự chuyển REFUND_NO_RESPONSE (mục E).

# 2. Lần thu đó ở bank-adapter nói gì
docker exec wf-postgres psql -U walletflow -d railsdb -c "
  SELECT id, status, operation, provider_ref, amount_minor, failure_code, updated_at
  FROM provider_transactions WHERE transaction_id = '<id>';"

# 3. Ledger đã ghi bút toán cho giao dịch này chưa — câu hỏi quyết định
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT id, tenant_id, type, created_at FROM journal_entries WHERE transaction_id = '<id>';"
```

`failure_code` nói ca nào:

| `failure_code` | Nghĩa | Xem mục |
|---|---|---|
| mã của ledger (`ACCOUNT_CLOSED`, `ACCOUNT_NOT_FOUND`...) | Thu được tiền, ledger từ chối ghi sổ | A |
| `BANK_AMOUNT_MISMATCH` | Xác nhận của provider lệch với lệnh đã gửi | B |
| `BANK_RESULT_CONFLICT` | Provider xác nhận đã thu SAU khi lệnh đã đóng là thất bại | B |
| `BANK_NO_RESPONSE` | bank-adapter không trả lời sau đủ số lần gửi | C |
| `REFUND_DECLINED` | Đã thử hoàn tự động, provider **từ chối dứt khoát** | E |
| `REFUND_NO_RESPONSE` | Lệnh hoàn gửi đủ số lần mà không có hồi âm | E |
| `REFUND_POSTED_CONFLICT` | Ledger ghi sổ TRONG KHI đang hoàn tiền | E |
| `REFUND_AMOUNT_MISMATCH` | Xác nhận hoàn lệch số tiền/ví so với lần nạp | E |

## A — Ledger từ chối ghi sổ (tiền đã thu)

Nguyên nhân gần như luôn là trạng thái tài khoản: ví bị đóng giữa lúc người dùng đang trả tiền.

> Mục này là đường đi khi **cờ hoàn tiền đang TẮT**. Cờ bật thì ca này không dừng ở `MANUAL_REVIEW`
> nữa: saga tự chuyển `REFUNDING` và gửi lệnh hoàn. Thấy một mã của ledger ở `MANUAL_REVIEW` trong khi
> cờ đang bật nghĩa là lời từ chối tới **trước** lúc bật cờ — xử lý theo mục này.

1. Xác nhận tiền đã thu thật: bước 2 ở trên phải cho `status = 'SUCCEEDED'`, và đối chiếu với cổng
   thanh toán (portal của provider) theo `provider_ref`. **Không tin một nguồn.**
2. **Bước 3 chẩn đoán trước tiên**: có bút toán `TOPUP` cho giao dịch này không?
   - **Có** → tiền ĐÃ vào ví. **Không hoàn tiền**, kể cả khi bước dưới thấy hàng rào: trong lúc rolling deploy
     lên ledger V6, một instance cũ (không khoá, không kiểm hàng rào) vẫn có thể ghi sổ bản gửi lại. Đây là
     ca lệch, không phải ca mất tiền — xử lý như mục B (mở issue), rồi đóng giao dịch theo mục D ở bước
     `POSTING` để saga nhận lại `LedgerPosted`.
   - **Không** → sang bước tiếp.
3. Kiểm ledger đã dựng **hàng rào** cho giao dịch này chưa (ADR-020, từ ledger V6):
   ```bash
   docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
     SELECT tenant_id, reason_code, reason, rejected_at FROM rejected_topups WHERE transaction_id = '<id>';"
   ```
   - **Có hàng** (và bước 2 không có bút toán) → giao dịch này **không bao giờ ghi sổ được nữa**, kể cả sau
     khi mở lại ví: ledger từ chối lại với đúng mã cũ. Đây là chủ ý — lời từ chối đó cho phép hoàn tiền, và
     ghi sổ sau khi đã hoàn là cho người dùng nhận tiền hai lần. Hàng không xoá được (trigger append-only),
     **đừng tìm cách gỡ**. Chỉ còn một đường: **hoàn tiền tay** (bên dưới); người dùng nạp lại bằng giao dịch
     mới.
   - **Không có hàng** (lời từ chối trước khi lên V6) → hỏi nghiệp vụ như cũ ở bước 4.
4. Hỏi nghiệp vụ: ví có mở lại được không? (chỉ khi KHÔNG có hàng rào)
   - **Mở lại được** → mở ví, rồi mở lại saga theo mục D. Lệnh ghi sổ được gửi lại với cùng
     `transactionId`, ledger ghi đúng MỘT bút toán.
   - **Không mở lại được** (đóng vĩnh viễn, KYC) → **hoàn tiền tay** qua portal của provider theo
     `provider_ref`, rồi đánh dấu đã xử lý:
     ```sql
     UPDATE transactions SET failure_code = 'REFUNDED_MANUALLY',
            failure_reason = 'Hoàn tay qua portal <provider>, ref <refund_ref>, bởi <ai>, <khi nào>',
            updated_at = now()
      WHERE id = '<id>' AND status = 'MANUAL_REVIEW';
     ```
     Giữ nguyên `status = 'MANUAL_REVIEW'`: chưa có `audit_events` (2.5), nên trạng thái này là dấu vết
     duy nhất cho thấy đã có người đụng vào. Đừng đổi sang `FAILED`.

## B — Hai bên nói lệch nhau

`BANK_AMOUNT_MISMATCH` / `BANK_RESULT_CONFLICT` hoặc alert `TopupResultConflict` không kèm manual review.

**Đây là bug, không phải sự cố vận hành.** Số tiền ta gửi đi và số tiền provider xác nhận không được
phép lệch; một lần thu không được phép vừa thất bại vừa thành công.

1. Lấy nguyên văn hai phía:
   ```bash
   docker exec wf-postgres psql -U walletflow -d railsdb -c "
     SELECT raw_request, raw_response FROM provider_transactions WHERE transaction_id = '<id>';"
   docker exec wf-postgres psql -U walletflow -d railsdb -c "
     SELECT provider, outcome, received_at FROM webhook_events
     WHERE provider_ref = '<ref>' ORDER BY received_at;"
   ```
2. Mở issue kèm hai payload đó **sau khi đã che dữ liệu thanh toán**.
3. Xử lý tiền của ca này theo mục A (mở lại được thì ghi sổ, không thì hoàn tay). Không sửa code vội —
   tiền của khách đi trước.

## C — bank-adapter không trả lời

Khác A/B: ở đây **chưa chắc** đã thu được tiền — ta mất liên lạc trước khi có kết quả.

1. Kiểm tra bank-adapter còn sống không (`service-down.md`), DLT có message không (`dlt-not-empty.md`),
   outbox có tắc không (`outbox-stalled.md`).
2. Hỏi provider trực tiếp theo `provider_ref` (bước 2 chẩn đoán + portal).
   - Provider nói **chưa thu** → mở lại saga theo mục D; lệnh thu được gửi lại, bank-adapter phát lại
     câu trả lời hiện tại.
   - Provider nói **đã thu** → xử lý như mục A.

## D — Mở lại một ca đã chuyển người xử lý

Phải đặt lại **cả hai** hàng, trong một transaction — đặt mỗi `transactions` thì job quét sẽ không
nhặt, đặt mỗi `saga_state` thì nó nhặt rồi đóng ngay.

```sql
BEGIN;
-- status đặt về đúng bước đang dở: AWAITING_PAYMENT nếu còn ở BANK_CHARGE, POSTING nếu đã qua
UPDATE transactions SET status = '<AWAITING_PAYMENT|POSTING>', failure_code = NULL,
       failure_reason = NULL, updated_at = now()
 WHERE id = '<id>' AND status = 'MANUAL_REVIEW';
UPDATE saga_state SET step_status = 'SENT', attempt_count = 0, deadline_at = now(), updated_at = now()
 WHERE transaction_id = '<id>' AND step_status = 'MANUAL_REVIEW';
COMMIT;
```

Lượt quét kế tiếp (≤15s) sẽ gửi lại lệnh của **đúng bước** `current_step` với `eventId` mới —
`CMD_BANK_CHARGE` nếu còn ở `BANK_CHARGE`, `CMD_POST_JOURNAL` nếu đã qua
(`StuckSagaIT.lenh_nap_qua_han_thi_gui_lai_lenh_thu` / `lenh_nap_qua_han_o_buoc_ghi_so`).

**Quên một trong hai câu UPDATE là ca nguy hiểm**: mở `transactions` mà để `saga_state` đóng thì job
quét không nhặt, và một `BankCompleted` giao lại sẽ rơi vào `SAGA_STATE_DESYNC` (quay lại
`MANUAL_REVIEW` — có test `TopupSagaIT.saga_lech_voi_giao_dich_thi_khong_bien_mat_im_lang`). Không
nguy hiểm về tiền, nhưng mất thêm một vòng.

> Quy trình mở lại cho **chuyển tiền** nằm ở [`saga-stuck.md`](saga-stuck.md) — câu lệnh khác
> (`status = 'PROCESSING'`), đừng dùng nhầm.

> **Lệnh nạp đang ở bước hoàn tiền** (`current_step = 'BANK_REFUND'`) thì đặt `status = 'REFUNDING'`,
> không phải `POSTING`. Đặt nhầm `POSTING` là bảo job quét gửi lại lệnh **ghi sổ** cho một giao dịch mà
> tiền đang trên đường về thẻ người dùng — vừa hoàn vừa cộng ví.

## E — Hoàn tiền tự động không thành công

Chỉ gặp khi cờ `TOPUP_REFUND_ENABLED` đang bật. Điểm chung của cả ba ca: **tiền vẫn ở chỗ provider**, và
máy đã hết đường tự gỡ.

Xem lần hoàn ở bank-adapter trước khi làm gì:

```bash
docker exec wf-postgres psql -U walletflow -d railsdb -c "
  SELECT id, operation, status, provider_ref, refund_of, amount_minor, failure_code, updated_at
  FROM provider_transactions WHERE transaction_id = '<id>' ORDER BY created_at;"
```

**`REFUND_DECLINED` — provider từ chối dứt khoát.** Trước 2.4c, VNPay luôn trả `REFUND_UNSUPPORTED`.
`failure_reason` của giao dịch giữ cả lời từ chối lẫn nguyên nhân phải hoàn. Hàng `REFUND` `FAILED` ở
bank-adapter là vĩnh viễn — **đừng gửi lại lệnh hoàn**, nó chỉ phát lại đúng lời từ chối đó. Hoàn tay qua
portal theo `provider_ref` của lần **thu**, rồi đánh dấu như mục A bước 4 (`REFUNDED_MANUALLY`, giữ
nguyên `status = 'MANUAL_REVIEW'`).

**`REFUND_NO_RESPONSE` — gửi đủ số lần, không có hồi âm.** Khác `BANK_NO_RESPONSE`: ở đó chưa chắc có
đồng nào đi, ở đây tiền chắc chắn **đã** bị trừ của người dùng.

1. bank-adapter còn sống không (`service-down.md`), DLT có message không (`dlt-not-empty.md`), outbox có
   tắc không (`outbox-stalled.md`). Lệnh hoàn thường mất trên đường vì một trong ba.
2. Không có hàng `REFUND` nào ở bank-adapter → lệnh chưa bao giờ tới. Sửa đường đi, rồi mở lại saga theo
   mục D với `status = 'REFUNDING'`; lượt quét kế tiếp gửi lại lệnh hoàn với cùng khoá nên provider chỉ
   hoàn một lần.
3. Có hàng `REFUND` ở `SUCCEEDED` → tiền đã về thật, chỉ mất câu trả lời. Mở lại saga theo mục D như trên;
   lệnh gửi lại là cách lấy lại câu trả lời đã mất, bank-adapter phát lại kết quả hiện có.
4. Có hàng `REFUND` ở `NEW`/`PENDING` → provider đang xử lý hoặc adapter đang thử lại. Chờ, đừng hoàn tay
   song song — hoàn hai lần là mất tiền của ta.

**`REFUND_POSTED_CONFLICT` — ledger ghi sổ trong khi đang hoàn.** Hàng rào `rejected_topups` sinh ra để ca
này không tồn tại; thấy nó nghĩa là có một instance ledger chưa lên V6, hoặc bút toán được ghi tay.
**Đây là ca mất tiền của công ty, không phải của khách** — ví có thể vừa được cộng vừa được hoàn.

1. Bút toán `TOPUP` có thật không (bước 3 chẩn đoán) và lần hoàn đã `SUCCEEDED` chưa (câu lệnh ở trên).
2. **Cả hai đều có** → ví đang giữ số tiền người dùng đã được trả lại. Dừng cờ hoàn tiền
   (`TOPUP_REFUND_ENABLED=false`) trên mọi instance **trước**, vì nguyên nhân gốc là một instance ledger
   thiếu hàng rào. Rồi mở issue mức nghiêm trọng; việc gỡ số dư là thao tác đụng tiền, phải chờ 2.5
   (`REVERSAL` có audit) hoặc một quyết định có hai người ký.
3. Chỉ có bút toán, lần hoàn chưa đi → huỷ ý định hoàn: giao dịch này thật sự đã thành công. Đóng theo mục
   D ở bước `POSTING` để saga nhận lại `LedgerPosted`.
4. Kiểm phiên bản Flyway của **mọi** instance ledger (`SELECT MAX(version) FROM flyway_schema_history`)
   trước khi bật lại cờ.

**`REFUND_AMOUNT_MISMATCH` — xác nhận hoàn nói một con số khác lần nạp.** bank-adapter đã kiểm lệnh hoàn
khớp với lần thu rồi, nên tới được đây nghĩa là bug ở một trong hai phía. Giao dịch KHÔNG được đóng: hoàn
thiếu thì người dùng mất phần chênh, hoàn thừa thì ta mất. Lấy `raw_response` của hàng `REFUND` và đối
chiếu với portal trước khi quyết; mở issue như mục B.

**Giao dịch `REFUNDING` mà `saga_state.current_step` không phải `BANK_REFUND`** (hoặc ngược lại): job quét
không gửi lệnh nào và chuyển `SAGA_STATE_DESYNC`. Gần như luôn là hệ quả của việc mở lại ca theo mục D mà
đặt nhầm bước. Đặt lại cho hai hàng **khớp nhau** rồi mở lại; đừng chỉ sửa một hàng.

## Sau sự cố

- Có bao nhiêu lệnh nạp còn kẹt cùng lý do? (`SELECT failure_code, count(*) ... GROUP BY 1`)
- Nếu là ca A do ví bị đóng giữa chừng: có nên chặn đóng ví khi còn lệnh nạp đang chờ không?
- Ca nào ở đây **đã** tự động hoá được (mục A với cờ bật) và ca nào chưa? Ghi lại số ca và lý do: đó là
  đầu vào để biết 2.4c (hoàn thật qua VNPay) và 2.5 (admin API) phải phủ những gì.
- Nếu ca phải hoàn tay là `REFUND_DECLINED` của VNPay: đếm chúng. Mỗi ca là một lần hoàn thật không để
  lại dấu vết nào trong `provider_transactions` (ADR-020 *Consequences*) — 2.5 phải có đường ghi nhận.
