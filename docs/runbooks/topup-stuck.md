# Runbook — `TopupManualReview` / `TopupResultConflict`

> **`TopupManualReview` — mức page.** Một lệnh nạp tiền đã **thu được tiền của người dùng** nhưng
> không ghi vào ví được. Giao dịch đứng ở `MANUAL_REVIEW` (ADR-019 §4, §5).
>
> **`TopupResultConflict` — mức cảnh báo.** bank-adapter nói hai điều trái nhau về cùng một lần thu,
> hoặc lời xác nhận lệch với lệnh ta gửi. Có thể kèm theo `TopupManualReview`, có thể không.

## Nguyên tắc vàng

**Tiền đã ra khỏi tài khoản của người dùng.** Khác hẳn `SagaManualReview` của chuyển tiền (ở đó tiền
*có thể* chưa đi): ở đây lệnh ghi sổ chỉ được gửi SAU khi provider xác nhận, nên tới được trạng thái
này nghĩa là ngân hàng **đã** trừ họ. Mỗi phút ở đây là một phút khách hàng mất tiền mà không có gì.

**Hai nguồn phải khớp trước khi quyết:** sổ cái của ta (`ledger`) và cổng thanh toán (`provider`).
Không quyết dựa trên một nguồn.

**Chưa có hoàn tiền tự động** — đang làm ở 2.4b ([ADR-020](../adr/ADR-020-topup-compensation-refund.md)):
PR1 đã có hàng rào ở ledger và lệnh hoàn ở bank-adapter, nhưng saga chưa gửi lệnh hoàn (PR2). Tới lúc đó mọi
ca ở đây kết thúc bằng một thao tác tay: hoặc ghi sổ được (chỉ khi ledger chưa dựng hàng rào — mục A), hoặc
hoàn tiền qua cổng của provider. Với VNPay, hoàn tự động chưa có cả sau PR2 (2.4c): bank-adapter trả
`REFUND_UNSUPPORTED` và ca đó vẫn chờ người hoàn tay qua portal.

## Chẩn đoán — 3 phút

```bash
# 1. Các lệnh nạp đang chờ người xử lý — kèm mã tham chiếu phía provider
docker exec wf-postgres psql -U walletflow -d txndb -c "
  SELECT t.id, t.tenant_id, t.to_wallet, t.amount_minor, t.currency, t.failure_code,
         t.provider, t.provider_ref, t.created_at, s.current_step, s.attempt_count
  FROM transactions t LEFT JOIN saga_state s ON s.transaction_id = t.id
  WHERE t.type = 'TOPUP' AND t.status = 'MANUAL_REVIEW' ORDER BY t.created_at;"

# 2. Lần thu đó ở bank-adapter nói gì
docker exec wf-postgres psql -U walletflow -d bankdb -c "
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

## A — Ledger từ chối ghi sổ (tiền đã thu)

Nguyên nhân gần như luôn là trạng thái tài khoản: ví bị đóng giữa lúc người dùng đang trả tiền.

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
   docker exec wf-postgres psql -U walletflow -d bankdb -c "
     SELECT raw_request, raw_response FROM provider_transactions WHERE transaction_id = '<id>';"
   docker exec wf-postgres psql -U walletflow -d bankdb -c "
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

## Sau sự cố

- Có bao nhiêu lệnh nạp còn kẹt cùng lý do? (`SELECT failure_code, count(*) ... GROUP BY 1`)
- Nếu là ca A do ví bị đóng giữa chừng: có nên chặn đóng ví khi còn lệnh nạp đang chờ không?
- Mọi ca ở đây đều sẽ tự động hoá được bằng 2.4b (hoàn tiền tự động). Ghi lại số ca và lý do để biết
  2.4b phải phủ những gì.
