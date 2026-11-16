# ADR-024 — Đối soát bank-adapter ↔ sổ cái: tiền qua cửa ngoài phải khớp sổ

**Trạng thái:** Accepted · 2026-11-16 · roadmap 3.1 PR2a · phần tầng 3 của GAP-A3 không cần sao kê provider
**Liên quan:** [ADR-023](ADR-023-reconciliation-internal.md) (khung đối soát tầng 1/2 — tái dùng nguyên),
[ADR-001](ADR-001-database-per-service.md) (DB per service),
[ADR-016](ADR-016-bank-adapter-charge-model.md) (kết quả thu chỉ chốt bằng `getStatus`),
[ADR-020](ADR-020-topup-compensation-refund.md) (hoàn tiền), [ADR-021](ADR-021-admin-review-audit.md) /
[ADR-022](ADR-022-ledger-reversal-operation.md) (xử lý ca có 4-eyes, `REVERSAL`)

## Context

Tầng 1 và tầng 2 (ADR-023) chỉ so dữ liệu NỘI BỘ của sổ cái: sổ cái tự cân, read model khớp sổ cái.
Không tầng nào thấy được hai ca nghiêm trọng nhất ở biên ngoài:

- provider đã thu tiền của khách mà sổ cái **không có** bút toán nạp (khách mất tiền);
- sổ cái **có** bút toán nạp mà không cổng nào thu (công ty mất tiền).

Roadmap §3.1 đặt cả hai vào tầng 3 cùng với sao kê/settlement file của provider. Nhưng phần lớn tầng 3
**không cần sao kê**: bank-adapter đã giữ sẵn sổ của mình về những gì provider xác nhận
(`provider_transactions`, chỉ chốt bằng `getStatus` — ADR-016). So sổ đó với sổ cái bắt được cả hai ca
trên, ngay hôm nay, không chờ câu hỏi còn mở "merchant sandbox VNPay có cho tải settlement file không".

Tầng 3 vì thế tách đôi: **PR2a** (ADR này) so `provider_transactions` ↔ sổ cái; **PR2b** so sao kê
provider ↔ `provider_transactions`, khi có câu trả lời của VNPay.

## Decision

### 1. Job nằm ở bank-adapter, đọc sổ cái qua HTTP, ghi break vào DB của chính nó

`ProviderLedgerReconciler` chạy ở bank-adapter, giữ `reconciliation_runs` / `reconciliation_breaks` ở
railsdb (V8) — cùng hình dạng với ledgerdb V8 và walletdb V2 (ADR-023 §1), kind `PROVIDER_LEDGER`.

Bank-adapter là bên giữ sự thật phía provider, và PR2b (sao kê) cũng sẽ nằm ở đây — một chỗ cho mọi
câu hỏi "provider nói gì". Sổ cái đọc qua hai endpoint MỚI, **chỉ đọc**, ở `/internal/entries`:
bank-adapter vẫn không ghi gì vào sổ cái, mọi thay đổi sổ cái vẫn chỉ đi qua Kafka command của saga.
Đây là lần đầu bank-adapter gọi ledger.

Quyết định đặt job ở bank-adapter đã được chốt 2026-11-16 khi duyệt kế hoạch.

### 2. Đơn vị so sánh: tiền RÒNG mỗi lệnh tiền, mỗi loại tiền

- **Phía provider** (`ProviderTransactionRepository.CHARGE_CHECK_SELECT`): số thu nếu lần thu
  `SUCCEEDED`, trừ tổng các lần hoàn `SUCCEEDED` của cùng lệnh.
- **Phía sổ cái** (`PostingRepository.externalMovementsOf`): `-SUM(postings vào EXTERNAL_BANK)` qua mọi
  bút toán của lệnh đó, theo loại tiền.

So RÒNG chứ không ghép từng thao tác (thu ↔ `TOPUP`, hoàn ↔ `REVERSAL`), vì đường bù trừ phổ biến nhất
không có bút toán nào để ghép: ledger từ chối lệnh nạp (`rejected_topups`, không có journal) rồi saga
hoàn tiền. Theo số ròng, ca đó là `+x −x = 0` ở adapter và `0` ở sổ cái — khớp, mà không cần job biết
saga đã đi đường nào. Nạp rồi bị đảo (`REVERSAL`) cũng tự ra `0` ở cả hai phía.

Phía sổ cái cố ý **không lọc theo loại bút toán**: mọi thứ chạm `EXTERNAL_BANK` đều là tiền qua cửa
ngoài, và một bút toán lạ chạm vào đó chính là thứ cần bắt. **Giới hạn:** điều này chỉ đúng với lệnh mà
bank-adapter BIẾT (chiều xuôi). Chiều ngược chỉ liệt kê bút toán `TOPUP` — xem *Nợ đã biết* #7.

### 3. Hai chiều quét, phủ hai tập RỜI nhau

1. **Xuôi** — mọi lệnh bank-adapter có hàng THU (kể cả `FAILED`), theo con trỏ trên khoá chính.
2. **Ngược** — mọi bút toán `TOPUP` của sổ cái (`GET /internal/entries/topups`, XUYÊN tenant, con trỏ
   `(created_at, id)`, partial index ledger V9) mà bank-adapter **không có hàng nào**.

Chiều xuôi không bao giờ thấy được một lần nạp mà adapter không biết; chiều ngược bỏ qua mọi lệnh
adapter biết, vì chiều xuôi đã so nó. Hai tập rời nhau ⇒ một lệnh lệch không bao giờ bị ghi hai lần
trong một lượt.

Endpoint `/topups` là đường đọc XUYÊN tenant đầu tiên của ledger. Chấp nhận vì: bank-adapter không thể
liệt kê trước tenant của thứ nó không biết (lấy danh sách tenant từ railsdb sẽ bỏ sót đúng tenant nó
chưa từng thấy); endpoint chỉ trả id (tenant, bút toán, lệnh, thời điểm) — không số tiền, không tài
khoản; và cùng mô hình tin cậy với mọi `/internal` hiện có (tin mạng, GAP-B5, mTLS ở Phase 4). Số tiền
vẫn hỏi lại trong đúng tenant qua `external-movements`.

### 4. Loại break theo CHIỀU của chênh lệch; không bao giờ tự sửa

`delta = sổ cái − provider` (cùng quy ước `delta = actual − expected` của ADR-023, với `expected` là
phía provider):

| Loại | `delta` | Nghĩa |
|---|---|---|
| `PROVIDER_MONEY_NOT_POSTED` | `< 0` | Khách đã trả, ví chưa nhận đủ |
| `POSTED_WITHOUT_PROVIDER_MONEY` | `> 0` | Tiền vào ví mà không ai trả |

Phân theo chiều chứ không theo "phía nào vắng mặt" hay tách riêng "lệch số tiền": hướng tiền là thứ
quyết định ai thiệt và có cần gọi người lúc nửa đêm không. Lệch một nửa và lệch toàn bộ là cùng một việc
với người trực.

`subject = <transactionId>:<currency>`: `EXTERNAL_BANK` là một tài khoản cho mỗi loại tiền, nên lệch ở
hai loại tiền là hai chỗ lệch; chỉ `transactionId` thì `uk_recon_break_open` gộp chúng và cái sau ghi đè
cái trước.

`auto_repaired` luôn `false`. Không tồn tại "số đúng" để chép về: sổ cái append-only, và nạp bù hay
đảo bút toán là quyết định có 4-eyes (ADR-021/022), không phải việc của một job.

Hai hàng rào chống báo động giả của ADR-023 §4 giữ nguyên ý nghĩa: chỉ so lệnh đã **yên** (mọi thao tác
đã kết thúc, không đổi gì trong `settle-window`, mặc định 1 giờ — che độ trễ outbox → Kafka → saga →
ledger kể cả lúc một service trong chuỗi khởi động lại), và **đọc lại cả hai phía** trước khi ghi.

### 5. Hoàn tay qua portal sinh break THẬT, người đóng

`MARK_REFUNDED_MANUALLY` (ADR-021) chốt ca ở transaction-service nhưng không tạo hàng `REFUND` ở
bank-adapter. Sổ của adapter vì thế vẫn tin là tiền còn đó ⇒ `PROVIDER_MONEY_NOT_POSTED`.

Đó là sự thật, không phải báo động giả: sổ của adapter **thiếu** một lần hoàn. Người đóng break đối chiếu
portal và ghi mã hoàn vào `closed_note`. Chốt 2026-11-16 khi duyệt kế hoạch, cân với hai phương án ở
*Alternatives*. Hệ quả cần biết: khi 2.4c PR2 còn bị chặn, **mọi** lần hoàn VNPay là hoàn tay, nên mỗi
lần như vậy để lại một break — đó là lý do loại này chỉ là ticket (§6), và là lý do API đóng break (3.3)
nên đi ngay sau PR này.

### 6. Metric và alert

Tái dùng ba metric của tầng 1/2 với `tier="bank"` (`walletflow.reconciliation.breaks`,
`walletflow.reconciliation.last_success_timestamp`, `walletflow.reconciliation.failures`), nên
`ReconciliationBreakOpen` (ticket) và `ReconciliationRunFailed` (ticket) tự phủ tầng này. Thêm:

- `ReconciliationStaleBank` — page, hai chu kỳ (50 giờ), cùng lý lẽ với `ReconciliationStaleWallet`.
- Gauge `walletflow.reconciliation.provider_ledger.open{type}` + alert `PostedWithoutProviderMoney` —
  **page**, `for: 0m`. Với loại này người dùng TIÊU ĐƯỢC tiền không ai trả, ngay bây giờ. Nguyên nhân
  đã biết duy nhất trong code là `REFUND_POSTED_CONFLICT` (`SagaService.postedWhileRefunding`: lệnh nạp
  tới ledger sau khi saga đã hoàn — adapter ròng 0, sổ cái còn bút toán nạp), xử lý bằng
  `REVERSE_LEDGER_ENTRY` (ADR-022); còn lại là lệnh ghi sổ đi vòng qua saga. Chiều ngược lại có nguyên
  nhân đã biết và vô hại hơn (§5), nên page cho nó là alert fatigue.
- Gauge làm mới theo nhịp RIÊNG (`gauge-refresh`, 5 phút), tách khỏi việc giành lượt. Thiếu nó thì alert
  page kêu tới tận lượt quét ngày hôm sau dù người trực đã đóng break, và một replica thua lượt giữ mãi
  con số đọc lúc khởi động (`max by (job)` cho nó thắng).
- Bộ lập lịch của bank-adapter tăng từ 4 lên 6 thread: lượt quét chạy lâu và nhịp gauge không được
  chiếm chỗ của outbox relay.

## Consequences

- Lần đầu có phép kiểm, khi chạy thật, rằng mọi đồng provider thu đều nằm trong sổ cái và ngược lại.
- Bank-adapter có phụ thuộc mới (đọc) vào ledger, qua `LEDGER_URL`. Ledger vắng mặt chỉ làm lượt đối soát
  `FAILED` (alert), không đụng đường thu tiền. Compose không thêm `depends_on`.
- Ledger có hai đường đọc mới và một partial index (V9); bank-adapter có hai bảng mới (V8). Chỉ expand.

## Nợ đã biết

1. **Quét toàn bộ mỗi lượt**, ở cả hai chiều, như tầng 2. Rẻ ở quy mô hiện tại; khi số lệnh lớn thì
   chuyển sang quét tăng dần theo cửa sổ thời gian + một lượt quét đầy đủ thưa hơn.
2. **Chưa phủ**: `FAILED(EXPIRED)` mà tiền về muộn, và lần thu lệch số tiền còn treo `PENDING` — sổ của
   adapter chưa coi chúng là "đã thu". Cả hai đã có alert riêng (`ExpiredChargeNeedsReview`,
   `ProviderAmountMismatch`); sao kê provider (PR2b) sẽ phủ.
3. **Lệnh đang chạy không được so** (còn `NEW`/`PENDING`, hoặc mới đổi trong `settle-window`). Một lệnh
   kẹt mãi ở `PENDING` không bao giờ được so ở đây — `PendingChargeOverdue` là lưới của ca đó. Một hàng
   kẹt ở **`NEW`** (thu hoặc hoàn) thì KHÔNG có lưới nào: ra khỏi phạm vi cả hai chiều quét, và
   `countOverdue` chỉ đếm `PENDING`. Cần một gauge "hàng đang chạy quá N ngày" — chưa làm.
4. **Giả định lần hoàn cùng loại tiền với lần thu**. Đúng với `RefundService` hiện tại (hoàn đúng hàng
   `refund_of`), nhưng không có ràng buộc DB nào cưỡng chế.
5. **Khoảng trống cửa sổ** của `fixedDelay` + làm tròn — y hệt ADR-023 *Amendments* A4.
6. **Đồng hồ JVM** của ledger (`@CreatedDate`): một bút toán ghi bởi instance lệch giờ có thể rơi "sau
   lưng" con trỏ chiều ngược. Không mất: lượt sau quét lại từ đầu.
7. **Chiều ngược chỉ thấy `TOPUP`** (phát hiện ở review). Ledger không ràng buộc loại bút toán nào được
   chạm tài khoản `SYSTEM` (`LedgerService.validateShape` chỉ kiểm số vế, vế khác 0, tổng = 0). Hệ quả, với
   lệnh bank-adapter KHÔNG biết, phép so này mù trước: một `TRANSFER`/`WITHDRAW` mang vế
   `EXTERNAL_BANK -x | ví +x` (tiền vào ví không ai trả, đúng loại "đi vòng qua saga"); một `REVERSAL` mang
   `transactionId` khác lệnh gốc (ledger cho phép; đường của transaction-service luôn dùng cùng id); và
   toàn bộ luồng rút khi 2.1 mở (chiều xuôi cũng chỉ quét `CHARGE`). Hai cách trả: (a) chiều ngược quét
   theo VẾ chạm `EXTERNAL_BANK` thay vì theo loại bút toán; (b) ledger cưỡng chế loại nào được chạm tài
   khoản `SYSTEM` + `REVERSAL` phải cùng `transactionId` với gốc, rồi nới bộ lọc. Phải trả trước khi 2.1
   (rút tiền) mở.
8. **Stack dev/E2E sinh break `POSTED_WITHOUT_PROVIDER_MONEY` hằng đêm**: `scripts/e2e/seed-topup.sh` và
   `lib.sh topup` nạp bằng lệnh `PostJournal TOPUP` thẳng, không qua bank-adapter. Phát hiện ĐÚNG thiết kế;
   trên stack sống lâu thì tắt bằng `walletflow.bank.reconciliation.enabled=false` hoặc chấp nhận.

## Alternatives rejected

- **Job ở transaction-service** — đã có tiền lệ ghép hai phía (`ResolutionService.resolveReversalTarget`)
  và biết trạng thái saga nên lọc được ca hợp lệ. Nhưng phải gọi lô sang CẢ bank-adapter lẫn ledger, và
  "lọc ca hợp lệ theo trạng thái saga" chính là tin vào máy trạng thái đang bị kiểm — một bug của saga
  sẽ tự che mình.
- **Chiếu `ledger.event.posted.v1` vào railsdb** rồi so tại chỗ — không gọi HTTP, nhưng tạo một bản sao
  thứ hai của sổ cái phải dựng lại được, cần replay, và chính projector đó cũng cần được đối soát.
- **Ghép từng thao tác** (thu ↔ `TOPUP`, hoàn ↔ `REVERSAL`) — đường bị ledger từ chối rồi hoàn không có
  bút toán nào để ghép (§2).
- **Ghi nhận lần hoàn tay vào bank-adapter** (txn-service gửi lệnh để adapter tạo hàng `REFUND` nguồn
  `MANUAL`) — sạch hơn về sổ sách nhưng thêm một lệnh Kafka, một nhánh máy trạng thái và hợp đồng mới;
  để sau, khi biết 2.4c có mở được không.
- **Loại trừ lệnh đã hoàn tay khỏi đối soát** — hết ồn, nhưng tạo một điểm mù đúng chỗ tiền đi qua tay
  người.
