# ADR-022 — Đảo bút toán như một thao tác vận hành

**Trạng thái:** Accepted · 2026-10-27 · roadmap 2.5 PR4 · đóng GAP-A4
**Liên quan:** [ADR-004](ADR-004-ledger-source-of-truth.md) (sổ cái là nguồn sự thật),
[ADR-011](ADR-011-ledger-account-lifecycle.md) §6 (luật REVERSAL ở ledger),
[ADR-020](ADR-020-topup-compensation-refund.md) (bù trừ lệnh nạp),
[ADR-021](ADR-021-admin-review-audit.md) (khu vực vận hành, 4-eyes, audit)

## Context

Ledger hỗ trợ `REVERSAL` từ Phase 1 nhưng **chưa ai gọi** — GAP-A4 trong register. ADR-021
*Amendments* A1 hoãn nó sang sau PR3 vì hai lý do: `entryId` của bút toán gốc không nằm ở txndb, và
quyết định đúng phụ thuộc lần hoàn ở bank-adapter đã xong chưa (dữ liệu thuộc railsdb). PR3 dựng hai
đường đọc `/internal` cho đúng hai thứ đó.

A1 cũng chốt phạm vi bằng cách đối chiếu từng `failure_code` đang thực sự được ghi: **`REVERSAL` áp
cho đúng một ca**, `REFUND_POSTED_CONFLICT`. Mọi ca còn lại không có bút toán nào để đảo — ledger từ
chối *trước khi* ghi, hoặc lệnh ghi sổ chưa từng được gửi.

Ca đó trông thế này: lệnh nạp bị từ chối ⇒ saga hoàn tiền cho người dùng; rồi ledger **vẫn ghi** bút
toán nạp (một instance chưa lên V6 nên thiếu hàng rào `rejected_topups`, hoặc bút toán được ghi tay).
Kết quả: ví được cộng **và** tiền đã về thẻ. Ví đang giữ một khoản người dùng đã được trả lại — mất
tiền của **công ty**, không phải của khách. Trước PR4, runbook nói "mở issue, chờ 2.5".

## Decision

### 1. Đảo bằng một bút toán mới, không sửa bút toán cũ

Không có gì mới ở đây — đó là ADR-004 — nhưng đáng nhắc lại vì nó loại bỏ mọi phương án "sửa cho
đúng": bảng ledger append-only, có trigger DB chặn UPDATE/DELETE. Vết sửa cũng là một sự kiện tiền
và cũng phải nằm trong sổ.

Lệnh đi qua `CMD_POST_JOURNAL` như mọi lệnh ghi sổ khác, với `type = REVERSAL` và
`referenceId = entryId` của bút toán gốc. **Không thêm topic, không thêm schema**: cả hai đã có
`REVERSAL` trong enum từ Phase 1.

Hai vế là hai vế của bút toán nạp đổi dấu. Ledger cưỡng chế đối xứng chính xác
(`REVERSAL_MISMATCH`, ADR-011 §6) và đó là hàng rào thật: một "REVERSAL" trỏ vào bút toán có thật
nhưng mang vế tuỳ ý là một đường rút tiền khỏi ví bất kỳ, kể cả ví đang bị đóng băng. Vì vậy số tiền
lấy từ chính hàng giao dịch, **không bao giờ** từ đầu vào của người gọi API.

### 2. Ba điều kiện trước khi được phép đề xuất, kiểm lúc ĐỀ XUẤT chứ không lúc duyệt

1. **Phải hỏi được cả ledger lẫn bank-adapter.** Không hỏi được ⇒ `503 CONTEXT_UNAVAILABLE`. Đây là
   chỗ quy ước "rỗng ≠ không hỏi được" của PR3 ([ADR-021](ADR-021-admin-review-audit.md) B1) trả
   công: đảo một bút toán vì *"ledger không trả lời nên chắc là chưa ai đảo"* là cách tạo ra một mất
   mát thứ hai.
2. **Phải có bút toán `TOPUP`, và chưa có `REVERSAL` nào.** Từ chối tại đây cho câu trả lời rõ ràng
   thay vì để ledger nổ `ALREADY_REVERSED` sau khi hai người đã ký.
3. **Lần hoàn ở provider phải `SUCCEEDED` và ĐÚNG số tiền.** Điều kiện cốt lõi: đảo bút toán nghĩa
   là lấy lại tiền khỏi ví *vì* người dùng đã được trả lại qua provider. Lần hoàn chưa xong mà đã
   đảo thì ta vừa rút tiền khỏi ví vừa chưa trả cho họ — **mất tiền của KHÁCH**, nặng hơn hẳn ca
   gốc. So cả `amountMinor` và `currency`, không chỉ "có một lần hoàn thành công": ta sắp rút khỏi
   ví TRỌN số đã nạp, nên một lần hoàn nhỏ hơn thế để lại phần chênh là tiền ta lấy của khách —
   chính saga cũng coi lệch số tiền hoàn là sự cố nghiêm trọng (`REFUND_AMOUNT_MISMATCH`).

Kiểm lúc đề xuất vì hai lý do. Kỹ thuật: lấy chúng cần gọi HTTP, mà `decide` chạy trong transaction
DB (CLAUDE.md cấm gọi HTTP trong transaction). Quan trọng hơn: **người duyệt phải thấy đúng bút toán
mà người đề xuất đã xem**. Tra lại lúc duyệt có thể ra một bút toán khác, và 4-eyes lúc đó chỉ là
hai người ký vào hai thứ khác nhau. Nên `entryId` được chốt vào `resolution_requests.target_entry_id`
(txndb V7).

`propose` vì thế không còn `@Transactional` mà dùng `TransactionTemplate` — tự gọi một method
`@Transactional` của chính mình đi thẳng, không qua proxy, nên sẽ không có transaction nào.

### 3. Thao tác này KHÔNG kết thúc khi người duyệt bấm nút

Khác cốt lõi với `MARK_REFUNDED_MANUALLY`: thao tác kia chỉ ghi nhận một việc đã xong (tiền đã hoàn
tay trước đó), còn thao tác này mới chỉ **gửi một lệnh** — và ledger có quyền từ chối.

Nên ca **không** được đóng lúc duyệt, và `resolved_at` **không** được đặt. Đóng lúc đó là một cái
bẫy: `resolved_at` khiến hàng rào `alreadyResolved` chặn luôn câu trả lời của ledger
([ADR-021](ADR-021-admin-review-audit.md) A5), nên một lệnh đảo **bị từ chối** sẽ biến mất không dấu
vết trong khi ca trông như đã xong.

| Sự kiện | Kết cục |
|---|---|
| Duyệt | `MANUAL_REVIEW` + `REVERSAL_SENT`, `resolved_at` vẫn NULL |
| `LedgerPosted(REVERSAL)` | `FAILED` + `LEDGER_REVERSED` + `resolved_at`, phát `TransactionFailed` |
| `LedgerRejected(REVERSAL)` | ở lại `MANUAL_REVIEW` + `REVERSAL_REJECTED`, counter + alert |

Câu trả lời của ledger tới qua Kafka, nơi không có gì đảm bảo đúng một lần hay đúng thứ tự. Nên
`onReversalRejected` ghi bằng một UPDATE **có điều kiện**
(`status='MANUAL_REVIEW' AND resolved_at IS NULL AND failure_code='REVERSAL_SENT'`) và chỉ tăng
counter khi thật sự ghi được. Thiếu điều kiện đó: một bản giao lại làm alert mức `page` kêu hai lần
cho một sự cố, còn một lời từ chối cũ tới **sau** khi lệnh đảo đã thành công sẽ kéo ca đã đóng ngược
về `MANUAL_REVIEW` — ca vừa có `resolved_at` vừa nằm trong hàng đợi, tức là hiện ra cho người trực
nhưng mọi thao tác đều bị `lockOpenCase` từ chối. Kẹt vĩnh viễn, và nói sai về tiền.

`REVERSAL_REJECTED` **vẫn đề xuất lại được** (`requireReversible` chấp nhận cả hai mã). Ca thật:
người dùng nạp lại tiền, nay ví đủ số để đảo. Bắt họ sửa DB tay để thử lại là đẩy một thao tác đụng
tiền ra ngoài đường có audit — đúng vấn đề mà ADR-021 mở đầu bằng. An toàn vì `resolveReversalTarget`
hỏi lại ledger và provider từ đầu, và `uk_journal_one_reversal` là chốt cuối.

`onPosted`/`onRejected` phải rẽ sớm theo `type == REVERSAL`: `lockOurs` lọc theo loại giao dịch, nên
một câu trả lời `REVERSAL` cho một giao dịch `TOPUP` sẽ bị lọc ra và **biến mất im lặng**.

### 4. Ví đã tiêu mất tiền ⇒ từ chối dứt khoát, không tạo số dư âm

Ca thường gặp nhất của nhánh từ chối là `INSUFFICIENT_FUNDS`: người dùng đã tiêu khoản được cộng
nhầm. Ledger từ chối, và **đó là hành vi đúng** — không nới `validateSufficientFunds` cho `REVERSAL`.

Lý do không chọn "cho phép ví âm để sổ sách phản ánh đúng việc khách đang nợ": số dư âm là một trạng
thái **chưa nơi nào trong hệ thống này xử lý**. Read model, API số dư, mọi kiểm tra trước khi chuyển
tiền đều ngầm giả định số dư ≥ 0. Tạo ra nó để sổ sách cho đẹp là đổi một vấn đề kế toán lấy một
vấn đề vận hành lớn hơn, lan ra mọi đường đọc.

Phương án đúng về kế toán — đảo phần còn lại, phần thiếu ghi vào một tài khoản phải thu — cần một
loại tài khoản mới và bút toán một phần, tức là một nghiệp vụ thu hồi nợ chưa tồn tại. Không làm ở
đây. Ca đó ở lại hàng đợi với `REVERSAL_REJECTED`; thu hồi là việc của con người.

### 5. Không mở bước saga cho đường này

Ban đầu `reverseLedgerEntry` mở một bước `REVERSE_JOURNAL` trên `saga_state` để job quét gửi lại khi
lệnh mất. Bỏ, vì nó không chạy được: ca đứng ở `MANUAL_REVIEW`, trạng thái mà
`StuckSagaScanner.isOpen` coi là đã đóng — job quét sẽ đóng saga ngay lượt sau, và cái `current_step`
còn sót lại chặn luôn lần đề xuất tiếp theo.

Dấu duy nhất là `failure_code = REVERSAL_SENT` trên hàng giao dịch, và nó cũng chính là hàng rào
chống gửi hai lần (kiểm `REFUND_POSTED_CONFLICT` sẽ từ chối mọi lần đề xuất sau).

**Đánh đổi phải biết: lệnh đảo mất trên đường KHÔNG được gửi lại tự động.** Ca nằm ở `REVERSAL_SENT`
cho tới khi có người nhìn — runbook `topup-stuck.md` mục G. Chấp nhận được vì đây là thao tác tay
hiếm có hai người ký, và ledger idempotent theo `uk_journal_one_reversal` nên đề xuất lại là an toàn.

## Consequences

**Được:**
- GAP-A4 đóng: có đường sửa một sự cố tiền qua API, có 4-eyes và audit cùng transaction. Kể cả khi
  ledger từ chối, người vận hành vẫn đề xuất lại được bằng API. Còn **đúng một** ca phải mở `psql`:
  lệnh đảo mất hẳn trên đường (`REVERSAL_SENT` đứng mãi), vì ta không phân biệt được "mất" với
  "đang bay" — runbook mục G.
- Bút toán đảo nối với bút toán gốc qua `reference_id` — đối soát Phase 3 đọc được cặp đó.
- `uk_journal_one_reversal` làm hàng rào cuối: hai lệnh đảo từ hai nơi vẫn chỉ ra một bút toán.
- Luật `REVERSAL` phía ledger đã có test từ Phase 1 — `LedgerServiceIT` (đối xứng đưa số dư về 0;
  chỉ hoàn tác một lần; bắt buộc `referenceId` hợp lệ) và `AccountLifecycleIT` (chạy được khi ví
  `CLOSED`; `REVERSAL_MISMATCH`). PR4 không lặp lại chúng, chỉ dựng đường gọi.

**Mất / còn mở:**
- **Không retry tự động** (§5). Alert `ReversalStuck` treo trên ca `REVERSAL_SENT` quá lâu là việc
  đáng làm tiếp; hiện chỉ có runbook.
- **Ví đã tiêu tiền thì máy hết đường** (§4). Không có nghiệp vụ khoản phải thu.
- **Chỉ áp cho `REFUND_POSTED_CONFLICT`.** Nếu sau này có ca khác thật sự cần đảo, điều kiện ở
  `reverseLedgerEntry` phải mở ra một cách tường minh, đừng nới thành "mọi ca MANUAL_REVIEW".
- **Kết quả từ ledger không ghi audit.** Dấu vết của *quyết định* đã có (propose + decide, cùng
  transaction với việc ghi); việc ledger ghi là hệ quả, không phải thao tác của người. Nhưng nó
  nghĩa là bảng `audit_events` không trả lời được "lệnh đảo đó cuối cùng ra sao" — phải đọc
  `failure_code`. Cân nhắc khi `audit_events` thành căn cứ đối soát.
- **`resolution_requests` vẫn không append-only** (ADR-021 A7) — nay bảng này còn mang `target_entry_id`,
  tức là một mẩu dữ liệu quyết định số tiền bị đảo. Đáng nâng lên append-only ở Phase 3.

## Alternatives rejected

- **Sửa trạng thái giao dịch thay vì ghi bút toán đảo:** read model sẽ khớp, nhưng sổ cái thì không —
  số dư ví vẫn giữ khoản cộng nhầm. Đây đúng là "nguồn sự thật thứ hai" mà §5.1 cấm.
- **Cho ledger tự đảo khi phát hiện xung đột:** nhanh hơn và không cần người. Nhưng ledger không biết
  lần hoàn ở provider đã xong chưa — và đó là điều kiện quyết định (§2.3). Một quyết định tiền dựa
  trên nửa dữ kiện, chạy tự động, là cách nhân một sự cố lên quy mô.
- **Đảo bằng một `transactionId` mới:** tránh được việc hai bút toán chung khoá idempotency, nhưng mất
  luôn sợi dây nối hai bút toán ở cấp giao dịch, và hàng đợi vận hành sẽ có hai ca cho cùng một sự cố.
- **Gộp vào 2.5 PR3:** PR3 là đường chỉ đọc. Trộn một thao tác đụng tiền vào đó làm review mất khả
  năng tập trung đúng chỗ nguy hiểm.
