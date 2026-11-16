# ADR-023 — Đối soát nội bộ: bất biến sổ cái và read model

**Trạng thái:** Accepted · 2026-11-16 · roadmap 3.1 PR1 · đóng phần nội bộ của GAP-A3
**Liên quan:** [ADR-001](ADR-001-database-per-service.md) (DB per service),
[ADR-004](ADR-004-ledger-source-of-truth.md) (sổ cái là nguồn sự thật),
[ADR-012](ADR-012-wallet-read-model.md) (read model của ví),
[ADR-010](ADR-010-tenant-propagation.md) (lan truyền tenant),
[ADR-021](ADR-021-admin-review-audit.md) *Amendments* A7 (nợ `resolution_requests`)

## Context

GAP-A3 (🔴 "Sai tiền không ai biết") là 🔴 duy nhất còn lại không bị chặn bởi pháp lý. Hiện
`grep "reconciliation"` **trống** trong toàn bộ `*.java`, `*.sql`, `*.yml` của repo: bất biến vàng
`SUM(postings) = 0` chỉ được kiểm **trong test**. `PostingRepository.totalOfTenant` /
`totalOfSystem` không có một người gọi nào trong production — chỉ `LedgerCommandFlowIT:158` và
`TenantIsolationIT:125`. Khi chạy thật, không có gì phát hiện được tiền tự sinh, tiền bốc hơi, hay
read model trôi khỏi sổ cái.

Phase 2 vừa nối hệ thống với tiền thật đi vào từ bên ngoài. Mọi đường ghi tiền mới (nạp, hoàn, đảo
bút toán vận hành) đều chưa có ai kiểm chứng *sau khi đã chạy*. Đây là lúc phải có.

Mục 3.1 của roadmap mô tả **ba tầng**. ADR này chỉ quyết hai tầng **nội bộ** (không cần dữ liệu từ
provider). Tầng 3 — đối soát với sao kê/settlement file của provider — để PR sau, vì nó phụ thuộc
một câu hỏi chưa trả lời được: merchant sandbox VNPay có cho tải settlement file hay không.

## Decision

### 1. Mỗi service tự giữ bảng đối soát của mình

Tầng 1 (`reconciliation_runs` / `reconciliation_breaks` ở **ledgerdb**), tầng 2 (hai bảng cùng tên ở
**walletdb**). Không có DB đối soát chung, không có service đối soát riêng.

Lý do là ADR-001: một job đối soát ghi chéo vào DB của service khác là chính thứ DB-per-service cấm,
và nó sẽ là **đường ghi thứ hai** vào ledgerdb bên cạnh Kafka command — đúng cái mà
`InternalAccountController` đã từ chối mở ("chỉ ĐỌC. Mọi thay đổi sổ cái vẫn đi qua Kafka command").

Đánh đổi đã biết: báo cáo đối soát tổng hợp (roadmap 3.2) phải gộp hai nguồn. Chấp nhận — gộp lúc
đọc rẻ hơn nhiều so với việc phá ranh giới ghi.

**Cùng hình dạng bảng ở cả hai service**, cố ý: khi 3.2 dựng báo cáo, hai nguồn ghép được mà không
phải dịch. Hình dạng không đưa vào `common-lib` (mỗi service có migration riêng, không chia sẻ
schema) nhưng cột và tên trạng thái phải giống hệt.

### 2. Tầng 1 — bất biến sổ cái, nhóm theo `(tenant, currency)`, **không** chỉ toàn cục

Roadmap §3.1 viết câu SQL nhóm theo `tenant_id, currency` nhưng `JOIN journal_entries`. Thực tế
**cả `postings` lẫn `journal_entries` đều không có cột `currency`** — nó nằm ở `accounts`. Câu đúng
join `accounts`:

```sql
SELECT p.tenant_id, a.currency, SUM(p.amount_minor)
  FROM postings p
  JOIN accounts a ON a.id = p.account_id AND a.tenant_id = p.tenant_id
 GROUP BY 1, 2
HAVING SUM(p.amount_minor) <> 0;
```

Chạy **cả** phép kiểm toàn hệ thống (`SUM` không nhóm) bên cạnh. Lý do đã nằm sẵn trong javadoc của
`PostingRepository.totalOfSystem` và nó vẫn đúng theo chiều ngược lại: một bút toán ghi nợ tenant A
và ghi có tenant B làm tổng **toàn cục vẫn bằng 0** trong khi sổ của cả hai khách đều sai. Phép kiểm
toàn cục một mình không đủ; phép kiểm theo nhóm một mình thì bỏ sót một vế trỏ vào tài khoản đã mất
(`JOIN` làm hàng biến mất). Hai phép kiểm bắt hai loại lỗi khác nhau — chạy cả hai.

**Không** thêm phép kiểm "mỗi `journal_entry` phải cân": nó không bắt thêm được ca nào mà phép kiểm
theo `(tenant, currency)` bỏ sót (một bút toán lệch làm nhóm của nó lệch). Nó chỉ *định vị* nhanh
hơn — việc của người điều tra, không phải của job. Để ngỏ cho 3.3.

### 3. Tầng 2 — hai phép so, rẻ trước đắt sau

**2a — tự kiểm, không qua mạng.** `V1__init_wallet.sql:46` đã ghi sẵn tính chất này:
`SUM(wallet_entries.amount_minor)` của một ví **phải** bằng `balance_minor` của nó. Một câu SQL
trong walletdb, bắt mọi lỗi của projector và mọi câu `UPDATE` gõ tay. Loại break:
`PROJECTION_SELF_MISMATCH`.

**2b — so với nguồn sự thật.** `balance_minor` so với `SUM(postings)` bên ledger. Đây mới là phép so
mà roadmap đòi; 2a không thay thế được nó (projector có thể bỏ sót **cả** hàng lịch sử lẫn số dư,
lúc đó 2a vẫn khớp trong khi ví thiếu tiền). Loại break: `LEDGER_MISMATCH`.

2a chạy trước và rẻ, nhưng **không** dùng làm bộ lọc cho 2b: ví lệch kiểu 2a và ví lệch kiểu 2b là
hai tập khác nhau, lọc bằng cái này sẽ giấu cái kia.

### 4. Cửa sổ ổn định — điều làm tầng 2 không kêu oan

Read model là *eventually* consistent. `WalletRebuildService` đã ghi rõ ở javadoc rằng event đang
bay làm phép so lệch tạm thời. Một job so thẳng sẽ báo break cho mọi ví vừa có giao dịch trong vài
giây trước đó — alert vô dụng, và alert vô dụng thì bị tắt.

Hai hàng rào:

1. **Chỉ so ví đã yên** — `wallets.updated_at < now() - settle-window` (mặc định 60 giây). Ví vừa
   đổi số dư được bỏ qua lượt này; đêm sau nó đã yên.
2. **So lại lần hai trước khi ghi break.** Ví đã qua hàng rào 1 vẫn có thể nhận một bút toán đúng
   lúc job đọc. Mỗi ứng viên break được đọc lại (cả hai phía) một lần nữa; chỉ còn lệch mới ghi.

Hệ quả cố ý: một ví *liên tục* giao dịch suốt cửa sổ sẽ không bao giờ được đối soát. Chấp nhận ở
PR1 — tầng 1 vẫn phủ nó (tiền của nó nằm trong tổng của tenant). Ghi lại như nợ, xử ở 3.2 bằng đối
soát theo ảnh chụp tại một thời điểm.

### 5. Lệch tầng 2 → **tự rebuild**, nhưng break **vẫn mở**

Ledger luôn thắng (ADR-004, ADR-012), nên để số dư sai nằm đó trong khi đã biết số đúng là chọn cái
tệ hơn: người dùng nhìn thấy số sai cho tới sáng hôm sau. Job gọi thẳng `WalletRebuildService` —
đường này đã có từ Phase 1, đã có `WalletRebuildIT`, và thứ tự khoá ví → đọc sổ cái của nó đã đúng
khi có event đang bay.

Nhưng break **không tự đóng**. Roadmap §3.1 nói "break phải được đóng bởi người" và lý do rất cụ
thể: rebuild chỉ chép lại con số đúng, nó **không sửa cái bug trong projector đã làm lệch**. Một
break tự sinh tự đóng mỗi đêm là một bug ẩn chạy mãi. Hàng break ghi `auto_repaired = true` để người
trực biết số dư đã đúng rồi — việc còn lại là tìm nguyên nhân, không phải chữa cháy.

Tầng 1 **không bao giờ** tự sửa: không tồn tại khái niệm "số đúng" để chép về, và sổ cái
append-only. Imbalance ⇒ gọi người, chấm hết.

### 6. Không khoá phân tán — giành lượt bằng chính `reconciliation_runs`

Repo không có ShedLock, không có advisory lock (ADR nào cũng chưa mở đường đó). Idiom đang dùng là
`FOR UPDATE SKIP LOCKED` cho job lấy lô. Nhưng đối soát **không** lấy lô — nó là một lượt quét duy
nhất, và hai instance cùng quét thì ghi hai bộ break trùng nhau.

Giành lượt bằng `UNIQUE (kind, window_start)` trên `reconciliation_runs`: mỗi lượt chốt vào một
*cửa sổ* tất định (làm tròn xuống theo chu kỳ), instance nào `INSERT` được thì chạy, instance thua
nổ `23505` và bỏ qua im lặng. Không thêm phụ thuộc, không có khoá nào để rò rỉ khi process chết —
hàng `RUNNING` bỏ lại là **dữ liệu nhìn thấy được**, tốt hơn một cái khoá vô hình.

Hệ quả: một lượt chết giữa chừng để lại hàng `RUNNING` và cửa sổ đó **không chạy lại**. Đúng ý:
metric `last_success` sẽ già đi và alert bắn. Job đối soát chết âm thầm còn tệ hơn không có job —
roadmap §3.1 nói thẳng điều đó.

### 7. Một break đang mở không được nhân bản mỗi lượt

`UNIQUE (tenant_id, type, subject) WHERE status = 'OPEN'`. Cùng một ví lệch mãi thì mỗi đêm cập nhật
hàng đang mở (số đo mới, `detected_at` mới, `times_seen + 1`), không sinh hàng thứ hai. Thiếu nó thì
sau một tháng một break duy nhất thành 30 hàng và hàng đợi của người trực vô dụng.

### 8. Trả nợ ADR-021 A7 trong cùng PR — `resolution_requests` append-only

A7 viết: *"`resolution_requests` không append-only: `APPROVED → PENDING` bằng SQL tay vẫn làm được.
Cân nhắc khi bảng này trở thành căn cứ đối soát ở Phase 3."* Giờ là lúc đó — bảng 4-eyes là căn cứ
giải thích vì sao một break được đóng.

Trigger theo đúng khuôn `txn_resolved_at_is_final` đã có ở V6:

- **Đã quyết là vĩnh viễn** — `OLD.status <> 'PENDING'` ⇒ không `UPDATE` được gì nữa.
- **Lúc còn PENDING**, mọi trường của người đề xuất bất biến (`tenant_id`, `transaction_id`,
  `action`, `maker_*`, `reason`, `target_entry_id`, `created_at`): chỉ được đi tới một quyết định,
  không được sửa chính đề xuất mà người thứ hai sắp ký vào.
- **Cấm `DELETE`** — luôn.

Siết luôn hai chỗ A7 nêu kèm: `ck_resolution_decided` đòi thêm `checker_role IS NOT NULL` khi đã
quyết, và cấm điền `checker_*` lúc còn `PENDING`.

## Consequences

- Bất biến vàng lần đầu được kiểm **khi chạy thật**, không chỉ trong test.
- Ba metric mới (`walletflow.reconciliation.breaks`, `walletflow.ledger.imbalance_minor`,
  `walletflow.reconciliation.last_success_timestamp`) + alert. Alert cho `last_success` là cái quan
  trọng nhất: nó bắt được chính job này chết.
- Ledger mở thêm **một** đường đọc nội bộ (số dư theo lô). Vẫn chỉ đọc.
- `POST /resolve` và `POST /decide` không đổi hành vi; chỉ một câu `UPDATE` gõ tay mới gặp trigger
  mới. Migration tương thích ngược với bản app đang chạy.
- Chưa phủ: tầng 3, ví giao dịch liên tục (§4), đối soát `provider_transactions` ↔ ledger.

## Amendments — 2026-11-16 (sau review)

### A1. `auto_repaired` phải là sự thật, không phải ý định

`WalletRebuildService.rebuild` **thoát sớm không đụng gì** khi ledger chưa có tài khoản. Bản đầu của
job ghi `auto_repaired = true` ngay khi *gọi* rebuild, nên một ví vừa lệch 2a vừa không có tài khoản
ledger sẽ mang cờ "máy đã sửa" trong khi số dư vẫn sai — và runbook dạy người trực đọc cờ đó thành
"số dư đã đúng rồi, chỉ cần tìm nguyên nhân". Nay cờ lấy từ `RebuildResult`: chỉ true khi số dư hoặc
số dòng lịch sử thực sự đổi.

### A2. Một lô hỏng không được đốt cả cửa sổ

`uk_recon_run_window` làm cửa sổ chỉ chạy một lần. Nếu cả vòng quét nằm trong một `try`, một
`LedgerUnavailableException` ở lô thứ ba ⇒ mất luôn mọi ví phía sau, và với tầng 2 thì **24 giờ
không ai kiểm**. Quá đắt cho một blip mạng. Nay bắt lỗi **theo lô**: lô hỏng bị bỏ qua, quét tiếp,
lượt kết thúc `FAILED` kèm số lô hỏng.

Vẫn **không** cho giành lại một cửa sổ `FAILED` — nó đòi đổi khoá chính của hàng run (FK từ
`reconciliation_breaks`). Thay vào đó mở counter `walletflow.reconciliation.failures` + alert
`ReconciliationRunFailed`: một lượt hỏng kêu **ngay**, không đợi `last_success` già đi.

### A3. Trần số chỗ lệch mỗi lượt

Mỗi break kéo theo một lời gọi ledger và một transaction ghi. Lệch diện rộng (projector hỏng, restore
nhầm một bản walletdb cũ) không có trần sẽ biến chính lượt đối soát thành sự cố. `max-breaks-per-run`
(mặc định 500): chạm trần thì dừng quét và gọi người — lệch diện rộng là ca **phải** có người.

### A4. Nợ đã biết, ghi ra để không ai tưởng đã an toàn

- **Rebuild giữ khoá ví trong lúc gọi HTTP sang ledger.** Ngoại lệ này có sẵn từ Phase 1 và được
  javadoc của `WalletRebuildService` nhận là chấp nhận được "cho một thao tác quản trị **hiếm**".
  Job này làm nó chạy tự động hàng loạt, nên tiền đề "hiếm" không còn đúng; thứ duy nhất còn giữ nó
  an toàn là `read-timeout` 5 giây. Chưa sửa ở PR này.
- **Cửa sổ có thể bị bỏ qua.** `fixedDelay` + làm tròn cửa sổ: một lượt tầng 1 bắt đầu 10:55 chạy 10
  phút ⇒ lượt kế 12:05 ⇒ cửa sổ 11:00 không bao giờ được quét, mà `last_success` vẫn trẻ nên không
  alert. Không mất dữ liệu (phép kiểm là toàn bảng, không incremental) nhưng khoảng trống là thật.
- **`fixedDelay` tính từ lúc khởi động**, nên tầng 2 chạy vào *giờ deploy + 24h*, không phải ban
  đêm như roadmap viết. Muốn đúng khung giờ thấp điểm thì phải chuyển sang cron.
- **Chưa có API đóng break** — runbook đóng bằng SQL tay, nên thao tác đó **không** để lại
  `audit_events`. `ReconciliationBreakRepository.close`/`findOpen` đã có sẵn, chờ controller ở 3.3.

## Alternatives rejected

- **Một service đối soát riêng (:8086)** — sạch về khái niệm, nhưng thêm module, failsafe, gate
  JaCoCo, compose, CI matrix cho một job đọc hai DB. Đổi ~2 ngày công lấy một ranh giới mà
  DB-per-service đã cho sẵn.
- **Gom bảng break về ledgerdb** — một chỗ duy nhất, dễ báo cáo, nhưng buộc wallet-service ghi chéo
  vào DB của ledger (§1).
- **Tầng 2 tự đóng break sau khi rebuild** — ít việc cho người trực nhất, và giấu đúng cái bug cần
  tìm (§5).
- **Chỉ ghi break, không tự rebuild** — an toàn tuyệt đối về mặt "máy không đụng số dư", nhưng
  rebuild đằng nào cũng chỉ chép lại từ ledger, và chờ người nghĩa là người dùng nhìn số sai lâu
  hơn.
- **ShedLock** — một phụ thuộc và một bảng khoá mới cho bài toán mà một `UNIQUE` đã giải (§6).
