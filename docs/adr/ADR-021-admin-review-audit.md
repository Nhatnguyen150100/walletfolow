# ADR-021 — Khu vực vận hành: hàng đợi cần can thiệp, dấu vết, và vai

**Trạng thái:** Accepted · **Ngày:** 2026-10-22 · **Milestone:** Phase 2 — 2.5
(PR1: queue + audit + nền RBAC · PR2: xử lý ca + 4-eyes · PR3: ngữ cảnh chéo service)

## Context

Sau 2.4, mọi ca mà máy không tự xử lý được đều dừng ở `MANUAL_REVIEW` với một `failure_code`. Hôm
nay cách duy nhất nhìn thấy chúng là mở `psql` theo runbook. Đó vừa là rào cản vận hành, vừa là một
lỗ hổng kiểm toán: **mọi thao tác chữa cháy đang diễn ra bằng SQL trực tiếp, không để lại dấu vết
nào** — chính là ô DoD cuối cùng của Phase 2 chưa đạt.

Khảo sát trước khi làm cho thấy mục này lớn hơn mô tả trong roadmap ở bốn chỗ:

- Vai chỉ có `USER`/`ADMIN`, và **gateway không kiểm vai ở bất kỳ path nào** — mỗi service tự so
  header (`AdminWalletController`). Cột `users.role` thậm chí không có `CHECK`.
- `audit_events` chỉ tồn tại ở authdb, và **không có `tenant_id`**.
- Ngữ cảnh mà người trực cần (raw response của provider, bút toán) nằm ở railsdb và ledgerdb, hai
  DB không có API đọc theo `transactionId` (ADR-001).
- 4-eyes chưa có gì: không bảng phê duyệt, không maker/checker, không trạng thái chờ duyệt.

Nên 2.5 tách ba PR. ADR này chốt phần nền (PR1) và ghi lại những gì PR2/PR3 phải theo.

## Decision

### 1. Admin API nằm trong transaction-service, không phải một service mới

Hàng đợi là một câu truy vấn trên `transactions` — service sở hữu bảng đó trả lời được ngay, không
cần gọi chéo. Một `admin-service` riêng sẽ phải gọi mạng cho **mọi** dòng của hàng đợi, và thêm một
module (pom, Dockerfile, compose, CI, jacoco gate) cho thứ chưa có nghiệp vụ riêng nào.

Đánh đổi: khi 2.5 PR3 cần raw response của provider và bút toán, transaction-service sẽ gọi
`/internal` của bank-adapter và ledger — đúng khuôn `WalletDirectory`/`LedgerClient` đã có. Nếu khu
vực vận hành lớn tới mức có nghiệp vụ riêng (phân ca, SLA, giao việc) thì tách là một ADR khác.

### 2. Phân quyền hai tầng: biên chặn THÔ, service quyết CHI TIẾT

Gateway chặn mọi path `/api/v1/admin/**` cho ba vai vận hành. Nó **không** biết ai được làm gì bên
trong — nhét luật nghiệp vụ vào biên là chẻ chúng ra hai nơi, và biên không có dữ liệu để quyết.

Service kiểm **lại** (`Operator.requireReader`). Không phải thừa: biên trong cụm chưa có mTLS
(Phase 4), nên một route cấu hình sai không được phép là thứ duy nhất đứng giữa khách hàng với dữ
liệu của người khác. Tầng này còn là nơi 2.5 PR2 phân biệt "xem được" với "duyệt được chuyện tiền".

**Tenant vẫn là ranh giới cứng.** Vai vận hành nới quyền *trong nội bộ một tenant* — người vận hành
không còn bị giới hạn ở giao dịch do chính mình tạo, nhưng không bao giờ thấy tenant khác (ADR-010).

### 3. Ba vai, tách theo thứ được LÀM

`ADMIN` (duyệt được thao tác đụng tiền) · `OPS` (xem và mở ca, không tự quyết chuyện tiền) ·
`AUDITOR` (chỉ đọc). `AUDITOR` tồn tại để người kiểm sổ không phải mượn một tài khoản có quyền ghi.

Hai mục còn mở, ghi ở đây để không ai tưởng chúng đã xong:
- **Vai trong JWT có TTL 15 phút** (ADR-009): hạ quyền một người không có hiệu lực ngay. Chấp nhận ở
  Phase 2; cơ chế thu hồi tức thì là một quyết định riêng, không tự chế.
- **Cấp vai vẫn là `UPDATE users SET role = ...` bằng tay.** `scripts/e2e/make-admin.sh` đã tự nhận
  "phải qua quy trình có audit" từ lâu mà chưa có quy trình nào. V3 thêm `CHECK` nên ít nhất gõ sai
  một vai sẽ hỏng ngay thay vì hỏng lúc nạp entity.

### 4. `audit_events` nằm cùng DB với thứ nó ghi lại

Một bảng ở txndb, không dùng lại bảng của auth-service. Audit đi qua mạng thì **mạng hỏng là mất
dấu vết, đúng lúc cần nhất**; cùng DB nghĩa là nó vào được cùng transaction với việc nó ghi lại —
điều kiện bắt buộc của PR2 ("không thao tác đụng tiền nào không để lại audit"). Giá phải trả: báo
cáo kiểm toán toàn hệ thống phải ghép nhiều nguồn. Đó là cái giá của DB-per-service, và nó rẻ hơn.

Bảng append-only bằng trigger ở DB, cùng khuôn sổ cái: một audit log sửa được thì không trả lời
được câu hỏi duy nhất nó sinh ra để trả lời.

### 5. Thao tác ĐỌC cũng ghi dấu vết, nhưng audit hỏng KHÔNG chặn việc đọc

Người vận hành xem dữ liệu tài chính của một khách hàng là việc cần giải trình được. Nhưng mất dấu
vết của một lần *xem* là chuyện đáng kêu to, không phải lý do để người trực không nhìn được hàng đợi
đúng lúc đang có sự cố — nên `AuditService` nuốt lỗi và log ERROR.

**PR2 phải làm ngược lại.** Dấu vết của thao tác đụng tiền vào CÙNG transaction với việc ghi tiền;
không ghi được audit thì không được ghi tiền. Đó là bất biến, không phải nỗ lực.

**Lần bị TỪ CHỐI cũng ghi** (`ADMIN_ACCESS_DENIED`, `VIEW_TRANSACTION_DENIED`) — một chuỗi 403/404
liên tiếp chính là hình dạng của một token bị đánh cắp hoặc của việc dò id. Gateway có `log.warn`,
nhưng log không phải sổ kiểm toán: không có `tenant_id`, không bất biến, và bị xoay vòng. Ngoại lệ:
401 (thiếu định danh) không ghi được vì không có `actor_id`, và một lời gọi không mang định danh gần
như luôn là gọi thẳng vào service — chỗ nhìn thấy nó là log + metric của gateway.

Bốn chi tiết kỹ thuật đã làm hỏng việc này trong lúc làm PR1, ghi lại để khỏi lặp:
- `REQUIRES_NEW` commit khi lời gọi qua **proxy** trả về, nên `try/catch` quanh `save()` *bên trong
  cùng bean* không bao giờ thấy lỗi lúc commit. Phải tách hai bean (`AuditService` → `AuditWriter`).
- Lời gọi nội bộ (`this.method()`) không đi qua proxy: một overload tiện lợi gọi method
  `@Transactional` cùng lớp khiến `REQUIRES_NEW` im lặng không áp dụng, `save` rơi vào transaction
  `readOnly` của người gọi và Hibernate **bỏ qua INSERT mà không báo gì**.
- `REQUIRES_NEW` **lồng trong một transaction đọc đang mở** giữ hai connection cùng lúc. Với pool
  mặc định 10, dùng chung với job quét saga và các Kafka listener, một nhúm người trực cùng mở hàng
  đợi là đủ để mỗi luồng giữ một connection và chờ cái thứ hai: request treo 30 giây rồi vẫn trả
  200, nhưng dấu vết bị nuốt. Nên `AdminReviewService` **không** có `@Transactional` — mỗi truy vấn
  trả connection về pool ngay.
- Ném lại exception từ trong một `@ExceptionHandler` **không** chuyển tiếp sang handler khác: nó
  thành 500. Audit cho lần bị từ chối phải ghi ở chỗ ném, không phải ở một advice.

### 6. Hàng đợi: cũ nhất trước, và gồm cả `REFUNDING`

Thứ tự đọc đúng của hàng đợi là thứ tự nó phải được xử lý — ở đây mỗi phút trôi qua là một phút
khách hàng mất tiền mà không có gì. `REFUNDING` vào hàng đợi dù máy đang tự xử lý: đứng quá lâu ở
đó là một sự cố (runbook `topup-stuck.md` mục E), và người trực cần thấy nó trước khi nó thành
`REFUND_NO_RESPONSE`.

Index riêng phần `idx_transactions_needs_review` (V5) phục vụ đúng câu này. Không có nó, hàng đợi
quét toàn bảng `transactions` — bảng chỉ mọc thêm.

## Consequences

**Được:**
- Người trực nhìn được hàng đợi và đủ ngữ cảnh saga để phân loại theo runbook mà không mở `psql`.
- Khu vực vận hành có một hàng rào ở biên, thay vì mỗi service một cách so chuỗi.
- Có chỗ để PR2 ghi dấu vết thao tác đụng tiền — ô DoD cuối của Phase 2.

**Mất / còn mở:**
- **Chưa xử lý được gì.** PR1 chỉ đọc; mọi thao tác chữa cháy vẫn là SQL tay theo runbook, vẫn không
  có dấu vết. Ô DoD chỉ đóng khi PR2 xong.
- **Chưa có đường ghi nhận lần hoàn tay qua portal** mà ADR-020 (*Consequences*, *Amendments* B3)
  đang nợ. Thuộc PR2.
- **Ca "cần người xem" của bank-adapter không nằm trong hàng đợi**: adapter chưa bao giờ ghi
  `MANUAL_REVIEW`, các ca đó chỉ là metric + log (`expired_unconfirmed`, `refund_conflict`...).
  Roadmap §2.3 có nhắc "bảng MANUAL_REVIEW để 2.5" — chưa làm, và cần một quyết định riêng về việc
  adapter có nên có trạng thái đó không.
- **Raw response của provider và bút toán chưa hiện ra** (PR3) — runbook vẫn là đường lấy chúng.
- **IP ghi vào audit là IP của gateway**, không phải của người vận hành. Cùng mục còn mở với
  `clientIp` của lệnh nạp (ADR-018): phải chốt số proxy tin cậy trước.
- Chưa có màn hình tra cứu audit — bảng ghi vào thì có, đọc ra thì chưa.
- **Audit của `LIST_MANUAL_REVIEW` chỉ ghi số dòng trả về**, không ghi đã nhìn thấy ca của những
  khách nào. Người kiểm sổ vì thế trả lời được "ai đã mở hàng đợi" nhưng không trả lời được "ai đã
  nhìn thấy dữ liệu của khách X". Đủ cho PR1; PR2 nên cân nhắc ghi danh sách id.
- **`actor_email` là dữ liệu cá nhân trong một bảng không xoá được.** Một yêu cầu xoá dữ liệu cá
  nhân sẽ không thực hiện được trên bảng này. Lựa chọn thay thế (chỉ giữ `actor_id`, join lúc đọc)
  phải cân với việc email là thứ duy nhất còn đọc được nếu tài khoản đã bị xoá khỏi `users`.
- **Danh sách vai sống ở bốn nơi** — `JwtAuthenticationFilter.OPERATOR_ROLES`, `Operator.MAY_READ`,
  `ERole`, và CHECK của auth V3 — không nơi nào là nguồn. Lệch nhau là một cánh cửa nửa mở (gateway
  cho vào, service không biết, hoặc ngược lại). Gateway không kéo được class có JPA, nhưng
  `AuthHeaders` cho thấy một hằng thuần đặt ở common-lib được.
- **Chưa có con trỏ phân trang**: chỉ `min(limit, 200)` ca đầu là với tới được. Cũ-nhất-trước làm
  cho 200 ca đó đúng là 200 ca cần xử lý trước, nên chấp nhận được — nhưng không có đường đọc hết
  một hàng đợi dài hơn thế.
- **`Operator` lấy tenant từ `TenantContext` (ThreadLocal) chứ không mang tenant trong chính nó.**
  Đúng hôm nay vì mọi đường đều là HTTP có `TenantFilter`. PR2 sẽ gọi audit từ luồng khác (Kafka,
  job nền) và lúc đó giá trị vô hình này là mắt xích đứt — nên đưa `tenantId` vào `Operator`.
- **Hàng đợi sắp theo `created_at` nhưng hiển thị `stuckForSeconds` tính từ `updated_at`**: một ca
  tạo 3 ngày trước vừa rơi vào `MANUAL_REVIEW` một phút trước vẫn đứng trên ca đã kẹt một giờ. Đây
  là chủ ý (đo "khách mất tiền bao lâu" chứ không phải "kẹt bao lâu"), nhưng nếu màn hình trình bày
  `stuckForSeconds` như độ ưu tiên thì hai con số đang nói hai chuyện khác nhau.

---

## Amendments — 2026-10-23 (2.5 PR2)

### A1. REVERSAL bị hoãn, và vì sao nó không phải ca chính

Roadmap mô tả `POST /resolve` là "ghi `REVERSAL` **hoặc** đánh dấu đã xử lý", như thể REVERSAL là ca
chính. Đối chiếu từng `failure_code` đang thực sự được ghi thì ngược lại: **REVERSAL chỉ áp dụng cho
đúng một ca** (`REFUND_POSTED_CONFLICT`). Mọi ca còn lại — ledger từ chối (`ACCOUNT_CLOSED`,
`ACCOUNT_FROZEN`, `NOT_FOUND`), `BANK_AMOUNT_MISMATCH`, `BANK_NO_RESPONSE`, `BANK_RESULT_CONFLICT`,
`REFUND_DECLINED`, `REFUND_NO_RESPONSE`, `SAGA_STATE_DESYNC` — **không có bút toán nào để đảo**:
ledger từ chối *trước khi* ghi, hoặc lệnh ghi sổ chưa từng được gửi.

Và ca duy nhất áp dụng được thì hiện làm chưa được: `entryId` không nằm ở txndb (`SagaService`
nhận nó trong `LedgerPostedEvent` rồi bỏ đi), còn quyết định đúng — đảo hay nhận bút toán — phụ
thuộc lần hoàn ở bank-adapter đã xong chưa, dữ liệu thuộc railsdb (PR3). Nên PR2 làm phần thay được
SQL tay, REVERSAL chờ PR3.

### A2. 4-eyes chỉ cho thao tác ĐỤNG TIỀN

`MARK_REFUNDED_MANUALLY` cần hai người: `OPS` hoặc `ADMIN` đề xuất, một `ADMIN` **khác** duyệt. Mở
lại một ca kẹt không nằm trong tập này — ledger idempotent theo `transactionId` nên lệnh gửi lại
không ghi bút toán thứ hai, và bắt nó chờ người thứ hai lúc 2 giờ sáng là đổi một rủi ro thật lấy
một thủ tục.

Bốn mắt được cưỡng chế ở **ba tầng**, cố ý: kiểm ở `ResolutionService`, điều kiện
`maker_id <> :checkerId` của câu UPDATE, và `CHECK ck_resolution_four_eyes` ở DB. Một thao tác đụng
tiền không được phép chỉ dựa vào một lớp kiểm tra.

Còn mở: **4-eyes không chặn được một người có hai tài khoản.** Đó là vấn đề quản trị danh tính, nằm
ngoài tầm với của code ở đây — nhưng đừng tưởng bảng này giải quyết nó.

### A3. `resolved_at` — dấu bền thứ hai, cùng hình dạng với hàng rào của 2.4b

Sau khi chốt, giao dịch về `FAILED`. Một `LedgerPosted` tới muộn rơi vào nhánh "ledger thắng" của
`SagaService` và đẩy nó thành `COMPLETED` — ví được cộng **trong khi** tiền đã hoàn tay, và giao
dịch trông như một lần nạp thành công nên không ai nhìn lại. Ca này có thật với lời từ chối **trước**
ledger V6: lúc đó chưa có `rejected_topups`, nên lệnh ghi sổ gửi lại vẫn ghi được sau khi ví mở lại.

`transactions.resolved_at` (V6) là dấu bền cho ca đó, đặt trên chính hàng mà `SagaService` đã khoá
và đọc sẵn — kiểm thêm một cột là miễn phí, còn đọc một bảng khác là một lượt đi về DB cho cả đường
chuyển tiền. Đây là hàng rào thứ ba cùng khuôn: `rejected_topups` (ledger, ADR-020 §1),
`current_step = BANK_REFUND` (saga, ADR-020 *Amendments* A1), và nay `resolved_at`.

### A4. Audit của thao tác đụng tiền: `MANDATORY`, không `REQUIRES_NEW`

`AuditWriter.writeInCurrentTransaction` dùng `MANDATORY` — không có transaction đang mở thì ném.
Chiều nguy hiểm **không phải** "audit hỏng thì tiền vẫn ghi" (với `REQUIRES_NEW` thì exception vẫn
kéo đổ transaction ngoài, nên chiều đó an toàn ở cả hai), mà là chiều ngược lại: dấu vết commit độc
lập rồi tiền rollback, để lại một dòng audit nói "đã duyệt và thực thi" cho việc chưa từng xảy ra.
**Sổ kiểm toán nói dối tệ hơn sổ kiểm toán thiếu.**

Hệ quả cho việc viết test: một test kiểu "audit hỏng ⇒ tiền rollback" **xanh với cả hai
propagation**, nên nó không ghim được thiết kế này. Thứ ghim được là gọi thẳng
`writeInCurrentTransaction` ngoài mọi transaction và đòi nó ném.

### A5. Chốt một ca là đẩy nó từ nhánh AN TOÀN sang nhánh NGUY HIỂM — hàng rào phải ở mọi handler

Bản đầu của PR2 chỉ đặt `resolved_at` ở `onPosted`. Review độc lập tìm ra chỗ đó là chưa đủ, và lý
do đáng ghi lại vì nó đúng loại sai lầm dễ lặp:

Trước PR2, các ca này nằm ở `MANUAL_REVIEW` — trạng thái mà **mọi** handler của saga bỏ qua an toàn
(`case MANUAL_REVIEW -> log.debug`). Việc chốt chuyển chúng sang `FAILED`, và `FAILED` là trạng thái
có nhánh xử lý thật ở hai chỗ: `onPosted` cho "ledger thắng" ⇒ `COMPLETED`, và `onBankCompleted` gọi
`startRefund` ⇒ **gửi lệnh hoàn THỨ HAI cho số tiền vừa được hoàn tay qua portal**. Tức là bản thân
việc chốt — thứ sinh ra để làm ca an toàn hơn — lại mở ra một đường mất tiền mới.

Nên `alreadyResolved()` được gọi ngay sau khi khoá ở **cả bốn** handler (`onPosted`,
`onBankCompleted`, `onBankFailed`, `onRejected`). Bất biến: **saga không bao giờ ghi tiếp vào một ca
đã được người vận hành chốt**, bất kể event nào tới và ca đang ở trạng thái nào.

`transactions.resolved_at` còn được trigger `trg_transactions_resolved_at_final` giữ: đặt được một
lần, không gỡ và không sửa. Gỡ nó bằng một câu `UPDATE` là mở lại đúng cửa hoàn-tiền-hai-lần — và
"sửa bằng SQL tay, không để lại dấu vết" chính là vấn đề mở đầu của ADR này.

### A6. Chỉ lệnh NẠP mới đánh dấu "đã hoàn tay" được

`MARK_REFUNDED_MANUALLY` đòi `type = TOPUP` và có `provider_ref`. Chuyển tiền không có provider nào
để hoàn qua portal, nên đánh dấu như vậy cho một `TRANSFER` (ca `LEDGER_NO_RESPONSE` là có thật) vừa
là lời khai sai, vừa nguy hiểm hơn thế: `resolved_at` sau đó chặn vĩnh viễn mọi `LedgerPosted` tới
sau, nghĩa là một quyết định vận hành **phủ quyết sổ cái** — ngược hẳn bất biến §1.

### A7. Còn mở sau PR2

- **`findPending` chưa có endpoint** "chờ tôi duyệt" — bảng ghi được, nhưng người duyệt phải biết
  `requestId` từ nơi khác. Thêm cùng màn hình tra cứu audit.
- **Lần DUYỆT thất bại (`CASE_NO_LONGER_OPEN`) không để lại dấu vết**: nó ném trong transaction nên
  cuộn luôn dòng audit. Một ADMIN cố duyệt một ca đã đóng là thông tin kiểm toán — nên ghi qua
  `AuditService` (nuốt lỗi) ở chỗ ném, cùng khuôn `ADMIN_ACCESS_DENIED`.
- **Hai POST không có `Idempotency-Key`**: khoá tự nhiên đủ để không làm hai lần, nhưng một lần gửi
  lại do timeout trả 409 và client không phân biệt được "tôi vừa thắng" với "người khác đã quyết".
- **`resolution_requests` không append-only**: `APPROVED → PENDING` bằng SQL tay vẫn làm được. Khác
  `audit_events` (có trigger) và khác `resolved_at` (nay có trigger). Cân nhắc khi bảng này trở
  thành căn cứ đối soát ở Phase 3.
- **`ck_resolution_decided` chưa đòi `checker_role IS NOT NULL`** khi đã quyết, và chưa cấm điền
  `checker_*` lúc còn `PENDING`.

## Alternatives rejected

- **Một `admin-service` riêng:** mọi dòng của hàng đợi thành một lời gọi mạng, cộng một module mới,
  cho thứ chưa có nghiệp vụ riêng nào. Tách khi nó thật sự có.
- **Dồn audit về auth-service:** một nơi tra cứu duy nhất, nhưng audit thành lời gọi mạng — mạng
  hỏng là mất dấu vết, và PR2 không thể đặt nó cùng transaction với việc ghi tiền.
- **Kiểm vai bằng Spring Security trong từng service:** đúng về lâu dài, nhưng hôm nay không service
  nào có Spring Security và mô hình tin cậy là "gateway verify một lần" (ADR-010). Thêm nó cho một
  endpoint là nửa vời ở cả hai phía; nó thuộc về Phase 4 cùng mTLS.
- **Để hàng đợi tự đọc chéo DB:** nhanh hơn và phá ADR-001. Một truy vấn xuyên DB là một chỗ mà
  schema của service khác trở thành API của nó.
