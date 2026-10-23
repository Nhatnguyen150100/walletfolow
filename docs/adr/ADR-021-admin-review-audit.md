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
