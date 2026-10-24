# ADR-017 — VNPay sandbox, hạn phiên của lần thu PENDING, che dữ liệu nhạy cảm trong log

**Trạng thái:** Accepted · **Ngày:** 2026-10-18 · **Milestone:** Phase 2 — 2.2b

## Context

ADR-016 chốt mô hình thu (chuyển hướng + xác nhận bất đồng bộ) và chọn VNPay sandbox cho 2.2b. ADR đó để
lại ba việc phải chốt trong 2.2b:

1. **Lần thu `PENDING` không có trạng thái kết thúc.**
   - Phiên bị người dùng bỏ dở được hỏi lại mỗi 30 giây, mãi mãi.
   - VNPay không có phiên phía server cho tới khi người dùng mở URL, nên "không tìm thấy giao dịch" là câu
     trả lời bình thường.
2. **Hàng lệch số tiền** (`ProviderAmountMismatch`) bị hỏi lại mỗi 30 giây, kèm một dòng log lỗi mỗi lần.
   Phải xử lý trước khi bật saga top-up (2.4).
3. **Masking log** (roadmap 2.2): chưa có cấu hình logback nào trong repo.

Thêm một khoảng trống phát hiện khi lập bản đồ code: VNPay **bắt buộc** `vnp_IpAddr` (IP người dùng),
nhưng lệnh thu `txn.command.bank-charge.v1` không mang IP.

## Decision

### 1. VNPay: URL ký tại chỗ, kết quả chỉ qua `querydr` đã kiểm chữ ký

- **Tạo phiên không gọi mạng.** URL thanh toán được dựng và ký HMAC-SHA512 tại adapter.
  - `vnp_TxnRef` = idempotency key bỏ dấu gạch (32 ký tự hex), nên gửi lại lệnh ra cùng mã tham chiếu.
  - `vnp_CreateDate` lưu vào `provider_created_at`: `querydr` phải gửi lại đúng mốc này
    (`vnp_TransactionDate`), sai một giây là "không tìm thấy".
  - Số tiền: `vnp_Amount = amountMinor × 100`; chỉ nhận VND. Loại tiền khác thì bị từ chối
    (`CURRENCY_NOT_SUPPORTED`); quá trần `vnp_Amount` thì `AMOUNT_LIMIT_EXCEEDED`.
- **Kết quả chỉ qua `querydr`** (ADR-016: webhook/return URL chỉ là gợi ý, IPN ở 2.3).
  - Câu trả lời phải **qua kiểm chữ ký**, đúng `vnp_Command`, đúng `vnp_TmnCode`, đúng `vnp_TxnRef` thì mới
    được đọc nghĩa. Sai **hoặc thiếu** bất kỳ cái nào thì coi là `ProviderUnavailableException`: câu trả lời
    giả mạo nói "đã thu" là tạo tiền từ không khí.
  - **Mọi điều kiện fail-closed, kể cả cho `vnp_ResponseCode=91`** (cập nhật 2026-10-19, xem *Amendment 1*).
- **Ánh xạ thận trọng:**

| VNPay | Trạng thái provider | Lý do |
|---|---|---|
| `00` / giao dịch `00`, loại `01` (thanh toán) | `SUCCEEDED` (số tiền = `vnp_Amount/100`, đối chiếu như ADR-016 §6b) | đã thu |
| `00` / giao dịch `00`, loại `02`/`03` (hoàn tiền) | `UNRESOLVED` | "thành công" của một lần HOÀN, mang số tiền gốc — cộng ví là cộng tiền đã trả lại |
| `00` / giao dịch `01`, `02` | `PENDING` — chưa thu | chưa trả xong / lần trả hỏng; phiên còn hạn thì người dùng vẫn trả lại được, nên **không** kết luận thất bại sớm |
| `00` / giao dịch khác (`04` đảo, `07` nghi gian lận, `05/06/09` hoàn) | `UNRESOLVED` | tiền có thể đã rời tài khoản người dùng |
| `91` không tìm thấy | `PENDING` — chưa thu | chưa ai mở URL |
| mã khác, sai chữ ký, không đọc được | `ProviderUnavailableException` | "không biết" |

- **Cấu hình không có mặc định:**
  - `VNPAY_TMN_CODE`, `VNPAY_HASH_SECRET`, `VNPAY_PAY_URL`, `VNPAY_API_URL`, `VNPAY_RETURN_URL`,
    `VNPAY_SERVER_IP` đều bắt buộc khi `BANK_PROVIDER=vnpay`.
  - Dưới profile `prod`, URL chứa `sandbox` bị từ chối khởi động: tiền "trả" ở sandbox là tiền giả mà saga vẫn
    cộng tiền thật (cùng lý do với mock, ADR-016 §6).
  - Dưới profile `prod`, URL phải là `https://`. Chữ ký không chống được việc phát lại một câu trả lời
    "chưa thu" CŨ nhưng thật của cùng giao dịch; chỉ TLS ngăn kẻ đứng giữa ép một lần thu đã trả thành
    `EXPIRED`.
  - Timeout HTTP client (connect 2 s, read 3 s) phải ngắn hơn `resilience.timeout` (5 s), kiểm lúc khởi động.
- **Tạo URL không đi qua circuit breaker** (`PaymentProvider.chargeUsesNetwork() = false`). Không có I/O nào
  để bảo vệ. Nếu đi qua, một đợt `querydr` lỗi sẽ mở mạch và chặn luôn mọi lần tạo URL nạp tiền (lệnh thử lại
  rồi vào DLT vô cớ).
- **Phiên chỉ được hỏi dưới đúng tài khoản merchant đã tạo nó** (cột `provider_account` = `vnp_TmnCode`).
  - Lý do: hỏi bằng tài khoản khác (sau khi đổi `VNPAY_TMN_CODE`) thì VNPay trả `91`, đúng chữ ký. Lần thu đã
    trả tiền sẽ thành `EXPIRED`.
  - Cách làm: job đối chiếu lọc theo tài khoản, và `refresh` từ chối hàng của tài khoản khác. Hàng bị bỏ lại hiện
    ở gauge quá hạn.
  - Hệ quả vận hành: phải xả hết hàng `PENDING` trước khi đổi tài khoản merchant (runbook).

### 2. `clientIp` tuỳ chọn trong lệnh thu

- Thêm field optional `clientIp` vào `txn.command.bank-charge.v1` (thêm field, vẫn `.v1`). Saga 2.4 lấy IP từ
  gateway.
- Thiếu, hoặc không vừa khuôn `[7,45]` ký tự, thì dùng `VNPAY_SERVER_IP`.
- IP **không** thuộc danh tính của lần thu: gửi lại khác IP vẫn là cùng một lần thu.
- Hiện chưa producer nào gửi `clientIp` (saga top-up là 2.4), nên mọi URL VNPay lúc này mang
  `VNPAY_SERVER_IP`.
- IP là dữ liệu cá nhân. Nó nằm ở:
  - `raw_request` (bằng chứng đối soát);
  - URL thanh toán có chữ ký (`vnp_IpAddr`), tức cả cột `payment_url`, payload outbox, topic
    `bank.event.charge-pending.v1` và nơi transaction-service lưu URL đó.
  - Không ở log (bộ che không che IP; code không log IP) và không ở `raw_response`.
  - Chính sách lưu giữ cho các cột/bảng này thuộc roadmap 5.4.

### 3. Hạn phiên: chỉ kết thúc khi provider khẳng định chưa thu

Chốt ngày 2026-10-18:
- **Hạn phiên 15 phút** (`walletflow.bank.session.ttl`, gửi VNPay qua `vnp_ExpireDate`).
- **Grace 15 phút** (`session.expiry-grace`), che độ trễ giữa lúc ngân hàng trừ tiền và lúc provider ghi nhận.

Lần thu sang `FAILED(EXPIRED)` khi **cả hai** cùng đúng:
1. `expires_at <= now() - grace`, so trong chính câu `UPDATE` bằng `now()` của DB. `expires_at` do app tính
   (đồng hồ app, cùng mốc gửi cho provider), nên lệch đồng hồ app/DB làm mốc hết hạn xê dịch đúng bằng độ
   lệch. Grace 15 phút nuốt độ lệch đó.
2. provider **vừa** trả lời `PENDING`, tức khẳng định chưa thu đồng nào.

**Hỏi xác nhận sau khi hết hạn — chỉ phát hiện, không sửa:**
- Kết luận `EXPIRED` dựa trên một câu trả lời "chưa thu" tại một thời điểm, nhưng tiền vẫn có thể về muộn
  (ngân hàng chậm). Vì vậy hàng `EXPIRED` giữ `next_check_at = now() + expired-confirm-after` (1 giờ), và
  job đối chiếu hỏi provider thêm một lần.
- Provider nay báo `SUCCEEDED` hoặc `UNRESOLVED`: tăng metric `walletflow.bank.expired_needs_review`, log
  lỗi, alert `ExpiredChargeNeedsReview` (page). Trạng thái **không** bị đổi tự động: event `failed` đã phát,
  saga đã kết thúc; sửa là việc của người (2.5).
- Provider không trả lời: hỏi lại sau `max-recheck-after`. Có câu trả lời thì thôi hỏi.
- Ca đua (provider báo đã thu đúng lúc luồng khác vừa đánh hết hạn) cũng đếm vào metric này.
- Tiền về sau lần xác nhận (hơn 1 giờ) chỉ được bắt bởi đối soát 3 tầng (roadmap 3.1). Đây là rủi ro còn
  lại đã chấp nhận.

Ngược lại:
- Provider không trả lời được, hoặc trả lời `UNRESOLVED`: **không bao giờ** tự hết hạn, dù quá hạn bao lâu.
  "Không biết" không bao giờ thành "thất bại" (§5.7).
- Provider báo `SUCCEEDED` sau hạn thì thành công vẫn thắng.

`EProviderState` có thêm `UNRESOLVED`. Ranh giới `PENDING`/`UNRESOLVED` chính là ranh giới "được phép tự hết
hạn" / "không bao giờ".

### 4. Backoff cấp số và metric quá hạn

- `next_check_at = now() + min(max-recheck-after, recheck-after × 2^check_attempts)`, tức 30 s, 1 m, 2 m,
  4 m, 8 m, rồi 10 m. `check_attempts` reset khi sang `PENDING`.
  - Áp cho **mọi** hàng, kể cả hàng lệch số tiền: đóng mục 2 của Context mà không cần trạng thái mới trước
    2.5.
  - Kết quả tới nhanh là việc của webhook (2.3), không phải của nhịp hỏi.
- Gauge `walletflow.bank.pending_overdue` = số hàng `PENDING` đã quá hạn + grace, cập nhật cuối mỗi lượt
  đối chiếu. Hàng như vậy lẽ ra đã kết thúc; còn lại tức là provider không trả lời, lơ lửng, lệch số tiền,
  hoặc thuộc provider cũ. Có alert `PendingChargeOverdue` (ticket).
- Vòng hỏi **xác nhận** sau `EXPIRED` có trần `max-confirm-attempts` (cập nhật 2026-10-19, xem *Amendment 2*).

### 5. Masking log ở common-lib, bật cho bank-adapter

- `SensitiveDataMasker` che các thứ sau:
  - `vnp_SecureHash`, `*secret`, `*password`, `*token`, `api_key` (cả `k=v` lẫn JSON);
  - `Bearer …` và JWT trần;
  - số thẻ 13–19 chữ số **qua kiểm tra Luhn** (giữ 4 số cuối).
- Converter `%maskedMsg` / `%maskedEx` và file `walletflow/logging/masking-console.xml` (appender CONSOLE
  của Boot với message + stack trace đã che).
- 2.2b chỉ bật cho bank-adapter (`logback-spring.xml`). Các service khác bật ở một PR sau.
- Đây là lưới an toàn, không thay quy tắc "không log bí mật".
- `VnPaySettings.toString()` cố ý không in hash secret.

## Hệ quả

- Migration `V2__pending_charge_lifecycle.sql` (expand):
  - cột `provider_created_at`, `expires_at`, `check_attempts`, `provider_account`;
  - index `(expires_at) WHERE status='PENDING'` và `(next_check_at) WHERE status='FAILED' AND next_check_at
    IS NOT NULL`.
  - Hàng 2.2a có `expires_at` và `provider_account` NULL, nên không bao giờ tự hết hạn và được coi là cùng
    tài khoản (phía an toàn; chỉ có hàng mock).
- `PaymentProvider`:
  - `getStatus(providerRef, sessionCreatedAt)`, `account()`, `chargeUsesNetwork()`;
  - `ChargeResult.Created` mang mốc tạo và hạn phiên, cả hai bắt buộc.
- `bank.event.failed.v1` có thêm giá trị `failureCode = EXPIRED`. Saga 2.4 coi đó là "người dùng không trả
  tiền", không bù trừ gì.
- **Rủi ro còn lại, phải kiểm tay trên sandbox thật** (CI không gọi ra mạng; `FakeVnPayServer` ký độc lập
  nhưng vẫn do mình viết):
  - Tài liệu VNPay liệt kê chuỗi ký câu trả lời `querydr` thiếu một dấu `|` giữa `vnp_Amount` và
    `vnp_BankCode`. Code giả định có dấu như mọi field khác.
  - Mã `91` có kèm chữ ký hay không. Nếu không, mọi phiên bỏ dở sẽ đứng `PENDING` (alert quá hạn kêu) chứ
    không hết hạn sai.
  - **Mã `91` có echo lại `vnp_TxnRef` hay không** — quan trọng nhất trong danh sách này sau *Amendment 1*.
    Nếu KHÔNG echo, mọi phiên bỏ dở sẽ đứng `PENDING` mãi và `PendingChargeOverdue` kêu đều: ồn ào nhưng
    không mất tiền. Phải kiểm trước khi bật `BANK_PROVIDER=vnpay` ngoài CI; nếu đúng là không echo thì mở
    lại quyết định này, đừng nới điều kiện kiểm.
  - VNPay có echo `vnp_RequestId` của request vào `vnp_ResponseId` không. Nếu có thì nên kiểm thêm để chặn
    phát lại một câu trả lời CŨ nhưng thật của chính giao dịch đó (xem *Rủi ro còn lại*).
  - `vnp_TransactionType` của giao dịch thanh toán đúng là `01` trong câu trả lời `querydr`. Nếu sandbox trả
    khác, mọi lần thu thành công sẽ thành `UNRESOLVED` (alert quá hạn kêu, không mất tiền).
- Alert mới: `PendingChargeOverdue` (ticket) và `ExpiredChargeNeedsReview` (page). Với backoff, một hàng lỗi
  vĩnh viễn chỉ sinh ~3 lần hỏi lỗi mỗi 30 phút, nên `BankStatusCheckFailing` (>10/30 m) không còn bắt được
  từng hàng kẹt; `PendingChargeOverdue` là lưới đỡ cho ca đó.
- IPN/return URL của VNPay vẫn chưa có endpoint (2.3). Trong 2.2b, kết quả chỉ tới qua job đối chiếu.

## Alternatives rejected

- **Ánh xạ `TransactionStatus=02` thành `FAILED` ngay:** báo thất bại cho một phiên còn hạn, khi người dùng có
  thể trả lại. Chờ hết hạn chỉ làm kết quả "thất bại" tới trễ tối đa ~30 phút, không bao giờ sai.
- **Tự hết hạn theo đồng hồ, không hỏi provider:** người dùng trả tiền ở giây cuối, ngân hàng ghi nhận
  chậm, thì ta báo thất bại cho tiền đã thu. Chỉ câu trả lời của provider mới kết thúc được lần thu.
- **Trạng thái `MANUAL_REVIEW` ngay ở 2.2b để ngừng hỏi hàng lệch số tiền:** cần quy trình người xử lý (2.5).
  Backoff + alert đủ cho tới lúc đó.
- **Mã hoá ngày tạo vào `providerRef`** để không đổi interface: gói ngữ nghĩa riêng của VNPay vào một mã mà
  2.3 (IPN) sẽ dùng để tra cứu, và phụ thuộc giới hạn độ dài `vnp_TxnRef`.
- **Tự lật `EXPIRED` thành `SUCCEEDED` khi tiền về muộn:** event `failed` đã phát và saga đã kết thúc. Lật lại
  ở adapter tạo ra hai kết quả mâu thuẫn cho cùng một giao dịch mà không ai bên dưới được thiết kế để xử lý.
  Phát hiện + page, rồi người xử lý (2.5).
- **WireMock cho test:** thêm dependency. Một fake trên `HttpServer` của JDK, tự ký và kiểm chữ ký độc lập với
  code chính, kiểm được nhiều hơn.
- **Bật masking cho mọi service ngay:** diff rộng; bank-adapter là nơi có bí mật của bên thứ ba.

## Amendments

Hai lỗ hổng do review độc lập của PR #29 phát hiện (sau khi #29 đã merge), sửa ở PR tiếp theo. Cả hai đều là
**siết chặt** quyết định cũ, không đảo ngược nó.

### Amendment 1 (2026-10-19) — kiểm câu trả lời `querydr` fail-closed

**Vấn đề.** Bản đầu bỏ qua kiểm `vnp_TxnRef` khi field đó rỗng/thiếu, và nhánh `vnp_ResponseCode=91` trả
`PENDING` **trước** mọi kiểm tra khác. Mà `PENDING` là căn cứ DUY NHẤT để `markExpired` kết thúc một lần thu.
Hệ quả: một câu trả lời `91` ký đúng nhưng **không gắn với giao dịch nào** (phát lại, hoặc proxy trả bừa) ép
được một lần thu người dùng ĐÃ TRẢ TIỀN sang `FAILED(EXPIRED)` — tiền nằm ở tài khoản merchant, ví không
được cộng. Ngoài ra câu trả lời của **lệnh khác** (`refund`) ký bằng cùng khoá cũng được chấp nhận làm bằng
chứng thu tiền thành công.

**Quyết định.** `verify()` bắt buộc khớp `vnp_Command = querydr`, `vnp_TmnCode`, `vnp_TxnRef` cho **mọi** câu
trả lời, không có ngoại lệ cho `91`. Thiếu field cũng là sai. Không khớp ⇒ `ProviderUnavailableException`
("không biết"), không bao giờ là một kết luận.

**Phạm vi — đọc kỹ.** Việc này chỉ chặn câu trả lời **không gắn với `providerRef` đang hỏi**. Một câu `91`
THẬT của chính ref đó, chụp lại trước khi người dùng trả tiền rồi phát lại sau, vẫn qua được mọi kiểm tra:
không có đối chiếu `vnp_RequestId`/`vnp_ResponseId` và không có mốc tươi. Đó vẫn là rủi ro còn lại, chỉ TLS
chặn (xem *Rủi ro còn lại*) — đừng đọc Amendment này thành "đã đóng hẳn đường phát lại".

**Đánh đổi đã chấp nhận.** Nếu VNPay thật không echo `vnp_TxnRef` khi trả `91`, phiên bỏ dở sẽ không bao giờ
tự hết hạn mà đứng `PENDING` cho tới khi `PendingChargeOverdue` kêu. Đó là hướng an toàn: ồn ào thay vì mất
tiền. Việc kiểm tay trên sandbox nằm ở *Hệ quả → phải kiểm tay*.

**Test chứng minh.** `VnPayChargeIT.tra_loi_91_thieu_ma_tham_chieu_khong_duoc_lam_het_han` (trước khi sửa:
hàng ra `FAILED`) và `VnPayChargeIT.tra_loi_cua_lenh_khac_khong_duoc_tin` (trước khi sửa: hàng ra `SUCCEEDED`).

### Amendment 2 (2026-10-19) — trần số lần hỏi xác nhận sau `EXPIRED`

**Vấn đề.** Vòng hỏi xác nhận chỉ dừng khi provider trả lời được. Provider trả mã lạ / sai chữ ký cho một ref
vĩnh viễn thì hàng bị lùi hạn thêm `max-recheck-after` rồi quay lại lô, **mãi mãi**. Hàng đã `FAILED` nên
`countOverdue` không đếm và `PendingChargeOverdue` không kêu — một hàng dội provider vô hạn mà không ai thấy.

**Quyết định.** Thêm `walletflow.bank.status-check.max-confirm-attempts` (mặc định 5). Hết lượt thì
`next_check_at = NULL` (rơi khỏi lô vĩnh viễn) và **counter** `walletflow.bank.expired_unconfirmed` tăng →
alert `ExpiredChargeUnconfirmed` (ticket).

- **Cột riêng `confirm_attempts`** (`V3__confirm_attempts.sql`, EXPAND-only), không dùng lại `check_attempts`:
  - Hàng đã `EXPIRED` từ trước mang `check_attempts` tích luỹ của giai đoạn `PENDING` (6–8 lượt là thường).
    Dùng chung cột thì chúng vượt trần ngay lần hỏi đầu — mất cơ hội phát hiện tiền về muộn và bắn alert giả.
    Cột mới mặc định 0 nên hàng cũ nhận đủ hạn mức, **không cần backfill**.
  - Hàng cũ đã xác nhận xong có `next_check_at` NULL và `check_attempts` lớn: dùng chung cột thì không phân
    biệt được "đã xác nhận" với "hết lượt".
  - Lúc rolling deploy, bản app cũ không bao giờ ghi cột này nên không làm hỏng bộ đếm của bản mới.
- **Counter chứ không gauge.** Gauge đếm trạng thái tích luỹ sẽ kêu mãi không tắt được: không có luồng nào
  đưa một hàng đã hết lượt ra khỏi điều kiện đếm, nên alert thứ hai không phân biệt được với alert thứ nhất
  và cả alert sẽ bị mute. Counter + `increase(...[1h])` tự giải, đồng bộ với `expired_needs_review`.
- **Chốt hạn mức SAU khi đã hỏi xong cả lô**, và chỉ chạm hàng còn `next_check_at`: hàng vừa nhận được câu
  trả lời ở đúng lượt chót đã được `finishExpiredConfirmation` đưa về NULL nên không bị tính oan là "không
  xác nhận được". Đây là ca biên thật, có test riêng.
- **Nhịp hỏi xác nhận giãn cấp số riêng** (`session.expired-confirm-after` → `max-confirm-recheck-after`,
  mặc định 1h → 16h), không dùng nhịp của `PENDING`. Với trần 5 lượt, cửa sổ phát hiện tiền về muộn kéo tới
  ~T+31h. Dùng nhịp `PENDING` (trần 10 phút) thì cửa sổ chỉ còn ~50 phút — hẹp hơn nhiều so với độ trễ chốt
  sổ thật của ngân hàng (thường tính bằng giờ tới T+1), tức là tự bịt mắt đúng thứ vòng này sinh ra để thấy.
- Khác `ExpiredChargeNeedsReview`: ở đó ta **biết** provider nói có tiền; ở đây ta **không biết gì**, và đã
  thôi hỏi. Chỉ đối soát tay với sao kê mới kết luận được (roadmap 3.1 tầng 3).

**Test chứng minh.** `PendingExpiryIT.vong_hoi_xac_nhan_co_tran_va_keu_khi_het_luot`,
`xac_nhan_duoc_o_luot_cuoi_thi_khong_keu` (ca biên), `hang_expired_cu_khong_bi_coi_la_het_luot` (hàng trước V3).

**Còn hở.** Hàng thuộc tài khoản merchant CŨ không bao giờ được `lockDueExpiredConfirmations` lấy (lọc
`provider_account`), nên `confirm_attempts` đứng ở 0 và chúng không vào counter này, cũng không vào
`countOverdue` (đã `FAILED`). Nhóm này vẫn chỉ được chặn bằng quy trình xả hàng trước khi đổi tài khoản
(runbook), chưa có alert. Ghi nhận, chưa sửa trong PR này.

### Amendment 3 (2026-10-24) — chaos test: provider chết 5 phút (DoD Phase 2)

Không đổi hành vi — chốt **tiêu chí** cho ô DoD "provider chết 5 phút → không mất tiền, không saga treo" và
ghi lại điều chaos test chứng minh được.

**Phạm vi thật.** Toxiproxy chỉ chặn được `querydr`, và đó cũng là lời gọi mạng DUY NHẤT tới VNPay: tạo phiên
ký URL tại chỗ (§1), hoàn tiền chưa gọi VNPay (ADR-020, 2.4c), IPN là VNPay gọi vào ta. `MockProvider` chạy
trong process nên không đặt proxy trước được; đường thu/hoàn của mock đã có `ProviderResilienceIT` và
`RefundBulkheadIsolationIT`.

**Tiêu chí (đã chốt với chủ dự án):**
- *Không mất tiền* = "không biết" không bao giờ thành `FAILED`/`EXPIRED`. Sống lại thì mỗi người đã trả tiền
  được **đúng một** `bank.event.completed` mang đúng số tiền, dù job đối chiếu và worker webhook đều chạy lại
  sau đó (tuần tự — ca hai đường chạy đua đúng cùng lúc thuộc `markSucceeded` "UPDATE trả 0 thì im lặng", chưa
  có chaos test riêng). Không lần thu nào mất lịch hỏi lại.
- *Không treo* = mọi saga có điểm kết thúc trong thời gian hữu hạn: trạng thái cuối, **hoặc**
  `MANUAL_REVIEW` có alert. Outage 5 phút phải tự hồi, không cần người.

**Điều đã chứng minh** (`ChaosProviderIT`, thời gian được tua bằng cách lùi `next_check_at`):
- Năm kiểu chết ở tầng TCP đều là "không biết": ngắt hẳn, nhận kết nối rồi im, reset, cắt ngang câu trả lời,
  chậm hơn read-timeout. Mỗi lời gọi bị chặn trước `resilience.timeout` nên thread đối soát không bị giữ.
- Mạch tự mở với đúng cửa sổ/ngưỡng production (chỉ rút `open-wait` cho test), rồi tự đóng lại khi
  provider ổn. Khi chập chờn, mạch đi đủ CLOSED→OPEN→HALF_OPEN→OPEN (lượt thăm dò gặp lúc vẫn chết)
  →HALF_OPEN→CLOSED, không lần thu nào bị chốt hai lần.
- Webhook đến lúc provider chết không bị bỏ: 5 phút chỉ tiêu ~5/10 lượt thử ở nhịp mặc định, và được xử lý
  (`RESOLVED`) khi provider sống lại. Outage dài hơn ~25′ thì webhook hết lượt (`ABANDONED`,
  alert `BankWebhookAbandoned`) — vô hại, job đối chiếu vẫn là lưới an toàn.
- Chết vắt qua hạn phiên + grace: hàng giữ `PENDING`, `PendingChargeOverdue` bật; sống lại thì người đã trả
  vẫn `SUCCEEDED` (thành công thắng dù quá hạn), người chưa trả mới `EXPIRED`.

**Mutation đã kiểm tay:** cho `VnPayProvider.getStatus` coi lỗi mạng là "chưa thu" → 7/8 ca đỏ. Ca quan trọng
nhất đỏ đúng chỗ: người đã trả tiền bị `EXPIRED` khi provider chết vắt qua hạn phiên.

**Phía saga — outage dài.** bank-adapter không phát gì khi provider chết, nên saga nạp ở `AWAITING_PAYMENT`
hết lượt sau ~55′ (`payment-deadline` 45′, rồi gửi lại sau 4′ và 6′ — nhịp `recordRetry` của ADR-019 §6)
→ `MANUAL_REVIEW(BANK_NO_RESPONSE)` + alert, không bao giờ `FAILED`
(`StuckSagaIT.lenh_nap_het_luot_o_buoc_thu_thi_chuyen_nguoi_xu_ly`). Nếu provider sống lại SAU mốc đó,
`completed` tới một saga đã ở diện người xử lý và bị bỏ qua có chủ ý (giữ `failure_code` gốc,
`SagaService.onBankCompleted`) — người trực xử lý theo `topup-stuck.md` mục C → A. Không mất trong im lặng,
nhưng là việc tay. Mốc tính từ lúc **tạo phiên**, không phải từ lúc provider chết: lần thu tự hồi nếu provider
sống lại trước ~45′ kể từ lúc tạo phiên (55′ trừ một nhịp hỏi tối đa `max-recheck-after` 10′). Phiên tạo ở
phút 0, người dùng trả tiền ở phút 14 đúng lúc provider chết tới phút 56: rơi vào nhánh này dù outage chỉ ~42′.

## Liên quan

- ADR-016 (mô hình thu), ADR-005 (tiền), ADR-009 (bí mật), roadmap 2.2–2.4, §5.6, §5.7.
- Runbook `payment-provider-down.md`; alert `PendingChargeOverdue`.
