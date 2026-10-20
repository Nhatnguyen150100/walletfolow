# ADR-018 — Nhận webhook/IPN: chữ ký là ranh giới tin cậy, webhook là gợi ý

**Trạng thái:** Accepted · **Ngày:** 2026-10-19 · **Milestone:** Phase 2 — 2.3a
· *Amendments 2026-10-20 (2.3b): worker xử lý + trang quay về — xem cuối file*

## Context

ADR-016 chốt mô hình thu: người dùng được chuyển hướng sang provider, rồi kết quả về theo hai đường —
webhook/IPN (nhanh) và job đối chiếu tự hỏi `getStatus` (chậm nhưng chắc). 2.2 đã có đường chậm. 2.3 thêm
đường nhanh.

Đây là endpoint HTTP **đầu tiên** của bank-adapter, và là endpoint **công khai** đầu tiên của toàn hệ
thống nằm ngoài auth: provider không có JWT của ta. Mọi thứ khác trong hệ thống đều dựa vào việc gateway
đã xác thực người gọi; ở đây không có gì như thế.

Thêm một ràng buộc từ thực tế VNPay: nó **không gửi event id** cho IPN. Không có trường nào trong payload
được tài liệu bảo đảm là duy nhất cho một lần gửi.

## Decision

### 1. Ranh giới tin cậy là CHỮ KÝ, không phải đường dẫn

- `verifyWebhook` nằm ở tầng provider, chạy **trước khi đọc bất cứ thứ gì** trong payload. Thiếu chữ ký,
  sai chữ ký, dựng lại không được chuỗi ký ⇒ từ chối. Không có ngoại lệ "trông có vẻ hợp lệ".
- Provider trong đường dẫn phải **khớp provider đang chạy**. Nếu không, một IPN gửi tới `/webhooks/vnpay`
  trong lúc hệ thống chạy `mock` sẽ được kiểm bằng khoá của mock.
- **Tham số lặp tên ⇒ từ chối.** Chọn bừa một giá trị là mở đường lách chữ ký: ký trên giá trị này, cho ta
  đọc giá trị kia.
- **Chuỗi ký phải encode cả tên lẫn giá trị.** Nối thô `k=v&k=v` thì một giá trị chứa `&` hoặc `=` dựng ra
  đúng chuỗi ký của một payload KHÁC: bắt được `?ref=A&state=PENDING` là phát lại được
  `?ref=A%26state%3DPENDING` với chính chữ ký cũ, trong khi `provider_ref` ghi vào DB thành thứ khác hẳn.
  Lỗ này từng có thật ở `MockProvider` và bị review bắt; nay cả hai provider dùng chung quy tắc encode của
  VNPay, có test `nhoi_dau_phan_cach_vao_gia_tri_khong_dung_lai_duoc_chu_ky`.
- **`vnp_TxnRef` phải đúng khuôn Alphanumeric ≤ 100** mới được dùng làm `providerRef`; sai khuôn thì coi như
  không có. Mã dài hơn cột `provider_ref VARCHAR(128)` sẽ thành lỗi ghi DB → 500 cho một request của người lạ.
- **Chữ ký sai thì KHÔNG ghi gì.** Ghi cả thứ chưa xác thực biến một endpoint ai cũng gọi được thành kênh
  bơm dữ liệu vào DB.

### 2. Webhook chỉ KÍCH HOẠT việc hỏi lại, không bao giờ là căn cứ ghi tiền

Nhắc lại ADR-016 §1 vì đây là chỗ dễ vi phạm nhất: chữ ký hợp lệ chỉ chứng minh *provider đã gửi cái này*,
không chứng minh *tiền đã về*. Một IPN thật, chụp lại và phát lại đúng lúc, vẫn là một IPN thật. Kết quả
cuối chỉ được chốt sau khi adapter tự `getStatus` (2.3b).

### 3. Khoá chống phát lại: SHA-256 của chuỗi đã ký

VNPay không có event id, nên khoá là `sha256(chuỗi provider đã ký)`, lưu ở
`webhook_events.provider_event_id` với `UNIQUE (provider, provider_event_id)`.

- IPN phát lại **y hệt** ⇒ cùng khoá ⇒ bỏ qua.
- IPN mang **trạng thái mới** (payload khác) ⇒ khoá khác ⇒ vẫn được xử lý. Đây là tính chất bắt buộc:
  webhook đến sai thứ tự và nhiều lần cho một giao dịch là chuyện bình thường, gộp chúng lại là bỏ mất
  kết quả cuối.
- Không chọn một trường của provider làm khoá (`vnp_TxnRef`, `vnp_TransactionNo`): không trường nào được
  bảo đảm duy nhất, và chọn sai thì hoặc dedup nhầm (mất state change thật) hoặc không dedup được.

### 4. Ghi trước, trả lời ngay, xử lý sau

`webhook_events` lưu nguyên văn payload rồi trả `200` **ngay**. Việc đọc nghĩa và gọi `getStatus` thuộc
worker bất đồng bộ (2.3b).

Lý do không xử lý tại chỗ: chốt kết quả cần một lời gọi mạng ra provider. Làm việc đó trong request của
webhook là nguyên nhân kinh điển của "provider retry 5 lần vì bạn trả lời chậm" — và mỗi retry lại là một
request nữa phải phục vụ.

### 5. Webhook đã qua chữ ký luôn trả 200, kể cả khi trùng

Với provider, "đã nhận" là thông điệp đúng; báo lỗi chỉ khiến nó gửi lại thứ ta đã có. VNPay đọc
`RspCode` trong thân JSON (`{"RspCode","Message"}`) — khuôn này của VNPay, nên đây là chỗ **duy nhất**
trong hệ thống không trả `ProblemDetail` (RFC 7807).

**Mã tham chiếu không tồn tại vẫn trả y hệt mã tham chiếu có thật**, và vẫn được ghi lại. Trả lời khác
nhau là để người ngoài dò xem lần thu nào có thật.

### 6. `tenant_id` nullable — ngoại lệ có chủ ý của ADR-010

Mọi bảng nghiệp vụ khác đều có `tenant_id NOT NULL`. Ở đây không thể: webhook tới từ Internet, không có
`TenantContext`, và tenant do bên ngoài gửi thì không bao giờ được tin. Tenant chỉ suy ra được bằng cách
tra `provider_ref` trong `provider_transactions` — việc của worker 2.3b. Ghi raw trước, phân giải sau.

### 7. Khoá ký của mock: không cấu hình thì TẮT

`walletflow.bank.mock.webhook-secret` không có giá trị mặc định (CLAUDE.md §6). Thiếu khoá ⇒ mock từ chối
mọi webhook. Không bao giờ là một khoá mặc định ai cũng đoán được, kể cả cho dev.

## Hệ quả

- Gateway có tiền tố công khai mới `/webhooks/**` (`JwtAuthenticationFilter.PUBLIC_PREFIXES`) + route
  `bank-webhooks` rate limit theo IP. Header danh tính client tự gửi vẫn bị xoá như mọi path công khai, và
  chặn path đi vòng (`/webhooks/../api/v1/...` ⇒ 400) vẫn chạy trước mọi so khớp tiền tố — cả hai đều có test.
- Metric `walletflow.bank.webhook.received{outcome=accepted|duplicate|rejected}` và gauge
  `walletflow.bank.webhook.unprocessed` (tồn đọng chưa xử lý).
- **Request bị từ chối log ở mức DEBUG, không WARN.** Endpoint công khai: mỗi request rác một dòng WARN là
  kênh bơm log cho người lạ và dìm chết tín hiệu thật. Nguồn alert là counter `{outcome=rejected}`.
- **Lỗi kỹ thuật ngoài dự kiến trả 500 nhưng vẫn đúng khuôn `{"RspCode","Message"}`** (`RspCode=99`), không
  phải ProblemDetail — nếu không, hợp đồng với VNPay bị phá đúng lúc hệ thống đang hỏng.
- `webhook_events` là bảng append-only trên thực tế: chỉ `processed_at` được ghi về sau.

### Phải kiểm tay trên sandbox trước khi bật `vnpay` ngoài CI

- VNPay IPN là GET hay POST, và chuỗi ký có đúng là query string đã sắp theo tên không (code giả định
  **giống hệt** cách dựng URL thanh toán — dùng chung một hàm `VnPaySigner.ipnSignedData`).
- VNPay có chấp nhận `RspCode=97` cho chữ ký sai, và có ngừng gửi lại khi nhận `00` không.

### Còn mở — biết và chấp nhận trong 2.3a

- ~~**Worker xử lý `webhook_events` chưa có**~~ — **đã làm ở 2.3b**, xem *Amendments* cuối file.
- **Rate limit theo IP sau LB gộp chung một khoá.** `ipKeyResolver` dùng `getRemoteAddress()` và gateway
  chưa có `XForwardedRemoteAddressResolver`; đặt sau ingress thì toàn bộ traffic webhook chung một bucket
  20/40, nên ai cũng làm IPN thật của VNPay bị 429. Đây là điểm yếu CÓ SẴN (route `/api/auth/**` cũng vậy)
  nhưng 2.3a là lần đầu nó nằm trên đường tiền. Phải chốt trước khi bật `vnpay` thật — cần một quyết định
  riêng về số proxy tin cậy (`trustAll()` thì ai cũng giả được IP và rate limit thành đồ trang trí).
- **Phát lại một IPN THẬT của chính ref đó** vẫn qua được: không có đối chiếu `vnp_RequestId`/mốc tươi. Chỉ
  TLS chặn — cùng rủi ro còn lại đã ghi ở ADR-017.
- ~~Return URL (người dùng quay về) chưa có.~~ — **đã làm ở 2.3b**.
- ~~Chưa có alert cho tồn đọng~~ — **đã đặt ở 2.3b** (`BankWebhookBacklog`).

## Alternatives rejected

- **Xử lý đồng bộ trong request webhook:** cần gọi mạng ra provider; provider timeout nhanh và sẽ retry,
  nhân đôi tải đúng lúc hệ thống đang chậm.
- **Ghi cả webhook sai chữ ký để điều tra:** biến endpoint công khai thành kênh ghi DB không giới hạn.
  Counter `rejected` + log là đủ để biết có ai đang gõ cửa.
- **Dùng `vnp_TxnRef` làm khoá dedup:** mỗi lần thu chỉ nhận được đúng một IPN, mọi IPN sửa trạng thái về
  sau bị bỏ im lặng.
- **Tin `tenant_id` từ payload webhook:** bên ngoài tự khai tenant là đọc/ghi được sổ của người khác (ADR-010).
- **Endpoint webhook đi qua JWT:** provider không có token của ta; không có cách nào cấp.

## Amendments — 2026-10-20 (2.3b: worker xử lý + trang quay về)

### A. Worker chỉ làm một việc: tra tenant rồi bảo adapter đi hỏi provider

`WebhookWorker` lấy lô `webhook_events` chưa xử lý (`FOR UPDATE SKIP LOCKED`, `ORDER BY next_attempt_at`),
mỗi hàng: tra `provider_ref` → `provider_transactions` ra tenant → `TenantContext.runWith` →
`ChargeStatusService.refresh` (hàng PENDING) hoặc `confirmExpired` (hàng đã `FAILED(EXPIRED)`) → đóng
`processed_at`.

**Webhook THÊM một lần nhìn, không bao giờ lấy đi một lần nhìn đã lên lịch.** Với hàng đã
`FAILED(EXPIRED)`, worker gọi `detectLateMoney` chứ không `confirmExpired`: lượt hỏi xác nhận theo lịch ở
T+`expired-confirm-after` (ADR-017) là thứ che độ trễ chốt sổ của ngân hàng, và theo hợp đồng 2.2c thì một
câu trả lời — kể cả "chưa thu" — đóng nó lại vĩnh viễn. Nếu webhook dùng chung đường đó thì một IPN tới vài
phút sau khi hết hạn, lúc provider đương nhiên còn nói "chưa thu", sẽ **xoá mất lượt hỏi ở T+1h** và tiền về
sau đó không còn ai phát hiện — một lỗ hổng mà chính người dùng kích hoạt được bằng cách phát lại return URL
của mình vào `/webhooks/`. `detectLateMoney` hỏi và kêu y hệt nhưng không chạm `next_check_at`. Hợp đồng
"hỏi đúng một lần" của ADR-017 giữ nguyên, không sửa gì ở đường theo lịch.

**Nó không đọc kết quả trong payload.** §2 ở trên là chỗ dễ vi phạm nhất và giờ có test giữ:
`WebhookWorkerIT.webhook_noi_thanh_cong_ma_provider_noi_chua_thi_khong_ghi_tien` — IPN ký đúng nói
`SUCCEEDED`, provider vẫn nói chưa thu, không một đồng nào được ghi. Mutation check: thay bước hỏi lại bằng
`markSucceeded` thì test đỏ.

Hệ quả: **webhook và job đối chiếu cùng hỏi provider về một hàng là bình thường**. Mọi chuyển trạng thái là
UPDATE có điều kiện nên chỉ luồng thắng phát event. Job đối chiếu vẫn là lưới an toàn; worker chỉ làm kết
quả tới nhanh hơn (nhịp 2s so với 30s+ của job).

### B. Lịch thử lại là trạng thái của hàng, không phải của process

V5 thêm `attempts` + `next_attempt_at` + `outcome`. Lùi lịch NGAY khi lấy lô, trước khi gọi provider — cùng
lý lẽ với `deferChecks` của lần thu: provider chết (hay process chết) thì hàng vẫn đã được lùi, nên nó không
bị thử lại mỗi lượt quét và **không chặn những webhook tới sau nó**. Thiếu điều này, đúng lúc provider chập
chờn — tức lúc tồn đọng lớn nhất — một hàng hỏng sẽ ngồi mãi ở đầu lô.

Bốn kết cục đóng hàng (`outcome`):
- `RESOLVED` — đã hỏi xong, dù câu trả lời là gì.
- `UNRESOLVED_REF` — chữ ký đúng nhưng mã tham chiếu không thuộc hệ thống. **Không phải ca đua**: hàng
  `provider_transactions` luôn tồn tại TRƯỚC khi người dùng được gửi sang provider, nên provider không thể
  biết một mã ta chưa ghi. Thử lại mãi sẽ không bao giờ ra, nên đóng ngay. Counter
  `walletflow.bank.webhook.unresolved` + alert: tới được đây nghĩa là ai đó **ký được bằng khoá của ta** —
  hoặc hai môi trường dùng chung `VNPAY_TMN_CODE`, hoặc khoá đã lộ.
- `FOREIGN_ACCOUNT` — lần thu thuộc **tài khoản merchant** khác tài khoản đang chạy (sau khi xoay
  `VNPAY_TMN_CODE`). Chỉ tài khoản đã tạo phiên mới tra được phiên đó, nên đây là **lỗi nghiệp vụ**: thử lại
  bao nhiêu lần cũng thế. Đóng ngay, counter riêng. Để nó rơi vào vòng retry rồi thành `ABANDONED` là bắn
  alert "provider không trả lời" cho một ca đổi cấu hình — người trực sẽ đi truy một sự cố không tồn tại
  (bất biến 7: phân loại lỗi TRƯỚC khi xử lý).
- `ABANDONED` — hết `max-attempts` lượt mà chưa hỏi được. Counter + alert; cần đối soát tay. Cùng lý lẽ trần
  với `expired_unconfirmed` (ADR-017): không có trần thì hàng ở lại tồn đọng mãi và dìm chết gauge.

`abandonExhausted` chạy SAU cả lô và chỉ chạm hàng còn `processed_at IS NULL`, nên webhook xử lý xong đúng ở
lượt chót không bị đánh nhầm là bỏ cuộc — ca biên thật, có test.

### C. Trang quay về: `GET /payments/return/{provider}`, CHỈ ĐỌC

Công khai như `/webhooks/**` (thêm vào `PUBLIC_PREFIXES` + route riêng, rate limit theo IP rộng hơn vì F5 là
chuyện thường). Chữ ký kiểm bằng chính `verifyWebhook` — một đường kiểm duy nhất, hai bản sao sẽ lệch và chỗ
lệch là một đường lách.

**Tham số redirect không bao giờ là kết quả**: nó đi qua trình duyệt người dùng, và ngay cả redirect thật
cũng chỉ nói "người dùng đã rời trang thanh toán". Endpoint trả trạng thái **đang ghi nhận trong DB**;
`PENDING` là câu trả lời bình thường và đúng. Việc ghi duy nhất là `expediteCheck` — kéo `next_check_at` về
`now()`, không đụng `check_attempts`, không đổi trạng thái. Tín hiệu về **thời điểm** thì tin được, về **kết
quả** thì không.

Khác `WebhookController` hai điểm: lỗi trả RFC 7807 (client là frontend của ta, không phải VNPay), và
**không ghi `webhook_events`** — mở trang không phải là một thông báo của provider.

`expediteCheck` lọc theo `(tenant_id, id)` của hàng vừa tra được, không theo `(provider, provider_ref)`: đây
là đường GHI duy nhất từ một endpoint công khai, và bất biến "mọi query lọc tenant" (ADR-010) tồn tại đúng
để chỗ này không âm thầm thành ghi chéo tenant vào ngày ai đó nới khoá unique trên `provider_ref`. Query
DUY NHẤT không lọc tenant vẫn là `findByProviderAndProviderRefAndOperation` — nó *chính là* bước suy ra
tenant, và ràng thêm `operation = CHARGE` để IPN của lần hoàn tiền (2.4) không bị dẫn vào đường của lần thu.

### D. Vận hành

- **Giờ mới được đăng ký URL IPN với VNPay** (2.3a cấm vì chưa ai đọc hàng).
- Alert mới: `BankWebhookBacklog` (gauge `unprocessed` > 20 trong 10′), `BankWebhookAbandoned`,
  `BankWebhookUnresolvedRef`, `BankWebhookForeignAccount`.
- **`XForwardedRemoteAddressResolver` giờ là điều kiện CẦN trước khi mở ra Internet thật**, không còn là nợ
  kỹ thuật có sẵn như ghi ở 2.3a: `/payments/return/**` là endpoint công khai đầu tiên có đường GHI (dù chỉ
  là kéo lịch). Rate limit theo IP sau LB gộp chung một bucket thì nó không hạn chế được ai cả.
- `VNPAY_RETURN_URL` nên trỏ tới `<gateway>/payments/return/vnpay` nếu frontend muốn dùng câu trả lời JSON
  này; trỏ thẳng vào trang frontend cũng được — adapter không phụ thuộc vào việc nó được gọi.

## Liên quan

- ADR-016 (mô hình thu), ADR-017 (VNPay, hạn phiên), ADR-010 (tenant), ADR-009 (bí mật), roadmap 2.3–2.4.
