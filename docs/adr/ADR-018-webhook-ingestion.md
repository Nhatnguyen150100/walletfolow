# ADR-018 — Nhận webhook/IPN: chữ ký là ranh giới tin cậy, webhook là gợi ý

**Trạng thái:** Accepted · **Ngày:** 2026-10-19 · **Milestone:** Phase 2 — 2.3a

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

- **Worker xử lý `webhook_events` chưa có**: hàng được ghi nhưng chưa ai đọc, nên gauge
  `walletflow.bank.webhook.unprocessed` sẽ chỉ tăng. Trong 2.3a kết quả **vẫn chỉ tới qua job đối chiếu như
  2.2** — webhook chưa rút ngắn được gì. Hệ quả vận hành: **chưa đăng ký URL IPN với VNPay cho tới 2.3b**;
  đăng ký sớm thì ta trả `RspCode=00` ("đừng gửi lại") cho sự kiện không ai xử lý. Tiền vẫn an toàn nhờ job
  đối chiếu, nhưng đó là mất một lớp lưới mà không ai thấy.
- **Rate limit theo IP sau LB gộp chung một khoá.** `ipKeyResolver` dùng `getRemoteAddress()` và gateway
  chưa có `XForwardedRemoteAddressResolver`; đặt sau ingress thì toàn bộ traffic webhook chung một bucket
  20/40, nên ai cũng làm IPN thật của VNPay bị 429. Đây là điểm yếu CÓ SẴN (route `/api/auth/**` cũng vậy)
  nhưng 2.3a là lần đầu nó nằm trên đường tiền. Phải chốt trước khi bật `vnpay` thật — cần một quyết định
  riêng về số proxy tin cậy (`trustAll()` thì ai cũng giả được IP và rate limit thành đồ trang trí).
- **Phát lại một IPN THẬT của chính ref đó** vẫn qua được: không có đối chiếu `vnp_RequestId`/mốc tươi. Chỉ
  TLS chặn — cùng rủi ro còn lại đã ghi ở ADR-017.
- Return URL (người dùng quay về) chưa có.
- Chưa có alert cho tồn đọng — gauge đã có, ngưỡng alert để 2.3b đặt khi biết nhịp thật.

## Alternatives rejected

- **Xử lý đồng bộ trong request webhook:** cần gọi mạng ra provider; provider timeout nhanh và sẽ retry,
  nhân đôi tải đúng lúc hệ thống đang chậm.
- **Ghi cả webhook sai chữ ký để điều tra:** biến endpoint công khai thành kênh ghi DB không giới hạn.
  Counter `rejected` + log là đủ để biết có ai đang gõ cửa.
- **Dùng `vnp_TxnRef` làm khoá dedup:** mỗi lần thu chỉ nhận được đúng một IPN, mọi IPN sửa trạng thái về
  sau bị bỏ im lặng.
- **Tin `tenant_id` từ payload webhook:** bên ngoài tự khai tenant là đọc/ghi được sổ của người khác (ADR-010).
- **Endpoint webhook đi qua JWT:** provider không có token của ta; không có cách nào cấp.

## Liên quan

- ADR-016 (mô hình thu), ADR-017 (VNPay, hạn phiên), ADR-010 (tenant), ADR-009 (bí mật), roadmap 2.3–2.4.
