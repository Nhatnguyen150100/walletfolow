# ADR-009 — JWT bất đối xứng (RS256) + JWKS thay cho secret chia sẻ

**Trạng thái:** Accepted · **Ngày:** 2026-10-16 · **Milestone:** Phase 0

## Context

Bản đầu dùng HS256 với một secret đối xứng, và secret đó có **giá trị mặc định được commit** trong
`auth-service/application.yml`, `api-gateway/application.yml` và `docker-compose.yml`. Hai vấn đề
độc lập nhau:

1. **Giá trị mặc định trong repo.** Ai đọc được repo cũng ký được token với `role=ADMIN`.
2. **Secret đối xứng, về bản chất.** Với HS256, khoá ký và khoá verify là MỘT. Gateway cần verify
   nên phải cầm khoá ký — nghĩa là gateway, thành phần duy nhất lộ ra Internet, có khả năng
   **phát hành** token tuỳ ý. Một lỗ hổng đọc biến môi trường ở gateway là chiếm trọn hệ thống.

Vấn đề thứ hai không sửa được bằng cách giấu secret kỹ hơn. Nó là tính chất của thuật toán.

Thêm một vấn đề vận hành: xoay secret HS256 đòi hỏi auth-service và gateway đổi cùng lúc. Giữa hai
lần deploy là một cửa sổ mà mọi request đều 401.

## Decision

- **RS256.** auth-service giữ private key và ký. Gateway chỉ có public key và chỉ verify được.
- **JWKS** (`GET /.well-known/jwks.json`) công bố public key. Gateway tự lấy, cache trong bộ nhớ,
  làm mới theo lịch. Mỗi token mang `kid` trong header; gặp `kid` lạ thì gateway làm mới ngay rồi
  từ chối request đó — client thử lại sau một nhịp là qua. **Xoay khoá không cần deploy đồng thời.**
- **`kid` sinh từ thumbprint (RFC 7638)** — tất định theo nội dung khoá, nên hai instance cùng cặp
  khoá luôn cho ra cùng `kid`, và `kid` đổi khi và chỉ khi khoá đổi.
- **Không có giá trị mặc định cho khoá.** Thiếu biến môi trường thì service **chết lúc khởi động**.
  CI có guardrail chặn secret mặc định quay lại.
- **Access token 15 phút + refresh token xoay vòng** (bản cũ: 24 giờ, không thu hồi được). Access
  token không thu hồi được vì verify là thao tác offline — thời hạn ngắn chính là cơ chế thu hồi
  duy nhất. Khả năng duy trì phiên dài chuyển sang refresh token, thứ **có** thu hồi được vì nó
  được tra trong DB mỗi lần dùng.

## Consequences

- ✅ Gateway bị chiếm cũng không phát hành được token.
- ✅ Xoay khoá không gây downtime.
- ✅ Thu hồi phiên trở nên khả thi (qua refresh token + `family_id`).
- ⚠️ Gateway phụ thuộc JWKS của auth-service. Giảm thiểu: cache trong bộ nhớ, và khi lấy JWKS thất
  bại thì **giữ nguyên bộ khoá cũ** — auth-service tạm thời không truy cập được không được phép làm
  gateway từ chối những token vẫn còn hợp lệ.
- ⚠️ RS256 verify tốn CPU hơn HS256. Không đáng kể ở quy mô này; nếu thành vấn đề thì chuyển EdDSA.
- ⚠️ Client phải xử lý refresh. Access token 15 phút nghĩa là app phải tự làm mới.
- ⚠️ Private key phải nằm trong secret manager ở môi trường thật — `.env` chỉ đủ cho dev
  (roadmap §4.3).

## Liên quan
`JwtKeyProvider`, `JwksKeyLocator`, `AuthFlowIT`, roadmap Phase 0 mục 0.2
