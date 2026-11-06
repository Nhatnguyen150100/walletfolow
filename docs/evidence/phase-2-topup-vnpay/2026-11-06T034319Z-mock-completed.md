# E2E nạp tiền — mock, completed (2026-11-06)

Sinh bởi `scripts/e2e/topup-vnpay.sh`. Bắt đầu 2026-11-06T03:42:25Z, dừng 2026-11-06T03:43:19Z (UTC).

> ⚠️ **Chạy với `mock` — không có tiền thật nào được thu.** Lần chạy này **không** đóng
> được ô DoD "nạp tiền thật qua sandbox", dù mọi kiểm tra có xanh.

## Đã kiểm

- Giao dịch: `4eea4b27-ef3f-42d9-84fe-e8fac9ba0a89` · ví `f5b52543-535b-4e10-ad93-3c02fdb0c215` · 10000 VND (minor) · Idempotency-Key `e2e-topup-44454f3c-608b-45e6-bad5-450c8df8797b`
- Trang thanh toán: `http://localhost:8085/mock-pay/MOCK-df5d0fb0-731a-434a-b40b-85da30fd9557`
- Gửi lại cùng `Idempotency-Key`: 202, `Idempotent-Replayed: true`, body khớp từng byte
- Lúc `AWAITING_PAYMENT`: sổ cái 0, chưa có bút toán, read model 0 — không ghi có trước khi thu
- Sổ cái ví: 10000 · read model: 10000 · đúng 1 bút toán · `SUM(postings) = 0`
- `provider_transactions`: SUCCEEDED, 10000 VND, đúng 1 lần thu, có `raw_response`
- Kết cục: **COMPLETED** · providerRef `MOCK-df5d0fb0-731a-434a-b40b-85da30fd9557`
- DLT: 0 trước, 0 sau — không message nào bị loại

## Còn cần bạn đính kèm (nếu là lần lấy bằng chứng)

- Ảnh chụp trang thanh toán VNPay và trang quay về.
- `docker compose -p wf-e2e logs bank-adapter transaction-service` của khoảng thời gian trên.

Mọi URL trong file đã bị cắt query string: nó mang `vnp_TmnCode` và `vnp_SecureHash`.
