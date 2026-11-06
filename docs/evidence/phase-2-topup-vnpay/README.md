# Bằng chứng — nạp tiền trên sandbox VNPay (DoD Phase 2, ô 1)

Thủ tục: [`docs/runbooks/e2e-topup-sandbox.md`](../../runbooks/e2e-topup-sandbox.md).
Sinh file ở đây bằng `scripts/e2e/topup-vnpay.sh`.

Ô DoD "nạp tiền thật qua sandbox → số dư tăng đúng" đòi *bằng chứng của một lần chạy*, không phải
một test. Phase 1 đóng ô tương đương bằng một lần chạy tay, nhưng bằng chứng chỉ nằm trong log CI
trên GitHub — tức là sẽ hết hạn. Thư mục này là nơi lưu cho Phase 2 trở đi.

## Đặt tên

`<thời điểm UTC>-<provider>-<kết cục>.md`, ví dụ `2026-11-06T031500Z-vnpay-completed.md`. Dùng cả
giờ-phút-giây chứ không chỉ ngày: vòng "hỏng → sửa → chạy lại" trong cùng một buổi sẽ ghi đè lên
nhau, mà lần hỏng mới là lần đáng giữ.

Kịch bản tự ghi, **kể cả khi hỏng** — một lần nạp thất bại trên sandbox thật đáng lưu ngang một lần
thành công, nó chính là lý do ô DoD còn trống. (Hỏng *trước* lúc lệnh nạp được nhận thì chưa có gì
để ghi.)

File sinh với `provider = mock` chỉ chứng minh kịch bản chạy đúng; nó **không** đóng được ô DoD và
tự mang cảnh báo đó ở đầu file.

## Trước khi commit

Rà `vnp_TmnCode`, `vnp_SecureHash`, số thẻ test trong **mọi** file bạn thêm vào đây (bất biến §6).
Kịch bản chỉ cắt query string của các URL nó ghi vào file bằng chứng; ảnh chụp màn hình, log
container, và **output của chính kịch bản** (nó phải in nguyên trang thanh toán ra để bạn mở được)
thì không ai cắt hộ.
