package com.walletflow.bank.provider.vnpay;

/**
 * Cấu hình kết nối VNPay (ADR-017). Mã merchant và hash secret chỉ đến từ biến môi trường, không bao giờ có
 * giá trị mặc định trong repo (bất biến §5.6) — {@code PaymentProviderConfig} kiểm đủ trước khi dựng.
 *
 * @param tmnCode    mã website merchant ({@code vnp_TmnCode})
 * @param hashSecret khoá ký HMAC-SHA512 — BÍ MẬT
 * @param payUrl     trang thanh toán (người dùng được chuyển hướng tới)
 * @param apiUrl     API server-to-server ({@code querydr})
 * @param returnUrl  nơi VNPay đưa người dùng về sau khi trả tiền — chỉ để HIỂN THỊ, không ghi kết quả
 * @param serverIp   IP máy chủ: gửi kèm querydr, và thay IP người dùng khi lệnh thu không mang IP
 * @param locale     ngôn ngữ trang thanh toán ({@code vn}/{@code en})
 * @param orderType  mã danh mục hàng hoá theo danh sách của VNPay
 */
public record VnPaySettings(String tmnCode, String hashSecret, String payUrl, String apiUrl, String returnUrl,
                            String serverIp, String locale, String orderType) {

    /** Không bao giờ in hash secret — record tự sinh toString in mọi field, và cấu hình hay bị log khi lỗi. */
    @Override
    public String toString() {
        return "VnPaySettings[tmnCode=" + tmnCode + ", hashSecret=***, payUrl=" + payUrl + ", apiUrl=" + apiUrl
                + ", returnUrl=" + returnUrl + ", locale=" + locale + ", orderType=" + orderType + "]";
    }
}
