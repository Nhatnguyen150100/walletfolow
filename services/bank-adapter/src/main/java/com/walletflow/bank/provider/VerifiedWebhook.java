package com.walletflow.bank.provider;

/**
 * Một webhook/IPN đã qua kiểm chữ ký (ADR-018). Chỉ được tạo bởi chính provider, sau khi chữ ký khớp —
 * nên sự tồn tại của một {@code VerifiedWebhook} là bằng chứng payload thật sự đến từ provider.
 *
 * @param providerRef mã tham chiếu phía provider đọc được từ payload (VNPay: {@code vnp_TxnRef});
 *                    {@code null} nếu payload hợp lệ chữ ký nhưng không mang mã nào
 * @param signedData  đúng chuỗi mà provider đã ký. Dùng để băm ra khoá chống phát lại: hai IPN khác nội
 *                    dung thì khác chuỗi này, nên không bao giờ bị gộp nhầm thành một
 */
public record VerifiedWebhook(String providerRef, String signedData) {
}
