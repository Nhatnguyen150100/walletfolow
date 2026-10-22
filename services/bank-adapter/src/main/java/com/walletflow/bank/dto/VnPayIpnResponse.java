package com.walletflow.bank.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Thân câu trả lời IPN theo đúng hợp đồng của VNPay: hai trường viết hoa {@code RspCode} và
 * {@code Message}. VNPay đọc {@code RspCode} để quyết định có gửi lại IPN hay không.
 *
 * <p>Cố ý không dùng {@code ProblemDetail} (RFC 7807) như phần còn lại của hệ thống: khuôn này do VNPay
 * quy định, không phải của ta.
 */
public record VnPayIpnResponse(@JsonProperty("RspCode") String rspCode,
                               @JsonProperty("Message") String message) {

    /** "Đã ghi nhận, đừng gửi lại nữa" — dùng cho cả webhook mới lẫn webhook trùng. */
    public static VnPayIpnResponse confirmed() {
        return new VnPayIpnResponse("00", "Confirm Success");
    }

    /**
     * Chữ ký sai. Mã {@code 97} là mã checksum của VNPay; không nói gì thêm về lý do, vì người gửi
     * một chữ ký sai không có quyền biết ta kiểm những gì.
     */
    public static VnPayIpnResponse invalidSignature() {
        return new VnPayIpnResponse("97", "Invalid Checksum");
    }
}
