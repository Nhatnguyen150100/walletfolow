package com.walletflow.common.event;

import java.util.UUID;

/**
 * Phiên thanh toán đã được tạo ở provider, đang chờ người dùng trả tiền. Topic
 * {@code bank.event.charge-pending.v1}, producer bank-adapter, consumer transaction-service (2.4).
 *
 * <p><b>Không phải kết quả thu tiền.</b> Cổng thanh toán nội địa chạy theo luồng chuyển hướng: người dùng
 * mở {@code paymentUrl}, trả tiền ở trang của provider, rồi provider báo kết quả sau (ADR-016). Kết quả
 * cuối đi ở {@code bank.event.completed.v1} / {@code bank.event.failed.v1}.
 *
 * @param providerRef mã giao dịch phía provider — dùng để hỏi lại trạng thái và khớp webhook (2.3)
 * @param paymentUrl  trang thanh toán của provider để client chuyển người dùng tới
 */
public record BankChargePendingEvent(
    UUID transactionId,
    UUID walletId,
    String provider,
    String providerRef,
    String paymentUrl
) {
}
