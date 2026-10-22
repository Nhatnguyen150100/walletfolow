package com.walletflow.common.event;

import java.util.UUID;

/**
 * Thao tác với provider thất bại DỨT KHOÁT. Topic {@code bank.event.failed.v1}, producer bank-adapter,
 * consumer transaction-service (2.4).
 *
 * <p>"Dứt khoát" là điểm mấu chốt: provider từ chối (thẻ bị từ chối, sai số tiền) hoặc xác nhận phiên
 * đã hỏng/hết hạn. Lỗi KỸ THUẬT (timeout, provider 5xx, circuit mở) không bao giờ thành event này — chúng
 * được thử lại rồi vào DLT, vì timeout không có nghĩa là tiền chưa đi (roadmap §5.7).
 *
 * <p>Chỉ mang {@code failureCode} ổn định, không mang câu chữ của provider (có thể lộ dữ liệu thanh toán).
 *
 * @param providerRef {@code null} khi provider từ chối ngay lúc tạo phiên, chưa kịp cấp mã
 */
public record BankFailedEvent(
    UUID transactionId,
    EBankOperation operation,
    UUID walletId,
    String provider,
    String providerRef,
    String failureCode
) {
}
