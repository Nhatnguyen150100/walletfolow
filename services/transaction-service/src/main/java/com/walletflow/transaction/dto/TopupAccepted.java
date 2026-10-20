package com.walletflow.transaction.dto;

import com.walletflow.common.money.ECurrency;
import com.walletflow.transaction.enums.ETransactionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Câu trả lời 202 cho lệnh nạp: lệnh đã được NHẬN, chưa có trang thanh toán.
 *
 * <p>Cố ý KHÔNG chờ provider cấp {@code paymentUrl} rồi mới trả lời: lệnh thu đi qua outbox + Kafka,
 * và giữ request HTTP lại chờ một cổng thanh toán bên ngoài là cách biến mọi sự cố của họ thành sự cố
 * của ta. Client lấy trang thanh toán bằng {@code GET /api/v1/transactions/{transactionId}} khi trạng
 * thái sang {@code AWAITING_PAYMENT}.
 */
public record TopupAccepted(
        UUID transactionId,
        ETransactionStatus status,
        UUID walletId,
        long amountMinor,
        ECurrency currency,
        Instant acceptedAt
) {
}
