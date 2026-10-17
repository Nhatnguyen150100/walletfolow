package com.walletflow.transaction.dto;

import com.walletflow.common.money.ECurrency;
import com.walletflow.transaction.enums.ETransactionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Câu trả lời 202: lệnh đã được NHẬN, chưa phải đã chuyển. Client theo dõi bằng
 * {@code GET /api/v1/transactions/{transactionId}}.
 */
public record TransferAccepted(
        UUID transactionId,
        ETransactionStatus status,
        UUID fromWalletId,
        UUID toWalletId,
        long amountMinor,
        ECurrency currency,
        Instant acceptedAt
) {
}
