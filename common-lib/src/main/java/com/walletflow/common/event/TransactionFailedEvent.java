package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Giao dịch thất bại — ledger từ chối. Topic {@code txn.event.failed.v1}, producer
 * transaction-service, consumer notification-service (1.4).
 *
 * <p>Chỉ mang {@code failureCode} (ổn định, ví dụ {@code INSUFFICIENT_FUNDS}), KHÔNG mang câu chữ lý
 * do của ledger: câu đó là thông điệp kỹ thuật (có thể chứa số dư nội bộ), không phải thứ hiển thị cho
 * người dùng. Consumer tự đổi mã ra lời theo ngôn ngữ của người nhận.
 */
public record TransactionFailedEvent(
    UUID transactionId,
    EJournalType type,
    UUID initiatedBy,
    UUID fromWalletId,
    UUID toWalletId,
    long amountMinor,
    ECurrency currency,
    String failureCode
) {
}
