package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Giao dịch đã hoàn tất — ledger đã ghi bút toán. Topic {@code txn.event.completed.v1}, producer
 * transaction-service, consumer notification-service (1.4).
 *
 * <p>Phát ĐÚNG MỘT LẦN cho mỗi lần giao dịch chuyển sang COMPLETED, với {@code eventId} tất định theo
 * {@code transactionId} — consumer dedup được nếu outbox giao lại.
 *
 * @param entryId bút toán ở ledger — để đối soát và để consumer tra ngược về sổ cái
 */
public record TransactionCompletedEvent(
    UUID transactionId,
    EJournalType type,
    UUID initiatedBy,
    UUID fromWalletId,
    UUID toWalletId,
    long amountMinor,
    ECurrency currency,
    UUID entryId
) {
}
