package com.walletflow.transaction.dto;

import com.walletflow.common.money.ECurrency;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.ETransactionStatus;

import java.time.Instant;
import java.util.UUID;

public record TransactionView(
        UUID transactionId,
        ETransactionStatus status,
        UUID fromWalletId,
        UUID toWalletId,
        long amountMinor,
        ECurrency currency,
        String note,
        String failureCode,
        Instant createdAt,
        Instant updatedAt
) {
    public static TransactionView of(Transaction t) {
        return new TransactionView(t.getId(), t.getStatus(), t.getFromWallet(), t.getToWallet(),
                t.getAmountMinor(), t.getCurrency(), t.getNote(), t.getFailureCode(),
                t.getCreatedAt(), t.getUpdatedAt());
    }
}
