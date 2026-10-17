package com.walletflow.wallet.dto;

import com.walletflow.common.event.EJournalType;
import com.walletflow.wallet.entity.WalletEntry;

import java.time.Instant;
import java.util.UUID;

/** Một dòng lịch sử giao dịch. {@code amountMinor} dương = tiền vào, âm = tiền ra. */
public record EntryView(
        UUID entryId,
        UUID transactionId,
        EJournalType type,
        long amountMinor,
        Instant postedAt
) {
    public static EntryView of(WalletEntry entry) {
        return new EntryView(entry.getEntryId(), entry.getTransactionId(), entry.getType(),
                entry.getAmountMinor(), entry.getPostedAt());
    }
}
