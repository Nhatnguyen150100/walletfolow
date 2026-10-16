package com.walletflow.common.ledger;

import com.walletflow.common.event.EJournalType;

import java.time.Instant;
import java.util.UUID;

/**
 * Một bút toán, nhìn từ phía MỘT tài khoản.
 *
 * @param entryId     id bút toán — cũng là {@code eventId} của {@code LedgerPostedEvent} tương ứng,
 *                    nên phía read model dùng nó làm khoá dedup
 * @param amountMinor tổng các vế của bút toán chạm tài khoản này (thường chỉ một vế)
 */
public record StatementLine(
    UUID entryId,
    UUID transactionId,
    EJournalType type,
    long amountMinor,
    Instant postedAt
) {
}
