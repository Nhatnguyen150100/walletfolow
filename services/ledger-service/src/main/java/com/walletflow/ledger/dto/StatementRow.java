package com.walletflow.ledger.dto;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.ledger.StatementLine;

import java.time.Instant;
import java.util.UUID;

/**
 * Hàng kết quả của truy vấn sao kê. Tách khỏi {@link StatementLine} (hợp đồng) vì {@code SUM} trong
 * JPQL trả {@code Long}, còn hợp đồng dùng {@code long} — để Hibernate tự khớp constructor giữa hai
 * kiểu đó là phụ thuộc vào một chi tiết cài đặt không được đảm bảo.
 */
public record StatementRow(
        UUID entryId,
        UUID transactionId,
        EJournalType type,
        Long amountMinor,
        Instant postedAt
) {

    public StatementLine toLine() {
        return new StatementLine(entryId, transactionId, type, amountMinor, postedAt);
    }
}
