package com.walletflow.ledger.dto;

import com.walletflow.common.event.EJournalType;

import java.time.Instant;
import java.util.UUID;

/**
 * Một hàng phẳng của truy vấn "mọi vế của mọi bút toán thuộc một lệnh tiền": thông tin bút toán lặp
 * lại trên mỗi vế. Service gom lại thành cây trước khi trả ra hợp đồng.
 *
 * <p>Gom ở tầng Java chứ không bằng hai truy vấn (bút toán, rồi các vế): hai truy vấn mở ra khoảng
 * trống giữa chúng, và với một bảng append-only thì một JOIN phẳng vừa đúng hơn vừa rẻ hơn.
 */
public record EntryLineRow(
        UUID entryId,
        EJournalType type,
        UUID referenceId,
        Instant postedAt,
        UUID accountId,
        String accountRef,
        long amountMinor
) {
}
