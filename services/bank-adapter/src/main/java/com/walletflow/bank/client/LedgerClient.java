package com.walletflow.bank.client;

import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Đường ĐỌC duy nhất từ bank-adapter sang sổ cái — phục vụ đối soát bank-adapter ↔ sổ cái
 * (roadmap 3.1 PR2a, ADR-024). Trước PR này bank-adapter không gọi ledger chút nào; nó vẫn KHÔNG ghi
 * gì vào sổ cái, mọi thay đổi sổ cái vẫn chỉ đi qua Kafka command của saga.
 *
 * <p>Sau một interface để test thay bằng fake trong {@code support/} (CLAUDE.md), không mock.
 *
 * <p>Ledger không trả lời được ⇒ {@link LedgerUnavailableException}, KHÔNG BAO GIỜ một câu trả lời rỗng:
 * với đối soát, "ledger không có bút toán" và "không hỏi được ledger" dẫn tới hai kết luận ngược nhau.
 */
public interface LedgerClient {

    /**
     * Tiền ròng qua {@code EXTERNAL_BANK} của các lệnh, trong tenant của {@code TenantContext}.
     *
     * @return {@code transactionId → (currency → netInMinor)}; lệnh vắng mặt = sổ cái chưa đưa đồng nào
     *         qua cửa ngoài cho lệnh đó
     */
    Map<UUID, Map<ECurrency, Long>> externalMovementsOf(Collection<UUID> transactionIds);

    /** Một trang bút toán {@code TOPUP} XUYÊN tenant theo con trỏ {@code (postedAt, entryId)}. */
    List<TopupEntryRef> topupsAfter(Instant afterPostedAt, UUID afterId, Instant before, int limit);
}
