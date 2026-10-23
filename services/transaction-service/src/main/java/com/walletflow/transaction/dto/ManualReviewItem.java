package com.walletflow.transaction.dto;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.money.ECurrency;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.ETransactionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Một dòng trong hàng đợi cần người can thiệp (roadmap 2.5).
 *
 * <p>Mang cả {@code failureCode} lẫn {@code failureReason}: mã bị ghi đè bởi mọi lần chuyển trạng
 * thái, nên với những ca đi qua nhiều bước (ví dụ hoàn tiền hết lượt gửi lại) nguyên nhân GỐC chỉ
 * còn trong phần lời. Người trực cần cả hai để phân loại theo runbook.
 */
public record ManualReviewItem(
        UUID transactionId,
        EJournalType type,
        ETransactionStatus status,
        long amountMinor,
        ECurrency currency,
        UUID toWalletId,
        String failureCode,
        String failureReason,
        String provider,
        String providerRef,
        Instant createdAt,
        Instant updatedAt,
        /** Bao lâu rồi chưa ai đụng tới — thứ quyết định ca nào xử lý trước. */
        long stuckForSeconds
) {
    public static ManualReviewItem of(Transaction t, Instant now) {
        return new ManualReviewItem(t.getId(), t.getType(), t.getStatus(), t.getAmountMinor(),
                t.getCurrency(), t.getToWallet(), t.getFailureCode(), t.getFailureReason(),
                t.getProvider(), t.getProviderRef(), t.getCreatedAt(), t.getUpdatedAt(),
                Math.max(0, now.getEpochSecond() - t.getUpdatedAt().getEpochSecond()));
    }
}
