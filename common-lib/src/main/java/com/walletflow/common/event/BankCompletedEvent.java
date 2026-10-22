package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Provider XÁC NHẬN thao tác đã thành công. Topic {@code bank.event.completed.v1}, producer bank-adapter,
 * consumer transaction-service (2.4).
 *
 * <p>Chỉ phát sau khi bank-adapter tự hỏi lại provider ({@code getStatus}) — webhook chỉ là gợi ý,
 * không phải sự thật (roadmap 2.3, ADR-016). Với {@code CHARGE}, đây là tín hiệu để saga ghi bút toán
 * nạp tiền; tiền vào ví khi ledger ghi xong, không phải khi nhận event này.
 */
public record BankCompletedEvent(
    UUID transactionId,
    EBankOperation operation,
    UUID walletId,
    String provider,
    String providerRef,
    long amountMinor,
    ECurrency currency
) {
}
