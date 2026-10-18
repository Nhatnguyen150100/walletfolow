package com.walletflow.bank.services;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.common.event.BankChargePendingEvent;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.BankFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.outbox.OutboxPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Phát event ứng với trạng thái HIỆN TẠI của một lần thu, qua outbox (bất biến §5.3).
 *
 * <p>eventId tất định theo {@code (hàng, trạng thái)}: mỗi hàng chỉ đi qua mỗi trạng thái một lần, nên
 * cùng eventId luôn là cùng PAYLOAD nghiệp vụ (chỉ {@code occurredAt}/{@code traceId} của envelope khác).
 * Nhờ vậy phát lại bao nhiêu lần (lệnh gửi lại, hai luồng đua nhau) thì consumer vẫn dedup được — và quan
 * trọng hơn, MỖI lần nhận lệnh đều có câu trả lời (ADR-016).
 */
@Component
@RequiredArgsConstructor
public class BankEventPublisher {

    private final OutboxPublisher outboxPublisher;

    @Transactional(propagation = Propagation.MANDATORY)
    public void publishOutcome(ProviderTransaction txn) {
        String key = txn.getWalletId().toString();   // cùng partition với mọi message khác của ví
        switch (txn.getStatus()) {
            case NEW -> {
                // Chưa có câu trả lời nào của provider để báo
            }
            case PENDING -> outboxPublisher.publish(WalletTopics.EVT_BANK_CHARGE_PENDING, key,
                    eventId("bank-charge-pending", txn), "BankChargePending",
                    new BankChargePendingEvent(txn.getTransactionId(), txn.getWalletId(), txn.getProvider(),
                            txn.getProviderRef(), txn.getPaymentUrl()));
            case SUCCEEDED -> outboxPublisher.publish(WalletTopics.EVT_BANK_COMPLETED, key,
                    eventId("bank-completed", txn), "BankCompleted",
                    new BankCompletedEvent(txn.getTransactionId(), txn.getOperation(), txn.getWalletId(),
                            txn.getProvider(), txn.getProviderRef(), txn.getAmountMinor(), txn.getCurrency()));
            case FAILED -> outboxPublisher.publish(WalletTopics.EVT_BANK_FAILED, key,
                    eventId("bank-failed", txn), "BankFailed",
                    new BankFailedEvent(txn.getTransactionId(), txn.getOperation(), txn.getWalletId(),
                            txn.getProvider(), txn.getProviderRef(), txn.getFailureCode()));
        }
    }

    private static UUID eventId(String kind, ProviderTransaction txn) {
        return UUID.nameUUIDFromBytes((kind + ":" + txn.getId()).getBytes(StandardCharsets.UTF_8));
    }
}
