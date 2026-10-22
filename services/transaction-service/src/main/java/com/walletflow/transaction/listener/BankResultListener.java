package com.walletflow.transaction.listener;

import com.walletflow.common.event.BankChargePendingEvent;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.BankFailedEvent;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.services.SagaService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu trả lời của bank-adapter cho saga nạp tiền (roadmap 2.4a) — BIÊN VÀO duy nhất đặt tenant cho
 * luồng event này.
 *
 * <p>Không cần bảng dedup, cùng lý do như {@link LedgerResultListener}: mọi chuyển trạng thái được
 * quyết dưới khoá hàng giao dịch theo máy trạng thái một chiều của {@link SagaService}, nên event lặp
 * gặp trạng thái đã đổi và không làm gì. Exception đều là lỗi kỹ thuật → retry → DLT (common-lib).
 */
@Component
@RequiredArgsConstructor
public class BankResultListener {

    private final EnvelopeReader envelopeReader;
    private final SagaService sagaService;

    @KafkaListener(topics = WalletTopics.EVT_BANK_CHARGE_PENDING)
    public void onChargePending(String message) {
        EventEnvelope<BankChargePendingEvent> envelope =
                envelopeReader.read(message, BankChargePendingEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> sagaService.onChargePending(envelope.payload()));
    }

    @KafkaListener(topics = WalletTopics.EVT_BANK_COMPLETED)
    public void onCompleted(String message) {
        EventEnvelope<BankCompletedEvent> envelope = envelopeReader.read(message, BankCompletedEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> sagaService.onBankCompleted(envelope.payload()));
    }

    @KafkaListener(topics = WalletTopics.EVT_BANK_FAILED)
    public void onFailed(String message) {
        EventEnvelope<BankFailedEvent> envelope = envelopeReader.read(message, BankFailedEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> sagaService.onBankFailed(envelope.payload()));
    }
}
