package com.walletflow.transaction.listener;

import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.services.SagaService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu trả lời của ledger cho saga — BIÊN VÀO duy nhất đặt tenant cho luồng event này.
 *
 * <p>Không cần bảng dedup: mọi chuyển trạng thái được quyết dưới khoá hàng giao dịch theo máy trạng
 * thái của {@link SagaService}, nên event lặp gặp trạng thái đã đổi và không làm gì. Exception đều là
 * lỗi kỹ thuật → retry → DLT (common-lib).
 */
@Component
@RequiredArgsConstructor
public class LedgerResultListener {

    private final EnvelopeReader envelopeReader;
    private final SagaService sagaService;

    @KafkaListener(topics = WalletTopics.EVT_LEDGER_POSTED)
    public void onPosted(String message) {
        EventEnvelope<LedgerPostedEvent> envelope = envelopeReader.read(message, LedgerPostedEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> sagaService.onPosted(envelope.payload()));
    }

    @KafkaListener(topics = WalletTopics.EVT_LEDGER_REJECTED)
    public void onRejected(String message) {
        EventEnvelope<LedgerRejectedEvent> envelope = envelopeReader.read(message, LedgerRejectedEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> sagaService.onRejected(envelope.payload()));
    }
}
