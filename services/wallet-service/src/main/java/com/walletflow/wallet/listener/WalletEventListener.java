package com.walletflow.wallet.listener;

import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.services.WalletProjector;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Cửa vào Kafka của wallet-service — BIÊN VÀO duy nhất đặt tenant cho luồng event.
 *
 * <p>Không có từ chối nghiệp vụ nào ở đây: một event của ledger là sự thật đã xảy ra, read model chỉ
 * có việc phản chiếu. Mọi exception đều là lỗi kỹ thuật → retry → DLT (common-lib). Message không đọc
 * được hoặc thiếu tenant/payload đã bị {@link EnvelopeReader} đẩy thẳng vào DLT.
 */
@Component
@RequiredArgsConstructor
public class WalletEventListener {

    private final EnvelopeReader envelopeReader;
    private final WalletProjector projector;

    @KafkaListener(topics = WalletTopics.EVT_LEDGER_POSTED)
    public void onLedgerPosted(String message) {
        EventEnvelope<LedgerPostedEvent> envelope = envelopeReader.read(message, LedgerPostedEvent.class);
        TenantContext.runWith(envelope.tenantId(),
                () -> projector.applyPosted(envelope.payload(), envelope.occurredAt()));
    }

    @KafkaListener(topics = WalletTopics.EVT_ACCOUNT_OPENED)
    public void onAccountOpened(String message) {
        EventEnvelope<AccountOpenedEvent> envelope = envelopeReader.read(message, AccountOpenedEvent.class);
        TenantContext.runWith(envelope.tenantId(), () -> projector.applyAccountOpened(envelope.payload()));
    }
}
