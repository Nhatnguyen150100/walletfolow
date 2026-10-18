package com.walletflow.bank.listener;

import com.walletflow.bank.services.ChargeService;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.kafka.PoisonMessageException;
import com.walletflow.common.tenant.TenantContext;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Cửa vào của bank-adapter từ saga (ADR-016).
 *
 * <p>Phân loại lỗi — ba loại, ba đường:
 * <ul>
 *   <li><b>Provider từ chối</b> là câu trả lời bình thường: {@code ChargeService} ghi nó và phát
 *       {@code bank.event.failed}. Không có exception nào ở đây.</li>
 *   <li><b>Lệnh hỏng</b> ({@link AppException}: số tiền không dương, cùng giao dịch mà khác số tiền) là bug
 *       producer — thử lại vô ích → {@link PoisonMessageException} → DLT ngay.</li>
 *   <li><b>Lỗi kỹ thuật</b> (provider timeout/5xx/mạch mở, DB lỗi) → để nổ ra: Kafka thử lại có backoff rồi
 *       vào DLT. Tuyệt đối không đổi thành "thất bại" — timeout không có nghĩa là chưa thu tiền.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class BankCommandListener {

    private final EnvelopeReader envelopeReader;
    private final ChargeService chargeService;

    @KafkaListener(topics = WalletTopics.CMD_BANK_CHARGE)
    public void onCharge(String message) {
        EventEnvelope<BankChargeCommand> envelope = envelopeReader.read(message, BankChargeCommand.class);

        // Biên vào của luồng Kafka: nơi duy nhất được đặt tenant (xem LedgerCommandListener)
        TenantContext.runWith(envelope.tenantId(), () -> {
            try {
                chargeService.charge(envelope.payload());
            } catch (AppException invalid) {
                throw new PoisonMessageException("Lệnh thu tiền không hợp lệ (eventId=" + envelope.eventId()
                        + "): " + invalid.getErrorCode() + " - " + invalid.getMessage(), invalid);
            }
        });
    }
}
