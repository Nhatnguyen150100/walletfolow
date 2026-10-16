package com.walletflow.ledger.listener;

import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.kafka.PoisonMessageException;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.ledger.services.LedgerCommandHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Cửa vào duy nhất của ledger từ phía Saga orchestrator (plan §4.3, §8.1).
 *
 * <p><b>Phân loại lỗi ở đây là phần quan trọng nhất của class này</b>, vì nó quyết định hệ thống mất
 * tiền hay không:
 * <ul>
 *   <li>{@link AppException} = <b>từ chối nghiệp vụ</b> (không đủ số dư, bút toán lệch, tài khoản
 *       không tồn tại). Đây là câu trả lời DỨT KHOÁT của ledger: thử lại 1000 lần vẫn vậy. Phát
 *       {@code LedgerRejectedEvent} để Saga xử lý tiếp.</li>
 *   <li>Mọi exception còn lại = <b>lỗi kỹ thuật</b> (DB chết, mất kết nối, vi phạm UNIQUE do đua).
 *       Cố tình để nó nổ ra: Kafka sẽ giao lại message, và nếu vẫn lỗi thì message vào DLT (plan
 *       §12) — có người xem, chứ không âm thầm mất.</li>
 * </ul>
 * Nhập nhằng hai loại này là lỗi đắt nhất trong Saga: coi một lỗi DB tạm thời là "ledger từ chối" sẽ
 * làm orchestrator hoàn tiền một giao dịch mà đáng lẽ chỉ cần thử lại.
 *
 * <p>Nhận {@code String} rồi tự parse envelope: xem {@link EnvelopeReader} về lý do không để
 * {@code JsonDeserializer} suy kiểu.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerCommandListener {

    private final EnvelopeReader envelopeReader;
    private final LedgerCommandHandler handler;

    @KafkaListener(topics = WalletTopics.CMD_POST_JOURNAL)
    public void onPostJournal(String message) {
        EventEnvelope<PostJournalCommand> envelope =
                envelopeReader.read(message, PostJournalCommand.class);
        PostJournalCommand command = envelope.payload();

        // Đây là BIÊN VÀO của luồng Kafka: nơi duy nhất được phép đặt tenant. Dùng runWith thay vì
        // set()+clear() vì thread của listener container được tái sử dụng cho message kế tiếp —
        // quên clear() nghĩa là message sau thừa hưởng tenant của message trước, và nó sẽ ghi sổ
        // cho đúng ví đó dưới sai khách hàng.
        TenantContext.runWith(envelope.tenantId(), () -> {
            try {
                boolean processed = handler.handle(envelope.eventId(), command);
                if (!processed) {
                    log.debug("Ledger: bỏ qua message trùng eventId={}", envelope.eventId());
                }
            } catch (AppException rejection) {
                log.warn("Ledger từ chối transaction {} ({}): {} - {}",
                        command.transactionId(), command.type(),
                        rejection.getErrorCode(), rejection.getMessage());
                handler.reject(envelope.eventId(), command, rejection);
            }
        });
    }

    /**
     * Mở tài khoản cho ví (roadmap 1.0b).
     *
     * <p>Phân loại lỗi KHÁC với {@link #onPostJournal}: ở đây không có saga nào chờ một event "bị từ
     * chối" để chạy bù trừ. Lệnh mở tài khoản không hợp lệ (ref không phải UUID chuẩn, trùng tài
     * khoản hệ thống) là <b>bug phía producer</b> — thử lại vô ích, nuốt đi thì ví nằm ở
     * {@code PENDING} mãi mà không ai biết. Nên nó đi thẳng vào DLT, nơi có alert và có người xem.
     */
    @KafkaListener(topics = WalletTopics.CMD_OPEN_ACCOUNT)
    public void onOpenAccount(String message) {
        EventEnvelope<OpenAccountCommand> envelope =
                envelopeReader.read(message, OpenAccountCommand.class);

        TenantContext.runWith(envelope.tenantId(), () -> {
            try {
                handler.openAccount(envelope.payload());
            } catch (AppException invalid) {
                throw new PoisonMessageException(
                        "Lệnh mở tài khoản không hợp lệ (eventId=" + envelope.eventId() + "): "
                                + invalid.getErrorCode() + " - " + invalid.getMessage(), invalid);
            }
        });
    }
}
