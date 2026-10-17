package com.walletflow.transaction.services;

import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.TransactionCompletedEvent;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * Máy trạng thái của saga chuyển tiền (roadmap 1.2b, ADR-014) — nhận câu trả lời của ledger.
 *
 * <pre>
 *   PROCESSING ──posted──▶ COMPLETED
 *   PROCESSING ──rejected─▶ FAILED
 *   PROCESSING ──quá hạn, hết lần thử──▶ MANUAL_REVIEW   (job quét saga treo)
 *   MANUAL_REVIEW ──posted──▶ COMPLETED   ·  ──rejected──▶ FAILED
 *   FAILED ──posted──▶ COMPLETED   ⚠ xung đột: ledger thắng, kèm cảnh báo
 *   COMPLETED ──bất cứ gì──▶ COMPLETED   (trạng thái cuối)
 * </pre>
 *
 * <p><b>Sổ cái là sự thật duy nhất về tiền.</b> {@code LedgerPosted} nghĩa là tiền ĐÃ đi — không trạng
 * thái nào ở đây được phủ nhận điều đó, kể cả FAILED. Ca FAILED → COMPLETED xảy ra được: lệnh gốc bị
 * từ chối (thiếu tiền) nhưng câu trả lời đến muộn, trong lúc đó job quét đã gửi lại và lần gửi lại
 * được ghi sổ (tiền vừa về). Người dùng đã được báo thất bại, nay được báo thành công — khó chịu
 * nhưng ĐÚNG; còn giữ FAILED thì báo cáo nói dối về tiền đã thật sự di chuyển.
 *
 * <p>Mỗi lần chuyển trạng thái phát ĐÚNG MỘT event cho notification-service, với eventId tất định —
 * chuyển trạng thái được quyết dưới khoá hàng giao dịch, nên event lặp của ledger không phát lại.
 */
@Slf4j
@Service
public class SagaService {

    private final TransactionRepository transactionRepository;
    private final SagaRepository sagaRepository;
    private final OutboxPublisher outboxPublisher;
    private final Counter statusConflicts;

    public SagaService(TransactionRepository transactionRepository, SagaRepository sagaRepository,
                       OutboxPublisher outboxPublisher, MeterRegistry meterRegistry) {
        this.transactionRepository = transactionRepository;
        this.sagaRepository = sagaRepository;
        this.outboxPublisher = outboxPublisher;
        // Đăng ký NGAY lúc khởi động, không phải lúc tăng lần đầu: series phải tồn tại ở 0 trước khi sự
        // cố xảy ra. Nếu nó xuất hiện lần đầu đã mang giá trị 1, `increase()` của Prometheus coi mẫu
        // đầu là mốc và trả 0 — alert không bao giờ bắn cho lần đầu, mà với sự cố hiếm thì lần đầu
        // thường cũng là lần duy nhất.
        this.statusConflicts = meterRegistry.counter("walletflow.saga.status_conflict",
                "from", "FAILED", "to", "COMPLETED");
    }

    @Transactional
    public void onPosted(LedgerPostedEvent event) {
        Optional<Transaction> locked = lockOurs(event.transactionId(), event.type().name());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        switch (txn.getStatus()) {
            case COMPLETED -> log.debug("Saga: {} đã COMPLETED — bỏ qua LedgerPosted lặp", txn.getId());
            case PROCESSING, MANUAL_REVIEW -> complete(txn, event);
            case FAILED -> {
                statusConflicts.increment();
                log.error("Saga: giao dịch {} đã báo FAILED nhưng ledger GHI SỔ bút toán {} — ledger thắng, "
                        + "chuyển COMPLETED. Người dùng đã nhận thông báo thất bại trước đó.",
                        txn.getId(), event.entryId());
                complete(txn, event);
            }
        }
    }

    @Transactional
    public void onRejected(LedgerRejectedEvent event) {
        Optional<Transaction> locked = lockOurs(event.transactionId(), event.type().name());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        switch (txn.getStatus()) {
            case PROCESSING, MANUAL_REVIEW -> fail(txn, event);
            case FAILED -> log.debug("Saga: {} đã FAILED — bỏ qua LedgerRejected lặp", txn.getId());
            // Posted và rejected đi trên hai topic, không có thứ tự: lời từ chối của lệnh GỐC có thể đến
            // sau bút toán của lần GỬI LẠI. Tiền đã đi — lời từ chối cũ không còn ý nghĩa.
            case COMPLETED -> log.warn("Saga: bỏ qua LedgerRejected ({}) cho {} vì ledger đã ghi sổ giao dịch này",
                    event.reasonCode(), txn.getId());
        }
    }

    /**
     * Khoá giao dịch của tenant hiện tại. Rỗng nếu không phải giao dịch của service này (ledger phát
     * event cho MỌI bút toán, kể cả của luồng khác) hoặc khác loại bút toán (một REVERSAL bù trừ cho
     * giao dịch này về sau không phải là câu trả lời cho lệnh TRANSFER).
     */
    private Optional<Transaction> lockOurs(UUID transactionId, String journalType) {
        UUID tenantId = TenantContext.require();
        // Saga TRƯỚC, giao dịch SAU — cùng thứ tự với StuckSagaScanner, nếu không hai bên có thể
        // deadlock khi job quét gửi lại đúng lúc câu trả lời của ledger tới.
        sagaRepository.lock(tenantId, transactionId);
        return transactionRepository.lockForSaga(tenantId, transactionId)
                .filter(txn -> txn.getType().name().equals(journalType));
    }

    private void complete(Transaction txn, LedgerPostedEvent event) {
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.COMPLETED.name(), null, null);
        sagaRepository.close(txn.getId(), "DONE");
        outboxPublisher.publish(
                WalletTopics.EVT_TXN_COMPLETED,
                txn.getFromWallet().toString(),
                stableEventId("txn-completed", txn.getId()),
                "TransactionCompleted",
                new TransactionCompletedEvent(txn.getId(), txn.getType(), txn.getInitiatedBy(),
                        txn.getFromWallet(), txn.getToWallet(), txn.getAmountMinor(), txn.getCurrency(),
                        event.entryId()));
        log.info("Saga: giao dịch {} COMPLETED (bút toán {})", txn.getId(), event.entryId());
    }

    private void fail(Transaction txn, LedgerRejectedEvent event) {
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.FAILED.name(), event.reasonCode(), event.reason());
        sagaRepository.close(txn.getId(), "FAILED");
        outboxPublisher.publish(
                WalletTopics.EVT_TXN_FAILED,
                txn.getFromWallet().toString(),
                stableEventId("txn-failed", txn.getId()),
                "TransactionFailed",
                new TransactionFailedEvent(txn.getId(), txn.getType(), txn.getInitiatedBy(),
                        txn.getFromWallet(), txn.getToWallet(), txn.getAmountMinor(), txn.getCurrency(),
                        event.reasonCode()));
        log.info("Saga: giao dịch {} FAILED ({})", txn.getId(), event.reasonCode());
    }

    /**
     * eventId tất định theo (loại kết quả, giao dịch): outbox giao lại thì consumer dedup được. Mỗi giao
     * dịch có tối đa một event mỗi loại — kể cả ca xung đột FAILED → COMPLETED, hai event có hai id.
     */
    static UUID stableEventId(String kind, UUID transactionId) {
        return UUID.nameUUIDFromBytes((kind + ":" + transactionId).getBytes(StandardCharsets.UTF_8));
    }
}
