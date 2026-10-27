package com.walletflow.transaction.services;

import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.entity.SagaState;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tìm saga đã gửi lệnh mà quá hạn chưa nhận được câu trả lời của ledger (roadmap 1.2b, ADR-014).
 *
 * <p>Saga treo khi lệnh hoặc câu trả lời mất ở đâu đó: relay chết, message vào DLT, consumer lỗi kéo
 * dài. Không có job này thì giao dịch nằm PROCESSING mãi, và không có lỗi nào được báo.
 *
 * <ul>
 *   <li><b>Còn lượt thử:</b> gửi lại lệnh với CÙNG {@code transactionId} (ledger chỉ ghi một bút toán)
 *       và eventId MỚI (eventId cũ bị ledger bỏ qua mà không phát lại kết quả — xem
 *       {@link LedgerCommandSender}). Hạn chót kế tiếp giãn dần theo số lần thử.</li>
 *   <li><b>Hết lượt:</b> MANUAL_REVIEW + metric cho alert. Không đoán FAILED (tiền có thể đã đi) hay
 *       COMPLETED (có thể chưa).</li>
 * </ul>
 *
 * <p>Chạy XUYÊN tenant: mỗi saga được xử lý trong {@code TenantContext} của chính nó, để lệnh gửi lại
 * mang đúng tenant trong envelope. Nhiều instance chạy cùng lúc an toàn nhờ {@code SKIP LOCKED}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.transaction.saga-scan", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class StuckSagaScanner {

    private final SagaRepository sagaRepository;
    private final TransactionRepository transactionRepository;
    private final LedgerCommandSender commandSender;
    private final BankCommandSender bankCommandSender;
    private final TransactionTemplate transactionTemplate;
    private final Duration deadline;
    private final int maxAttempts;
    private final int batchSize;
    private final Counter retries;
    private final Counter manualReviews;
    private final Counter scanFailures;
    private final Counter topupRefundsFailed;

    public StuckSagaScanner(
            SagaRepository sagaRepository,
            TransactionRepository transactionRepository,
            LedgerCommandSender commandSender,
            BankCommandSender bankCommandSender,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager,
            @Value("${walletflow.transaction.saga-deadline:PT2M}") Duration deadline,
            @Value("${walletflow.transaction.saga-scan.max-attempts:3}") int maxAttempts,
            @Value("${walletflow.transaction.saga-scan.batch-size:50}") int batchSize) {
        this.sagaRepository = sagaRepository;
        this.transactionRepository = transactionRepository;
        this.commandSender = commandSender;
        this.bankCommandSender = bankCommandSender;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.deadline = deadline;
        this.maxAttempts = maxAttempts;
        this.batchSize = batchSize;
        // Đăng ký lúc khởi động để series tồn tại ở 0 — xem SagaService về vì sao `increase()` bỏ lỡ
        // lần tăng đầu tiên của một counter sinh ra muộn.
        this.retries = meterRegistry.counter("walletflow.saga.retries");
        this.manualReviews = meterRegistry.counter("walletflow.saga.manual_review");
        this.scanFailures = meterRegistry.counter("walletflow.saga.scan_failures");
        // CÙNG counter với SagaService: "hoàn không được" là một sự kiện nghiệp vụ, không phải hai —
        // dù nó tới từ lời từ chối của provider hay từ việc gửi hết lượt mà không ai trả lời. Alert
        // TopupRefundFailed treo trên counter này, và runbook mục E xếp cả hai ca dưới nó.
        this.topupRefundsFailed = meterRegistry.counter("walletflow.topup.refund_failed");
    }

    /** @return số saga đã xử lý (kể cả saga xử lý lỗi) trong lượt này */
    @Scheduled(fixedDelayString = "${walletflow.transaction.saga-scan.interval-ms:15000}",
            initialDelayString = "${walletflow.transaction.saga-scan.interval-ms:15000}")
    public int scan() {
        int handled = 0;
        while (handled < batchSize) {
            AtomicReference<SagaState> current = new AtomicReference<>();
            try {
                // Mỗi saga một transaction riêng: một saga lỗi không kéo cả lô rollback, và khoá hàng
                // (SKIP LOCKED) được nhả ngay sau khi xử lý xong saga đó.
                Boolean found = transactionTemplate.execute(status -> handleNextOverdue(current));
                if (!Boolean.TRUE.equals(found)) {
                    break;
                }
            } catch (RuntimeException failure) {
                if (current.get() == null) {
                    throw failure;   // lỗi trước khi giành được saga nào (DB chết...) — để scheduler log
                }
                postpone(current.get(), failure);
            }
            handled++;
        }
        return handled;
    }

    /**
     * Saga xử lý lỗi: đẩy hạn chót của RIÊNG nó lùi lại, trong transaction mới (transaction cũ đã
     * rollback). Không làm vậy thì nó vẫn là saga quá hạn lâu nhất, đứng đầu hàng ở MỌI lượt quét, lỗi
     * lại, và mọi saga treo khác phía sau không bao giờ tới lượt — trong im lặng.
     */
    private void postpone(SagaState saga, RuntimeException failure) {
        scanFailures.increment();
        log.error("Saga: xử lý saga {} lỗi — đẩy lùi hạn chót để không chặn các saga khác",
                saga.getTransactionId(), failure);
        transactionTemplate.executeWithoutResult(status ->
                sagaRepository.postpone(saga.getTenantId(), saga.getTransactionId(), deadline.toSeconds()));
    }

    private boolean handleNextOverdue(AtomicReference<SagaState> current) {
        Optional<SagaState> next = sagaRepository.lockNextOverdueAcrossTenants();
        if (next.isEmpty()) {
            return false;
        }
        SagaState saga = next.get();
        current.set(saga);
        TenantContext.runWith(saga.getTenantId(), () -> handle(saga));
        return true;
    }

    /**
     * Trạng thái mà một saga còn mở được phép đứng ở. Lệnh nạp nằm ngoài PROCESSING gần như suốt vòng
     * đời (chờ người dùng trả tiền, rồi chờ ledger), nên nếu chỉ chấp nhận PROCESSING thì job này sẽ
     * đóng nhầm mọi saga nạp đang khoẻ mạnh.
     */
    private static boolean isOpen(ETransactionStatus status) {
        return status == ETransactionStatus.PROCESSING
                || status == ETransactionStatus.AWAITING_PAYMENT
                || status == ETransactionStatus.POSTING
                // Bù trừ (2.4b): thiếu trạng thái này thì job quét ĐÓNG mọi saga đang hoàn tiền, và
                // lệnh hoàn mất trên đường không bao giờ được gửi lại — tiền của người dùng ở lại chỗ
                // provider trong im lặng. Compensation phải luôn đi tới cùng (roadmap §2.4).
                || status == ETransactionStatus.REFUNDING;
    }

    private void handle(SagaState saga) {
        Optional<Transaction> found = transactionRepository.lockForSaga(saga.getTenantId(), saga.getTransactionId());
        if (found.isEmpty() || !isOpen(found.get().getStatus())) {
            // Saga mở mà giao dịch đã đóng: hai thứ được ghi cùng transaction nên bình thường không xảy
            // ra. Đóng saga để không quét lại mãi, và kêu to để có người xem. (Mở lại một ca
            // MANUAL_REVIEW phải đặt CẢ giao dịch về PROCESSING — xem runbook saga-stuck.md.)
            log.warn("Saga: {} còn SENT nhưng giao dịch là {} — đóng saga", saga.getTransactionId(),
                    found.map(t -> t.getStatus().name()).orElse("không tồn tại"));
            sagaRepository.close(saga.getTenantId(), saga.getTransactionId(), "DONE");
            return;
        }
        Transaction txn = found.get();

        // Hai hàng phải đồng ý với nhau về việc "đang hoàn tiền hay không" — chúng được ghi cùng một
        // transaction nên chỉ sửa tay mới làm lệch (runbook mục D, đặt nhầm bước). Lệch mà vẫn gửi là
        // đúng cái runbook cảnh báo: gửi lệnh GHI SỔ cho giao dịch đang hoàn tiền thì ví được cộng số
        // tiền vừa trả về thẻ; gửi lệnh HOÀN cho giao dịch đang chờ ghi sổ thì trả lại tiền của một lần
        // nạp sắp thành công. Cưỡng chế bằng code, không bằng một dòng tài liệu.
        if ((txn.getStatus() == ETransactionStatus.REFUNDING)
                != SagaService.STEP_BANK_REFUND.equals(saga.getCurrentStep())) {
            transactionRepository.transition(txn.getTenantId(), txn.getId(),
                    ETransactionStatus.MANUAL_REVIEW.name(), "SAGA_STATE_DESYNC",
                    "Giao dịch " + txn.getStatus() + " nhưng saga đứng ở bước " + saga.getCurrentStep()
                            + " — không gửi lại lệnh nào");
            sagaRepository.close(txn.getTenantId(), txn.getId(), "MANUAL_REVIEW");
            manualReviews.increment();
            log.error("Saga: giao dịch {} đang {} nhưng saga ở bước {} — KHÔNG gửi lại lệnh nào, chuyển "
                    + "người xử lý", txn.getId(), txn.getStatus(), saga.getCurrentStep());
            return;
        }

        if (saga.getAttemptCount() >= maxAttempts) {
            giveUp(saga, txn);
            return;
        }

        UUID newEventId = UUID.randomUUID();
        resend(saga, txn, newEventId);
        // Giãn dần: lần thử thứ n chờ n lần hạn chót — ledger đang quá tải thì không dội thêm
        long nextDeadline = deadline.toSeconds() * (saga.getAttemptCount() + 1L);
        sagaRepository.recordRetry(txn.getTenantId(), txn.getId(), newEventId, nextDeadline);
        retries.increment();
        log.warn("Saga: giao dịch {} quá hạn chưa có câu trả lời ở bước {} — gửi lại lần {} (eventId {})",
                txn.getId(), saga.getCurrentStep(), saga.getAttemptCount() + 1, newEventId);
    }

    /**
     * Hết lượt gửi lại: chuyển người xử lý. Không đoán FAILED (tiền có thể đã đi) hay COMPLETED (có thể
     * chưa) — chỉ ghi lại ai đã im lặng, để runbook phân loại được.
     */
    private void giveUp(SagaState saga, Transaction txn) {
        // Mã + người im lặng đi cùng nhau: một switch duy nhất để chúng không bao giờ lệch nhau khi
        // thêm một bước saga mới.
        record NoAnswer(String code, String who) { }
        NoAnswer silent = switch (saga.getCurrentStep()) {
            case SagaService.STEP_BANK_CHARGE -> new NoAnswer("BANK_NO_RESPONSE", "Cổng thanh toán");
            // Mã riêng: ca này là tiền ĐÃ thu của người dùng mà lệnh trả lại không có hồi âm — runbook
            // xử lý khác hẳn một lần thu không trả lời (ở đó chưa chắc có đồng nào đi).
            case SagaService.STEP_BANK_REFUND ->
                    new NoAnswer("REFUND_NO_RESPONSE", "Cổng thanh toán (lệnh hoàn)");
            default -> new NoAnswer("LEDGER_NO_RESPONSE", "Ledger");
        };
        // Nguyên nhân phải hoàn đi kèm vào failure_reason: transition() ghi đè failure_code, mà mã gốc
        // (ví đã đóng...) là thứ runbook mục E cần để quyết. Mất nó thì chỉ còn log.
        String why = silent.who() + " không trả lời sau " + saga.getAttemptCount() + " lần gửi"
                + (SagaService.STEP_BANK_REFUND.equals(saga.getCurrentStep())
                        ? ". Nguyên nhân phải hoàn: " + refundReason(txn) : "");
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.MANUAL_REVIEW.name(), silent.code(), why);
        sagaRepository.close(txn.getTenantId(), txn.getId(), "MANUAL_REVIEW");
        manualReviews.increment();
        if (SagaService.STEP_BANK_REFUND.equals(saga.getCurrentStep())) {
            // Tiền của người dùng vẫn ở chỗ provider mà máy đã hết đường tự gỡ — đúng định nghĩa của
            // alert TopupRefundFailed. Không tăng thì runbook trỏ tới một alert không bao giờ kêu.
            topupRefundsFailed.increment();
        }
        log.error("Saga: giao dịch {} chuyển MANUAL_REVIEW sau {} lần gửi không có câu trả lời ở bước {} — "
                + "cần người đối chiếu", txn.getId(), saga.getAttemptCount(), saga.getCurrentStep());
    }

    /**
     * Gửi lại lệnh của ĐÚNG bước saga đang đứng. Gửi nhầm lệnh là hỏng nặng: một lệnh nạp còn đang chờ
     * người dùng trả tiền mà bị gửi lệnh ghi sổ thì ví được cộng số tiền chưa ai trả.
     *
     * <p>Cả hai lệnh đều an toàn khi gửi lại: ledger idempotent theo {@code transactionId} (một bút
     * toán duy nhất), bank-adapter idempotent theo {@code (tenant, transactionId)} (một phiên thanh
     * toán duy nhất, và phát lại câu trả lời hiện tại — ADR-016).
     */
    private void resend(SagaState saga, Transaction txn, UUID eventId) {
        // Bước bù trừ được kiểm TRƯỚC nhánh theo loại giao dịch: một lệnh nạp đang hoàn tiền mà bị gửi
        // lại lệnh GHI SỔ thì ví được cộng số tiền vừa trả về thẻ người dùng — vừa hoàn vừa cộng.
        // (Cặp (status, current_step) đã được handle() kiểm khớp trước khi tới đây.)
        if (SagaService.STEP_BANK_REFUND.equals(saga.getCurrentStep())) {
            bankCommandSender.sendRefund(txn.getId(), txn.getToWallet(), txn.getAmountMinor(),
                    txn.getCurrency(), refundReason(txn), eventId);
            return;
        }
        if (SagaService.STEP_BANK_CHARGE.equals(saga.getCurrentStep())) {
            bankCommandSender.sendCharge(txn.getId(), txn.getToWallet(), txn.getAmountMinor(),
                    txn.getCurrency(), null, eventId);
            return;
        }
        switch (txn.getType()) {
            case TRANSFER -> commandSender.sendTransfer(txn.getId(), txn.getFromWallet(), txn.getToWallet(),
                    txn.getAmountMinor(), txn.getCurrency(), eventId);
            case TOPUP -> commandSender.sendTopup(txn.getId(), txn.getToWallet(), txn.getAmountMinor(),
                    txn.getCurrency(), eventId);
            // WITHDRAW/REVERSAL chưa có luồng nào tạo ra saga — gặp là bug, và bug trên đường tiền thì
            // dừng lại chứ không đoán một lệnh để gửi.
            default -> throw new IllegalStateException(
                    "Không biết gửi lại lệnh nào cho giao dịch loại " + txn.getType());
        }
    }

    /**
     * Mã nguyên nhân phải hoàn — {@code SagaService} ghi nó vào {@code failure_code} trong cùng
     * transaction với việc gửi lệnh hoàn đầu tiên. {@code reasonCode} là field bắt buộc của schema nên
     * có giá trị thay thế, phòng ca cột bị sửa tay thành null.
     */
    private static String refundReason(Transaction txn) {
        return txn.getFailureCode() != null ? txn.getFailureCode() : "TOPUP_REFUND";
    }
}
