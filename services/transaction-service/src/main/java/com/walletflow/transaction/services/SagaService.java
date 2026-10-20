package com.walletflow.transaction.services;

import com.walletflow.common.event.BankChargePendingEvent;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.BankFailedEvent;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Máy trạng thái của saga (roadmap 1.2b + 2.4a, ADR-014 + ADR-019) — nhận câu trả lời của ledger và
 * của bank-adapter.
 *
 * <p><b>Chuyển tiền</b> — một bước, ghi sổ ngay:
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
 * <p><b>Nạp tiền</b> — hai bước, và thứ tự của chúng là cả vấn đề: phải thu được tiền thật TRƯỚC khi
 * ghi sổ, nếu không ví được cộng số tiền chưa ai trả.
 *
 * <pre>
 *   PROCESSING ──charge-pending──▶ AWAITING_PAYMENT ──bank-completed──▶ POSTING ──posted──▶ COMPLETED
 *   PROCESSING / AWAITING_PAYMENT ──bank-failed──▶ FAILED            (không ai mất tiền)
 *   POSTING ──rejected──▶ MANUAL_REVIEW                              (⚠ đã thu tiền mà không ghi sổ được)
 *   FAILED ──bank-completed──▶ MANUAL_REVIEW                         (⚠ hai kết quả trái nhau)
 * </pre>
 *
 * <p>Máy trạng thái này chỉ TIẾN một chiều. Đó là thứ khiến event đến sai thứ tự (chuyện thường ngày
 * với ba producer trên bốn topic) trở nên vô hại: event cũ gặp trạng thái đã đi xa hơn thì không làm
 * gì, thay vì kéo giao dịch ngược về một bước đã qua.
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

    /** Bước "thu tiền qua cổng thanh toán" — chỉ luồng nạp có (2.4a). */
    public static final String STEP_BANK_CHARGE = "BANK_CHARGE";

    /** Bước "ghi sổ" — bước duy nhất của chuyển tiền, bước thứ hai của nạp tiền. */
    public static final String STEP_POST_JOURNAL = "POST_JOURNAL";

    private final TransactionRepository transactionRepository;
    private final SagaRepository sagaRepository;
    private final LedgerCommandSender ledgerCommandSender;
    private final OutboxPublisher outboxPublisher;
    private final Duration postJournalDeadline;
    private final Duration paymentDeadline;
    private final Counter statusConflicts;
    private final Counter topupNeedsReview;
    private final Counter topupResultConflicts;

    public SagaService(TransactionRepository transactionRepository, SagaRepository sagaRepository,
                       LedgerCommandSender ledgerCommandSender, OutboxPublisher outboxPublisher,
                       MeterRegistry meterRegistry,
                       @Value("${walletflow.transaction.saga-deadline:PT2M}") Duration postJournalDeadline,
                       @Value("${walletflow.transaction.topup.payment-deadline:PT45M}") Duration paymentDeadline) {
        this.transactionRepository = transactionRepository;
        this.sagaRepository = sagaRepository;
        this.ledgerCommandSender = ledgerCommandSender;
        this.outboxPublisher = outboxPublisher;
        this.postJournalDeadline = postJournalDeadline;
        this.paymentDeadline = paymentDeadline;
        // Đăng ký NGAY lúc khởi động, không phải lúc tăng lần đầu: series phải tồn tại ở 0 trước khi sự
        // cố xảy ra. Nếu nó xuất hiện lần đầu đã mang giá trị 1, `increase()` của Prometheus coi mẫu
        // đầu là mốc và trả 0 — alert không bao giờ bắn cho lần đầu, mà với sự cố hiếm thì lần đầu
        // thường cũng là lần duy nhất.
        this.statusConflicts = meterRegistry.counter("walletflow.saga.status_conflict",
                "from", "FAILED", "to", "COMPLETED");
        // Tiền đã thu của người dùng mà không vào được ví — ca duy nhất ở 2.4a cần người can thiệp.
        this.topupNeedsReview = meterRegistry.counter("walletflow.topup.manual_review");
        // bank-adapter nói hai điều trái nhau về cùng một lần thu: bất thường nghiêm trọng, không tự sửa.
        this.topupResultConflicts = meterRegistry.counter("walletflow.topup.result_conflict");
    }

    /**
     * Provider đã cấp trang thanh toán — chuyển sang phòng chờ và lưu đường dẫn để client đưa người
     * dùng tới (roadmap 2.4a).
     *
     * <p>KHÔNG phải kết quả thu tiền (ADR-016): không bút toán nào được tạo ở đây.
     */
    @Transactional
    public void onChargePending(BankChargePendingEvent event) {
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        int updated = transactionRepository.markAwaitingPayment(txn.getTenantId(), txn.getId(),
                event.provider(), event.providerRef(), event.paymentUrl());
        if (updated == 0) {
            // Lời báo tạo phiên đến SAU kết quả cuối (hai topic, không có thứ tự) hoặc đến lần thứ hai.
            log.debug("Saga: bỏ qua BankChargePending cho {} vì lệnh nạp đã ở {}", txn.getId(), txn.getStatus());
            return;
        }
        // Hạn chót của bước thu tiền phải phủ được quãng người dùng ngồi trả tiền, nếu không job quét
        // coi lệnh là treo và gửi lại lệnh thu trong lúc trang thanh toán vẫn đang mở.
        sagaRepository.postpone(txn.getId(), paymentDeadline.toSeconds());
        log.info("Saga: lệnh nạp {} đã có phiên thanh toán {} ở {} — chờ người dùng trả tiền",
                txn.getId(), event.providerRef(), event.provider());
    }

    /**
     * Provider XÁC NHẬN đã thu được tiền → giờ mới được ghi sổ (roadmap 2.4a, ADR-016 §1).
     *
     * <pre>
     *   PROCESSING / AWAITING_PAYMENT ──▶ POSTING  (gửi lệnh ghi sổ)
     *   POSTING / COMPLETED ──▶ không đổi          (bản lặp — lệnh ghi sổ đã gửi rồi)
     *   FAILED / MANUAL_REVIEW ──▶ MANUAL_REVIEW   (⚠ hai kết quả trái nhau)
     * </pre>
     */
    @Transactional
    public void onBankCompleted(BankCompletedEvent event) {
        if (event.operation() != EBankOperation.CHARGE) {
            // REFUND/PAYOUT chưa có luồng nào ở 2.4a. Bỏ qua chứ không ném: ném là message quay lại mãi
            // rồi vào DLT, trong khi đây không phải lỗi mà là tính năng chưa tới (2.4b, 2.1).
            log.debug("Saga: bỏ qua BankCompleted operation={} — chưa có luồng xử lý", event.operation());
            return;
        }
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        switch (txn.getStatus()) {
            case PROCESSING, AWAITING_PAYMENT -> startPosting(txn, event);
            case POSTING, COMPLETED ->
                    log.debug("Saga: {} đã qua bước ghi sổ — bỏ qua BankCompleted lặp", txn.getId());
            // Đã ở diện người xử lý thì để nguyên. needsReview() ghi đè failure_code, mà mã đó chính là
            // thứ runbook dùng để phân loại ca: một bản lặp của Kafka (hoặc câu trả lời bank-adapter
            // phát lại sau khi job quét gửi lại lệnh thu) sẽ xoá mất nguyên nhân gốc — "ví đã đóng"
            // thành "hai bên nói lệch nhau" — và đẩy người xử lý sang đúng mục sai của runbook.
            case MANUAL_REVIEW -> log.debug("Saga: {} đã ở diện người xử lý — bỏ qua BankCompleted lặp",
                    txn.getId());
            case FAILED -> {
                topupResultConflicts.increment();
                log.error("Saga: lệnh nạp {} đã ở FAILED nhưng provider {} lại xác nhận ĐÃ THU (ref {}) — "
                        + "không tự ghi sổ, cần người đối chiếu với provider",
                        txn.getId(), event.provider(), event.providerRef());
                needsReview(txn, "BANK_RESULT_CONFLICT",
                        "Provider xác nhận đã thu sau khi lệnh nạp đã báo thất bại");
            }
        }
    }

    /**
     * Provider từ chối DỨT KHOÁT (người dùng không trả, phiên hết hạn, bị từ chối) → lệnh nạp thất
     * bại. Không có tiền nào đã đi, nên đây là một kết cục sạch (roadmap 2.4a).
     */
    @Transactional
    public void onBankFailed(BankFailedEvent event) {
        if (event.operation() != EBankOperation.CHARGE) {
            log.debug("Saga: bỏ qua BankFailed operation={} — chưa có luồng xử lý", event.operation());
            return;
        }
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        switch (txn.getStatus()) {
            case PROCESSING, AWAITING_PAYMENT -> fail(txn, event.failureCode(),
                    "Cổng thanh toán từ chối lần thu: " + event.failureCode());
            case FAILED, MANUAL_REVIEW -> log.debug("Saga: {} đã đóng — bỏ qua BankFailed lặp", txn.getId());
            // Đã quyết ghi sổ dựa trên một lời XÁC NHẬN trước đó của chính provider. Không kéo ngược
            // trạng thái: máy trạng thái chỉ tiến một chiều, đó là tính chất khiến event đến sai thứ tự
            // trở nên vô hại. Nhưng đây là mâu thuẫn thật — kêu to để có người đối chiếu.
            case POSTING, COMPLETED -> {
                topupResultConflicts.increment();
                log.error("Saga: provider {} báo THẤT BẠI ({}) cho lệnh nạp {} đã ở {} sau khi đã xác nhận "
                        + "thu được tiền — giữ nguyên trạng thái, cần người đối chiếu",
                        event.provider(), event.failureCode(), txn.getId(), txn.getStatus());
            }
        }
    }

    /**
     * Gửi lệnh ghi sổ cho lần nạp và chuyển saga sang bước {@code POST_JOURNAL}.
     *
     * <p>Kiểm lại số tiền/loại tiền/ví trước khi ghi: lệnh thu và lời xác nhận đi qua một hệ thống
     * bên ngoài, và một lời xác nhận lệch với lệnh ta gửi là thứ KHÔNG được đem đi ghi sổ. Ghi theo
     * con số của ta thì sai sổ với ngân hàng; ghi theo con số của họ thì ví được cộng số tiền người
     * dùng không yêu cầu.
     */
    private void startPosting(Transaction txn, BankCompletedEvent event) {
        // So từ phía TA: event.walletId() null (producer hỏng) phải là "lệch", không phải NPE — NPE
        // biến một message hỏng thành lỗi kỹ thuật, tức là retry rồi DLT thay vì từ chối dứt khoát.
        if (event.amountMinor() != txn.getAmountMinor() || event.currency() != txn.getCurrency()
                || !txn.getToWallet().equals(event.walletId())) {
            topupResultConflicts.increment();
            log.error("Saga: xác nhận thu tiền của lệnh nạp {} lệch với lệnh đã gửi — lệnh {} {} vào ví {}, "
                    + "xác nhận {} {} vào ví {} (ref {}). Không ghi sổ.",
                    txn.getId(), txn.getAmountMinor(), txn.getCurrency(), txn.getToWallet(),
                    event.amountMinor(), event.currency(), event.walletId(), event.providerRef());
            needsReview(txn, "BANK_AMOUNT_MISMATCH", "Xác nhận thu tiền lệch với lệnh đã gửi");
            return;
        }

        UUID commandEventId = UUID.randomUUID();
        int advanced = sagaRepository.advanceStep(txn.getTenantId(), txn.getId(), STEP_BANK_CHARGE,
                STEP_POST_JOURNAL, commandEventId, postJournalDeadline.toSeconds());
        if (advanced == 0) {
            // Saga đã đóng hoặc đã qua bước này, dù giao dịch còn mở. Hai hàng được ghi cùng transaction
            // nên bình thường không lệch nhau (hay gặp nhất: có người mở lại hàng transactions mà quên
            // saga_state — xem runbook topup-stuck.md mục D). KHÔNG ghi sổ dựa trên một trạng thái
            // không hiểu được, nhưng cũng KHÔNG được lặng lẽ bỏ đi: tiền đã thu của người dùng rồi, mà
            // giao dịch thì không còn đường nào — job quét không nhặt saga đã đóng, và không counter
            // nào tăng. Chuyển người xử lý là cách duy nhất ca này được nhìn thấy.
            topupResultConflicts.increment();
            log.error("Saga: lệnh nạp {} ở {} nhưng saga không còn ở bước {} — tiền ĐÃ thu (ref {}), "
                    + "không ghi sổ được, chuyển người xử lý",
                    txn.getId(), txn.getStatus(), STEP_BANK_CHARGE, event.providerRef());
            needsReview(txn, "SAGA_STATE_DESYNC",
                    "Đã thu được tiền nhưng saga không còn ở bước thu — cần đối chiếu rồi ghi sổ hoặc hoàn tiền");
            return;
        }
        transactionRepository.markPosting(txn.getTenantId(), txn.getId(), event.provider(), event.providerRef());
        ledgerCommandSender.sendTopup(txn.getId(), txn.getToWallet(), txn.getAmountMinor(),
                txn.getCurrency(), commandEventId);
        log.info("Saga: lệnh nạp {} đã được provider {} xác nhận (ref {}) — gửi lệnh ghi sổ {}",
                txn.getId(), event.provider(), event.providerRef(), commandEventId);
    }

    /**
     * Khoá một lệnh NẠP của tenant hiện tại. Lọc theo loại vì bank-adapter phát event theo
     * {@code transactionId}, và một ngày nào đó id đó có thể thuộc luồng rút (2.1) — lúc ấy xử lý nó
     * bằng máy trạng thái của luồng nạp là sai.
     */
    private Optional<Transaction> lockTopup(UUID transactionId) {
        return lockOurs(transactionId, EJournalType.TOPUP.name());
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
            case PROCESSING, POSTING, MANUAL_REVIEW -> complete(txn, event);
            // Ledger ghi bút toán cho một lệnh nạp còn đang chờ người dùng trả tiền. Đây KHÔNG phải
            // "sai thứ tự": lệnh ghi sổ chỉ ra đời ở startPosting, cùng transaction với việc đặt
            // POSTING, nên ở trạng thái này ta chưa từng ra lệnh ghi sổ nào. Ledger vẫn thắng (bút
            // toán là sự thật về tiền), nhưng có một bút toán không ai giải thích được — đếm lại.
            case AWAITING_PAYMENT -> {
                topupResultConflicts.increment();
                log.error("Saga: {} còn AWAITING_PAYMENT nhưng ledger đã ghi bút toán {} mà ta CHƯA ra "
                        + "lệnh — ledger thắng, nhưng cần người truy nguồn bút toán này",
                        txn.getId(), event.entryId());
                complete(txn, event);
            }
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
            case PROCESSING, POSTING, MANUAL_REVIEW -> rejectOpen(txn, event);
            case FAILED -> log.debug("Saga: {} đã FAILED — bỏ qua LedgerRejected lặp", txn.getId());
            // Posted và rejected đi trên hai topic, không có thứ tự: lời từ chối của lệnh GỐC có thể đến
            // sau bút toán của lần GỬI LẠI. Tiền đã đi — lời từ chối cũ không còn ý nghĩa.
            case COMPLETED -> log.warn("Saga: bỏ qua LedgerRejected ({}) cho {} vì ledger đã ghi sổ giao dịch này",
                    event.reasonCode(), txn.getId());
            // Chưa gửi lệnh ghi sổ nào thì không có lời từ chối nào ứng với trạng thái này.
            case AWAITING_PAYMENT -> log.warn("Saga: bỏ qua LedgerRejected ({}) cho {} vì lệnh nạp còn đang chờ "
                    + "người dùng trả tiền", event.reasonCode(), txn.getId());
        }
    }

    /**
     * Ledger từ chối một giao dịch còn mở. Chuyển tiền thì đây là kết cục bình thường (thiếu tiền) →
     * FAILED. Nạp tiền thì KHÁC HẲN: ta chỉ gửi lệnh ghi sổ SAU khi provider xác nhận đã thu được
     * tiền, nên từ chối ở đây nghĩa là tiền đã ra khỏi tài khoản ngân hàng của người dùng mà không vào
     * được ví. Đánh FAILED là báo "không mất tiền" trong khi họ đã bị trừ.
     */
    private void rejectOpen(Transaction txn, LedgerRejectedEvent event) {
        if (txn.getType() != EJournalType.TOPUP) {
            fail(txn, event.reasonCode(), event.reason());
            return;
        }
        // Lệnh ghi sổ của một lần nạp CHỈ được xếp vào outbox ở startPosting, và cùng transaction đó
        // đặt trạng thái POSTING. Nên một lời từ chối gửi tới lệnh nạp chưa POSTING không ứng với lệnh
        // nào của ta. Đóng saga ở đây là bỏ rơi một lệnh nạp còn sống: job quét không nhặt saga đã
        // đóng, nên không ai gửi lại lệnh thu, và ca này còn bị đếm vào alert mức page dù chưa ai mất
        // đồng nào.
        if (txn.getStatus() != ETransactionStatus.POSTING) {
            log.warn("Saga: bỏ qua LedgerRejected ({}) cho lệnh nạp {} ở {} — chưa gửi lệnh ghi sổ nào",
                    event.reasonCode(), txn.getId(), txn.getStatus());
            return;
        }
        log.error("Saga: ledger TỪ CHỐI lệnh nạp {} ({}) nhưng tiền đã thu ở provider {} (ref {}) — cần "
                + "người hoàn tiền thủ công; hoàn tự động là 2.4b",
                txn.getId(), event.reasonCode(), txn.getProvider(), txn.getProviderRef());
        needsReview(txn, event.reasonCode(), event.reason());
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
                notifyKey(txn),
                stableEventId("txn-completed", txn.getId()),
                "TransactionCompleted",
                new TransactionCompletedEvent(txn.getId(), txn.getType(), txn.getInitiatedBy(),
                        txn.getFromWallet(), txn.getToWallet(), txn.getAmountMinor(), txn.getCurrency(),
                        event.entryId()));
        log.info("Saga: giao dịch {} COMPLETED (bút toán {})", txn.getId(), event.entryId());
    }

    private void fail(Transaction txn, String failureCode, String failureReason) {
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.FAILED.name(), failureCode, failureReason);
        sagaRepository.close(txn.getId(), "FAILED");
        outboxPublisher.publish(
                WalletTopics.EVT_TXN_FAILED,
                notifyKey(txn),
                stableEventId("txn-failed", txn.getId()),
                "TransactionFailed",
                new TransactionFailedEvent(txn.getId(), txn.getType(), txn.getInitiatedBy(),
                        txn.getFromWallet(), txn.getToWallet(), txn.getAmountMinor(), txn.getCurrency(),
                        failureCode));
        log.info("Saga: giao dịch {} FAILED ({})", txn.getId(), failureCode);
    }

    /**
     * Partition key của event báo cho người dùng. Lệnh nạp không có ví nguồn (V3), nên dùng ví đích —
     * cái cần giữ đúng thứ tự là chuỗi event của MỘT ví, và với mỗi loại giao dịch chỉ có một ví là
     * ví của người nhận thông báo.
     */
    private static String notifyKey(Transaction txn) {
        return (txn.getFromWallet() != null ? txn.getFromWallet() : txn.getToWallet()).toString();
    }

    /**
     * Chuyển một lệnh nạp sang diện cần người xử lý. Dùng cho mọi ca mà tiền có thể ĐANG ở chỗ provider
     * mà không vào được ví: ledger từ chối, hoặc hai kết quả trái nhau từ bank-adapter.
     *
     * <p>Cố ý KHÔNG tự đánh FAILED: FAILED nói với người dùng "không mất tiền", mà ở đây tiền đã bị
     * trừ ở ngân hàng của họ. Hoàn tiền tự động là việc của 2.4b; tới lúc đó, một người phải nhìn và
     * quyết (roadmap §5.7).
     */
    private void needsReview(Transaction txn, String failureCode, String failureReason) {
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.MANUAL_REVIEW.name(), failureCode, failureReason);
        sagaRepository.close(txn.getId(), "MANUAL_REVIEW");
        topupNeedsReview.increment();
    }

    /**
     * eventId tất định theo (loại kết quả, giao dịch): outbox giao lại thì consumer dedup được. Mỗi giao
     * dịch có tối đa một event mỗi loại — kể cả ca xung đột FAILED → COMPLETED, hai event có hai id.
     */
    static UUID stableEventId(String kind, UUID transactionId) {
        return UUID.nameUUIDFromBytes((kind + ":" + transactionId).getBytes(StandardCharsets.UTF_8));
    }
}
