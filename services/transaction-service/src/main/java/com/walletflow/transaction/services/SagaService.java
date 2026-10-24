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
 *   POSTING ──rejected──▶ REFUNDING ──refund-completed──▶ FAILED     (⚠ đã thu tiền mà không ghi sổ được)
 *   FAILED ──bank-completed──▶ REFUNDING                             (⚠ hai kết quả trái nhau)
 *   REFUNDING ──refund-failed──▶ MANUAL_REVIEW                       (⚠ không hoàn được — người hoàn tay)
 * </pre>
 *
 * <p>Hai mũi tên vào {@code REFUNDING} chỉ tồn tại khi cờ {@code walletflow.transaction.topup
 * .refund-enabled} bật (2.4b, ADR-020); tắt thì cả hai ca dừng ở {@code MANUAL_REVIEW} như 2.4a. Cờ
 * mặc định TẮT vì hoàn tiền tự động chỉ an toàn khi mọi instance ledger đã có hàng rào
 * {@code rejected_topups} (ledger V6): không có hàng rào thì một bản gửi lại của lệnh ghi sổ vẫn có
 * thể được ghi sau khi ta đã hoàn tiền — ví được cộng VÀ tiền đã về thẻ, tức là tạo ra tiền.
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

    /** Bước bù trừ của nạp tiền: trả lại lần thu đã thành công mà không ghi sổ được (2.4b). */
    public static final String STEP_BANK_REFUND = "BANK_REFUND";

    /** Mã ghi lại khi lệnh hoàn bị provider từ chối dứt khoát — ca phải hoàn tay qua portal. */
    static final String REFUND_DECLINED = "REFUND_DECLINED";

    private final TransactionRepository transactionRepository;
    private final SagaRepository sagaRepository;
    private final LedgerCommandSender ledgerCommandSender;
    private final BankCommandSender bankCommandSender;
    private final OutboxPublisher outboxPublisher;
    private final Duration postJournalDeadline;
    private final Duration paymentDeadline;
    private final boolean refundEnabled;
    private final Counter statusConflicts;
    private final Counter topupNeedsReview;
    private final Counter topupResultConflicts;
    private final Counter topupRefundsStarted;
    private final Counter topupRefundsFailed;

    public SagaService(TransactionRepository transactionRepository, SagaRepository sagaRepository,
                       LedgerCommandSender ledgerCommandSender, BankCommandSender bankCommandSender,
                       OutboxPublisher outboxPublisher,
                       MeterRegistry meterRegistry,
                       @Value("${walletflow.transaction.saga-deadline:PT2M}") Duration postJournalDeadline,
                       @Value("${walletflow.transaction.topup.payment-deadline:PT45M}") Duration paymentDeadline,
                       @Value("${walletflow.transaction.topup.refund-enabled:false}") boolean refundEnabled) {
        this.transactionRepository = transactionRepository;
        this.sagaRepository = sagaRepository;
        this.ledgerCommandSender = ledgerCommandSender;
        this.bankCommandSender = bankCommandSender;
        this.outboxPublisher = outboxPublisher;
        this.postJournalDeadline = postJournalDeadline;
        this.paymentDeadline = paymentDeadline;
        this.refundEnabled = refundEnabled;
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
        // Đã thu tiền mà không ghi sổ được -> đang trả lại. Hệ thống tự xử lý, nhưng tần suất tăng đột
        // biến nghĩa là ledger đang từ chối hàng loạt — thứ phải nhìn thấy trước khi khách hàng kêu.
        this.topupRefundsStarted = meterRegistry.counter("walletflow.topup.refund_started");
        // Hoàn KHÔNG được: tiền của người dùng vẫn ở chỗ provider và chỉ người mới gỡ được.
        this.topupRefundsFailed = meterRegistry.counter("walletflow.topup.refund_failed");
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
     *   FAILED ──▶ REFUNDING | MANUAL_REVIEW       (⚠ hai kết quả trái nhau — tiền phải trả lại)
     *   MANUAL_REVIEW ──▶ không đổi                (đã ở diện người xử lý)
     * </pre>
     */
    @Transactional
    public void onBankCompleted(BankCompletedEvent event) {
        if (event.operation() == EBankOperation.REFUND) {
            onRefundCompleted(event);
            return;
        }
        if (event.operation() != EBankOperation.CHARGE) {
            // PAYOUT thuộc luồng rút tiền (2.1), chưa mở. Bỏ qua chứ không ném: ném là message quay lại
            // mãi rồi vào DLT, trong khi đây không phải lỗi mà là tính năng chưa tới.
            log.debug("Saga: bỏ qua BankCompleted operation={} — chưa có luồng xử lý", event.operation());
            return;
        }
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();
        if (alreadyResolved(txn, "provider " + event.provider() + " xác nhận ĐÃ THU (ref "
                + event.providerRef() + ")")) {
            return;
        }

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
            // Lệnh hoàn đã gửi: lời xác nhận thu tiền ở đây là bản lặp của chính lời đã đưa ta tới đây.
            case REFUNDING -> log.debug("Saga: {} đang hoàn tiền — bỏ qua BankCompleted lặp", txn.getId());
            case FAILED -> {
                // FAILED sau khi đã hoàn tiền là kết cục ĐÚNG của bù trừ, không phải xung đột. Lời xác
                // nhận thu tiền ở đây là bản lặp của chính lời đã khởi động lần hoàn đó (outbox
                // at-least-once, hoặc bank-adapter phát lại câu trả lời hiện tại sau một lần gửi lại —
                // ADR-016). Không có dấu này thì mỗi bản lặp kéo một giao dịch đã đóng đúng sang
                // MANUAL_REVIEW, xoá mất failure_code gốc và bắn alert mức page cho một ca không sai gì.
                if (refundStarted(txn)) {
                    log.debug("Saga: {} đã hoàn tiền xong — bỏ qua BankCompleted lặp", txn.getId());
                    return;
                }
                topupResultConflicts.increment();
                log.error("Saga: lệnh nạp {} đã ở FAILED nhưng provider {} lại xác nhận ĐÃ THU (ref {}) — "
                        + "không ghi sổ, trả lại tiền",
                        txn.getId(), event.provider(), event.providerRef());
                startRefund(txn, "BANK_RESULT_CONFLICT",
                        "Provider xác nhận đã thu sau khi lệnh nạp đã báo thất bại");
            }
        }
    }

    /**
     * bank-adapter đã hoàn xong tiền cho một lần nạp không ghi sổ được (2.4b, ADR-020) — kết cục là
     * {@code FAILED}, và lần này FAILED nói đúng sự thật: người dùng không mất tiền.
     *
     * <p>Giữ nguyên {@code failure_code} của nguyên nhân gốc (mã từ chối của ledger,
     * {@code BANK_RESULT_CONFLICT}) thay vì đặt một mã "đã hoàn": mã đó là thứ báo cáo dùng để biết vì
     * sao lệnh nạp hỏng, còn việc đã hoàn thì đọc được ở lịch sử trạng thái và ở bank-adapter.
     */
    private void onRefundCompleted(BankCompletedEvent event) {
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();
        if (txn.getStatus() != ETransactionStatus.REFUNDING) {
            onRefundCompletedLate(txn, event);
            return;
        }
        // Lớp phòng thủ thứ hai sau bank-adapter (nó kiểm lệnh hoàn khớp từng con số với lần thu, lệch
        // thì DLT). Hoàn một số tiền khác số đã thu là trả người dùng tiền họ chưa từng trả, hoặc giữ
        // lại một phần tiền của họ — không được đóng giao dịch như thể mọi thứ ổn.
        if (event.amountMinor() != txn.getAmountMinor() || event.currency() != txn.getCurrency()
                || !txn.getToWallet().equals(event.walletId())) {
            topupResultConflicts.increment();
            log.error("Saga: xác nhận hoàn tiền của lệnh nạp {} lệch với lần thu — nạp {} {} vào ví {}, "
                    + "hoàn {} {} vào ví {} (provider {} ref {}). Không đóng giao dịch.",
                    txn.getId(), txn.getAmountMinor(), txn.getCurrency(), txn.getToWallet(),
                    event.amountMinor(), event.currency(), event.walletId(), event.provider(),
                    event.providerRef());
            needsReview(txn, "REFUND_AMOUNT_MISMATCH",
                    "Số tiền hoàn lệch với số đã thu — đối chiếu với provider trước khi đóng");
            return;
        }
        log.info("Saga: đã hoàn {} {} cho lệnh nạp {} ({}) — provider {} ref {}", txn.getAmountMinor(),
                txn.getCurrency(), txn.getId(), txn.getFailureCode(), event.provider(), event.providerRef());
        fail(txn, refundCause(txn), "Đã hoàn tiền cho người dùng. Nguyên nhân: " + causeReason(txn));
    }

    /**
     * Xác nhận hoàn tiền tới khi giao dịch không còn chờ nó. Ba ca rất khác nhau, nên không gộp vào một
     * dòng {@code debug} — ca đầu là tiền bị nhân đôi.
     */
    private void onRefundCompletedLate(Transaction txn, BankCompletedEvent event) {
        switch (txn.getStatus()) {
            // Ví ĐÃ được cộng và tiền cũng ĐÃ về thẻ. Đây là tín hiệu rõ ràng nhất của ca tạo ra tiền,
            // và nếu nuốt ở mức debug thì không ai biết — giao dịch trông như một lần nạp thành công.
            case COMPLETED -> {
                topupResultConflicts.increment();
                log.error("Saga: lệnh nạp {} đã COMPLETED mà provider {} lại xác nhận ĐÃ HOÀN (ref {}) — ví "
                        + "được cộng VÀ tiền đã trả lại. Cần người đối chiếu NGAY.",
                        txn.getId(), event.provider(), event.providerRef());
            }
            // Job quét đã bỏ cuộc (REFUND_NO_RESPONSE) trong khi lệnh hoàn vẫn đang chạy ở bank-adapter.
            // Không tự đóng — người đang cầm ca này cần biết tiền đã thật sự về để khỏi hoàn tay lần hai.
            case MANUAL_REVIEW -> log.warn("Saga: lệnh nạp {} đang chờ người xử lý ({}) nhưng tiền ĐÃ hoàn "
                    + "xong ở provider {} ref {} — đừng hoàn tay lần nữa",
                    txn.getId(), txn.getFailureCode(), event.provider(), event.providerRef());
            // FAILED: bản lặp của chính lời xác nhận đã đóng giao dịch này. Đúng và vô hại.
            default -> log.debug("Saga: bỏ qua xác nhận hoàn tiền cho {} đang {} — không có lần hoàn nào "
                    + "đang chờ", txn.getId(), txn.getStatus());
        }
    }

    /**
     * Provider từ chối DỨT KHOÁT lệnh hoàn (VNPay trả {@code REFUND_UNSUPPORTED} tới trước 2.4c) — tiền
     * của người dùng vẫn đang ở chỗ provider và máy không còn đường nào tự gỡ: một hàng hoàn
     * {@code FAILED} ở bank-adapter là vĩnh viễn, gửi lại chỉ phát lại đúng lời từ chối đó (ADR-020 §3).
     * Người phải hoàn tay qua portal của provider.
     */
    private void onRefundFailed(BankFailedEvent event) {
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();
        if (txn.getStatus() != ETransactionStatus.REFUNDING) {
            log.debug("Saga: bỏ qua lời từ chối hoàn tiền cho {} đang {} — không có lần hoàn nào đang chờ",
                    txn.getId(), txn.getStatus());
            return;
        }
        topupRefundsFailed.increment();
        log.error("Saga: KHÔNG hoàn được tiền cho lệnh nạp {} ({}): provider {} từ chối ({}) — tiền vẫn ở "
                + "provider, phải hoàn TAY qua portal", txn.getId(), txn.getFailureCode(), event.provider(),
                event.failureCode());
        needsReview(txn, REFUND_DECLINED, "Cổng thanh toán từ chối hoàn tiền (" + event.failureCode()
                + "). Nguyên nhân phải hoàn: " + refundCause(txn));
    }

    /**
     * Provider từ chối DỨT KHOÁT (người dùng không trả, phiên hết hạn, bị từ chối) → lệnh nạp thất
     * bại. Không có tiền nào đã đi, nên đây là một kết cục sạch (roadmap 2.4a).
     */
    @Transactional
    public void onBankFailed(BankFailedEvent event) {
        if (event.operation() == EBankOperation.REFUND) {
            onRefundFailed(event);
            return;
        }
        if (event.operation() != EBankOperation.CHARGE) {
            log.debug("Saga: bỏ qua BankFailed operation={} — chưa có luồng xử lý", event.operation());
            return;
        }
        Optional<Transaction> locked = lockTopup(event.transactionId());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();
        if (alreadyResolved(txn, "provider " + event.provider() + " từ chối lần thu ("
                + event.failureCode() + ")")) {
            return;
        }

        switch (txn.getStatus()) {
            case PROCESSING, AWAITING_PAYMENT -> fail(txn, event.failureCode(),
                    "Cổng thanh toán từ chối lần thu: " + event.failureCode());
            case FAILED, MANUAL_REVIEW -> log.debug("Saga: {} đã đóng — bỏ qua BankFailed lặp", txn.getId());
            // Đã quyết ghi sổ dựa trên một lời XÁC NHẬN trước đó của chính provider. Không kéo ngược
            // trạng thái: máy trạng thái chỉ tiến một chiều, đó là tính chất khiến event đến sai thứ tự
            // trở nên vô hại. Nhưng đây là mâu thuẫn thật — kêu to để có người đối chiếu.
            case POSTING, COMPLETED, REFUNDING -> {
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

    /**
     * Giao dịch này đã từng vào đường hoàn tiền chưa (2.4b, ADR-020). Đọc dưới khoá mà
     * {@link #lockOurs} đã giữ, nên giá trị là của chính lúc này.
     *
     * <p>Vì sao không dùng trạng thái {@code REFUNDING}: trạng thái đó chỉ sống vài giây — hoàn xong là
     * {@code FAILED}, hoàn hỏng là {@code MANUAL_REVIEW}. Mọi hàng rào đặt trên nó biến mất đúng lúc nó
     * cần nhất, vì event tới muộn (bản gửi lại nằm trên topic, DLT được giải phóng, instance ledger cũ)
     * là thứ tới SAU. {@code current_step} thì bền: {@link SagaRepository#close} chỉ đổi
     * {@code step_status}, nên một khi saga đã bước sang {@code BANK_REFUND} thì dấu đó ở lại vĩnh viễn.
     *
     * <p>Đây là hàng rào tương ứng với {@code rejected_topups} của ledger (ADR-020 §1), nhưng ở phía
     * saga và cho câu hỏi ngược lại: ledger chặn "ghi sổ sau khi đã từ chối", còn cái này chặn "báo
     * thành công sau khi đã trả tiền về cho người dùng".
     *
     * <p>Chỉ nạp hàng saga cho lệnh NẠP: {@code BANK_REFUND} là bước của riêng luồng nạp, nên chuyển
     * tiền (đường nóng) không phải trả thêm một lượt đi về DB cho một câu hỏi luôn có cùng câu trả lời.
     */
    /**
     * Hàng rào cho ca đã được một người vận hành CHỐT (2.5 PR2, ADR-021 *Amendments* A3). Saga không
     * bao giờ được ghi tiếp vào một ca như thế, bất kể event nào tới và bất kể ca đang ở trạng thái
     * nào.
     *
     * <p>Vì sao phải ở MỌI handler chứ không riêng {@code onPosted}: sau khi chốt, ca về
     * {@code FAILED}. Trước 2.5 PR2 những ca này nằm ở {@code MANUAL_REVIEW}, nơi mọi handler bỏ qua
     * bản lặp một cách an toàn — chuyển chúng sang {@code FAILED} là đẩy chúng vào đúng những nhánh
     * nguy hiểm: {@code onPosted} cho "ledger thắng" (ví được cộng trong khi tiền đã hoàn tay), và
     * {@code onBankCompleted} gửi LỆNH HOÀN THỨ HAI cho số tiền vừa được hoàn tay qua portal.
     *
     * <p>Mỗi lần chạm vào đây là một bất thường thật: hoặc có một bút toán/lần thu không ai giải
     * thích được, hoặc người vận hành chốt một ca chưa xong. Đếm và kêu to, không im lặng.
     */
    private boolean alreadyResolved(Transaction txn, String what) {
        if (txn.getResolvedAt() == null) {
            return false;
        }
        topupResultConflicts.increment();
        log.error("Saga: {} cho giao dịch {} mà người vận hành đã CHỐT lúc {} — không ghi gì thêm, "
                + "cần người đối chiếu ngay (tiền có thể đã được xử lý tay rồi)",
                what, txn.getId(), txn.getResolvedAt());
        return true;
    }

    private boolean refundStarted(Transaction txn) {
        if (txn.getType() != EJournalType.TOPUP) {
            return false;
        }
        return sagaRepository.findByTenantIdAndTransactionId(txn.getTenantId(), txn.getId())
                .map(saga -> STEP_BANK_REFUND.equals(saga.getCurrentStep()))
                .orElse(false);
    }

    @Transactional
    public void onPosted(LedgerPostedEvent event) {
        Optional<Transaction> locked = lockOurs(event.transactionId(), event.type().name());
        if (locked.isEmpty()) {
            return;
        }
        Transaction txn = locked.get();

        // Hàng rào TRƯỚC máy trạng thái: một giao dịch đã vào đường hoàn tiền thì không bao giờ được
        // COMPLETED, bất kể nó đang đứng ở đâu. Kiểm theo trạng thái thôi là không đủ — hoàn xong thì
        // trạng thái là FAILED, và `case FAILED` bên dưới cho "ledger thắng" (đúng với chuyển tiền,
        // nhưng ở đây là vừa cộng ví vừa đã trả tiền về thẻ, và còn báo THÀNH CÔNG cho người dùng).
        if (refundStarted(txn)) {
            postedWhileRefunding(txn, event);
            return;
        }

        if (alreadyResolved(txn, "ledger GHI SỔ bút toán " + event.entryId())) {
            return;
        }

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
            // Không tới được: REFUNDING kéo theo current_step = BANK_REFUND, nên hàng rào ở trên đã bắt.
            // Giữ nhánh vì switch phải phủ hết enum — và để nó đi đúng một chỗ với hàng rào.
            case REFUNDING -> postedWhileRefunding(txn, event);
        }
    }

    /**
     * Ledger ghi bút toán cho một lệnh nạp đã vào đường hoàn tiền — ca mà hàng rào {@code rejected_topups}
     * của ledger (V6, ADR-020 §1) sinh ra để nó KHÔNG xảy ra. Thấy nó nghĩa là có một instance ledger
     * chưa lên V6, hoặc bút toán được ghi tay.
     *
     * <p>Không tự quyết bên nào thắng. "Ledger thắng" (COMPLETED) ở đây nghĩa là ví được cộng trong khi
     * tiền cũng đã (hoặc đang) về thẻ người dùng — đúng nghĩa tạo ra tiền, và tệ hơn nữa là nó báo
     * THÀNH CÔNG nên không ai nhìn lại. Dừng ở {@code MANUAL_REVIEW}, kêu to, để người đối chiếu.
     */
    private void postedWhileRefunding(Transaction txn, LedgerPostedEvent event) {
        switch (txn.getStatus()) {
            // Đã ở diện người xử lý: không ghi đè failure_code (mã cũ là thứ runbook dùng để phân loại),
            // và không đếm lại — mỗi bản giao lại của cùng một LedgerPosted mà tăng counter thì alert
            // TopupResultConflict kêu nhiều lần cho đúng một sự cố.
            case MANUAL_REVIEW -> log.debug("Saga: {} đã ở diện người xử lý — bỏ qua LedgerPosted lặp",
                    txn.getId());
            // Không tự lật COMPLETED: bút toán là sự thật về tiền, sửa nó phải bằng REVERSAL có audit
            // (2.5), không phải bằng cách viết lại trạng thái. Cặp này không dựng được bằng code hôm nay.
            case COMPLETED -> log.error("Saga: {} đã COMPLETED mà lại vào đường hoàn tiền — bút toán {} "
                    + "giữ nguyên, cần người đối chiếu với lần hoàn ở provider", txn.getId(), event.entryId());
            default -> {
                topupResultConflicts.increment();
                log.error("Saga: ledger GHI SỔ bút toán {} cho lệnh nạp {} đã vào đường hoàn tiền (đang {}) "
                        + "— ví có thể vừa được cộng VỪA được hoàn. Dừng lại, cần người đối chiếu ngay.",
                        event.entryId(), txn.getId(), txn.getStatus());
                needsReview(txn, "REFUND_POSTED_CONFLICT",
                        "Ledger ghi sổ trong khi/sau khi hoàn tiền — đối chiếu bút toán với lần hoàn ở provider");
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
        if (alreadyResolved(txn, "ledger TỪ CHỐI (" + event.reasonCode() + ")")) {
            return;
        }

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
            // Đã nhận lời từ chối này rồi (hàng rào của ledger phát LẠI đúng lời từ chối đầu cho mọi bản
            // gửi lại — ADR-020 §1), và lệnh hoàn đã đi. Không gửi lệnh hoàn thứ hai.
            case REFUNDING -> log.debug("Saga: {} đang hoàn tiền — bỏ qua LedgerRejected ({}) lặp",
                    txn.getId(), event.reasonCode());
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
        log.error("Saga: ledger TỪ CHỐI lệnh nạp {} ({}) nhưng tiền đã thu ở provider {} (ref {}) — phải "
                + "trả lại cho người dùng",
                txn.getId(), event.reasonCode(), txn.getProvider(), txn.getProviderRef());
        startRefund(txn, event.reasonCode(), event.reason());
    }

    /**
     * Gửi lệnh hoàn tiền và chuyển lệnh nạp sang {@code REFUNDING} (2.4b, ADR-020). Chỉ hai ca vào đây:
     * ledger từ chối ghi sổ, và {@code BANK_RESULT_CONFLICT} — hai ca mà ta biết CHẮC là đã thu được
     * tiền và ví chưa được cộng. Những ca "không biết chắc" ({@code BANK_AMOUNT_MISMATCH},
     * {@code BANK_NO_RESPONSE}, {@code SAGA_STATE_DESYNC}) vẫn chờ người: không biết đã thu bao nhiêu,
     * hay đã thu chưa, thì không biết phải hoàn bao nhiêu.
     *
     * <p>Cờ tắt ⇒ giữ nguyên hành vi 2.4a ({@code MANUAL_REVIEW}). Xem javadoc của lớp về vì sao hoàn
     * tiền tự động chỉ an toàn sau khi mọi instance ledger đã có hàng rào của V6.
     *
     * <p>Số tiền hoàn lấy từ giao dịch của TA, không từ event: ta chỉ tới được đây sau khi lời xác nhận
     * thu tiền đã được đối chiếu khớp từng con số với lệnh (xem {@link #startPosting}). Và bank-adapter
     * còn kiểm lại lần nữa với lần thu của chính nó.
     */
    private void startRefund(Transaction txn, String reasonCode, String reason) {
        if (!refundEnabled) {
            needsReview(txn, reasonCode, reason);
            return;
        }
        UUID commandEventId = UUID.randomUUID();
        int opened = sagaRepository.startCompensation(txn.getTenantId(), txn.getId(), STEP_BANK_REFUND,
                commandEventId, postJournalDeadline.toSeconds());
        if (opened == 0) {
            // Saga đã ở bước hoàn rồi mà giao dịch thì chưa REFUNDING. Người gọi đã loại ca thường gặp
            // (bản lặp sau khi hoàn xong — refundStarted ở onBankCompleted), nên tới được đây nghĩa là
            // hai hàng thật sự lệch nhau: chúng được ghi cùng một transaction nên chỉ có sửa tay mới
            // tạo ra. Không gửi lệnh hoàn thứ hai dựa trên một trạng thái không hiểu được.
            topupResultConflicts.increment();
            log.error("Saga: lệnh nạp {} ở {} nhưng saga đã ở bước {} — không gửi lệnh hoàn, chuyển người xử lý",
                    txn.getId(), txn.getStatus(), STEP_BANK_REFUND);
            needsReview(txn, "SAGA_STATE_DESYNC",
                    "Saga đã ở bước hoàn tiền mà giao dịch thì chưa — đối chiếu với bank-adapter trước khi hoàn");
            return;
        }
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.REFUNDING.name(), reasonCode, reason);
        bankCommandSender.sendRefund(txn.getId(), txn.getToWallet(), txn.getAmountMinor(), txn.getCurrency(),
                reasonCode, commandEventId);
        topupRefundsStarted.increment();
        log.warn("Saga: gửi lệnh hoàn {} {} cho lệnh nạp {} ({}) — eventId {}", txn.getAmountMinor(),
                txn.getCurrency(), txn.getId(), reasonCode, commandEventId);
    }

    /**
     * Mã nguyên nhân phải hoàn, đã được ghi vào {@code failure_code} lúc vào {@code REFUNDING}. Hàng
     * được đọc lại dưới khoá ở mỗi event nên giá trị này là của lần ghi đó; chỉ phòng ca có người sửa
     * tay cột đó thành null, vì {@code reasonCode} của lệnh hoàn là field bắt buộc của schema.
     */
    private static String refundCause(Transaction txn) {
        return txn.getFailureCode() != null ? txn.getFailureCode() : "TOPUP_REFUND";
    }

    private static String causeReason(Transaction txn) {
        return txn.getFailureReason() != null ? txn.getFailureReason() : refundCause(txn);
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
     * trừ ở ngân hàng của họ. Từ 2.4b, hai ca biết chắc đã thu tiền được hoàn tự động
     * ({@link #startRefund}) và chỉ về FAILED sau khi hoàn xong; đây là nơi dừng của những ca còn lại —
     * không biết đã thu bao nhiêu, hay đã thu chưa, thì một người phải nhìn và quyết (roadmap §5.7).
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
