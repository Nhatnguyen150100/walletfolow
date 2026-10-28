package com.walletflow.transaction.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.client.LedgerEntriesClient;
import com.walletflow.transaction.client.ProviderContextClient;
import com.walletflow.transaction.entity.ResolutionRequest;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.EResolutionAction;
import com.walletflow.transaction.enums.EResolutionStatus;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.ResolutionRequestRepository;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Xử lý ca cần can thiệp, với 4-eyes cho thao tác đụng tiền (roadmap 2.5 PR2, GAP-A4, ADR-021).
 *
 * <p><b>Khác hẳn {@link AdminReviewService} ở hai điểm, và cả hai đều cố ý:</b>
 * <ul>
 *   <li>Có {@code @Transactional}: tiền và dấu vết phải cùng commit hoặc cùng rollback. "Không có
 *       thao tác đụng tiền nào không để lại audit" là một bất biến, không phải một nỗ lực — nên
 *       audit đi qua {@link AuditWriter#writeInCurrentTransaction} (ném, không nuốt), chứ KHÔNG
 *       qua {@link AuditService}.</li>
 *   <li>Đọc lại dưới KHOÁ và kiểm trạng thái, thay vì đọc ảnh chụp: giữa lúc người vận hành nhìn
 *       hàng đợi và lúc họ bấm nút, saga có thể đã tự xử lý xong ca đó.</li>
 * </ul>
 *
 * <p>Thứ tự khoá SAGA trước, GIAO DỊCH sau — giống mọi đường khác trong service này, nếu không hai
 * bên có thể deadlock khi job quét chạm đúng giao dịch người vận hành đang chốt.
 */
@Slf4j
@Service
public class ResolutionService {

    /** Ca DUY NHẤT đảo bút toán được — xem {@link EResolutionAction#REVERSE_LEDGER_ENTRY}. */
    static final String REFUND_POSTED_CONFLICT = "REFUND_POSTED_CONFLICT";

    /** Đã gửi lệnh đảo, đang chờ ledger trả lời. Cũng là hàng rào chống gửi lệnh thứ hai. */
    static final String REVERSAL_SENT = "REVERSAL_SENT";

    /**
     * Ledger đã từ chối một lần đảo. Vẫn đề xuất lại được: điều kiện thật sự nằm ở
     * {@code resolveReversalTarget} (hỏi lại ledger và provider) và ở
     * {@code uk_journal_one_reversal}, nên bắt người vận hành sửa DB tay để thử lại chỉ là đẩy họ
     * ra ngoài đường có audit — đúng vấn đề mà ADR-021 mở đầu bằng.
     */
    static final String REVERSAL_REJECTED = "REVERSAL_REJECTED";

    private final TransactionRepository transactionRepository;
    private final ResolutionRequestRepository resolutionRepository;
    private final SagaRepository sagaRepository;
    private final AuditWriter auditWriter;
    private final OutboxPublisher outboxPublisher;
    private final ObjectMapper objectMapper;
    private final LedgerEntriesClient ledgerEntriesClient;
    private final ProviderContextClient providerContextClient;
    private final LedgerCommandSender ledgerCommandSender;
    private final TransactionTemplate transactionTemplate;

    public ResolutionService(TransactionRepository transactionRepository,
                             ResolutionRequestRepository resolutionRepository,
                             SagaRepository sagaRepository,
                             AuditWriter auditWriter,
                             OutboxPublisher outboxPublisher,
                             ObjectMapper objectMapper,
                             LedgerEntriesClient ledgerEntriesClient,
                             ProviderContextClient providerContextClient,
                             LedgerCommandSender ledgerCommandSender,
                             PlatformTransactionManager transactionManager) {
        this.transactionRepository = transactionRepository;
        this.resolutionRepository = resolutionRepository;
        this.sagaRepository = sagaRepository;
        this.auditWriter = auditWriter;
        this.outboxPublisher = outboxPublisher;
        this.objectMapper = objectMapper;
        this.ledgerEntriesClient = ledgerEntriesClient;
        this.providerContextClient = providerContextClient;
        this.ledgerCommandSender = ledgerCommandSender;
        // `propose` phải gọi HTTP sang ledger và bank-adapter TRƯỚC khi mở transaction (CLAUDE.md:
        // không gọi HTTP/Kafka trong transaction DB). Tự gọi một method `@Transactional` của chính
        // mình thì đi thẳng, không qua proxy, nên không có transaction nào — dùng template như
        // TransferService.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Đề xuất một thao tác đụng tiền. Chưa có gì xảy ra với tiền ở đây — nhưng vẫn ghi dấu vết, vì
     * ai đề xuất cái gì là một nửa của câu chuyện mà 4-eyes sinh ra để kể.
     *
     * @return id của yêu cầu, để người duyệt gọi {@link #decide}
     */
    public UUID propose(Operator maker, UUID transactionId, EResolutionAction action,
                        String reason, String evidence) {
        // NGOÀI transaction: với REVERSE_LEDGER_ENTRY, bước này hỏi ledger "bút toán nào" và hỏi
        // bank-adapter "tiền đã thật sự hoàn chưa". Hai lời gọi HTTP, nên phải xong trước khi mở
        // transaction. Kết quả được CHỐT vào hàng yêu cầu — người duyệt sẽ nhìn đúng bút toán này.
        final UUID targetEntryId = action == EResolutionAction.REVERSE_LEDGER_ENTRY
                ? prepareReversal(transactionId)
                : null;
        return transactionTemplate.execute(status ->
                proposeInTransaction(maker, transactionId, action, reason, evidence, targetEntryId));
    }

    /**
     * Lọc sớm rồi tra bút toán — chạy NGOÀI transaction, trước khi có hàng nào được ghi.
     *
     * <p>Bước đọc (không khoá) ở đầu làm hai việc:
     * <ul>
     *   <li>Một {@code transactionId} không thuộc tenant này không được phép trở thành một truy vấn
     *       ở ledger và bank-adapter. Không có nó thì gửi id bất kỳ là ta đi hỏi hộ — và câu trả lời
     *       404 ở đây giống hệt đường {@code MARK_REFUNDED_MANUALLY}, nên "không tồn tại" và "của
     *       tenant khác" vẫn không phân biệt được từ bên ngoài.</li>
     *   <li>Ca đã ở {@code REVERSAL_SENT} bị chặn NGAY, thay vì để hai người ký vào một yêu cầu chắc
     *       chắn bị từ chối lúc duyệt.</li>
     * </ul>
     *
     * <p>Chỉ là bộ lọc, không phải hàng rào: {@code reverseLedgerEntry} kiểm lại dưới khoá, vì giữa
     * đề xuất và duyệt saga có thể đã đổi ca.
     */
    private UUID prepareReversal(UUID transactionId) {
        Transaction txn = transactionRepository
                .findByTenantIdAndId(TenantContext.require(), transactionId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy giao dịch"));
        requireReversible(txn);
        return resolveReversalTarget(txn);
    }

    /**
     * Ca có thuộc diện đảo bút toán được không — gọi ở CẢ HAI chỗ: lọc sớm lúc đề xuất (chưa khoá) và
     * cưỡng chế lúc duyệt (dưới khoá). Một mình bộ lọc sớm không đủ: giữa đề xuất và duyệt, saga có
     * thể đã đổi ca.
     *
     * <p>Mọi ca {@code MANUAL_REVIEW} khác không có bút toán nào để đảo: ledger từ chối TRƯỚC khi
     * ghi, hoặc lệnh ghi sổ chưa từng được gửi (ADR-021 A1). Đảo "cho chắc" ở những ca đó là ghi một
     * bút toán rút tiền khỏi ví mà không có bút toán gốc nào tương ứng.
     *
     * <p>Ca đã gửi lệnh đảo cũng rơi vào đây, vì {@code failure_code} lúc đó là {@code REVERSAL_SENT}
     * chứ không còn {@code REFUND_POSTED_CONFLICT} — nên đây cũng là hàng rào chống gửi lệnh thứ hai.
     */
    private static void requireReversible(Transaction txn) {
        if (txn.getType() != EJournalType.TOPUP
                || !(REFUND_POSTED_CONFLICT.equals(txn.getFailureCode())
                        || REVERSAL_REJECTED.equals(txn.getFailureCode()))) {
            throw new ConflictException("NOT_A_POSTED_CONFLICT",
                    "Chỉ đảo bút toán được cho lệnh NẠP ở diện " + REFUND_POSTED_CONFLICT);
        }
    }

    /**
     * Tìm bút toán sẽ bị đảo, và đòi đủ bằng chứng trước khi cho phép đề xuất (ADR-022).
     *
     * <p>Ba điều kiện, mỗi điều kiện chặn một cách làm mất tiền khác nhau:
     * <ol>
     *   <li><b>Phải hỏi được CẢ HAI service.</b> Không hỏi được ⇒ 503, không cho đề xuất. Đây là chỗ
     *       mà quy ước "rỗng ≠ không hỏi được" của PR3 trả công: đảo một bút toán vì "ledger không
     *       trả lời nên chắc là chưa hoàn" là cách tạo ra một mất mát thứ hai.</li>
     *   <li><b>Phải có bút toán {@code TOPUP}.</b> Không có ⇒ không có gì để đảo.</li>
     *   <li><b>Lần hoàn ở provider phải {@code SUCCEEDED}.</b> Đây là điều kiện cốt lõi: đảo bút toán
     *       nghĩa là lấy lại tiền khỏi ví vì người dùng ĐÃ được trả lại qua provider. Nếu lần hoàn
     *       chưa xong, ta vừa rút tiền khỏi ví vừa chưa trả cho họ — mất tiền của KHÁCH, nặng hơn
     *       hẳn ca gốc (vốn chỉ mất tiền của công ty).</li>
     * </ol>
     *
     * <p>Bút toán đã bị đảo rồi thì từ chối tại đây cho câu trả lời rõ ràng, thay vì để ledger nổ
     * {@code ALREADY_REVERSED} sau khi hai người đã ký.
     */
    private UUID resolveReversalTarget(Transaction txn) {
        UUID transactionId = txn.getId();
        TransactionEntries entries = ledgerEntriesClient.entriesOf(transactionId)
                .orElseThrow(() -> unavailable("ledger"));
        ProviderContext provider = providerContextClient.contextOf(transactionId)
                .orElseThrow(() -> unavailable("bank-adapter"));

        if (entries.entries().stream().anyMatch(e -> e.type() == EJournalType.REVERSAL)) {
            throw new ConflictException("ALREADY_REVERSED",
                    "Bút toán của giao dịch này đã được đảo rồi — đọc lại trước khi quyết");
        }
        JournalEntryView target = entries.entries().stream()
                .filter(e -> e.type() == EJournalType.TOPUP)
                .findFirst()
                .orElseThrow(() -> new ConflictException("NO_ENTRY_TO_REVERSE",
                        "Ledger chưa ghi bút toán nạp nào cho giao dịch này — không có gì để đảo"));

        // So ĐỦ số tiền và loại tiền, không chỉ "có một lần hoàn thành công". Ta sắp rút khỏi ví
        // TRỌN số đã nạp (`txn.getAmountMinor()`); nếu lần hoàn ghi nhận ở provider nhỏ hơn thế thì
        // phần chênh là tiền ta lấy của khách — chính saga cũng coi lệch số tiền hoàn là sự cố
        // nghiêm trọng (REFUND_AMOUNT_MISMATCH).
        boolean refunded = provider.operations().stream()
                .anyMatch(op -> op.operation() == EBankOperation.REFUND
                        && "SUCCEEDED".equals(op.status())
                        && op.amountMinor() == txn.getAmountMinor()
                        && op.currency() == txn.getCurrency());
        if (!refunded) {
            throw new ConflictException("REFUND_NOT_CONFIRMED",
                    "Chưa có lần hoàn nào THÀNH CÔNG đúng " + txn.getAmountMinor() + " "
                            + txn.getCurrency() + " ở cổng thanh toán — đảo bút toán lúc này là rút "
                            + "tiền khỏi ví người dùng trong khi họ chưa được trả lại đủ");
        }
        return target.entryId();
    }

    /**
     * 503 chứ không 500: chưa có gì xảy ra, và gửi lại sau khi service kia hồi là an toàn.
     * Tên service vào message vì người đọc nó là người vận hành, không phải người dùng cuối.
     */
    private static AppException unavailable(String service) {
        return new AppException(HttpStatus.SERVICE_UNAVAILABLE, "CONTEXT_UNAVAILABLE",
                "Không hỏi được " + service + " để xác minh — chưa đủ căn cứ để đề xuất đảo bút toán");
    }

    private UUID proposeInTransaction(Operator maker, UUID transactionId, EResolutionAction action,
                                      String reason, String evidence, UUID targetEntryId) {
        UUID tenantId = TenantContext.require();
        Transaction txn = lockOpenCase(tenantId, transactionId);
        if (action == EResolutionAction.REVERSE_LEDGER_ENTRY) {
            // Kiểm LẠI dưới khoá: giữa lần đọc không khoá ở `prepareReversal` và lúc INSERT, saga có
            // thể đã đổi ca. Không có dòng này thì một yêu cầu mang `target_entry_id` đã cũ vẫn được
            // ghi, và chỉ bị chặn ở `decide` — sau khi đã tốn một chữ ký.
            requireReversible(txn);
        }

        UUID requestId = UUID.randomUUID();
        int created = resolutionRepository.propose(requestId, tenantId, transactionId, action.name(),
                maker.id(), maker.email(), maker.role(), reason, evidence, targetEntryId);
        if (created == 0) {
            // Khoá riêng phần uk_resolution_one_open: hai người đề xuất hai thứ khác nhau rồi người
            // thứ ba duyệt nhầm cái nào cũng được — đó là 4-eyes trên giấy, không phải trong thực tế.
            throw new ConflictException("RESOLUTION_ALREADY_PENDING",
                    "Giao dịch này đã có một yêu cầu đang chờ duyệt");
        }

        auditWriter.writeInCurrentTransaction(maker, "RESOLUTION_PROPOSED", transactionId.toString(),
                json(Map.of("requestId", requestId.toString(), "action", action.name(),
                        "reason", reason, "status", txn.getStatus().name())));
        log.warn("Vận hành: {} đề xuất {} cho giao dịch {} — chờ một ADMIN khác duyệt",
                maker.id(), action, transactionId);
        return requestId;
    }

    /**
     * Duyệt hoặc từ chối. Duyệt thì THỰC THI ngay trong cùng transaction — không có trạng thái "đã
     * duyệt nhưng chưa làm", vì đó là một chỗ nữa để mất dấu vết.
     */
    @Transactional
    public void decide(Operator checker, UUID requestId, boolean approve, String note) {
        UUID tenantId = TenantContext.require();
        // Đọc KHÔNG khoá trước, chỉ để biết yêu cầu này thuộc giao dịch nào. Khoá theo thứ tự thống
        // nhất với mọi đường khác — saga → giao dịch → yêu cầu — rồi mới khoá hàng yêu cầu. Khoá
        // hàng yêu cầu TRƯỚC (như bản đầu) là đi ngược chiều với `propose`, và hai luồng song song
        // trên cùng một giao dịch có thể ôm nhau: `decide` giữ hàng yêu cầu chờ hàng giao dịch, còn
        // `propose` giữ hàng giao dịch rồi chờ ở `ON CONFLICT` của chính hàng yêu cầu đó.
        UUID transactionId = resolutionRepository.findTransactionId(tenantId, requestId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy yêu cầu"));
        sagaRepository.lock(tenantId, transactionId);
        transactionRepository.lockForSaga(tenantId, transactionId);

        ResolutionRequest request = resolutionRepository.lock(tenantId, requestId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy yêu cầu"));
        if (request.getStatus() != EResolutionStatus.PENDING) {
            throw new ConflictException("RESOLUTION_ALREADY_DECIDED",
                    "Yêu cầu này đã được quyết rồi");
        }
        if (request.getMakerId().equals(checker.id())) {
            // 4-eyes. Còn hai hàng rào nữa phía sau (điều kiện của câu UPDATE, và CHECK ở DB) —
            // một thao tác đụng tiền không được phép chỉ dựa vào một lớp kiểm tra.
            throw new ForbiddenException("FOUR_EYES_VIOLATION",
                    "Người duyệt phải khác người đề xuất");
        }

        EResolutionStatus outcome = approve ? EResolutionStatus.APPROVED : EResolutionStatus.REJECTED;
        int decided = resolutionRepository.decide(tenantId, requestId, outcome.name(),
                checker.id(), checker.email(), checker.role());
        if (decided == 0) {
            throw new ConflictException("RESOLUTION_ALREADY_DECIDED",
                    "Yêu cầu này vừa được người khác quyết");
        }

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("requestId", requestId.toString());
        detail.put("action", request.getAction().name());
        detail.put("makerId", request.getMakerId().toString());
        detail.put("outcome", outcome.name());
        if (note != null) {
            detail.put("note", note);
        }

        if (approve) {
            execute(checker, request, detail);
        } else {
            log.warn("Vận hành: {} TỪ CHỐI yêu cầu {} cho giao dịch {}",
                    checker.id(), requestId, request.getTransactionId());
        }
        auditWriter.writeInCurrentTransaction(checker, "RESOLUTION_" + outcome.name(),
                request.getTransactionId().toString(), json(detail));
    }

    /**
     * Thực thi thao tác đã được duyệt. Chạy trong transaction của {@link #decide}, nên hỏng ở đây là
     * cả quyết định lẫn dấu vết cùng quay lại.
     */
    private void execute(Operator checker, ResolutionRequest request, Map<String, Object> detail) {
        Transaction txn = lockOpenCase(request.getTenantId(), request.getTransactionId());

        // switch trên enum, KHÔNG có `default` — compiler bắt buộc phủ hết. Một `if` bỏ sót nhánh
        // thì `decide` vẫn ghi APPROVED và vẫn ghi dấu vết "đã duyệt và thực thi" cho việc không
        // xảy ra: đúng chiều nguy hiểm mà ADR-021 A4 gọi tên. (Nhánh REVERSE_LEDGER_ENTRY của PR4
        // được thêm vào đúng nhờ cơ chế này.)
        switch (request.getAction()) {
            case MARK_REFUNDED_MANUALLY -> markRefundedManually(checker, request, txn, detail);
            case REVERSE_LEDGER_ENTRY -> reverseLedgerEntry(checker, request, txn, detail);
        }
    }

    /**
     * Gửi lệnh đảo bút toán sang ledger (ADR-022).
     *
     * <p><b>KHÔNG đóng ca ở đây, và không đặt {@code resolved_at}.</b> Đây là điểm khác cốt lõi với
     * {@link #markRefundedManually}: thao tác kia kết thúc ngay khi người duyệt bấm nút (tiền đã về
     * trước đó rồi, ta chỉ ghi nhận), còn thao tác này mới chỉ GỬI một lệnh — và ledger có quyền từ
     * chối, chẳng hạn khi ví đã tiêu mất tiền ({@code INSUFFICIENT_FUNDS}).
     *
     * <p>Đóng ca lúc này sẽ là một cái bẫy: {@code resolved_at} khiến hàng rào {@code alreadyResolved}
     * chặn luôn câu trả lời của ledger (ADR-021 A5), nên một lệnh đảo BỊ TỪ CHỐI sẽ biến mất không
     * dấu vết và ca thì trông như đã xong. Ca đóng khi {@code LedgerPosted(REVERSAL)} về; ledger từ
     * chối thì nó ở lại hàng đợi với mã mới.
     */
    private void reverseLedgerEntry(Operator checker, ResolutionRequest request, Transaction txn,
                                    Map<String, Object> detail) {
        requireReversible(txn);

        UUID eventId = UUID.randomUUID();
        // `failure_code` là dấu DUY NHẤT của việc "đã gửi lệnh đảo", và nó cũng chính là hàng rào
        // chống gửi hai lần: kiểm REFUND_POSTED_CONFLICT ở trên sẽ từ chối mọi lần đề xuất sau, vì
        // mã lúc đó đã là REVERSAL_SENT.
        //
        // CỐ Ý KHÔNG mở một bước saga cho đường này. Saga là của máy; ca này đứng ở MANUAL_REVIEW,
        // trạng thái mà `StuckSagaScanner.isOpen` coi là đã đóng — mở bước ở đây chỉ làm job quét
        // đóng nó ngay lượt sau, và cái `current_step` còn sót lại sẽ chặn luôn lần đề xuất tiếp
        // theo. Đổi lại: lệnh đảo mất trên đường KHÔNG được gửi lại tự động, ca nằm ở
        // `REVERSAL_SENT` cho tới khi có người nhìn (alert + runbook mục G). Chấp nhận được vì đây
        // là thao tác tay hiếm, và ledger idempotent theo `uk_journal_one_reversal` nên đề xuất lại
        // sau khi gỡ mã là an toàn.
        transactionRepository.transition(txn.getTenantId(), txn.getId(),
                ETransactionStatus.MANUAL_REVIEW.name(), REVERSAL_SENT,
                "Đã gửi lệnh đảo bút toán " + request.getTargetEntryId() + " — chờ ledger trả lời. "
                        + request.getReason());

        ledgerCommandSender.sendReversal(txn.getId(), request.getTargetEntryId(), txn.getToWallet(),
                txn.getAmountMinor(), txn.getCurrency(), eventId);

        detail.put("targetEntryId", request.getTargetEntryId().toString());
        detail.put("commandEventId", eventId.toString());
        detail.put("newStatus", ETransactionStatus.MANUAL_REVIEW.name());
        log.warn("Vận hành: {} duyệt ĐẢO bút toán {} của giao dịch {} — lệnh đã gửi, chờ ledger",
                checker.id(), request.getTargetEntryId(), txn.getId());
    }

    /**
     * Người vận hành đã hoàn tiền tay qua portal; đóng ca ở {@code FAILED} — lúc này {@code FAILED}
     * nói đúng sự thật: người dùng không mất tiền (ADR-020).
     */
    private void markRefundedManually(Operator checker, ResolutionRequest request, Transaction txn,
                                      Map<String, Object> detail) {
        // Chỉ áp cho lệnh NẠP: chuyển tiền không có provider nào để hoàn qua portal, nên đánh dấu
        // "đã hoàn tay" cho nó là một lời khai sai — và tệ hơn, `resolved_at` sau đó chặn vĩnh viễn
        // mọi `LedgerPosted` tới sau, tức là một quyết định vận hành phủ quyết sổ cái (bất biến §1).
        if (txn.getType() != EJournalType.TOPUP || txn.getProviderRef() == null) {
            throw new ConflictException("NOT_A_TOPUP_WITH_PROVIDER",
                    "Chỉ đánh dấu hoàn tay được cho lệnh NẠP đã có mã tham chiếu ở provider");
        }

        int resolved = transactionRepository.resolveManualReview(txn.getTenantId(), txn.getId(),
                ETransactionStatus.FAILED.name(), "REFUNDED_MANUALLY",
                "Hoàn tay qua portal. " + request.getReason()
                        + (request.getEvidence() == null ? "" : " [ref " + request.getEvidence() + "]"));
        if (resolved == 0) {
            // Ca đã đổi trạng thái giữa lúc đề xuất và lúc duyệt — saga tự xử lý xong, hoặc đã có
            // người chốt. Không ghi đè: trạng thái mới có thể là COMPLETED.
            throw new ConflictException("CASE_NO_LONGER_OPEN",
                    "Ca này không còn ở diện chờ người xử lý — đọc lại trước khi quyết");
        }
        sagaRepository.close(txn.getTenantId(), txn.getId(), "FAILED");

        // Mỗi lần chuyển trạng thái phát ĐÚNG MỘT event (bất biến của SagaService), và ca
        // MANUAL_REVIEW chưa từng phát gì — không có dòng này thì người dùng không bao giờ nhận kết
        // cục, và đối soát Phase 3 thấy một giao dịch không có kết thúc. Qua outbox, trong chính
        // transaction này: dual-write luôn đi qua OutboxPublisher (CLAUDE.md §3).
        outboxPublisher.publish(
                WalletTopics.EVT_TXN_FAILED,
                txn.getToWallet().toString(),
                SagaService.stableEventId("txn-failed", txn.getId()),
                "TransactionFailed",
                new TransactionFailedEvent(txn.getId(), txn.getType(), txn.getInitiatedBy(),
                        txn.getFromWallet(), txn.getToWallet(), txn.getAmountMinor(),
                        txn.getCurrency(), "REFUNDED_MANUALLY"));

        detail.put("newStatus", ETransactionStatus.FAILED.name());
        log.warn("Vận hành: {} duyệt hoàn tay cho giao dịch {} — ca đóng ở FAILED",
                checker.id(), txn.getId());
    }

    /**
     * Khoá ca và đòi nó còn ở diện chờ người. Saga trước, giao dịch sau.
     *
     * <p>Trả 404 cho cả "không tồn tại" lẫn "của tenant khác" như mọi đường khác; nhưng ca tồn tại
     * mà đã đóng thì trả 409 — người vận hành cần phân biệt "gõ nhầm id" với "ai đó vừa xử lý xong".
     */
    private Transaction lockOpenCase(UUID tenantId, UUID transactionId) {
        sagaRepository.lock(tenantId, transactionId);
        Transaction txn = transactionRepository.lockForSaga(tenantId, transactionId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy giao dịch"));
        if (txn.getStatus() != ETransactionStatus.MANUAL_REVIEW || txn.getResolvedAt() != null) {
            throw new ConflictException("CASE_NO_LONGER_OPEN",
                    "Ca này không còn ở diện chờ người xử lý (đang " + txn.getStatus() + ")");
        }
        return txn;
    }

    private String json(Map<String, ?> detail) {
        try {
            return objectMapper.writeValueAsString(detail);
        } catch (Exception malformed) {
            // Không nuốt: cột là jsonb, và một tài liệu hỏng sẽ làm INSERT nổ ở cuối transaction —
            // tức là tiền đã ghi rồi mới hỏng. Nổ ngay ở đây thì không có gì được ghi cả.
            throw new BadRequestException("AUDIT_DETAIL_INVALID", "Không dựng được dấu vết cho thao tác");
        }
    }
}
