package com.walletflow.transaction.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.entity.ResolutionRequest;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.enums.EResolutionAction;
import com.walletflow.transaction.enums.EResolutionStatus;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.ResolutionRequestRepository;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;
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
@RequiredArgsConstructor
public class ResolutionService {

    private final TransactionRepository transactionRepository;
    private final ResolutionRequestRepository resolutionRepository;
    private final SagaRepository sagaRepository;
    private final AuditWriter auditWriter;
    private final OutboxPublisher outboxPublisher;
    private final ObjectMapper objectMapper;

    /**
     * Đề xuất một thao tác đụng tiền. Chưa có gì xảy ra với tiền ở đây — nhưng vẫn ghi dấu vết, vì
     * ai đề xuất cái gì là một nửa của câu chuyện mà 4-eyes sinh ra để kể.
     *
     * @return id của yêu cầu, để người duyệt gọi {@link #decide}
     */
    @Transactional
    public UUID propose(Operator maker, UUID transactionId, EResolutionAction action,
                        String reason, String evidence) {
        UUID tenantId = TenantContext.require();
        Transaction txn = lockOpenCase(tenantId, transactionId);

        UUID requestId = UUID.randomUUID();
        int created = resolutionRepository.propose(requestId, tenantId, transactionId, action.name(),
                maker.id(), maker.email(), maker.role(), reason, evidence);
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
        // (PR3 sẽ thêm REVERSAL) thì `decide` vẫn ghi APPROVED và vẫn ghi dấu vết "đã duyệt và thực
        // thi" cho việc không xảy ra: đúng chiều nguy hiểm mà ADR-021 A4 gọi tên.
        switch (request.getAction()) {
            case MARK_REFUNDED_MANUALLY -> markRefundedManually(checker, request, txn, detail);
        }
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
