package com.walletflow.transaction.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.reconciliation.CloseBreakRequest;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.transaction.client.BreakClient;
import com.walletflow.transaction.client.EBreakSource;
import com.walletflow.transaction.dto.BreakQueue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Màn hình chỗ lệch đối soát của người trực (roadmap 3.3, ADR-025).
 *
 * <p>Gom ba nguồn ({@code ledger}, {@code wallet}, {@code bank}) và cho phép ĐÓNG một chỗ lệch. Dữ
 * liệu ở lại DB của service sở hữu; chỉ màn hình, vai và dấu vết nằm ở đây.
 *
 * <p>Cố ý KHÔNG có {@code @Transactional} ở mức class: mọi method đều gọi HTTP, và CLAUDE.md cấm gọi
 * HTTP trong transaction DB. Chỗ cần transaction dùng {@link TransactionTemplate} tường minh, đúng
 * khuôn {@code ResolutionService.propose}.
 */
@Slf4j
@Service
public class BreakReviewService {

    /** Trần cho MỖI nguồn — khớp `BreakAdminService.MAX_LIMIT` của ba service sở hữu. */
    public static final int MAX_LIMIT_PER_SOURCE = 200;

    static final String ACTION_LIST = "LIST_RECONCILIATION_BREAKS";
    static final String ACTION_REQUESTED = "BREAK_CLOSE_REQUESTED";
    static final String ACTION_CLOSED = "BREAK_CLOSED";
    static final String ACTION_FAILED = "BREAK_CLOSE_FAILED";

    private final BreakClient breakClient;
    private final AuditService auditService;
    private final AuditWriter auditWriter;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Map<EBreakSource, Counter> unavailable = new EnumMap<>(EBreakSource.class);

    public BreakReviewService(BreakClient breakClient,
                              AuditService auditService,
                              AuditWriter auditWriter,
                              ObjectMapper objectMapper,
                              PlatformTransactionManager transactionManager,
                              MeterRegistry meterRegistry) {
        this.breakClient = breakClient;
        this.auditService = auditService;
        this.auditWriter = auditWriter;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        // REQUIRES_NEW chứ không để mặc định REQUIRED: bảo đảm của ADR-025 §2 là "audit ý định đã
        // COMMIT trước khi gọi đóng". Với REQUIRED, ngày ai đó thêm @Transactional vào một caller thì
        // template này nhập vào transaction đó — audit không còn commit trước, VÀ lời gọi HTTP chạy
        // trong transaction DB (CLAUDE.md cấm). Cả hai bất biến hỏng im lặng. Một dòng để khoá lại.
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        // Đăng ký NGAY lúc khởi động, không đợi lần hỏng đầu: một series chỉ ra đời khi đã hỏng thì
        // không có đường 0 để so, và alert không phân biệt được "chưa bao giờ hỏng" với "chưa scrape".
        // Tag `screen` để không trộn với counter cùng tên của AdminReviewService — khi alert kêu,
        // phải biết hỏng ở màn can thiệp hay màn đối soát.
        for (EBreakSource source : EBreakSource.values()) {
            unavailable.put(source, meterRegistry.counter("walletflow.admin.context_unavailable",
                    Tags.of("service", source.slug(), "screen", "breaks")));
        }
    }

    /** Hàng đợi gom ba nguồn. Nguồn nào không hỏi được thì ô đó {@code available = false}. */
    public BreakQueue queue(Operator actor, int limit) {
        // Kẹp ở ĐÂY nữa dù ba owner đều tự kẹp: `limit` áp cho MỖI nguồn, nên không kẹp thì một lần
        // mở màn hình trả về tới 3 × 200 dòng. Cùng quy ước với AdminReviewService.
        limit = Math.clamp(limit, 1, MAX_LIMIT_PER_SOURCE);
        List<BreakQueue.BreakSourceView> sources = new ArrayList<>();
        boolean complete = true;
        int total = 0;

        for (EBreakSource source : EBreakSource.values()) {
            Optional<List<ReconciliationBreakView>> found = breakClient.openBreaks(source, limit);
            if (found.isEmpty()) {
                complete = false;
                unavailable.get(source).increment();
                sources.add(new BreakQueue.BreakSourceView(source.slug(), false, List.of()));
                continue;
            }
            List<ReconciliationBreakView> breaks = found.get();
            total += breaks.size();
            sources.add(new BreakQueue.BreakSourceView(source.slug(), true, breaks));
        }

        // Thao tác ĐỌC: audit nuốt lỗi — không chặn người trực nhìn hàng đợi lúc đang có sự cố.
        auditService.record(actor, ACTION_LIST, null,
                Map.of("limit", limit, "complete", complete, "totalOpen", total));
        return new BreakQueue(List.copyOf(sources), complete, total);
    }

    /**
     * Đóng một chỗ lệch ở service sở hữu nó.
     *
     * <p><b>Thứ tự là phần quan trọng nhất của method này (ADR-025 §2).</b> Việc đóng nằm ở DB của
     * service khác, sau một lời gọi HTTP, nên không transaction nào bao được cả audit lẫn nó. Khi
     * không thể nguyên tử thì phải chọn chiều hỏng:
     * <ol>
     *   <li>Ghi dấu vết Ý ĐỊNH trước, trong transaction của chính nó, <b>không nuốt lỗi</b> — audit
     *       hỏng thì không đóng gì cả.</li>
     *   <li>Gọi đóng.</li>
     *   <li>Ghi dấu vết KẾT QUẢ, nuốt lỗi — tới đây việc đã xảy ra rồi, làm hỏng câu trả lời vì
     *       không ghi nổi dòng thứ hai là vô ích.</li>
     * </ol>
     * Hỏng ở giữa ⇒ audit nói "đã thử", break vẫn mở: audit THỪA một dòng, không bao giờ THIẾU.
     *
     * @throws BadRequestException       lý do rỗng hoặc quá dài
     * @throws ResourceNotFoundException không có chỗ lệch đang mở nào khớp (gồm cả "của tenant khác")
     * @throws AppException              503 khi không hỏi được service sở hữu
     */
    public void close(Operator actor, EBreakSource source, UUID breakId, String reason) {
        String note = requireReason(reason);
        String target = source.slug() + ":" + breakId;

        transactionTemplate.executeWithoutResult(status ->
                auditWriter.writeInCurrentTransaction(actor, ACTION_REQUESTED, target,
                        detail(Map.of("source", source.slug(), "breakId", breakId.toString(),
                                "reason", note))));

        BreakClient.CloseOutcome outcome = breakClient.close(source, breakId, actor.id(), note);

        auditService.record(actor,
                outcome == BreakClient.CloseOutcome.CLOSED ? ACTION_CLOSED : ACTION_FAILED,
                target, Map.of("source", source.slug(), "outcome", outcome.name()));

        switch (outcome) {
            case CLOSED -> log.info("Người {} đóng chỗ lệch {} ở {}", actor.id(), breakId, source.slug());
            case NOT_FOUND -> throw new ResourceNotFoundException(
                    "Không có chỗ lệch đang mở " + breakId + " ở " + source.slug());
            // 503 chứ không 500: service sở hữu có thể đã đóng rồi mà câu trả lời lạc mất. Người trực
            // phải mở lại hàng đợi để biết, chứ không được gửi lại một cách mù quáng.
            case UNAVAILABLE -> throw new AppException(HttpStatus.SERVICE_UNAVAILABLE,
                    "BREAK_SOURCE_UNAVAILABLE",
                    "Không hỏi được " + source.slug() + " để đóng chỗ lệch này");
            // Service sở hữu TỪ CHỐI dứt khoát (4xx không phải 404) — chắc chắn CHƯA đóng gì.
            // Phân loại lỗi trước khi xử lý (CLAUDE.md §7): gộp nó vào 503 là đổi một sự thật chắc
            // chắn lấy một trạng thái mơ hồ, bắt người trực đi dò lại hàng đợi mà không cần.
            // Người trực không sửa được ca này — nó là lệch hợp đồng giữa hai service.
            case REJECTED -> throw new AppException(HttpStatus.BAD_GATEWAY,
                    "BREAK_SOURCE_CONTRACT_MISMATCH",
                    source.slug() + " từ chối lệnh đóng — chưa đóng gì cả, đây là sự cố cấu hình");
        }
    }

    /**
     * Phòng thủ CHIỀU SÂU, không phải đường chính: qua HTTP thì {@code @Valid} trên
     * {@code CloseBreakBody} đã chặn trước (và trả {@code VALIDATION_ERROR}). Nhánh này giữ lời hứa
     * cho người gọi thẳng từ Java, và cho ngày ai đó gỡ {@code @Valid}.
     */
    private static String requireReason(String reason) {
        String note = reason == null ? "" : reason.strip();
        if (note.isEmpty()) {
            throw new BadRequestException("MISSING_CLOSE_REASON", "Phải ghi lý do đóng chỗ lệch này");
        }
        if (note.length() > CloseBreakRequest.MAX_NOTE) {
            throw new BadRequestException("CLOSE_REASON_TOO_LONG",
                    "Lý do tối đa " + CloseBreakRequest.MAX_NOTE + " ký tự");
        }
        return note;
    }

    /**
     * Cột {@code detail} là {@code jsonb}, nên một tài liệu hỏng vì ghép chuỗi sẽ làm INSERT nổ —
     * và ở đường ý định thì nổ nghĩa là KHÔNG đóng được break. Dựng bằng Jackson, không ghép tay.
     */
    private String detail(Map<String, ?> values) {
        try {
            return objectMapper.writeValueAsString(values);
        } catch (Exception failure) {
            throw new IllegalStateException("Không dựng được detail cho audit", failure);
        }
    }
}
