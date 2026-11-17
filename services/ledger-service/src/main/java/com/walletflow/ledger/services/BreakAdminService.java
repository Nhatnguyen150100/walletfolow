package com.walletflow.ledger.services;

import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.reconciliation.CloseBreakRequest;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.entity.ReconciliationBreak;
import com.walletflow.ledger.repositories.ReconciliationBreakRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Đọc và đóng các chỗ lệch của service này (roadmap 3.3, ADR-025).
 *
 * <p>Người gọi là transaction-service — nơi giữ màn hình của người trực, vai, và `audit_events`
 * (ADR-025 §1). Service này cố ý KHÔNG biết gì về vai: biên quyền nằm ở nơi có mô hình vai, còn ở
 * đây thì tenant là ranh giới duy nhất, và nó phải chặt.
 *
 * <p><b>Dấu vết của việc đóng nằm ở txndb, không ở đây.</b> Nghĩa là một lời gọi đóng tới thẳng
 * service này (bỏ qua transaction-service) sẽ KHÔNG để lại audit ở đâu cả. Chấp nhận được chừng nào
 * `/internal/**` còn không có route ở gateway và còn nằm trong mạng nội bộ (GAP-B5); Phase 4 đóng
 * ranh giới đó bằng mTLS. *
 * <p><b>Bản sinh ba</b> với hai service còn lại (ledger / wallet / bank-adapter) — ba bản hiện giống
 * hệt nhau kể cả {@code MAX_LIMIT}. Sửa một thì sửa cả ba: đây là bản sao của một luật BẢO MẬT (lọc
 * tenant, và "không tồn tại" với "của người khác" trả cùng một lỗi), loại luật lệch đi âm thầm nhất.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BreakAdminService {

    /** Trần một lần đọc — cùng con số với hàng đợi `manual-review` của transaction-service. */
    public static final int MAX_LIMIT = 200;

    private final ReconciliationBreakRepository breakRepository;

    /** Hàng đợi của người trực: break đang mở của tenant hiện tại, CŨ NHẤT TRƯỚC. */
    @Transactional(readOnly = true)
    public List<ReconciliationBreakView> openBreaks(int limit) {
        UUID tenantId = TenantContext.require();
        int capped = Math.clamp(limit, 1, MAX_LIMIT);
        return breakRepository.findOpen(tenantId, Limit.of(capped)).stream()
                .map(BreakAdminService::toView)
                .toList();
    }

    /**
     * Đóng một chỗ lệch. Đóng KHÔNG sửa nguyên nhân: lượt đối soát sau gặp lại đúng chỗ đó sẽ ghi
     * một hàng MỚI ({@code uk_recon_break_open} chỉ ràng buộc hàng {@code OPEN}). Đóng nhầm thì đêm
     * sau nó quay lại, chứ không biến mất vĩnh viễn.
     *
     * @throws BadRequestException     thiếu lý do, hoặc thiếu người đóng
     * @throws ResourceNotFoundException không có break đang mở nào khớp — <b>cùng một lỗi</b> cho
     *                                   "không tồn tại" và "của tenant khác", chống dò id (CLAUDE.md)
     */
    @Transactional
    public void close(UUID breakId, UUID closedBy, String note) {
        UUID tenantId = TenantContext.require();
        // Qua HTTP thì Spring đã chặn trước (header `X-User-Id` là bắt buộc), nên nhánh này chỉ
        // bảo vệ người gọi thẳng từ Java. Giữ lại: rẻ, và một lần đóng không biết ai đóng là vô nghĩa.
        if (closedBy == null) {
            throw new BadRequestException("Thiếu định danh người đóng");
        }
        String reason = note == null ? "" : note.strip();
        if (reason.isEmpty()) {
            // Đóng break là tuyên bố "đã hiểu nguyên nhân". Một tuyên bố không giải thích được thì
            // không nên xảy ra — cùng lý lẽ với resolution_requests.reason (ADR-021).
            throw new BadRequestException("MISSING_CLOSE_REASON",
                    "Phải ghi lý do đóng chỗ lệch này");
        }
        if (reason.length() > CloseBreakRequest.MAX_NOTE) {
            throw new BadRequestException("CLOSE_REASON_TOO_LONG",
                    "Lý do tối đa " + CloseBreakRequest.MAX_NOTE + " ký tự");
        }

        int closed = breakRepository.close(breakId, tenantId, closedBy, reason);
        if (closed == 0) {
            throw new ResourceNotFoundException("Không có chỗ lệch đang mở " + breakId);
        }
        // KHÔNG log `reason`: người trực gõ tự do vào đó và hoàn toàn có thể dán số tài khoản hay
        // số điện thoại của khách. Nội dung đã nằm an toàn ở `closed_note` + `audit_events`.
        log.warn("Đối soát: chỗ lệch {} (tenant {}) được {} đóng", breakId, tenantId, closedBy);
    }

    private static ReconciliationBreakView toView(ReconciliationBreak row) {
        return new ReconciliationBreakView(
                row.getId(), row.getTenantId(), row.getType(), row.getSubject(), row.getCurrency(),
                row.getExpectedMinor(), row.getActualMinor(), row.getDeltaMinor(),
                row.isAutoRepaired(), row.getTimesSeen(), row.getDetail(),
                row.getFirstSeenAt(), row.getDetectedAt());
    }
}
