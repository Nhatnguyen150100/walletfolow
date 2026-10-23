package com.walletflow.transaction.controller;

import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.transaction.dto.ManualReviewDetail;
import com.walletflow.transaction.dto.ManualReviewItem;
import com.walletflow.transaction.services.AdminReviewService;
import com.walletflow.transaction.services.AuditService;
import com.walletflow.transaction.services.Operator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Khu vực vận hành: xem những giao dịch máy không tự xử lý được (roadmap 2.5 PR1, ADR-021).
 *
 * <p>Gateway đã chặn mọi path {@code /api/v1/admin/**} cho ba vai vận hành, nhưng
 * {@link Operator#requireReader} kiểm lại ở đây — biên trong cụm chưa có mTLS (Phase 4), và route
 * cấu hình sai không được phép là thứ duy nhất giữ dữ liệu khách hàng.
 *
 * <p>CHỈ ĐỌC. Thao tác xử lý (REVERSAL, đánh dấu đã hoàn tay) và 4-eyes là 2.5 PR2.
 */
@RestController
@RequestMapping("/api/v1/admin/transactions")
@RequiredArgsConstructor
@Tag(name = "Admin · Manual review", description = "Giao dịch cần người can thiệp")
public class AdminReviewController {

    private final AdminReviewService service;
    private final AuditService auditService;

    @GetMapping("/manual-review")
    @Operation(summary = "Hàng đợi cần can thiệp — cũ nhất trước, trong tenant của người gọi")
    public ResponseEntity<BaseResponse<List<ManualReviewItem>>> queue(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @RequestParam(defaultValue = "50") int limit,
            HttpServletRequest request) {
        Operator actor = reader(actorId, actorEmail, actorRole, request);
        return ResponseBuilder.success("Hàng đợi cần can thiệp", service.queue(actor, limit));
    }

    @GetMapping("/{transactionId}")
    @Operation(summary = "Một ca kèm trạng thái saga — đủ để phân loại theo runbook")
    public ResponseEntity<BaseResponse<ManualReviewDetail>> detail(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @PathVariable UUID transactionId,
            HttpServletRequest request) {
        Operator actor = reader(actorId, actorEmail, actorRole, request);
        return ResponseBuilder.success("Chi tiết giao dịch", service.detail(actor, transactionId));
    }

    /**
     * Dựng {@link Operator}, và ghi dấu vết khi bị TỪ CHỐI trước khi ném tiếp.
     *
     * <p>Lần bị từ chối còn đáng ghi hơn lần thành công: một chuỗi 403 liên tiếp chính là hình dạng
     * của một token bị đánh cắp đâm vào khu vực quản trị. Gateway có {@code log.warn}, nhưng log
     * không phải sổ kiểm toán — nó không có {@code tenant_id}, không bất biến, và bị xoay vòng.
     *
     * <p>Bắt ở ĐÂY chứ không phải trong một {@code @ExceptionHandler}: ném lại từ trong exception
     * handler thì Spring không chuyển tiếp sang handler khác, nó thành 500.
     *
     * <p>Chỉ ghi cho 403. {@code UnauthorizedException} (thiếu định danh) không có {@code actorId}
     * để ghi vào {@code audit_events.actor_id} (NOT NULL), và một lời gọi không mang định danh thì
     * gần như luôn là gọi thẳng vào service, bỏ qua biên — chỗ nhìn thấy nó là log và metric của
     * gateway, không phải sổ kiểm toán theo người.
     */
    private Operator reader(UUID actorId, String actorEmail, String actorRole,
                            HttpServletRequest request) {
        try {
            return Operator.requireReader(actorId, actorEmail, actorRole, clientIp(request));
        } catch (ForbiddenException denied) {
            auditService.record(
                    new Operator(actorId, actorEmail, String.valueOf(actorRole), clientIp(request)),
                    "ADMIN_ACCESS_DENIED", null,
                    Map.of("path", request.getRequestURI(), "errorCode", denied.getErrorCode()));
            throw denied;
        }
    }

    /**
     * IP ghi vào audit. Sau gateway đây là IP của gateway chứ không phải của người vận hành — lấy
     * đúng IP cần chốt số proxy tin cậy trước (mục còn mở của ADR-018, chung với {@code clientIp}
     * của lệnh nạp). Ghi thứ đang có còn hơn để trống: nó vẫn phân biệt được lời gọi đi qua biên
     * với lời gọi từ bên trong cụm.
     */
    private static String clientIp(HttpServletRequest request) {
        return request.getRemoteAddr();
    }
}
