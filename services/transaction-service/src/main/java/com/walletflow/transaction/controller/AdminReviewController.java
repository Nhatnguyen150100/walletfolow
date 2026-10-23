package com.walletflow.transaction.controller;

import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.transaction.dto.DecisionRequest;
import com.walletflow.transaction.dto.ManualReviewDetail;
import com.walletflow.transaction.dto.ManualReviewItem;
import com.walletflow.transaction.dto.ResolveRequest;
import com.walletflow.transaction.services.AdminReviewService;
import com.walletflow.transaction.services.AuditService;
import com.walletflow.transaction.services.Operator;
import com.walletflow.transaction.services.ResolutionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Khu vực vận hành: xem những giao dịch máy không tự xử lý được (roadmap 2.5 PR1, ADR-021).
 *
 * <p>Gateway đã chặn mọi path {@code /api/v1/admin/**} cho ba vai vận hành, nhưng
 * {@link Operator#requireReader} kiểm lại ở đây — biên trong cụm chưa có mTLS (Phase 4), và route
 * cấu hình sai không được phép là thứ duy nhất giữ dữ liệu khách hàng.
 *
 * <p>Đọc hàng đợi, và xử lý ca với 4-eyes (2.5 PR2). `REVERSAL` chưa có — nó chỉ áp dụng cho một
 * ca duy nhất và ca đó cần `entryId` + bằng chứng từ bank-adapter (ADR-021 *Amendments* A1).
 */
@RestController
@RequestMapping("/api/v1/admin/transactions")
@RequiredArgsConstructor
@Tag(name = "Admin · Manual review", description = "Giao dịch cần người can thiệp")
public class AdminReviewController {

    private final AdminReviewService service;
    private final ResolutionService resolutionService;
    private final AuditService auditService;

    /**
     * Đề xuất một thao tác xử lý ca — chưa đụng tiền, phải có người THỨ HAI duyệt (4-eyes, GAP-A4).
     *
     * <p>Trả {@code 202 Accepted}: yêu cầu đã được NHẬN, chưa có gì xảy ra với tiền. Id trả về là
     * thứ người duyệt dùng ở {@link #decide}.
     */
    @PostMapping("/{transactionId}/resolve")
    @Operation(summary = "Đề xuất xử lý một ca — cần một ADMIN khác duyệt trước khi có hiệu lực")
    public ResponseEntity<BaseResponse<Map<String, UUID>>> propose(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @PathVariable UUID transactionId,
            @Valid @RequestBody ResolveRequest body,
            HttpServletRequest request) {
        Operator maker = guard(() -> Operator.requireProposer(actorId, actorEmail, actorRole,
                clientIp(request)), actorId, actorEmail, actorRole, request);
        UUID requestId = resolutionService.propose(maker, transactionId, body.action(),
                body.reason(), body.evidence());
        return ResponseEntity.accepted().body(ResponseBuilder
                .<Map<String, UUID>>success("Đã ghi nhận, chờ người thứ hai duyệt",
                        Map.of("resolutionRequestId", requestId)).getBody());
    }

    /**
     * Duyệt hoặc từ chối một yêu cầu. Duyệt thì thao tác được THỰC THI ngay, trong cùng transaction
     * với dấu vết của nó.
     */
    @PostMapping("/resolutions/{requestId}/decision")
    @Operation(summary = "Duyệt/từ chối — người duyệt phải KHÁC người đề xuất, và phải là ADMIN")
    public ResponseEntity<BaseResponse<Void>> decide(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @PathVariable UUID requestId,
            @Valid @RequestBody DecisionRequest body,
            HttpServletRequest request) {
        Operator checker = guard(() -> Operator.requireApprover(actorId, actorEmail, actorRole,
                clientIp(request)), actorId, actorEmail, actorRole, request);
        resolutionService.decide(checker, requestId, body.approve(), body.note());
        return ResponseBuilder.success(Boolean.TRUE.equals(body.approve())
                ? "Đã duyệt và thực thi" : "Đã từ chối");
    }

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
        return guard(() -> Operator.requireReader(actorId, actorEmail, actorRole, clientIp(request)),
                actorId, actorEmail, actorRole, request);
    }

    /**
     * Chạy một phép kiểm vai, ghi dấu vết nếu bị từ chối. Dùng chung cho cả ba cửa (đọc, đề xuất,
     * duyệt) — một người thử POST vào thao tác mình không có quyền là thứ đáng ghi hơn cả.
     */
    private Operator guard(Supplier<Operator> check, UUID actorId, String actorEmail,
                           String actorRole, HttpServletRequest request) {
        try {
            return check.get();
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
