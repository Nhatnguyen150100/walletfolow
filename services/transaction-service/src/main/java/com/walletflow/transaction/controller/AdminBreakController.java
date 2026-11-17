package com.walletflow.transaction.controller;

import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.transaction.client.EBreakSource;
import com.walletflow.transaction.dto.BreakQueue;
import com.walletflow.transaction.dto.CloseBreakBody;
import com.walletflow.transaction.services.AuditService;
import com.walletflow.transaction.services.BreakReviewService;
import com.walletflow.transaction.services.Operator;
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

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Hàng đợi chỗ lệch đối soát của người trực (roadmap 3.3, ADR-025).
 *
 * <p>Trước màn hình này, ba job đối soát PHÁT HIỆN được lệch nhưng không có đường nào kết thúc một
 * ca: runbook bảo chạy {@code UPDATE} bằng tay, và thao tác đó không để lại dấu vết nào.
 *
 * <p>Đóng break KHÔNG cần 4-eyes (ADR-025 §3): nó không tạo chuyển động tiền. Thao tác đụng tiền
 * thật vẫn đi {@code AdminReviewController} — {@code /resolve} + {@code /decision}.
 */
@RestController
@RequestMapping("/api/v1/admin/reconciliation/breaks")
@RequiredArgsConstructor
@Tag(name = "Admin · Đối soát", description = "Chỗ lệch cần người kết luận")
public class AdminBreakController {

    private final BreakReviewService service;
    private final AuditService auditService;

    @GetMapping
    @Operation(summary = "Hàng đợi chỗ lệch đang mở, gom từ ledger / wallet / bank")
    public ResponseEntity<BaseResponse<BreakQueue>> queue(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @RequestParam(defaultValue = "50") int limit,
            HttpServletRequest request) {
        Operator actor = guard(
                () -> Operator.requireReader(actorId, actorEmail, actorRole, clientIp(request)),
                actorId, actorEmail, actorRole, request);
        return ResponseBuilder.success("Hàng đợi chỗ lệch đối soát", service.queue(actor, limit));
    }

    /**
     * Đóng một chỗ lệch. Lý do là BẮT BUỘC — đóng break là tuyên bố "đã hiểu nguyên nhân".
     *
     * <p>{@code source} là slug của service sở hữu ({@code ledger} / {@code wallet} / {@code bank}),
     * không phải chuỗi tự do: nó chọn đích của lời gọi mạng tiếp theo (xem {@link EBreakSource}).
     */
    @PostMapping("/{source}/{breakId}/close")
    @Operation(summary = "Đóng một chỗ lệch sau khi đã kết luận nguyên nhân")
    public ResponseEntity<BaseResponse<Void>> close(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID actorId,
            @RequestHeader(value = AuthHeaders.USER_EMAIL, required = false) String actorEmail,
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String actorRole,
            @PathVariable String source,
            @PathVariable UUID breakId,
            @Valid @RequestBody CloseBreakBody body,
            HttpServletRequest request) {
        Operator actor = guard(
                () -> Operator.requireBreakCloser(actorId, actorEmail, actorRole, clientIp(request)),
                actorId, actorEmail, actorRole, request);
        service.close(actor, parseSource(source), breakId, body.reason());
        return ResponseBuilder.success("Đã đóng chỗ lệch");
    }

    /** Slug lạ là dữ liệu người dùng gõ sai ⇒ 400, không phải 500. */
    private static EBreakSource parseSource(String slug) {
        try {
            return EBreakSource.fromSlug(slug);
        } catch (IllegalArgumentException unknown) {
            throw new BadRequestException("UNKNOWN_BREAK_SOURCE", unknown.getMessage());
        }
    }

    // Hai helper dưới đây là bản sinh đôi của AdminReviewController — giữ ở đây thay vì tách dùng
    // chung để diff của PR này không đụng vào khu vực vận hành đang chạy. Đổi một bên thì xem bên kia.

    /** Ghi dấu vết khi một vai không đủ quyền cố gọi — thứ đáng ghi hơn cả một lần xem thành công. */
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

    private static String clientIp(HttpServletRequest request) {
        return request.getRemoteAddr();
    }
}
