package com.walletflow.ledger.controller;

import com.walletflow.common.reconciliation.CloseBreakRequest;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.ledger.services.BreakAdminService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * API NỘI BỘ cho hàng đợi chỗ lệch của sổ cái (roadmap 3.3, ADR-025 §1).
 *
 * <p>Không có route nào ở gateway trỏ tới {@code /internal/**}; chỉ transaction-service gọi, để gom
 * ba nguồn break vào một màn hình và ghi `audit_events` ở txndb.
 *
 * <p><b>Khác các controller {@code /internal} còn lại: ở đây có một đường GHI.</b> Nó không ghi
 * tiền — đóng break chỉ ghi lại phán đoán của con người, không tạo bút toán nào — nên nó không phá
 * nguyên tắc "mọi thay đổi sổ cái đi qua Kafka command" (xem {@code InternalAccountController}).
 *
 * <p>Tenant từ {@code X-Tenant-Id} qua {@code TenantFilter}; người đóng từ {@code X-User-Id} —
 * không bao giờ lấy từ body (CLAUDE.md). Trả thẳng record hợp đồng, không bọc {@code BaseResponse}:
 * người gọi là máy.
 *
 * <p><b>Bản sinh ba</b> với hai service còn lại — sửa một thì sửa cả ba.
 */
@RestController
@RequestMapping("/internal/reconciliation/breaks")
@RequiredArgsConstructor
public class InternalReconciliationController {

    private final BreakAdminService breakAdminService;

    @GetMapping
    public List<ReconciliationBreakView> open(@RequestParam(defaultValue = "50") int limit) {
        return breakAdminService.openBreaks(limit);
    }

    @PostMapping("/{breakId}/close")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void close(@PathVariable UUID breakId,
                      @RequestHeader(AuthHeaders.USER_ID) UUID closedBy,
                      @RequestBody CloseBreakRequest request) {
        breakAdminService.close(breakId, closedBy, request == null ? null : request.note());
    }
}
