package com.walletflow.common.reconciliation;

/**
 * Thân của {@code POST /internal/reconciliation/breaks/{id}/close} (ADR-025).
 *
 * <p>Chỉ mang LÝ DO. Người đóng đi theo header {@code X-User-Id} như mọi đường khác trong hệ thống,
 * không nằm trong body — CLAUDE.md: "danh tính lấy từ header gateway, không bao giờ tin body". Tenant
 * đi theo {@code X-Tenant-Id} qua {@code TenantFilter}.
 *
 * <p>{@code note} bắt buộc không rỗng, cưỡng chế ở tầng service: đóng break là tuyên bố "đã hiểu
 * nguyên nhân", và một tuyên bố không giải thích được thì không nên xảy ra (cùng lý lẽ với
 * {@code resolution_requests.reason}). Cột {@code closed_note} ở DB không có ràng buộc NOT NULL vì
 * nó có từ trước ADR này.
 */
public record CloseBreakRequest(String note) {

    /** Dài hơn thì không phải lý do nữa, mà là một báo cáo điều tra — chỗ của nó là issue. */
    public static final int MAX_NOTE = 1000;
}
