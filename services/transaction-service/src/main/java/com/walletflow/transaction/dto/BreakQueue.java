package com.walletflow.transaction.dto;

import com.walletflow.common.reconciliation.ReconciliationBreakView;

import java.util.List;

/**
 * Hàng đợi chỗ lệch gom từ ba service (roadmap 3.3, ADR-025 §1).
 *
 * @param sources  mỗi nguồn một ô, LUÔN đủ ba ô kể cả khi hỏi không được
 * @param complete hỏi được cả ba nguồn hay chưa. Người trực cần biết: {@code totalOpen = 0} với
 *                 {@code complete = false} KHÔNG có nghĩa là không còn việc gì
 * @param totalOpen tổng số chỗ lệch ĐẾM ĐƯỢC — chỉ gồm các nguồn hỏi được
 */
public record BreakQueue(
        List<BreakSourceView> sources,
        boolean complete,
        int totalOpen) {

    /**
     * Một nguồn trong hàng đợi.
     *
     * <p>{@code available = false} với {@code breaks} rỗng là "KHÔNG BIẾT", khác hẳn
     * {@code available = true} với {@code breaks} rỗng là "không có gì" (ADR-021 B1). Hai thứ đó
     * trông giống nhau trên màn hình nếu chỉ nhìn danh sách — nên cờ này không phải trang trí.
     */
    public record BreakSourceView(
            String source,
            boolean available,
            List<ReconciliationBreakView> breaks) {
    }
}
