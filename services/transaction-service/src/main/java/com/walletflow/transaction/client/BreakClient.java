package com.walletflow.transaction.client;

import com.walletflow.common.reconciliation.ReconciliationBreakView;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Cổng đọc và đóng chỗ lệch ở ba service sở hữu dữ liệu (roadmap 3.3, ADR-025 §1).
 *
 * <p>Tách thành interface để test thay được bằng fake, như {@code LedgerEntriesClient} và
 * {@code ProviderContextClient} của 2.5 PR3.
 */
public interface BreakClient {

    /**
     * Hàng đợi chỗ lệch đang mở ở MỘT nguồn, cũ nhất trước.
     *
     * <p><b>{@code Optional.empty()} nghĩa là KHÔNG HỎI ĐƯỢC, không phải "không có break".</b> Quy
     * ước này là của ADR-021 B1 và ở đây nó quan trọng hơn mọi chỗ khác: "không có chỗ lệch nào" là
     * câu cho phép người trực đi ngủ, còn "tôi không biết" thì không.
     */
    Optional<List<ReconciliationBreakView>> openBreaks(EBreakSource source, int limit);

    /** Kết cục của một lần đóng — ba ca phải phân biệt được vì chúng thành ba mã HTTP khác nhau. */
    enum CloseOutcome {
        /** Đã đóng. */
        CLOSED,
        /** Không có chỗ lệch đang mở nào khớp: sai id, của tenant khác, hoặc đã đóng rồi. */
        NOT_FOUND,
        /** Không hỏi được service sở hữu. KHÔNG được coi là đã đóng, cũng không phải không tồn tại. */
        UNAVAILABLE,
        /**
         * Service sở hữu TỪ CHỐI dứt khoát (4xx không phải 404) — chắc chắn chưa đóng gì.
         *
         * <p>Tách khỏi {@link #UNAVAILABLE} vì hai thứ nói hai điều khác hẳn với người trực: ở đây
         * ta BIẾT chắc không có gì xảy ra, còn ở kia thì không biết. Người trực chỉ phải đi dò lại
         * hàng đợi trong ca thứ hai (CLAUDE.md §7: phân loại lỗi trước khi xử lý).
         */
        REJECTED
    }

    /**
     * Đóng một chỗ lệch ở service sở hữu nó.
     *
     * @param closedBy người đóng — đi theo header {@code X-User-Id}, không nằm trong body
     */
    CloseOutcome close(EBreakSource source, UUID breakId, UUID closedBy, String note);
}
