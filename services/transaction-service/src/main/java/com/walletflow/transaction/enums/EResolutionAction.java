package com.walletflow.transaction.enums;

/**
 * Thao tác xử lý một ca cần can thiệp (roadmap 2.5, ADR-021 và ADR-022).
 *
 * <p>Tập này CHỈ gồm thao tác ĐỤNG TIỀN, vì mỗi giá trị ở đây kéo theo 4-eyes và audit cùng
 * transaction. Mở lại một ca kẹt không nằm ở đây: nó không tạo chuyển động tiền nào (ledger
 * idempotent theo {@code transactionId} nên lệnh gửi lại không ghi bút toán thứ hai).
 */
public enum EResolutionAction {
    /**
     * Người vận hành đã hoàn tiền TAY qua portal của provider; đánh dấu ca đã xong.
     *
     * <p>Lần hoàn đó không để lại dấu vết nào ở bank-adapter (ADR-020 *Consequences*), nên bản ghi
     * này cùng với {@code audit_events} là dấu vết DUY NHẤT của việc tiền đã về với người dùng.
     */
    MARK_REFUNDED_MANUALLY,

    /**
     * Đảo một bút toán đã ghi nhầm, bằng một bút toán {@code REVERSAL} đối xứng (ADR-022).
     *
     * <p>Áp cho ĐÚNG MỘT ca: {@code REFUND_POSTED_CONFLICT} — ledger ghi bút toán nạp trong khi tiền
     * cũng đã được hoàn, nên ví đang giữ một khoản người dùng đã được trả lại. Mọi ca
     * {@code MANUAL_REVIEW} khác không có bút toán nào để đảo: ledger từ chối *trước* khi ghi, hoặc
     * lệnh ghi sổ chưa từng được gửi (ADR-021 *Amendments* A1).
     *
     * <p>Khác {@link #MARK_REFUNDED_MANUALLY} ở một điểm quyết định cách viết code: thao tác này
     * <b>không kết thúc khi người duyệt bấm nút</b>. Nó gửi một lệnh sang ledger và ledger có quyền
     * TỪ CHỐI (ví đã tiêu mất tiền ⇒ {@code INSUFFICIENT_FUNDS}). Nên ca KHÔNG được đóng lúc duyệt;
     * nó đóng khi {@code LedgerPosted(REVERSAL)} về, và ở lại hàng đợi nếu ledger từ chối.
     */
    REVERSE_LEDGER_ENTRY
}
