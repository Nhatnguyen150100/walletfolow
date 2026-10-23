package com.walletflow.transaction.enums;

/**
 * Thao tác xử lý một ca cần can thiệp (roadmap 2.5 PR2, ADR-021).
 *
 * <p>Chỉ có MỘT giá trị, và đó là chủ ý: đây là tập thao tác ĐỤNG TIỀN cần 4-eyes. Mở lại một ca kẹt
 * không nằm ở đây vì nó không tạo chuyển động tiền nào (ledger idempotent theo {@code transactionId}
 * nên lệnh gửi lại không ghi bút toán thứ hai).
 *
 * <p>{@code REVERSAL} sẽ vào đây khi làm được — nó cần {@code entryId} của bút toán gốc, thứ txndb
 * chưa lưu, và cần bằng chứng từ bank-adapter để biết lần hoàn đã xong chưa (2.5 PR3).
 */
public enum EResolutionAction {
    /**
     * Người vận hành đã hoàn tiền TAY qua portal của provider; đánh dấu ca đã xong.
     *
     * <p>Lần hoàn đó không để lại dấu vết nào ở bank-adapter (ADR-020 *Consequences*), nên bản ghi
     * này cùng với {@code audit_events} là dấu vết DUY NHẤT của việc tiền đã về với người dùng.
     */
    MARK_REFUNDED_MANUALLY
}
