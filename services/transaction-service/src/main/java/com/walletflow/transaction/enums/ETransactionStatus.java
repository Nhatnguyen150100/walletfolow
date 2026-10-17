package com.walletflow.transaction.enums;

/**
 * Trạng thái của MỘT lệnh chuyển tiền — trả lời câu hỏi "lệnh của tôi tới đâu rồi", không phải "tiền
 * đã đi chưa". Tiền chỉ đi khi ledger ghi bút toán.
 */
public enum ETransactionStatus {
    /** Đã nhận, đã gửi lệnh ghi sổ, đang chờ ledger trả lời. */
    PROCESSING,
    /** Ledger đã ghi bút toán. Trạng thái cuối — không gì kéo nó về được. */
    COMPLETED,
    /** Ledger từ chối, kèm mã lỗi. */
    FAILED,
    /**
     * Ledger không trả lời sau đủ số lần gửi lại. Không đoán là thành công hay thất bại: tiền có thể
     * đã đi (bút toán ghi xong, event mất) hoặc chưa. Người xử lý nhìn sổ cái và quyết; nếu
     * {@code LedgerPosted} đến muộn, saga tự chuyển sang {@link #COMPLETED}.
     */
    MANUAL_REVIEW
}
