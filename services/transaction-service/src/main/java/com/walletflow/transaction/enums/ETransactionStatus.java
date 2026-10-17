package com.walletflow.transaction.enums;

/**
 * Trạng thái của MỘT lệnh chuyển tiền — trả lời câu hỏi "lệnh của tôi tới đâu rồi", không phải "tiền
 * đã đi chưa". Tiền chỉ đi khi ledger ghi bút toán.
 */
public enum ETransactionStatus {
    /** Đã nhận, đã gửi lệnh ghi sổ, đang chờ ledger trả lời. */
    PROCESSING,
    /** Ledger đã ghi bút toán (1.2b). */
    COMPLETED,
    /** Ledger từ chối, kèm mã lỗi (1.2b). */
    FAILED
}
