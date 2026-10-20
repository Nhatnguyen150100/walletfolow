package com.walletflow.transaction.enums;

/**
 * Trạng thái của MỘT lệnh chuyển tiền — trả lời câu hỏi "lệnh của tôi tới đâu rồi", không phải "tiền
 * đã đi chưa". Tiền chỉ đi khi ledger ghi bút toán.
 */
public enum ETransactionStatus {
    /** Đã nhận, đã gửi lệnh (ghi sổ với chuyển tiền, thu tiền với nạp tiền), đang chờ máy trả lời. */
    PROCESSING,
    /**
     * Nạp tiền: provider đã cấp trang thanh toán, đang chờ NGƯỜI DÙNG trả tiền. Không phải lỗi và
     * không phải treo — chờ ở đây hàng chục phút là bình thường, và kết thúc bằng "không trả" cũng là
     * một kết cục hợp lệ (bank-adapter hết hạn phiên rồi báo thất bại — ADR-017).
     */
    AWAITING_PAYMENT,
    /**
     * Nạp tiền: provider đã XÁC NHẬN thu được tiền, lệnh ghi sổ đã gửi, đang chờ ledger. Cửa sổ nguy
     * hiểm nhất của luồng nạp — tiền đã ở chỗ ta nhưng chưa vào ví ai, nên phải phân biệt được với
     * PROCESSING (chưa thu được đồng nào) khi đối soát.
     */
    POSTING,
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
