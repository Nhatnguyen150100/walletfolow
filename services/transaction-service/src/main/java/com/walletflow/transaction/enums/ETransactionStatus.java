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
    /**
     * Nạp tiền: đã thu được tiền nhưng KHÔNG ghi sổ được (ledger từ chối, hoặc provider xác nhận đã thu
     * sau khi lệnh đã đóng thất bại), và lệnh hoàn tiền đã gửi sang bank-adapter — đang chờ nó trả lời
     * (roadmap 2.4b, ADR-020).
     *
     * <p>Trạng thái duy nhất đi ra từ một trạng thái "đã đóng": ca {@code BANK_RESULT_CONFLICT} bắt đầu
     * từ {@link #FAILED}. Bù trừ là bước TIẾP của saga, không phải quay lui — từ đây chỉ có hai đường
     * ra: hoàn xong → {@link #FAILED}, hoàn không được → {@link #MANUAL_REVIEW}.
     */
    REFUNDING,
    /** Ledger đã ghi bút toán. Trạng thái cuối — không gì kéo nó về được. */
    COMPLETED,
    /**
     * Ledger từ chối, kèm mã lỗi. Với nạp tiền còn một nghĩa thứ hai từ 2.4b: đã thu được tiền nhưng
     * không ghi sổ được và <b>đã hoàn trả xong</b> — cả hai nghĩa đều là "người dùng không mất tiền",
     * nên vẫn là một trạng thái. Phân biệt bằng {@code failure_code} + log của bước hoàn.
     */
    FAILED,
    /**
     * Ledger không trả lời sau đủ số lần gửi lại. Không đoán là thành công hay thất bại: tiền có thể
     * đã đi (bút toán ghi xong, event mất) hoặc chưa. Người xử lý nhìn sổ cái và quyết; nếu
     * {@code LedgerPosted} đến muộn, saga tự chuyển sang {@link #COMPLETED}.
     */
    MANUAL_REVIEW
}
