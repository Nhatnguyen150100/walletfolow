package com.walletflow.bank.enums;

/**
 * Trạng thái của một phiên thanh toán theo lời PROVIDER (kết quả của {@code getStatus}).
 *
 * <p>Ranh giới giữa {@link #PENDING} và {@link #UNRESOLVED} là ranh giới giữa "được phép tự kết thúc khi hết
 * hạn" và "không bao giờ" (ADR-017): chỉ khi provider KHẲNG ĐỊNH chưa thu đồng nào thì việc phiên quá hạn mới
 * đủ để kết luận thất bại.
 */
public enum EProviderState {
    /**
     * Provider khẳng định CHƯA thu tiền: người dùng chưa trả xong, hoặc chưa từng mở trang thanh toán
     * (VNPay: "không tìm thấy giao dịch"). Quá hạn phiên + grace mà vẫn thế thì lần thu kết thúc
     * {@code FAILED(EXPIRED)}.
     */
    PENDING,
    SUCCEEDED,
    FAILED,
    /**
     * Provider trả lời được nhưng KHÔNG phải kết quả cuối, và cũng không phải "chưa thu": giao dịch đang bị
     * đảo, nghi gian lận, đang hoàn... Tiền có thể đã rời tài khoản người dùng — không bao giờ tự hết hạn,
     * giữ PENDING và chờ người (metric {@code walletflow.bank.pending_overdue}, 2.5).
     */
    UNRESOLVED
}
